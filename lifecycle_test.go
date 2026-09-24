package ccache

import (
	"errors"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/karlseguin/ccache/v3/assert"
)

// ---------------------------------------------------------------------------
// Deterministic test infrastructure, shared by the contract tests.
//
// These helpers stay inside the ccache package on purpose: they drive the
// cache exclusively through its public entry points (New/Get/Set/Fetch/
// Replace/Delete/Clear/Stop/SyncUpdates), but they are allowed to observe the
// internal itemState of items returned by those entry points, so that failures
// can point at the first violated invariant instead of a plain true/false.
//
// The fake clock is the only test-only seam. It is injected through the
// unexported Configuration.now field, which is the same clock the production
// path uses for expiration (see bucket.newItem and Item.now): tests do not
// reimplement any expiration, promotion or deletion logic.
// ---------------------------------------------------------------------------

// fakeClock is a manually advanced monotonic clock used to drive expiration
// without depending on real wall time or scheduler timing.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// barrier releases every waiter at the same instant (Close semantics), giving
// contract tests an explicit synchronization point instead of relying on the
// scheduler.
type barrier struct {
	ch chan struct{}
}

func newBarrier() *barrier {
	return &barrier{ch: make(chan struct{})}
}

func (b *barrier) Wait()    { <-b.ch }
func (b *barrier) Release() { close(b.ch) }

// deleteRecorder is an onDelete callback that verifies at-most-once ownership:
// it counts callbacks per item identity (key + value, which distinguishes
// overwritten generations of the same key) and records the first key for which
// the callback fired more than once.
type deleteRecorder struct {
	mu         sync.Mutex
	counts     map[string]int
	violations []string
	reentrant  func()
}

func newDeleteRecorder() *deleteRecorder {
	return &deleteRecorder{counts: make(map[string]int)}
}

func (r *deleteRecorder) callback(item *Item[string]) {
	r.mu.Lock()
	id := item.key + "=" + item.value
	n := r.counts[id] + 1
	r.counts[id] = n
	if n == 2 {
		r.violations = append(r.violations, id)
	}
	reentrant := r.reentrant
	r.mu.Unlock()
	if reentrant != nil {
		reentrant()
	}
}

func (r *deleteRecorder) count(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[id]
}

func (r *deleteRecorder) total() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.counts)
}

// firstViolation returns the first item whose onDelete callback fired more
// than once, or "" if callback ownership held.
func (r *deleteRecorder) firstViolation() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.violations) == 0 {
		return ""
	}
	return r.violations[0]
}

func newRecordedCache(clock func() time.Time) (*Cache[string], *deleteRecorder) {
	recorder := newDeleteRecorder()
	config := Configure[string]().GetsPerPromote(1).OnDelete(recorder.callback)
	if clock != nil {
		config.now = clock
	}
	return New(config), recorder
}

// assertCacheInvariants is the checkable core of the item state machine.
// context identifies the phase that failed. It reports the first violated
// invariant together with the offending key and states, rather than a boolean.
//
// Invariants checked after the worker is quiescent:
//  1. every node of the eviction list is in statePromoted;
//  2. list size accounting equals the worker's GetSize;
//  3. every item still in a bucket is in statePending or statePromoted, and
//     every statePromoted item is reachable from the eviction list;
//  4. the list is a well-formed doubly-linked chain.
func assertCacheInvariants(t *testing.T, c *Cache[string], context string) {
	t.Helper()
	c.SyncUpdates()

	listItems := make(map[*Item[string]]bool)
	var listSize int64
	nodeCount := 0
	for node := c.list.Head; node != nil; node = node.next {
		if s := node.state(); s != statePromoted {
			t.Fatalf("%s: first list invariant violation: list node key=%q is in state %s, want %s", context, node.key, s, statePromoted)
		}
		if listItems[node] {
			t.Fatalf("%s: first list invariant violation: key=%q appears twice in the eviction list", context, node.key)
		}
		listItems[node] = true
		listSize += node.size
		nodeCount++
		if nodeCount > 1<<20 {
			t.Fatalf("%s: first list invariant violation: cycle detected in the eviction list", context)
		}
	}
	if c.list.Tail != nil && c.list.Tail.next != nil {
		t.Fatalf("%s: first list invariant violation: tail node key=%q has a successor", context, c.list.Tail.key)
	}
	if size := c.GetSize(); size != listSize {
		t.Fatalf("%s: first size invariant violation: GetSize()=%d but promoted items sum to %d across %d list nodes", context, size, listSize, nodeCount)
	}

	for _, b := range c.buckets {
		b.forEachFunc(func(key string, item *Item[string]) bool {
			s := item.state()
			switch s {
			case statePending:
			case statePromoted:
				if !listItems[item] {
					t.Fatalf("%s: first bucket invariant violation: key=%q is %s but is not linked into the eviction list (size accounting would diverge)", context, key, s)
				}
			default:
				t.Fatalf("%s: first bucket invariant violation: key=%q is in a bucket in terminal state %s", context, key, s)
			}
			return true
		})
	}
}

// snapshot captures the deterministic, caller-visible result of a cache: sorted
// keys and size. Identical inputs must produce identical snapshots regardless
// of scheduling.
func snapshot(t *testing.T, c *Cache[string], context string) ([]string, int64) {
	t.Helper()
	var keys []string
	c.ForEachFunc(func(key string, item *Item[string]) bool {
		keys = append(keys, key)
		return true
	})
	sort.Strings(keys)
	size := c.GetSize()
	t.Logf("%s: snapshot keys=%v size=%d itemCount=%d", context, keys, size, c.ItemCount())
	return keys, size
}

func assertCallbackOwnership(t *testing.T, recorder *deleteRecorder, context string) {
	t.Helper()
	if id := recorder.firstViolation(); id != "" {
		t.Fatalf("%s: first callback ownership violation: onDelete fired more than once for item %q", context, id)
	}
}

func runWithTimeout(t *testing.T, name string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: timed out; a global lock is likely held across a reentrant callback", name)
	}
}

// ---------------------------------------------------------------------------
// State machine: pending -> promoted -> deleted, with the exact terminal
// transition driving callback ownership. Driven through a real cache.
// ---------------------------------------------------------------------------

func Test_Contract_StateMachine_PromoteDeleteTransitions(t *testing.T) {
	cache, recorder := newRecordedCache(nil)
	defer cache.Stop()

	cache.Set("k", "v", time.Minute)
	item := cache.GetWithoutPromote("k")
	if got, want := item.state(), statePending; got != want {
		t.Fatalf("first state violation: new item state=%s, want %s", got, want)
	}

	cache.SyncUpdates()
	if got, want := item.state(), statePromoted; got != want {
		t.Fatalf("first state violation: item after promotion state=%s, want %s", got, want)
	}
	assert.Equal(t, cache.GetSize(), int64(1))

	if promoted := cache.doPromote(item); promoted {
		t.Fatal("first state violation: doPromote on an already promoted item reported a new promotion")
	}

	cache.Delete("k")
	cache.SyncUpdates()
	if got, want := item.state(), stateDeleted; got != want {
		t.Fatalf("first state violation: item after delete state=%s, want %s", got, want)
	}
	assert.Nil(t, cache.Get("k"))
	assert.Equal(t, cache.GetSize(), int64(0))
	assert.Equal(t, recorder.count("k=v"), 1)

	// A late promotion of the retired item must not resurrect it.
	if promoted := cache.doPromote(item); promoted {
		t.Fatal("first resurrection violation: late doPromote resurrected a deleted item")
	}
	if got, want := item.state(), stateDeleted; got != want {
		t.Fatalf("first resurrection violation: late promotion changed state to %s, want %s", got, want)
	}
	assert.Equal(t, cache.GetSize(), int64(0))

	// Retirement is idempotent and cannot fire the callback a second time.
	cache.doDelete(item)
	assertCallbackOwnership(t, recorder, "duplicate delete")
	assert.Equal(t, recorder.count("k=v"), 1)
	assertCacheInvariants(t, cache, "state machine transitions")
}

// ---------------------------------------------------------------------------
// Promote/delete race: public Get (which queues a promote) and Delete fire at
// the same instant behind one synchronization barrier. Both worker orderings
// must converge to the same retired item with at most one callback.
// ---------------------------------------------------------------------------

func Test_Contract_PromoteDeleteRace_NoResurrection(t *testing.T) {
	const rounds = 200
	for round := 0; round < rounds; round++ {
		cache, recorder := newRecordedCache(nil)
		cache.Set("k", "v", time.Minute)
		cache.SyncUpdates()
		item := cache.GetWithoutPromote("k")

		start := newBarrier()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			start.Wait()
			cache.Get("k")
		}()
		go func() {
			defer wg.Done()
			start.Wait()
			cache.Delete("k")
		}()
		start.Release()
		wg.Wait()
		cache.SyncUpdates()

		if got := cache.Get("k"); got != nil {
			t.Fatalf("round %d: first race violation: Get returned item %q after delete", round, got.key)
		}
		if got, want := item.state(), stateDeleted; got != want {
			t.Fatalf("round %d: first race violation: surviving item state=%s, want %s", round, got, want)
		}
		if size := cache.GetSize(); size != 0 {
			t.Fatalf("round %d: first race violation: GetSize=%d, want 0 (retired item was size-accounted)", round, size)
		}
		if n := recorder.count("k=v"); n > 1 {
			t.Fatalf("round %d: first race violation: onDelete fired %d times, want at most 1", round, n)
		}
		assertCacheInvariants(t, cache, "promote/delete race")
		cache.Stop()
	}
}

// ---------------------------------------------------------------------------
// Overwrite: the displaced item is retired exactly once and a queued promote
// for it must never bring it back into the eviction list.
// ---------------------------------------------------------------------------

func Test_Contract_Overwrite_RetiresOldItem(t *testing.T) {
	cache, recorder := newRecordedCache(nil)
	defer cache.Stop()

	cache.Set("k", "v1", time.Minute)
	cache.SyncUpdates()
	old := cache.GetWithoutPromote("k")

	cache.Set("k", "v2", time.Minute)
	cache.SyncUpdates()
	assert.Equal(t, cache.Get("k").Value(), "v2")
	assert.Equal(t, cache.GetSize(), int64(1))
	if got, want := old.state(), stateDeleted; got != want {
		t.Fatalf("first overwrite violation: displaced item state=%s, want %s", got, want)
	}
	assert.Equal(t, recorder.count("k=v1"), 1)
	assert.Equal(t, recorder.count("k=v2"), 0)

	if promoted := cache.doPromote(old); promoted {
		t.Fatal("first overwrite violation: late promote resurrected the displaced item")
	}
	assert.Equal(t, cache.GetSize(), int64(1))
	assertCallbackOwnership(t, recorder, "overwrite")
	assertCacheInvariants(t, cache, "overwrite")
}

func Test_Contract_OverwriteBeforePromote_LeavesNoTrace(t *testing.T) {
	cache, recorder := newRecordedCache(nil)
	defer cache.Stop()

	cache.Set("k", "v1", time.Minute)
	cache.Set("k", "v2", time.Minute)
	cache.SyncUpdates()
	assert.Equal(t, cache.Get("k").Value(), "v2")
	assert.Equal(t, cache.GetSize(), int64(1))
	if n := recorder.total(); n > 1 {
		t.Fatalf("first overwrite-before-promote violation: %d retired callbacks, want at most 1", n)
	}
	assertCallbackOwnership(t, recorder, "overwrite before promote")
	assertCacheInvariants(t, cache, "overwrite before promote")
}
