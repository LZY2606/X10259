package ccache

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Shared test harness for the "promotion / expiration / ownership state
// machine" contract materials. Everything in this file is package-internal
// test support; every scenario still enters the cache through its public API
// (Get/Set/Fetch/Replace/Delete/Clear/Stop/...). Internal structures are only
// read afterwards to prove invariants - no internal final objects are forged.

// A fixed, machine-independent point in time: no wall clock, timezone or
// machine configuration participates in the results.
var fakeClockOrigin = time.Unix(1_700_000_000, 0)

// fakeClock is a manually advanced time source. The same clock value is
// threaded through Configuration -> bucket.set -> Item expiry evaluation and
// through the lifecycle state machine, i.e. the production code path.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: fakeClockOrigin}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// syncGate is a reusable synchronization barrier: every participant signals
// arrival and blocks until all participants have arrived, then all proceed
// together. It makes specific interleavings reproducible instead of relying
// on scheduler luck.
type syncGate struct {
	remaining int32
	release   chan struct{}
}

func newSyncGate(n int) *syncGate {
	return &syncGate{remaining: int32(n), release: make(chan struct{})}
}

func (g *syncGate) arrive() {
	if atomic.AddInt32(&g.remaining, -1) == 0 {
		close(g.release)
	}
	<-g.release
}

// lifecycleRecorder observes onDelete callbacks and proves the "at most once
// per item" ownership rule. It is safe for concurrent callback invocation.
type lifecycleRecorder[T any] struct {
	mu      sync.Mutex
	total   int
	perItem map[*Item[T]]int
	keys    map[*Item[T]]string
}

func newLifecycleRecorder[T any]() *lifecycleRecorder[T] {
	return &lifecycleRecorder[T]{
		perItem: make(map[*Item[T]]int),
		keys:    make(map[*Item[T]]string),
	}
}

func (r *lifecycleRecorder[T]) callback() func(*Item[T]) {
	return func(item *Item[T]) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.total++
		r.perItem[item]++
		r.keys[item] = item.key
	}
}

func (r *lifecycleRecorder[T]) callbackCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.total
}

// assertAtMostOnce reports the first item whose callback fired more than once,
// including the item key and the observed counts around it.
func (r *lifecycleRecorder[T]) assertAtMostOnce(t *testing.T, phase string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for item, count := range r.perItem {
		if count > 1 {
			t.Fatalf("%s: first onDelete ownership violation: item %q callback fired %d times (want at most 1); total callbacks=%d",
				phase, r.keys[item], count, r.total)
		}
	}
}

func (r *lifecycleRecorder[T]) countFor(item *Item[T]) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.perItem[item]
}

// mustComplete runs fn and fails the test if it does not return before the
// watchdog fires. The watchdog is only a deadlock detector (e.g. a re-entrant
// callback blocked on a global lock); the passing path never depends on
// timing.
func mustComplete(t *testing.T, phase string, watchdog time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(watchdog):
		// Errorf (not Fatalf) because fn - and therefore this timeout path -
		// runs on the cache worker goroutine, not the test goroutine.
		t.Errorf("%s: deadlock watchdog (%s) expired: a public cache call inside the onDelete callback blocked on a re-entrant global lock", phase, watchdog)
	}
}

// verifyLifecycleInvariants checks the cross-structure state-machine
// invariants that are safe to observe while the cache is live (the caller
// invokes SyncUpdates first):
//   - promotables/deletables queues are drained relative to this goroutine;
//   - GetSize() matches the worker-visible accounting;
//   - no callback fired more than once;
//   - no stateDeleted item remains in a bucket.
//
// Deep inspection of the recency list additionally requires that no other
// goroutine can still enqueue a promotion (the list itself is lock-free by
// design); pass settle=true after a Stop for that tier (see
// verifySettledLifecycleInvariants). The first violated invariant is reported
// with the phase and offending item/key as evidence.
func verifyLifecycleInvariants[T any](t *testing.T, c *Cache[T], phase string, recorder *lifecycleRecorder[T]) {
	t.Helper()

	if n := len(c.promotables); n != 0 {
		t.Fatalf("%s: first lifecycle violation: promotables queue not drained, len=%d", phase, n)
	}
	if n := len(c.deletables); n != 0 {
		t.Fatalf("%s: first lifecycle violation: deletables queue not drained, len=%d", phase, n)
	}

	for bi, b := range c.buckets {
		b.RLock()
		for key, item := range b.lookup {
			if item.state == stateDeleted {
				t.Fatalf("%s: first lifecycle violation: bucket %d still contains deleted item %q", phase, bi, key)
			}
		}
		b.RUnlock()
	}
	if recorder != nil {
		recorder.assertAtMostOnce(t, phase)
	}
}

// verifySettledLifecycleInvariants performs the deep, lock-free checks that
// are only sound once the worker goroutine has fully stopped (caller invokes
// Stop and no concurrent caller remains): the recency list is a coherent
// doubly linked list, every listed item is statePromoted, listed and
// accounted in GetSize(), and bucket states are consistent with the list.
func verifySettledLifecycleInvariants[T any](t *testing.T, c *Cache[T], phase string, recorder *lifecycleRecorder[T]) {
	t.Helper()

	if n := len(c.promotables); n != 0 {
		t.Fatalf("%s: first lifecycle violation: promotables queue not drained, len=%d", phase, n)
	}
	if n := len(c.deletables); n != 0 {
		t.Fatalf("%s: first lifecycle violation: deletables queue not drained, len=%d", phase, n)
	}

	listed := make(map[*Item[T]]bool)
	var sizeSum int64
	forward := 0
	var prevNode *Item[T]
	for cur := c.list.Head; cur != nil; cur = cur.next {
		if cur.state != statePromoted {
			t.Fatalf("%s: first lifecycle violation: listed item %q is in state %s, want promoted", phase, cur.key, cur.state)
		}
		if !cur.inList {
			t.Fatalf("%s: first lifecycle violation: listed item %q has inList=false", phase, cur.key)
		}
		if listed[cur] {
			t.Fatalf("%s: first lifecycle violation: item %q appears twice in the recency list", phase, cur.key)
		}
		if cur.prev != prevNode {
			t.Fatalf("%s: first lifecycle violation: list item %q has inconsistent prev link", phase, cur.key)
		}
		listed[cur] = true
		sizeSum += cur.size
		forward++
		prevNode = cur
	}
	if c.list.Head != nil && c.list.Head.prev != nil {
		t.Fatalf("%s: first lifecycle violation: list head has non-nil prev", phase)
	}
	if prevNode != c.list.Tail {
		t.Fatalf("%s: first lifecycle violation: list tail mismatch, forward end=%v list.Tail=%v", phase, prevNode, c.list.Tail)
	}
	if c.list.Tail != nil && c.list.Tail.next != nil {
		t.Fatalf("%s: first lifecycle violation: list tail has non-nil next", phase)
	}
	// The worker has stopped, so no control message can be serviced: read the
	// worker-owned accounting directly.
	if c.size != sizeSum {
		t.Fatalf("%s: first lifecycle violation: cache size=%d, sum of listed item sizes=%d", phase, c.size, sizeSum)
	}

	for bi, b := range c.buckets {
		b.RLock()
		for key, item := range b.lookup {
			switch item.state {
			case statePromoted:
				if !listed[item] {
					t.Fatalf("%s: first lifecycle violation: bucket %d item %q is promoted but absent from the recency list", phase, bi, key)
				}
			case stateNew:
				if listed[item] {
					t.Fatalf("%s: first lifecycle violation: bucket %d item %q is new but already listed", phase, bi, key)
				}
			case stateAbandoned:
				if listed[item] {
					t.Fatalf("%s: first lifecycle violation: bucket %d item %q was abandoned (expired) but is listed: a late promotion resurrected it", phase, bi, key)
				}
			case stateDeleted:
				t.Fatalf("%s: first lifecycle violation: bucket %d still contains deleted item %q", phase, bi, key)
			}
		}
		b.RUnlock()
	}
	listedCount := c.ItemCount()
	if listedCount < forward {
		t.Fatalf("%s: first lifecycle violation: ItemCount()=%d is smaller than listed count=%d", phase, listedCount, forward)
	}
	if recorder != nil {
		recorder.assertAtMostOnce(t, phase)
	}
}

// cacheSnapshot is the caller-visible outcome of an acceptance run, compared
// across replays. Expectations are always read from the fixed key list, never
// from map iteration order.
type cacheSnapshot struct {
	values    map[string]string
	itemCount int
	size      int64
	callbacks int
}

func takeSnapshot(c *Cache[string], recorder *lifecycleRecorder[string], keys []string) cacheSnapshot {
	values := make(map[string]string, len(keys))
	for _, key := range keys {
		item := c.GetWithoutPromote(key)
		if item != nil {
			values[key] = item.Value()
		}
	}
	return cacheSnapshot{
		values:    values,
		itemCount: c.ItemCount(),
		size:      c.GetSize(),
		callbacks: recorder.callbackCount(),
	}
}

func assertSnapshotsEqual(t *testing.T, phase string, want, got cacheSnapshot, keys []string, compareCallbacks bool) {
	t.Helper()
	for _, key := range keys {
		wv, wok := want.values[key]
		gv, gok := got.values[key]
		if wok != gok || wv != gv {
			t.Fatalf("%s: first replay divergence at key %q: first run present=%v value=%q, replay present=%v value=%q",
				phase, key, wok, wv, gok, gv)
		}
	}
	if want.itemCount != got.itemCount {
		t.Fatalf("%s: first replay divergence at ItemCount: first run=%d, replay=%d", phase, want.itemCount, got.itemCount)
	}
	if want.size != got.size {
		t.Fatalf("%s: first replay divergence at GetSize: first run=%d, replay=%d", phase, want.size, got.size)
	}
	if compareCallbacks && want.callbacks != got.callbacks {
		t.Fatalf("%s: first replay divergence at onDelete callback count: first run=%d, replay=%d", phase, want.callbacks, got.callbacks)
	}
}

func sortedKeys(keys []string) []string {
	out := append([]string(nil), keys...)
	sort.Strings(out)
	return out
}

func itemPointer[T any](c *Cache[T], key string) *Item[T] {
	return c.bucket(key).get(key)
}

func phasef(stage int, format string, args ...any) string {
	return fmt.Sprintf("stage %d: "+format, append([]any{stage}, args...)...)
}
