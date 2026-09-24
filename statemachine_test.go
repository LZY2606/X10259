package ccache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/karlseguin/ccache/v3/assert"
)

// fakeClock is a manually advanced clock installed behind the same timeNow
// indirection the production code uses, so expiry decisions in tests go
// through the exact production code path.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func installFakeClock(t *testing.T) *fakeClock {
	t.Helper()
	fc := &fakeClock{now: time.Now()}
	original := timeNow
	timeNow = func() time.Time {
		fc.mu.Lock()
		defer fc.mu.Unlock()
		return fc.now
	}
	t.Cleanup(func() { timeNow = original })
	return fc
}

func (fc *fakeClock) advance(d time.Duration) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.now = fc.now.Add(d)
}

// assertCacheInvariants walks the LRU list and cross-checks it against the
// buckets and the size accounting. It reports the first violation it finds,
// with enough context (key, state, counts) to locate the breach.
func assertCacheInvariants[T any](t *testing.T, c *Cache[T], ctx string) {
	t.Helper()
	c.SyncUpdates()
	count := 0
	var size int64
	var prev *Item[T]
	for item := c.list.Head; item != nil; item = item.next {
		if item.state != stateActive {
			t.Fatalf("%s: first violation: list item %q is %s, want active", ctx, item.key, item.state)
		}
		if item.prev != prev {
			t.Fatalf("%s: first violation: broken prev linkage at item %q", ctx, item.key)
		}
		prev = item
		count++
		size += item.size
	}
	if c.list.Tail != prev {
		t.Fatalf("%s: first violation: list tail is %v, want %v", ctx, c.list.Tail, prev)
	}
	if got := c.ItemCount(); got != count {
		t.Fatalf("%s: first violation: buckets hold %d items, LRU list holds %d", ctx, got, count)
	}
	if got := c.GetSize(); got != size {
		t.Fatalf("%s: first violation: GetSize()=%d, sum of list item sizes=%d", ctx, got, size)
	}
}

func Test_StateMachine_PendingToActiveOnFirstPromote(t *testing.T) {
	cache := New(Configure[string]())
	defer cache.Stop()

	cache.Set("a", "1", time.Minute)
	cache.SyncUpdates()

	item := cache.GetWithoutPromote("a")
	assert.Equal(t, item.state, stateActive)
	assert.Equal(t, cache.GetSize(), 1)
	assertCacheInvariants(t, cache, "after first promote")
}

func Test_StateMachine_PendingItemClearedWithoutCallback(t *testing.T) {
	callbacks := int32(0)
	cache := New(Configure[string]().OnDelete(func(item *Item[string]) {
		atomic.AddInt32(&callbacks, 1)
	}))
	defer cache.Stop()

	// The item is still pending (its first promotion sits in the queue)
	// when Clear runs: it transitions pending -> removed without ever
	// joining the LRU list, and per the cache's contract no onDelete
	// callback fires for an item that never became active.
	cache.Set("a", "1", time.Minute)
	cache.Clear()

	item := cache.GetWithoutPromote("a")
	assert.Equal(t, item == nil, true)
	assert.Equal(t, cache.GetSize(), 0)
	assert.Equal(t, atomic.LoadInt32(&callbacks), 0)
	assert.Equal(t, cache.ItemCount(), 0)
}

func Test_StateMachine_SyncUpdatesProcessesPromotesBeforeDeletes(t *testing.T) {
	// SyncUpdates drains the promote queue before the delete queue, so a
	// Set followed by a Delete (with no sync in between) still transitions
	// the item through active: the callback fires exactly once and the
	// size accounting returns to 0.
	callbacks := int32(0)
	cache := New(Configure[string]().OnDelete(func(item *Item[string]) {
		atomic.AddInt32(&callbacks, 1)
	}))
	defer cache.Stop()

	cache.Set("a", "1", time.Minute)
	cache.Delete("a")
	cache.SyncUpdates()

	assert.Equal(t, cache.Get("a") == nil, true)
	assert.Equal(t, cache.GetSize(), 0)
	assert.Equal(t, atomic.LoadInt32(&callbacks), 1)
	assertCacheInvariants(t, cache, "set+delete before sync")
}

func Test_StateMachine_PromoteDeleteRaceConvergesToRemoved(t *testing.T) {
	// Every round queues a promotion and a deletion for the same active item
	// before the worker drains either queue. Whichever message the worker
	// picks up first, the end state must be identical: removed, unlisted,
	// size 0, callback exactly once.
	for round := 0; round < 200; round++ {
		callbacks := int32(0)
		cache := New(Configure[string]().GetsPerPromote(1).OnDelete(func(item *Item[string]) {
			atomic.AddInt32(&callbacks, 1)
		}))

		cache.Set("k", "v", time.Minute)
		cache.SyncUpdates()

		item := cache.Get("k") // queues a promotion
		if item == nil {
			t.Fatalf("round %d: first violation: Get returned nil for a live item", round)
		}
		if !cache.Delete("k") { // queues a deletion
			t.Fatalf("round %d: first violation: Delete reported the item missing", round)
		}
		cache.SyncUpdates()

		if item.state != stateRemoved {
			t.Fatalf("round %d: first violation: item state is %s, want removed", round, item.state)
		}
		if got := atomic.LoadInt32(&callbacks); got != 1 {
			t.Fatalf("round %d: first violation: onDelete fired %d times, want exactly 1", round, got)
		}
		if got := cache.Get("k"); got != nil {
			t.Fatalf("round %d: first violation: deleted item still visible: %v", round, got)
		}
		if got := cache.GetSize(); got != 0 {
			t.Fatalf("round %d: first violation: GetSize()=%d, want 0", round, got)
		}
		assertCacheInvariants(t, cache, fmt.Sprintf("round %d", round))
		cache.Stop()
	}
}

func Test_StateMachine_ExpiredItemNotResurrectedByLatePromotion(t *testing.T) {
	fc := installFakeClock(t)
	callbacks := int32(0)
	cache := New(Configure[string]().GetsPerPromote(1).OnDelete(func(item *Item[string]) {
		atomic.AddInt32(&callbacks, 1)
	}))
	defer cache.Stop()

	cache.Set("k", "v", time.Minute)
	cache.SyncUpdates()

	// Queue a promotion while the item is still live...
	item := cache.Get("k")
	if item == nil {
		t.Fatal("first violation: Get returned nil for a live item")
	}
	// ...then let it expire and delete it before the worker drains the queues.
	fc.advance(2 * time.Minute)
	if !item.Expired() {
		t.Fatal("first violation: fake clock advance did not expire the item")
	}
	cache.Delete("k")
	cache.SyncUpdates()

	if item.state != stateRemoved {
		t.Fatalf("first violation: expired item resurrected to state %s by a late promotion", item.state)
	}
	if got := cache.Get("k"); got != nil {
		t.Fatalf("first violation: expired+deleted item still visible: %v", got)
	}
	if got := atomic.LoadInt32(&callbacks); got != 1 {
		t.Fatalf("first violation: onDelete fired %d times, want exactly 1", got)
	}
	if got := cache.GetSize(); got != 0 {
		t.Fatalf("first violation: GetSize()=%d, want 0", got)
	}
	assertCacheInvariants(t, cache, "late promotion after expiry")
}

func Test_StateMachine_OverwriteTransfersOwnershipToNewItem(t *testing.T) {
	var mu sync.Mutex
	deleted := make([]string, 0)
	cache := New(Configure[*SizedItem]().OnDelete(func(item *Item[*SizedItem]) {
		mu.Lock()
		defer mu.Unlock()
		deleted = append(deleted, fmt.Sprintf("%d", item.value.id))
	}))
	defer cache.Stop()

	cache.Set("k", &SizedItem{1, 10}, time.Minute)
	cache.SyncUpdates()
	// Overwrite: the old item must be retired exactly once and only the new
	// item's size may remain accounted.
	cache.Set("k", &SizedItem{2, 25}, time.Minute)
	cache.SyncUpdates()

	assert.Equal(t, cache.Get("k").Value().id, 2)
	assert.Equal(t, cache.GetSize(), 25)
	mu.Lock()
	if len(deleted) != 1 || deleted[0] != "1" {
		t.Fatalf("first violation: onDelete saw %v, want exactly [1]", deleted)
	}
	mu.Unlock()
	assertCacheInvariants(t, cache, "after overwrite")

	// Overwriting again retires item 2; the already-removed item 1 must not
	// be double-counted or re-callbacked.
	cache.Set("k", &SizedItem{3, 7}, time.Minute)
	cache.SyncUpdates()
	assert.Equal(t, cache.GetSize(), 7)
	mu.Lock()
	if len(deleted) != 2 || deleted[1] != "2" {
		t.Fatalf("first violation: onDelete saw %v, want exactly [1 2]", deleted)
	}
	mu.Unlock()
	assertCacheInvariants(t, cache, "after second overwrite")
}

func Test_StateMachine_ClearTransitionsEverythingToRemoved(t *testing.T) {
	fc := installFakeClock(t)
	cache := New(Configure[string]())
	defer cache.Stop()

	cache.Set("a", "1", time.Minute)
	cache.Set("b", "2", time.Minute)
	cache.SyncUpdates()
	itemA := cache.GetWithoutPromote("a")
	itemB := cache.GetWithoutPromote("b")

	// Queue more promotions and deletions, then clear before the worker
	// drains them: clear must win and leave no half-updated state behind.
	cache.Get("a")
	cache.Delete("b")
	cache.Clear()

	if itemA.state != stateRemoved || itemB.state != stateRemoved {
		t.Fatalf("first violation: after Clear states are a=%s b=%s, want both removed",
			itemA.state, itemB.state)
	}
	assert.Equal(t, cache.ItemCount(), 0)
	assert.Equal(t, cache.GetSize(), 0)
	assertCacheInvariants(t, cache, "after clear")

	// The cache keeps honoring its public contract after a clear.
	fc.advance(time.Minute)
	cache.Set("c", "3", time.Minute)
	cache.SyncUpdates()
	assert.Equal(t, cache.Get("c").Value(), "3")
	assert.Equal(t, cache.GetSize(), 1)
	assertCacheInvariants(t, cache, "after reuse post-clear")
}

func Test_StateMachine_StopDrainsDeleteQueue(t *testing.T) {
	callbacks := int32(0)
	cache := New(Configure[string]().OnDelete(func(item *Item[string]) {
		atomic.AddInt32(&callbacks, 1)
	}))

	cache.Set("a", "1", time.Minute)
	cache.SyncUpdates()
	cache.Delete("a")
	// Stop syncs pending updates and then drains the delete queue: the
	// callback ownership transfer must complete before Stop returns.
	cache.Stop()

	if got := atomic.LoadInt32(&callbacks); got != 1 {
		t.Fatalf("first violation: onDelete fired %d times after Stop, want exactly 1", got)
	}
}

func Test_StateMachine_LayeredCacheSharesTransitions(t *testing.T) {
	callbacks := int32(0)
	cache := Layered(Configure[string]().GetsPerPromote(1).OnDelete(func(item *Item[string]) {
		atomic.AddInt32(&callbacks, 1)
	}))
	defer cache.Stop()

	cache.Set("g", "k", "v1", time.Minute)
	cache.SyncUpdates()
	// overwrite + queued promotion racing a delete, through the layered entry
	cache.Set("g", "k", "v2", time.Minute)
	cache.Get("g", "k")
	cache.Delete("g", "k")
	cache.SyncUpdates()

	if got := cache.Get("g", "k"); got != nil {
		t.Fatalf("first violation: deleted layered item still visible: %v", got)
	}
	if got := atomic.LoadInt32(&callbacks); got != 2 {
		t.Fatalf("first violation: onDelete fired %d times, want 2 (overwritten v1 + deleted v2)", got)
	}
	if got := cache.GetSize(); got != 0 {
		t.Fatalf("first violation: GetSize()=%d, want 0", got)
	}
}
