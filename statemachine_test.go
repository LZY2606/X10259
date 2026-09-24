package ccache

// Regression materials for "promotion, expiration and ownership state
// machine": how an item moves between the bucket lookup, the promotables
// queue / LRU list and the delete queue, and how promotion, expiration and
// onDelete callback ownership behave on every transition.
//
// Every scenario enters through the cache's public API (New/Get/Set/Fetch/
// Replace/Delete/Extend/Clear/Stop and the control barriers); tests never
// construct the cache's internal objects directly. A fake clock (clock.go)
// and synchronization barriers (SyncUpdates, WaitGroups and channels) make
// interleavings and expirations deterministic.
//
// The single inspectable state machine exercised here is:
//
//	Set                -> PENDING   (in bucket, promotion queued)
//	PENDING  + doPromote -> PROMOTED (in bucket and linked into the LRU list)
//	PENDING  + doDelete  -> RETIRED  (never reached the list, no callback)
//	PROMOTED + doDelete  -> RETIRED  (onDelete fires exactly once)
//	PROMOTED + GC        -> RETIRED  (onDelete fires exactly once)
//	PROMOTED + Get(alive)-> PROMOTED (moved to the front of the LRU list)
//	any      + Clear     -> RETIRED  (no callback)
//	RETIRED  + late promote -> RETIRED (promotions == -2 makes revival impossible)
//
// Expiration is orthogonal to ownership: Item.Expired() is a pure function
// of the clock, Get never promotes an expired item, and an expired item can
// be overwritten or deleted like any other. Callback ownership means that
// onDelete fires at most once per item and never while holding a lock that
// the callback could re-enter.
//
// inspectOwnership reads only production structures (the bucket lookup map,
// Item.inList and Item.promotions) behind a SyncUpdates barrier; it does not
// duplicate any production algorithm.

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock installs an in-memory clock behind the package-level timeNow
// seam. It is restored on test cleanup; tests using it never call
// t.Parallel, so no two caches observe different clocks.
type fakeClock struct {
	nanos int64
}

func newFakeClock() *fakeClock {
	return &fakeClock{nanos: time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC).UnixNano()}
}

func (f *fakeClock) now() time.Time {
	return time.Unix(0, atomic.LoadInt64(&f.nanos))
}

func (f *fakeClock) advance(d time.Duration) {
	atomic.AddInt64(&f.nanos, int64(d))
}

func installFakeClock(t *testing.T) *fakeClock {
	t.Helper()
	clock := newFakeClock()
	previous := timeNow
	timeNow = clock.now
	t.Cleanup(func() { timeNow = previous })
	return clock
}

// ownership is the single inspectable classification of an item's state
// across the bucket lookup, the LRU list and the delete queue.
type ownership int

const (
	ownerAbsent ownership = iota
	ownerPending
	ownerPromoted
	ownerRetired
)

func (o ownership) String() string {
	switch o {
	case ownerAbsent:
		return "absent"
	case ownerPending:
		return "pending"
	case ownerPromoted:
		return "promoted"
	case ownerRetired:
		return "retired"
	default:
		return "unknown"
	}
}

func inspectOwnership[T any](c *Cache[T], item *Item[T]) ownership {
	inBucket := c.bucket(item.key).get(item.key) == item
	switch {
	case item.promotions == -2:
		return ownerRetired
	case inBucket && item.inList:
		return ownerPromoted
	case inBucket:
		return ownerPending
	default:
		return ownerAbsent
	}
}

func requireOwnership[T any](t *testing.T, c *Cache[T], step string, item *Item[T], want ownership) {
	t.Helper()
	got := inspectOwnership(c, item)
	if got != want {
		t.Fatalf("%s: key %q ownership = %s, want %s (inList=%v promotions=%d expired=%v): first ownership violation",
			step, item.Key(), got, want, item.inList, item.promotions, item.Expired())
	}
}

func requireStats[T any](t *testing.T, c *Cache[T], step string, size int64, count int) {
	t.Helper()
	if got := c.GetSize(); got != size {
		t.Fatalf("%s: GetSize() = %d, want %d (first accounting violation)", step, got, size)
	}
	if got := c.ItemCount(); got != count {
		t.Fatalf("%s: ItemCount() = %d, want %d (first accounting violation)", step, got, count)
	}
}

// deleteRecorder counts onDelete invocations per item pointer. All
// invocations happen on the cache's worker goroutine; tests read the record
// only after a SyncUpdates barrier.
type deleteRecorder[T any] struct {
	perItem map[*Item[T]]int
	total   int
}

func newDeleteRecorder[T any]() *deleteRecorder[T] {
	return &deleteRecorder[T]{perItem: make(map[*Item[T]]int)}
}

func (d *deleteRecorder[T]) callback(item *Item[T]) {
	d.total++
	d.perItem[item]++
}

func (d *deleteRecorder[T]) requireAtMostOnce(t *testing.T, step string) {
	t.Helper()
	for item, calls := range d.perItem {
		if calls > 1 {
			t.Fatalf("%s: onDelete fired %d times for key %q, want at most once", step, calls, item.Key())
		}
	}
}

// Test_StateMachine_PromoteDeleteRaceConverges parks the worker inside a
// delete callback so that promote and delete messages pile up and are then
// processed in random order. Both interleavings must converge to the same
// retired state: no resurrection, correct accounting, at most one callback.
func Test_StateMachine_PromoteDeleteRaceConverges(t *testing.T) {
	installFakeClock(t)
	rec := newDeleteRecorder[int]()

	release := make(chan struct{})
	entered := make(chan struct{})
	cache := New(Configure[int]().OnDelete(func(item *Item[int]) {
		rec.callback(item)
		if item.Key() == "gate" {
			close(entered)
			<-release
		}
	}))
	defer cache.Stop()

	const batch = 64
	keys := make([]string, batch)
	refs := make([]*Item[int], batch)
	expired := make([]bool, batch)

	cache.Set("gate", 0, time.Minute)
	cache.SyncUpdates()
	cache.Delete("gate")
	<-entered

	for i := 0; i < batch; i++ {
		key := "k" + strconv.Itoa(i)
		keys[i] = key
		ttl := time.Minute
		if i%2 == 0 {
			ttl = -time.Minute // expired the moment it is written
			expired[i] = true
		}
		cache.Set(key, i, ttl)
		refs[i] = cache.GetWithoutPromote(key)
		cache.Delete(key)
	}
	close(release)
	cache.SyncUpdates()

	deletedBeforeRecovery := rec.total
	if deletedBeforeRecovery <= 1 || deletedBeforeRecovery >= batch+1 {
		t.Fatalf("parked-worker race: onDelete total = %d, want between 2 and %d: both promote-first and delete-first interleavings must occur",
			deletedBeforeRecovery, batch)
	}
	rec.requireAtMostOnce(t, "parked-worker race")
	requireStats(t, cache, "parked-worker race", 0, 0)
	for i, key := range keys {
		if item := cache.Get(key); item != nil {
			t.Fatalf("key %q: Get after delete = %v, want nil (item resurrected by a late promotion)", key, item.Value())
		}
		requireOwnership(t, cache, "parked-worker race", refs[i], ownerRetired)
		if expired[i] && !refs[i].Expired() {
			t.Fatalf("key %q: expired item lost its expiration during the promote/delete race", key)
		}
	}

	// Recovery: the same keys are repopulated and retired again through the
	// same public path, with exactly one callback per replacement item.
	for i, key := range keys {
		cache.Set(key, i+1000, time.Minute)
	}
	cache.SyncUpdates()
	requireStats(t, cache, "recovery after race", batch, batch)
	for i, key := range keys {
		item := cache.Get(key)
		if item == nil || item.Value() != i+1000 {
			t.Fatalf("key %q: Get after recovery = %v, want value %d", key, item, i+1000)
		}
		cache.Delete(key)
	}
	cache.SyncUpdates()
	requireStats(t, cache, "recovery after race", 0, 0)
	if rec.total != deletedBeforeRecovery+batch {
		t.Fatalf("recovery: onDelete total = %d, want %d (exactly one callback per repopulated item)",
			rec.total, deletedBeforeRecovery+batch)
	}
	rec.requireAtMostOnce(t, "recovery after race")
}

// Test_StateMachine_ExpiredItemNotRevivedByLatePromotion proves that a
// promotion queued while an item is alive can never resurrect it once it has
// expired and been deleted, and that Get on an expired item neither promotes
// it nor changes its expiration.
func Test_StateMachine_ExpiredItemNotRevivedByLatePromotion(t *testing.T) {
	clock := installFakeClock(t)
	rec := newDeleteRecorder[int]()
	cache := New(Configure[int]().GetsPerPromote(1).OnDelete(rec.callback))
	defer cache.Stop()

	cache.Set("a", 1, time.Minute)
	cache.Set("b", 2, time.Minute)
	cache.Set("c", 3, time.Minute)
	cache.SyncUpdates()
	refA := cache.GetWithoutPromote("a")
	refB := cache.GetWithoutPromote("b")
	refC := cache.GetWithoutPromote("c")
	requireOwnership(t, cache, "setup", refA, ownerPromoted)
	requireOwnership(t, cache, "setup", refC, ownerPromoted)
	if head := cache.list.Head; head != refC {
		t.Fatalf("setup: LRU head = %q, want %q", head.Key(), "c")
	}

	// This promotion is queued while "a" is alive, but becomes late: the
	// clock expires the item and the delete is queued before the worker can
	// drain either message.
	cache.Get("a")
	clock.advance(2 * time.Minute)
	if !refA.Expired() {
		t.Fatal("fake clock: item a should be expired after advancing past its TTL")
	}
	cache.Delete("a")
	cache.SyncUpdates()

	requireOwnership(t, cache, "late promotion after expiration+delete", refA, ownerRetired)
	requireStats(t, cache, "late promotion after expiration+delete", 2, 2)
	if item := cache.Get("a"); item != nil {
		t.Fatalf("key %q: Get = %v, want nil (expired item revived by a late promotion)", "a", item.Value())
	}
	if rec.perItem[refA] != 1 {
		t.Fatalf("key %q: onDelete fired %d times, want exactly 1", "a", rec.perItem[refA])
	}

	// Get is documented to return expired items, but it must not promote
	// them: the LRU head staying at "c" is the observable proof.
	if got := cache.Get("b"); got != refB || !got.Expired() {
		t.Fatalf("key %q: Get of expired item = %v, want the stored, expired item", "b", got)
	}
	cache.SyncUpdates()
	if head := cache.list.Head; head != refC {
		t.Fatalf("expired Get promoted an item: LRU head = %q, want %q", head.Key(), "c")
	}

	cache.Delete("b")
	cache.Delete("c")
	cache.SyncUpdates()
	requireStats(t, cache, "final cleanup", 0, 0)
	requireOwnership(t, cache, "final cleanup", refB, ownerRetired)
	requireOwnership(t, cache, "final cleanup", refC, ownerRetired)
	rec.requireAtMostOnce(t, "final cleanup")
}
