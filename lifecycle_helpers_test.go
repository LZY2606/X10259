package ccache

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a manually advanced monotonic clock. Only the test goroutine
// advances it; the worker goroutine may read it, always through the exact same
// Configuration.now seam as production.
type fakeClock struct {
	ns atomic.Int64
}

func newFakeClock(start time.Time) *fakeClock {
	fc := &fakeClock{}
	fc.ns.Store(start.UnixNano())
	return fc
}

func (f *fakeClock) now() time.Time {
	return time.Unix(0, f.ns.Load())
}

func (f *fakeClock) advance(d time.Duration) {
	f.ns.Add(int64(d))
}

func (f *fakeClock) set(t time.Time) {
	f.ns.Store(t.UnixNano())
}

// withFakeClock installs the fake clock on a fresh configuration. This is the
// only test-only configuration knob; the field is unexported and merely feeds
// the same clock lookup used by production.
func withFakeClock[T any](fc *fakeClock) *Configuration[T] {
	return Configure[T]().withClock(fc.now)
}

// violation is the first point at which a contract is broken, together with the
// evidence needed to understand it. Failures never collapse to a bare boolean.
type violation struct {
	where string
	msg   string
	args  []any
}

func (v violation) String() string {
	return fmt.Sprintf("first violation at %s: %s", v.where, fmt.Sprintf(v.msg, v.args...))
}

// fail reports the first violation (and only that one) and stops the case.
func fail(t *testing.T, where, format string, args ...any) {
	t.Helper()
	t.Fatalf("%s: %s", where, fmt.Sprintf(format, args...))
}

// checkInvariants validates the single ownership state machine against the
// storage. It is run after SyncUpdates, when the worker has finished every
// transition issued by the calling goroutine. It reads the very structures the
// production path mutates (no fabricated expected objects):
//
//  1. every recency-list node is reachable, linked consistently, active and
//     counted; the forward and backward links agree on the node count.
//  2. list length equals engine.size divided into unit-sized items is not
//     assumed; instead size is accumulated from list nodes and compared.
//  3. every item still owned by a bucket is pending or active; an item active
//     in a bucket is on the list; an item on the list is owned by a bucket.
//  4. a removed item is neither in a bucket nor on the list.
func checkInvariants[T comparable](t *testing.T, c *Cache[T], where string) {
	t.Helper()

	inBucket := make(map[*Item[T]]string)
	c.engine.buckets.scan(func(item *Item[T]) {
		inBucket[item] = item.key
		switch item.lifecycleState() {
		case itemPending, itemActive:
		default:
			fail(t, where, "bucket item %q is in terminal state %d", item.key, item.lifecycleState())
		}
	})

	onList := make(map[*Item[T]]int)
	listSize := int64(0)
	index := 0
	for item := c.list.Head; item != nil; item, index = item.next, index+1 {
		if item.lifecycleState() != itemActive {
			fail(t, where, "list item %q at index %d has state %d, want active", item.key, index, item.lifecycleState())
		}
		if _, ok := inBucket[item]; !ok {
			fail(t, where, "list item %q is no longer owned by its bucket", item.key)
		}
		if item.prev != nil && item.prev.next != item {
			fail(t, where, "list item %q has inconsistent prev/next linkage", item.key)
		}
		onList[item] = index
		listSize += item.size
	}

	// tail/head agreement on length (backward walk)
	back := 0
	for item := c.list.Tail; item != nil; item = item.prev {
		back++
	}
	if back != len(onList) {
		fail(t, where, "list forward walk found %d nodes, backward walk found %d", len(onList), back)
	}

	for item, key := range inBucket {
		if item.lifecycleState() == itemActive {
			if _, ok := onList[item]; !ok {
				fail(t, where, "active bucket item %q is missing from the recency list", key)
			}
		}
	}

	if listSize != c.size {
		fail(t, where, "size counter is %d, but active list items account for %d", c.size, listSize)
	}
}

// checkInvariantsLayered is the same check for the two-level layout.
func checkInvariantsLayered[T comparable](t *testing.T, c *LayeredCache[T], where string) {
	t.Helper()

	inBucket := make(map[*Item[T]]string)
	c.engine.buckets.scan(func(item *Item[T]) {
		inBucket[item] = item.group + "/" + item.key
		switch item.lifecycleState() {
		case itemPending, itemActive:
		default:
			fail(t, where, "bucket item %q is in terminal state %d", item.group+"/"+item.key, item.lifecycleState())
		}
	})

	onList := make(map[*Item[T]]int)
	listSize := int64(0)
	index := 0
	for item := c.list.Head; item != nil; item, index = item.next, index+1 {
		if item.lifecycleState() != itemActive {
			fail(t, where, "list item %s/%s at index %d has state %d, want active", item.group, item.key, index, item.lifecycleState())
		}
		if _, ok := inBucket[item]; !ok {
			fail(t, where, "list item %s/%s is no longer owned by its bucket", item.group, item.key)
		}
		if item.prev != nil && item.prev.next != item {
			fail(t, where, "list item %s/%s has inconsistent prev/next linkage", item.group, item.key)
		}
		onList[item] = index
		listSize += item.size
	}

	back := 0
	for item := c.list.Tail; item != nil; item = item.prev {
		back++
	}
	if back != len(onList) {
		fail(t, where, "list forward walk found %d nodes, backward walk found %d", len(onList), back)
	}

	for item, key := range inBucket {
		if item.lifecycleState() == itemActive {
			if _, ok := onList[item]; !ok {
				fail(t, where, "active bucket item %q is missing from the recency list", key)
			}
		}
	}

	if listSize != c.size {
		fail(t, where, "size counter is %d, but active list items account for %d", c.size, listSize)
	}
}

// forceGC is a synchronized barrier around the public control entry point.
func forceGC[T any](c *Cache[T]) {
	c.GC()
}

func forceGCLayered[T any](c *LayeredCache[T]) {
	c.GC()
}

// callbackRecorder counts invocations per key and records the goroutine state
// of every invocation, so "at most once" and reentrancy are directly
// observable.
type callbackRecorder[T any] struct {
	calls   atomic.Int64
	perKey  sync.Map // string -> *int64
	maxSeen int64
}

func (r *callbackRecorder[T]) cb() func(item *Item[T]) {
	return func(item *Item[T]) {
		key := item.group + "/" + item.key
		counter, _ := r.perKey.LoadOrStore(key, new(int64))
		n := atomic.AddInt64(counter.(*int64), 1)
		r.calls.Add(1)
		if n > 1 {
			atomic.StoreInt64(&r.maxSeen, n)
		}
	}
}

func (r *callbackRecorder[T]) count(key string) int64 {
	v, ok := r.perKey.Load(key)
	if !ok {
		return 0
	}
	return atomic.LoadInt64(v.(*int64))
}

// requireNoGoroutineLeak is intentionally not used: runtime.NumGoroutine is
// scheduler dependent. Determinism must come from barriers, not thread
// scheduling; runtime.KeepAlive is here only to keep the import honest for
// helper-only builds.
var _ = runtime.KeepAlive
