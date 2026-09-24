package ccache

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file holds the regression material for "promotion, expiry and
// ownership state machine" work. Every step enters the cache through its
// public API (Get/Set/Setnx/Setnx2/Fetch/Replace/Extend/Delete/Clear/Stop
// plus the control commands SyncUpdates/GetSize/GetDropped). Internal
// state (bucket membership, LRU list membership, the promotions marker) is
// only ever *observed*, never fabricated, and only after SyncUpdates has
// quiesced the worker for the calling goroutine.
//
// The single checkable state machine for one key is:
//
//	absent  --Set/Setnx/Fetch-->  pending   (in a bucket, promotion queued)
//	pending --worker doPromote--> promoted  (in a bucket and in the LRU list)
//	promoted --Get x getsPerPromote--> promoted (moved to front)
//	promoted --Delete/overwrite/gc--> deleted (out of bucket+list, onDelete
//	                                      fired exactly once, promotions=-2)
//	deleted  --late promotion-->  deleted   (dropped via the -2 marker)
//
// After SyncUpdates only `absent` and `promoted` may be observed, and the
// cache size must equal the number of promoted items (all test values have
// size 1). Ownership of the onDelete callback is "at most once per item
// instance, never while holding a lock that re-entrant public calls need".

// ---------------------------------------------------------------------------
// fake clock
// ---------------------------------------------------------------------------

type smClock struct {
	mu  sync.Mutex
	now time.Time
}

func newSMClock() *smClock {
	return &smClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *smClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *smClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// newSMCache builds a cache driven by the fake clock. It uses the internal
// (unexported) Configuration.now hook, which feeds the exact same code
// paths production uses with time.Now (bucket.set/setnx/setnx2 expiry
// computation and Item.Expired/TTL/Extend).
func newSMCache(clock *smClock, rec *deleteRecorder, tweak func(*Configuration[string])) *Cache[string] {
	config := Configure[string]()
	config.now = clock.Now
	if rec != nil {
		config.OnDelete(rec.callback())
	}
	if tweak != nil {
		tweak(config)
	}
	return New(config)
}

// ---------------------------------------------------------------------------
// onDelete recorder (callback ownership oracle)
// ---------------------------------------------------------------------------

type deleteRecorder struct {
	mu      sync.Mutex
	perItem map[*Item[string]]int
	perKey  map[string]int
	order   []string
	// hook, when set, runs outside the recorder lock after each callback.
	// Used by the reentrancy test.
	hook func(item *Item[string])
}

func newDeleteRecorder() *deleteRecorder {
	return &deleteRecorder{
		perItem: make(map[*Item[string]]int),
		perKey:  make(map[string]int),
	}
}

func (r *deleteRecorder) callback() func(*Item[string]) {
	return func(item *Item[string]) {
		r.mu.Lock()
		r.perItem[item]++
		r.perKey[item.key]++
		r.order = append(r.order, item.key)
		hook := r.hook
		r.mu.Unlock()
		if hook != nil {
			hook(item)
		}
	}
}

func (r *deleteRecorder) keyCount(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.perKey[key]
}

func (r *deleteRecorder) total() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.order)
}

func (r *deleteRecorder) sortedKeys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := append([]string(nil), r.order...)
	sort.Strings(out)
	return out
}

// assertOwnership proves the "at most once per item instance" part of the
// callback ownership contract and reports the first violating item.
func (r *deleteRecorder) assertOwnership(t *testing.T, step string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for item, count := range r.perItem {
		if count > 1 {
			t.Fatalf("[%s] onDelete fired %d times for item key=%q value=%q; contract is at-most-once per item instance",
				step, count, item.key, item.value)
		}
	}
}

// ---------------------------------------------------------------------------
// state machine observation
// ---------------------------------------------------------------------------

type itemState int

const (
	stateAbsent   itemState = iota // not in any bucket, not in the LRU list
	statePending                   // in a bucket, promotion not yet applied
	statePromoted                  // in a bucket and in the LRU list
)

func (s itemState) String() string {
	switch s {
	case stateAbsent:
		return "absent"
	case statePending:
		return "pending(in-bucket, promotion in flight)"
	case statePromoted:
		return "promoted(in-bucket + in-list)"
	}
	return "unknown"
}

func observeItem(c *Cache[string], key string) (itemState, *Item[string]) {
	item := c.bucket(key).get(key)
	if item == nil {
		return stateAbsent, nil
	}
	if item.inList {
		return statePromoted, item
	}
	return statePending, item
}

func expectState(t *testing.T, step string, c *Cache[string], key string, want itemState) *Item[string] {
	t.Helper()
	got, item := observeItem(c, key)
	if got != want {
		t.Fatalf("[%s] key=%q state=%s, want %s (size=%d, count=%d)",
			step, key, got, want, c.GetSize(), c.ItemCount())
	}
	return item
}

// assertSettled is the single checkable state-machine invariant, evaluated
// after SyncUpdates: every bucket item is promoted, every list item is in
// its bucket, list links are consistent, and size/count agree. The first
// violation is reported with the offending key and surrounding context.
func assertSettled(t *testing.T, step string, c *Cache[string], rec *deleteRecorder) {
	t.Helper()
	c.SyncUpdates()

	listLen := 0
	for item := c.list.Head; item != nil; item = item.next {
		listLen++
		if !item.inList {
			t.Fatalf("[%s] list node key=%q has inList=false", step, item.key)
		}
		if item.promotions == -2 {
			t.Fatalf("[%s] list node key=%q carries the deleted marker (-2)", step, item.key)
		}
		if owner := c.bucket(item.key).get(item.key); owner != item {
			t.Fatalf("[%s] orphaned list node key=%q: not reachable from its bucket (bucket holds %p, list holds %p)",
				step, item.key, owner, item)
		}
	}
	backLen := 0
	for item := c.list.Tail; item != nil; item = item.prev {
		backLen++
	}
	if backLen != listLen {
		t.Fatalf("[%s] broken list links: forward walk=%d nodes, backward walk=%d nodes",
			step, listLen, backLen)
	}

	var pendingKey string
	c.ForEachFunc(func(key string, item *Item[string]) bool {
		if !item.inList && pendingKey == "" {
			pendingKey = key
		}
		return true
	})
	if pendingKey != "" {
		t.Fatalf("[%s] key=%q stuck in pending state after SyncUpdates (in bucket, not in list)",
			step, pendingKey)
	}

	if size := c.GetSize(); size != int64(listLen) {
		t.Fatalf("[%s] size mismatch: GetSize()=%d but LRU list holds %d items (ItemCount=%d)",
			step, size, listLen, c.ItemCount())
	}
	if count := c.ItemCount(); count != listLen {
		t.Fatalf("[%s] count mismatch: ItemCount()=%d but LRU list holds %d items",
			step, count, listLen)
	}
	if rec != nil {
		rec.assertOwnership(t, step)
	}
}

// snapshot captures the caller-visible result plus the internal invariants
// that prove cache/concurrency/eviction semantics.
type snapshot struct {
	pairs     []string // sorted "key=value" for every cached key
	itemCount int
	size      int64
	dropped   int
	deletes   []string // sorted multiset of onDelete keys
}

func takeSnapshot(c *Cache[string], rec *deleteRecorder) snapshot {
	c.SyncUpdates()
	snap := snapshot{
		itemCount: c.ItemCount(),
		size:      c.GetSize(),
		dropped:   c.GetDropped(),
		deletes:   rec.sortedKeys(),
	}
	c.ForEachFunc(func(key string, item *Item[string]) bool {
		snap.pairs = append(snap.pairs, key+"="+item.value)
		return true
	})
	sort.Strings(snap.pairs)
	return snap
}

func assertSnapshotEqual(t *testing.T, step string, a, b snapshot) {
	t.Helper()
	if !reflect.DeepEqual(a.pairs, b.pairs) {
		t.Fatalf("[%s] cached pairs diverge:\n first: %v\nsecond: %v", step, a.pairs, b.pairs)
	}
	if a.itemCount != b.itemCount || a.size != b.size {
		t.Fatalf("[%s] counters diverge: first count=%d size=%d, second count=%d size=%d",
			step, a.itemCount, a.size, b.itemCount, b.size)
	}
	if a.dropped != b.dropped {
		t.Fatalf("[%s] dropped counters diverge: %d vs %d", step, a.dropped, b.dropped)
	}
	if !reflect.DeepEqual(a.deletes, b.deletes) {
		t.Fatalf("[%s] onDelete key multisets diverge:\n first: %v\nsecond: %v",
			step, a.deletes, b.deletes)
	}
}

// startBarrier releases all participating goroutines at the same instant.
type startBarrier struct {
	ch chan struct{}
}

func newStartBarrier() *startBarrier { return &startBarrier{ch: make(chan struct{})} }
func (b *startBarrier) wait()        { <-b.ch }
func (b *startBarrier) release()     { close(b.ch) }

// waitFor blocks until cond is true or the timeout elapses; it reports
// which condition failed, with context, instead of a bare true/false.
func waitFor(t *testing.T, step, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("[%s] timed out waiting for %s", step, what)
}
