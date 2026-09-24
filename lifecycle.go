package ccache

// This file is the single place that defines how an item moves between a
// bucket (visibility to readers), the promotables queue, the LRU list (size
// accounting and eviction ownership) and the deletables queue (teardown and
// onDelete ownership). Both Cache.worker and LayeredCache.worker drive their
// items through the transitions below; no other code may mutate the
// worker-owned fields involved (Item.inList, Item.promotions).
//
// The lifecycle, as seen by the worker goroutine (its only mutator):
//
//	absent   --Set/Setnx/Fetch-->     pending   in a bucket, promotion queued
//	pending  --worker:promote-->      listed    in a bucket + LRU list, size accounted
//	listed   --worker:promote-->      listed    moved to front every getsPerPromote
//	pending|listed --Delete/overwrite/DeleteFunc/Clear/gc--> dead
//	dead     --worker:late promote--> dead      dropped: never resurrected
//
// Expiry never moves an item out of this machine by itself: an expired item
// stays in its bucket (and list) until an explicit delete, an overwrite, a
// Clear, or GC removes it. In particular a promotion that was queued before
// the item expired (or before it was deleted) must not resurrect it: the
// dead marker is checked first in promoteTransition.
//
// onDelete ownership: the worker fires onDelete exactly once per item, and
// only on the listed -> dead transition (an item that never entered the list
// never owned cache size, so there is nothing to tear down). The callback
// runs on the worker goroutine while holding no bucket or list lock, so it
// may safely re-enter the cache's public API.
//
// promotionsDead is the sentinel value of Item.promotions that marks an item
// as terminally dead. It is written only by the worker goroutine (directly,
// or via bucket.clear while every bucket is halted).
const promotionsDead = -2

// kill terminally marks an item. Worker goroutine only.
func (i *Item[T]) kill() {
	i.promotions = promotionsDead
}

// dead reports whether the item has been terminally removed. A dead item can
// never be promoted again, so a late promotion sitting in the promotables
// queue is dropped instead of resurrecting the item into the LRU list.
// Worker goroutine only.
func (i *Item[T]) dead() bool {
	return i.promotions == promotionsDead
}

// itemState is the observable lifecycle state of an item, derived from the
// worker-owned fields. It exists so tests (and debugging) can assert on the
// state machine above instead of on the raw encoding.
type itemState int

const (
	// itemStateAbsent: the key is not visible in any bucket.
	itemStateAbsent itemState = iota
	// itemStatePending: visible in a bucket, promotion queued or in flight,
	// not yet in the LRU list and not accounted in the cache size.
	itemStatePending
	// itemStateListed: visible in a bucket and linked into the LRU list;
	// its size is accounted in the cache size.
	itemStateListed
	// itemStateDead: terminally removed; late promotions are dropped.
	itemStateDead
)

func (s itemState) String() string {
	switch s {
	case itemStateAbsent:
		return "absent"
	case itemStatePending:
		return "pending"
	case itemStateListed:
		return "listed"
	case itemStateDead:
		return "dead"
	}
	return "unknown"
}

// promoteTransition applies the worker-side promotion of item to the LRU
// list. It returns true when the item entered the list, meaning the caller
// must account item.size in the cache size (and consider running GC).
//
// A dead item (deleted, cleared or evicted after its promotion was queued)
// is dropped: promotions never resurrect an item. An already listed item is
// moved to the front every getsPerPromote promotions.
func promoteTransition[T any](list *List[T], item *Item[T], getsPerPromote int32) bool {
	if item.dead() {
		return false
	}
	if item.inList {
		if item.shouldPromote(getsPerPromote) {
			list.MoveToFront(item)
			item.promotions = 0
		}
		return false
	}
	list.Insert(item)
	return true
}

// deleteTransition applies the worker-side deletion of item: it removes the
// item from the LRU list when listed, fires onDelete (at most once, and only
// for an item that owned cache size), and terminally marks the item so that
// any late promotion is dropped. It returns the size the caller must
// subtract from the cache size (0 when the item was not listed).
func deleteTransition[T any](list *List[T], item *Item[T], onDelete func(*Item[T])) int64 {
	if !item.inList {
		item.kill()
		return 0
	}
	size := item.size
	if onDelete != nil {
		onDelete(item)
	}
	list.Remove(item)
	item.kill()
	return size
}

// itemInspection is a consistent snapshot of an item's lifecycle state as
// seen by the worker goroutine, plus the worker-owned cache-wide invariants
// (size and list length) that tests assert on.
type itemInspection struct {
	state      itemState
	inBucket   bool
	inList     bool
	promotions int32
	itemSize   int64
	cacheSize  int64
	listLen    int
}

// inspect returns the lifecycle state of key. It is deliberately unexported:
// it is a test/debug surface that reuses the production worker goroutine
// (via a control message, exactly like GetSize/SyncUpdates) instead of
// duplicating or peeking at state from another goroutine. It must not be
// called after Stop, for the same reason GetSize must not.
func (c *Cache[T]) inspect(key string) itemInspection {
	res := make(chan itemInspection, 1)
	c.control <- controlInspect{key: key, res: res}
	return <-res
}

// inspectItem runs on the worker goroutine, which owns every field it reads.
func (c *Cache[T]) inspectItem(key string) itemInspection {
	inspection := itemInspection{cacheSize: c.size}
	for item := c.list.Head; item != nil; item = item.next {
		inspection.listLen++
	}
	item := c.bucket(key).get(key)
	if item == nil {
		inspection.state = itemStateAbsent
		return inspection
	}
	inspection.inBucket = true
	inspection.inList = item.inList
	inspection.promotions = item.promotions
	inspection.itemSize = item.size
	switch {
	case item.dead():
		inspection.state = itemStateDead
	case item.inList:
		inspection.state = itemStateListed
	default:
		inspection.state = itemStatePending
	}
	return inspection
}
