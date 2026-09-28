package ccache

import "sync/atomic"

// The lifecycle of an Item is a single state machine shared by Cache and
// LayeredCache as the item moves between the bucket, the promotables queue,
// the LRU list and the delete queue. Except for kill() during Clear (which
// runs halted, holding every bucket lock), all transitions happen on the
// cache's single worker goroutine, so transitions are race-free by
// construction:
//
//	pending --(doPromote)--> listed   item enters the LRU list, size accounted
//	listed  --(doPromote)--> listed   item moves to the front of the list
//	pending --(doPromote)--> pending  late promotion of an expired item: dropped
//	*       --(doDelete/gc/clear)--> dead
//
// List membership itself is tracked by Item.inList, maintained by
// List.Insert/List.Remove on the worker goroutine. The rules enforced here:
//
//   - dead is terminal: a deleted item is never revived by a late promotion
//     (doPromote checks killed()).
//   - an expired item never enters the list through a late promotion
//     (doPromote checks Expired() before inserting).
//   - the OnDelete callback fires at most once per item: ownership is
//     transferred exactly once via claimDeleteCallback, and the callback
//     runs on the worker goroutine without holding any bucket lock, so it
//     may safely re-enter the cache's public API (Get/Set/Delete/...).
const (
	itemAlive int32 = iota
	itemDead
)

// kill transitions the item to its terminal dead state. It is idempotent.
func (i *Item[T]) kill() {
	atomic.StoreInt32(&i.state, itemDead)
}

// killed reports whether the item has reached its terminal dead state.
func (i *Item[T]) killed() bool {
	return atomic.LoadInt32(&i.state) == itemDead
}

// claimDeleteCallback transfers ownership of the delete callback to the
// caller. It returns true exactly once per item, which is how the "OnDelete
// fires at most once" contract is enforced.
func (i *Item[T]) claimDeleteCallback() bool {
	return atomic.CompareAndSwapInt32(&i.notified, 0, 1)
}

// notifyDelete invokes the OnDelete callback if the caller won ownership of
// it. Shared by Cache and LayeredCache, from both their doDelete and gc
// paths, so that callback ownership is decided in exactly one place. It is
// only ever called from the worker goroutine and never while holding a
// bucket lock, so the callback may re-enter the cache's public API.
func notifyDelete[T any](onDelete func(*Item[T]), item *Item[T]) {
	if onDelete != nil && item.claimDeleteCallback() {
		onDelete(item)
	}
}
