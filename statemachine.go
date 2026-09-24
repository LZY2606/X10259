package ccache

import "sync/atomic"

// This file defines the single lifecycle state machine that every cached
// item moves through. Promotion, expiration and delete-callback ownership
// used to be encoded implicitly (a promotions == -2 sentinel plus an
// inList flag maintained by the eviction list). They are now explicit
// states with named transitions, so the whole lifecycle can be checked in
// one place - by the worker that drives it and by tests that inspect it.
//
// States:
//
//	statePending   the item is visible in a bucket, but is not (yet) in
//	               the eviction list; its size is not accounted
//	statePromoted  the item is in the eviction list; its size is accounted
//	stateDeleted   terminal; the item was removed and must never come back
//
// Ownership rules:
//
//   - The bucket lookup maps own key visibility (Get/Set/Delete/...). They
//     are updated synchronously by caller goroutines under bucket locks.
//   - The cache worker goroutine owns every lifecycle transition below. No
//     other goroutine may read or write Item.state. Clear also runs on the
//     worker, via a control message.
//   - The onDelete callback is owned by the worker: it fires at most once
//     per item, on the statePromoted -> stateDeleted transition, and it is
//     invoked while holding no bucket lock, so a callback may safely
//     re-enter the cache's public data-path methods.
//   - An expired item is never admitted into the eviction list, and a
//     deleted item is never revived: neither can (re-)enter statePromoted,
//     no matter how late a promotion arrives.
type itemState int32

const (
	statePending itemState = iota
	statePromoted
	stateDeleted
)

func (s itemState) String() string {
	switch s {
	case statePending:
		return "pending"
	case statePromoted:
		return "promoted"
	case stateDeleted:
		return "deleted"
	}
	return "unknown"
}

// lifecycle returns the item's current state. It may only be called from
// the cache worker goroutine, or from tests that have quiesced the worker
// for the item (e.g. via SyncUpdates). It reads the exact field the
// production transitions operate on; there is no test-only copy of this
// logic.
func (i *Item[T]) lifecycle() itemState {
	return itemState(i.state)
}

// transitionPromote applies the promotion half of the lifecycle on behalf
// of the cache worker. now is the worker's clock reading (the production
// path passes timeNow().UnixNano()). It reports the size delta to apply to
// the cache and whether the item was newly admitted into the eviction
// list (which is what may trigger GC).
func (i *Item[T]) transitionPromote(list *List[T], getsPerPromote int32, now int64) (sizeDelta int64, admitted bool) {
	switch i.lifecycle() {
	case stateDeleted:
		// A late promotion must never revive a deleted item.
		return 0, false
	case statePromoted:
		if i.shouldPromote(getsPerPromote) {
			list.MoveToFront(i)
			i.promotions = 0
		}
		return 0, false
	default:
		// statePending: first admission into the eviction list.
		if atomic.LoadInt64(&i.expires) < now {
			// An expired item is not admitted: a promotion that
			// arrives after expiry cannot bring the item back.
			return 0, false
		}
		list.Insert(i)
		i.state = int32(statePromoted)
		return i.size, true
	}
}

// transitionDelete moves the item to stateDeleted on behalf of the cache
// worker. It reports the size delta to apply to the cache (<= 0) and
// whether the worker must fire the onDelete callback. The callback fires
// at most once per item: only the statePromoted -> stateDeleted transition
// owns it. A pending item was never admitted, so the cache never took
// callback ownership of it; a deleted item absorbs duplicate deletes.
func (i *Item[T]) transitionDelete(list *List[T]) (sizeDelta int64, fireCallback bool) {
	if i.lifecycle() == statePromoted {
		list.Remove(i)
		i.state = int32(stateDeleted)
		return -i.size, true
	}
	i.state = int32(stateDeleted)
	return 0, false
}

// abandon forces the item into stateDeleted without firing the onDelete
// callback. Only Clear uses it: clearing resets the whole cache and
// abandons ownership of every item, wherever it is currently queued.
func (i *Item[T]) abandon() {
	i.state = int32(stateDeleted)
}
