package ccache

import "time"

// itemState is the lifecycle state of an Item as it moves between a bucket,
// the promotables queue, the LRU list and the delete queue. All transitions
// are performed by the cache's single worker goroutine (or by the worker
// while every bucket is halted during Clear), so no atomicity is required.
//
// The valid transitions are:
//
//	statePending  -> stateActive   (first promotion processed by the worker)
//	statePending  -> stateRemoved  (deleted / cleared before ever promoted)
//	stateActive   -> stateActive   (re-promotion moves the item to the front)
//	stateActive   -> stateRemoved  (delete, GC eviction or clear)
//
// stateRemoved is terminal: a delete that already won the race must never be
// undone by a late promotion, and the onDelete callback is owned by whichever
// transition first moves the item out of stateActive, guaranteeing the
// callback fires at most once.
type itemState int32

const (
	// statePending means the item is known to a bucket but its first
	// promotion is still queued in the promotables channel.
	statePending itemState = iota
	// stateActive means the item has been promoted into the LRU list.
	stateActive
	// stateRemoved means the item has been deleted, evicted or cleared.
	stateRemoved
)

func (s itemState) String() string {
	switch s {
	case statePending:
		return "pending"
	case stateActive:
		return "active"
	case stateRemoved:
		return "removed"
	}
	return "unknown"
}

// timeNow is the clock behind every expiry decision (Set/Setnx/Setnx2 expiry
// stamping, Item.Expired, Item.TTL, Item.Extend and the Get fast-path). It is
// package-private so tests can install a fake clock; production code and the
// test hooks share this exact same code path, there is no test-only branch.
var timeNow = time.Now

// promoteItem applies a single promotion message to an item. It reports
// whether the item entered the LRU list (a pending -> active transition),
// which is what may push the cache beyond its max size and trigger GC.
// A removed item is never resurrected: the message is dropped.
func promoteItem[T any](list *List[T], size *int64, item *Item[T], getsPerPromote int32) bool {
	switch item.state {
	case stateRemoved:
		// deleted (or cleared) before this promotion was processed
		return false
	case stateActive:
		if item.shouldPromote(getsPerPromote) {
			list.MoveToFront(item)
			item.promotions = 0
		}
		return false
	}
	// statePending -> stateActive
	item.state = stateActive
	*size += item.size
	list.Insert(item)
	return true
}

// removeItem transitions an item to stateRemoved exactly once. The caller
// that performs the active -> removed transition owns the onDelete callback;
// every later removal attempt for the same item is a no-op, so the callback
// fires at most once and never while holding any bucket or cache lock.
// Items removed while still pending never joined the LRU list and, per the
// cache's established contract, do not trigger the callback.
func removeItem[T any](list *List[T], size *int64, item *Item[T], onDelete func(item *Item[T])) {
	if item.state != stateActive {
		item.state = stateRemoved
		return
	}
	item.state = stateRemoved
	*size -= item.size
	list.Remove(item)
	if onDelete != nil {
		onDelete(item)
	}
}
