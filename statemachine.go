package ccache

// The lifecycle of a cached item is a single state machine, owned by the
// cache's worker goroutine. Every transition of an item between the bucket,
// the promotables queue, the LRU list and the deletables queue goes through
// the functions in this file, so the ownership rules for promotion, expiry
// and the OnDelete callback live in exactly one place.
//
//	statePending --promote--> stateActive --promote--> stateActive (moved to front)
//	statePending --delete/clear--> stateDead
//	stateActive  --delete/gc-----> stateDead
//
// stateDead is terminal: a late promotion (one that was already queued when
// the item was deleted, expired-then-deleted, overwritten or cleared) must
// never resurrect a dead item back into the LRU list.
//
// The OnDelete callback is owned by the transition into stateDead: it fires
// at most once per item, from the worker goroutine, while no bucket lock is
// held, so a callback may safely re-enter the cache's public API.
type itemState int8

const (
	statePending itemState = iota // in a bucket, not yet in the LRU list
	stateActive                   // in the LRU list, counted against the cache size
	stateDead                     // terminal: deleted, overwritten, cleared or pruned
)

func (s itemState) String() string {
	switch s {
	case statePending:
		return "pending"
	case stateActive:
		return "active"
	case stateDead:
		return "dead"
	}
	return "unknown"
}

// promotionsDead is the sentinel value of Item.promotions marking an item as
// dead. It is only ever read and written by the worker goroutine (Clear runs
// on the worker while holding every bucket lock).
const promotionsDead = -2

// lifecycleState inspects the item's current state. It is derived from the
// two fields the worker already maintains (promotions and inList), so the
// state machine adds no per-item memory.
func (i *Item[T]) lifecycleState() itemState {
	if i.promotions == promotionsDead {
		return stateDead
	}
	if i.inList {
		return stateActive
	}
	return statePending
}

// promoteItem applies a promotion that was queued via the promotables
// channel. It reports whether the item transitioned from pending to active
// (i.e. whether the cache size grew and a GC check is needed).
//
// A dead item is left untouched: this is the guard that keeps an expired or
// deleted item from being resurrected by a promotion that was queued before
// the deletion was processed.
func promoteItem[T any](item *Item[T], list *List[T], size *int64, getsPerPromote int32) bool {
	switch item.lifecycleState() {
	case stateDead:
		return false
	case stateActive:
		if item.shouldPromote(getsPerPromote) {
			list.MoveToFront(item)
			item.promotions = 0
		}
		return false
	}
	*size += item.size
	list.Insert(item)
	return true
}

// retireItem transitions an item into the dead state, removing it from the
// LRU list and shrinking the cache size when the item was active. The
// OnDelete callback is fired here, exactly once per item, and only for items
// that were active (matching the cache's long-standing semantics). It returns
// true when this call owned the transition into the dead state.
func retireItem[T any](item *Item[T], list *List[T], size *int64, onDelete func(*Item[T])) bool {
	if item.lifecycleState() == stateDead {
		return false
	}
	active := item.inList
	if active {
		*size -= item.size
		list.Remove(item)
	}
	item.promotions = promotionsDead
	if active && onDelete != nil {
		onDelete(item)
	}
	return true
}

// killItem transitions a pending item into the dead state without a callback.
// It is used for items that never reached the LRU list (Clear, and deletes
// that were processed before the item's first promotion).
func killItem[T any](item *Item[T]) {
	item.promotions = promotionsDead
}
