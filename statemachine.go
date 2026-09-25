package ccache

// itemState identifies where an Item sits in the cache's ownership lifecycle.
// The lifecycle is a single, small state machine shared by Cache and
// LayeredCache:
//
//	statePending --promote--> stateListed   (enters the LRU list, size grows)
//	stateListed  --promote--> stateListed   (position refresh, no size change)
//	statePending --delete---> stateDeleted  (never listed: no callback, no size change)
//	stateListed  --delete---> stateDeleted  (exactly one onDelete callback)
//	stateDeleted --any------> stateDeleted  (terminal: a late promotion must
//	                                         never resurrect the item)
//
// All transitions are driven by the cache's single worker goroutine, or under
// a full cache halt (Clear), so they need no atomic operations and can be
// inspected deterministically from tests behind SyncUpdates barriers.
type itemState int8

const (
	// statePending: reachable from its bucket, but not yet inserted into the
	// LRU list. Does not count against the cache size yet.
	statePending itemState = iota
	// stateListed: in the LRU list and counted against the cache size.
	stateListed
	// stateDeleted: terminal. The cache no longer owns the item; the delete
	// callback has fired (or will never fire) and late promotions are dropped.
	stateDeleted
)

func (s itemState) String() string {
	switch s {
	case statePending:
		return "pending"
	case stateListed:
		return "listed"
	case stateDeleted:
		return "deleted"
	}
	return "unknown"
}

// itemOwner applies the item lifecycle state machine on behalf of a cache.
// It owns the LRU list and the size counter so that Cache and LayeredCache
// share one implementation of promotion, expiration-unaware deletion and
// callback ownership, instead of keeping two copies in sync.
//
// It is only ever driven by the cache's worker goroutine (or under a full
// halt), and it never holds a bucket lock while invoking onDelete, so a
// callback may safely re-enter the cache's data-path methods (Get/Set/Delete/
// ...) without deadlocking on a lock the worker holds.
type itemOwner[T any] struct {
	list           *List[T]
	size           int64
	getsPerPromote int32
	onDelete       func(item *Item[T])
}

func newItemOwner[T any](getsPerPromote int32, onDelete func(item *Item[T])) *itemOwner[T] {
	return &itemOwner[T]{
		list:           NewList[T](),
		getsPerPromote: getsPerPromote,
		onDelete:       onDelete,
	}
}

// promote handles an item arriving on the promotables channel. It returns true
// only when the item transitioned pending -> listed, i.e. when the cache size
// grew and the caller should consider garbage collection.
func (o *itemOwner[T]) promote(item *Item[T]) bool {
	switch item.state {
	case stateDeleted:
		// A deleted item must never be resurrected by a late promotion.
		return false
	case stateListed:
		if item.shouldPromote(o.getsPerPromote) {
			o.list.MoveToFront(item)
			item.promotions = 0
		}
		return false
	default: // statePending
		item.state = stateListed
		o.size += item.size
		o.list.Insert(item)
		return true
	}
}

// delete handles an item arriving on the deletables channel. The onDelete
// callback fires exactly once, only on the listed -> deleted transition, and
// runs without any bucket or list lock held by the worker.
func (o *itemOwner[T]) delete(item *Item[T]) {
	if item.state != stateListed {
		item.state = stateDeleted
		return
	}
	o.size -= item.size
	item.state = stateDeleted
	if o.onDelete != nil {
		o.onDelete(item)
	}
	o.list.Remove(item)
}

// evict transitions a listed item to deleted during garbage collection. The
// caller is responsible for removing the item from its bucket. Returns the
// size that was released.
func (o *itemOwner[T]) evict(item *Item[T]) int64 {
	if item.state != stateListed {
		return 0
	}
	released := item.size
	o.size -= released
	item.state = stateDeleted
	if o.onDelete != nil {
		o.onDelete(item)
	}
	o.list.Remove(item)
	return released
}

// reset drops the whole LRU list and size accounting. The caller must have
// already moved every bucket item to stateDeleted (see bucket.clear).
func (o *itemOwner[T]) reset() {
	o.list = NewList[T]()
	o.size = 0
}
