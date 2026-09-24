package ccache

import "sync/atomic"

// itemState is the lifecycle state of an Item with respect to the cache's
// promotion, expiration and deletion machinery. Every Item moves through a
// single, checkable state machine:
//
//	statePending  -- created and visible in a bucket, but not yet in the
//	                 eviction list; its size is not accounted and no onDelete
//	                 callback is owed for it yet.
//	statePromoted -- in the eviction list; its size is accounted and exactly
//	                 one onDelete callback is owed when it retires.
//	stateDeleted  -- terminal. The item has been removed (Delete, overwrite,
//	                 GC, Clear or Stop-drain). Late promotions are no-ops and
//	                 can never resurrect it, expired or not.
//
// All transitions are driven by the cache's single worker goroutine (Clear
// included, which runs as a control command inside the worker), so the
// goroutine that wins the transition to stateDeleted is the sole owner of the
// onDelete callback: the callback fires at most once per item and never while
// holding a bucket lock, which keeps cache-reentrant callbacks safe.
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

// state returns the current lifecycle state of the item.
func (i *Item[T]) state() itemState {
	return itemState(atomic.LoadInt32(&i.lifecycle))
}

// transition atomically moves the item from one of the expected states to the
// target state, returning true if this caller performed the transition. The
// caller that wins a transition owns the associated side effects (list
// membership, size accounting, onDelete callback).
func (i *Item[T]) transition(target itemState, from ...itemState) bool {
	for _, expected := range from {
		if atomic.CompareAndSwapInt32(&i.lifecycle, int32(expected), int32(target)) {
			return true
		}
	}
	return false
}

// retire moves the item to the terminal stateDeleted state from any
// non-terminal state. It reports whether the caller performed the transition
// (and thus owns the retirement side effects).
func (i *Item[T]) retire() bool {
	return i.transition(stateDeleted, statePending, statePromoted)
}
