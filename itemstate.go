package ccache

import "sync/atomic"

// Lifecycle states of an item.
//
// An item is always in exactly one of these states, independent of the
// structures that reference it (a bucket lookup, the promotables queue, the
// deletables queue or the LRU list). Every move between those structures must
// go through the state transitions below, which makes the ordering of queued
// promotions and deletions unambiguous: a queue message may only touch the LRU
// list if it owns the state it expects, so a late promotion can never revive
// an item whose deletion (or eviction) already won, and an item can leave the
// promoted state, firing the onDelete callback, exactly once.
const (
	// stateQueued: reachable from a bucket lookup but not present in the LRU
	// list. Its promotion may be queued, or may have been dropped because the
	// promotion buffer was full.
	stateQueued int32 = iota
	// statePromoted: owns a node in the LRU list and counts toward size.
	statePromoted
	// stateDeleted: terminal. The item must stay out of the LRU list.
	stateDeleted
)

// claimPromote moves a Queued item to Promoted. It fails if deletion (or a
// prior promotion) won first; the caller then must not touch the list/size.
func (i *Item[T]) claimPromote() bool {
	return atomic.CompareAndSwapInt32(&i.state, stateQueued, statePromoted)
}

// claimPromotedRemoval moves a Promoted item to Deleted. Both the Delete path
// and the GC eviction path remove list nodes, and a Delete message may race an
// in-progress eviction; the CAS guarantees that exactly one of them detaches
// the node, accounts for the size and fires the onDelete callback.
func (i *Item[T]) claimPromotedRemoval() bool {
	return atomic.CompareAndSwapInt32(&i.state, statePromoted, stateDeleted)
}

// claimQueuedRemoval retires an item that never reached the LRU list. Nothing
// has to be detached and no size was ever counted for it.
func (i *Item[T]) claimQueuedRemoval() bool {
	return atomic.CompareAndSwapInt32(&i.state, stateQueued, stateDeleted)
}

func (i *Item[T]) isDeleted() bool {
	return atomic.LoadInt32(&i.state) == stateDeleted
}

func (i *Item[T]) stateName() string {
	switch atomic.LoadInt32(&i.state) {
	case stateQueued:
		return "queued"
	case statePromoted:
		return "promoted"
	default:
		return "deleted"
	}
}

// markDeleted retires every item in a structure that is being wholesale
// discarded (Clear). Afterwards no queue message may re-insert the item.
func (i *Item[T]) markDeleted() {
	atomic.StoreInt32(&i.state, stateDeleted)
}
