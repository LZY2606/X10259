package ccache

import "sync/atomic"

// lifecycleState is the single, inspectable state of an Item.
//
// An item exists in exactly one of three locations the cache keeps state in:
//
//   - the keyed lookup of one of the cache's shards ("the bucket"),
//   - the promotables queue (an item that exists in a bucket but has not been
//     admitted to the recency list yet), and
//   - the delete queue / the recency list owned by the cache's single worker
//     goroutine.
//
// Lifecycle transitions are the only place that moves an item between those
// locations, decides whether a promotion is still valid (in particular,
// whether the item has expired or has been overwritten), and decides who owns
// the OnDelete callback. All transitions run on the worker goroutine, so the
// state is also a synchronization barrier between a caller's bucket mutation
// and the worker's list/size bookkeeping.
type lifecycleState uint32

const (
	// statePending: the item is keyed in a bucket but is not owned by the
	// recency list. Its space is not counted in the cache size and its
	// removal must not invoke the OnDelete callback.
	statePending lifecycleState = iota

	// statePromoted: the item is keyed in a bucket and owned by the recency
	// list. Its space is counted in the cache size; leaving this state is the
	// one and only transition that owns the OnDelete callback.
	statePromoted

	// stateDeleted: terminal state. The item no longer owns any cache
	// structure and must never be admitted to the recency list, regardless of
	// which stale promotion is still queued behind it.
	stateDeleted
)

// lifecycleEvent selects one of the state transitions. The machine is
// deliberately explicit: there is no "setter" that simply assigns a state.
type lifecycleEvent uint8

const (
	// eventPromote admits a pending item to the recency list.
	eventPromote lifecycleEvent = iota
	// eventDelete removes the item (either from the pending or the promoted
	// state) and takes ownership of the OnDelete callback when the item owned
	// recency-list space.
	eventDelete
	// eventEvict is a memory-pressure eviction performed by the worker. It is
	// only valid from the promoted state and always owns the callback.
	eventEvict
	// eventDiscard abandons an item that never owned recency-list space
	// (Clear, or an item whose bucket slot has been taken by an overwrite).
	// It never invokes the OnDelete callback.
	eventDiscard
)

// transition runs one edge of the lifecycle state machine. The bool result is
// whether the edge was taken; on an invalid edge the item keeps its state and
// the caller is responsible for doing nothing (the ownership of any structure
// or callback has already been resolved elsewhere). When the edge is taken,
// onDeleteOwner is true exactly when the item was removed while owning
// recency-list space; that is the sole condition under which the OnDelete
// callback may run, at most once for the item's lifetime.
//
// The full transition table:
//
//	Pending  + Promote  -> Promoted,  callback: no
//	Promoted + Promote  -> Promoted,  callback: no  (recency reorder only)
//	Pending  + Delete   -> Deleted,   callback: no
//	Promoted + Delete   -> Deleted,   callback: yes
//	Pending  + Evict    -> invalid    (pending items cannot be evicted)
//	Promoted + Evict    -> Deleted,   callback: yes
//	Pending  + Discard  -> Deleted,   callback: no
//	Promoted + Discard  -> Deleted,   callback: no  (Clear never callbacks)
//	Deleted  + anything -> invalid
func (s lifecycleState) transition(event lifecycleEvent) (next lifecycleState, taken bool, onDeleteOwner bool) {
	switch s {
	case statePending:
		switch event {
		case eventPromote:
			return statePromoted, true, false
		case eventDelete, eventDiscard:
			return stateDeleted, true, false
		}
	case statePromoted:
		switch event {
		case eventPromote:
			return statePromoted, true, false
		case eventDelete, eventEvict:
			return stateDeleted, true, true
		case eventDiscard:
			return stateDeleted, true, false
		}
	}
	return s, false, false
}

// lifecycleState loads the item's current state. The state is only mutated on
// the worker goroutine, but reads from tests (and, defensively, from other
// goroutines) must not race.
func (i *Item[T]) lifecycleState() lifecycleState {
	return lifecycleState(atomic.LoadUint32(&i.state))
}

// transit applies an event to the item using compare-and-swap. It mirrors
// transition() but the CAS is what guarantees that, under a concurrent
// Delete/overwrite/clear, the edge that observed the old state is the one that
// owns its consequences (size delta, list removal and the single OnDelete
// callback).
func (i *Item[T]) transit(from lifecycleState, event lifecycleEvent) (lifecycleState, bool, bool) {
	next, valid, onDeleteOwner := from.transition(event)
	if !valid {
		return from, false, false
	}
	if !atomic.CompareAndSwapUint32(&i.state, uint32(from), uint32(next)) {
		// The item changed state between the worker's read and the CAS. The
		// winner of that race owns the transition; reload so the caller can
		// report the observed state in its failure evidence.
		return i.lifecycleState(), false, false
	}
	return next, true, onDeleteOwner
}

// markDeleted forces an item to the terminal state without going through an
// event. It is used by Clear on items that are about to be orphaned with their
// whole bucket: a later stale promotion must be rejected and a later queued
// delete must not double-handle the item.
func (i *Item[T]) markDeleted() {
	atomic.StoreUint32(&i.state, uint32(stateDeleted))
}
