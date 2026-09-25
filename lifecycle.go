package ccache

// The item lifecycle is the single state machine that governs how an Item
// moves between a bucket (the public lookup), the promotables queue, the
// recency list and the deletables queue, and who owns the onDelete callback
// at each point. Both Cache and LayeredCache drive their items through the
// transitions in this file and nowhere else, so the promotion, expiration
// and callback-ownership rules exist exactly once.
//
// States:
//
//	stateNew       - created by a public entry point (Set/Fetch/...), sitting
//	                 in a bucket, promotion possibly still queued.
//	statePromoted  - linked into the recency list. The item owns exactly one
//	                 future onDelete callback, transferred to the worker on
//	                 the Promoted -> Deleted transition.
//	stateDeleted   - terminal. Reached via Delete/overwrite/GC. The callback
//	                 fires at most once, exactly when a Promoted item is
//	                 deleted, and is invoked by the worker goroutine while
//	                 holding no bucket or cache lock (re-entrant cache calls
//	                 from inside the callback cannot deadlock on a global lock).
//	stateAbandoned - terminal. Reached when an item is dropped without ever
//	                 owning a callback: Clear, a stop-drain, or a late
//	                 promotion that arrives after the item already expired.
//
// Invariants enforced here:
//   - a terminal item (Deleted/Abandoned) is never resurrected by a late
//     promotion, no matter how the promotables and deletables queues race;
//   - an expired item is never admitted into the recency list;
//   - deletion is idempotent, so a duplicated or drained delete is a no-op;
//   - the onDelete callback ownership moves Promoted -> Deleted exactly once.
//
// All transitions are executed by the cache's single worker goroutine (or
// under a full halt), so the state field itself needs no synchronization.
type itemState int8

const (
	stateNew itemState = iota
	statePromoted
	stateDeleted
	stateAbandoned
)

func (s itemState) String() string {
	switch s {
	case stateNew:
		return "new"
	case statePromoted:
		return "promoted"
	case stateDeleted:
		return "deleted"
	case stateAbandoned:
		return "abandoned"
	}
	return "unknown"
}

// lifecycleEffect tells the caller (the cache worker) which externally
// visible effect a transition produced, so size accounting and the onDelete
// callback stay in the cache while the state rules stay here.
type lifecycleEffect int8

const (
	effectNone lifecycleEffect = iota
	// The item transitioned New -> Promoted: the caller accounts its size
	// and may need to GC.
	effectEnteredList
	// The item transitioned Promoted -> Deleted: the caller reclaims its
	// size and fires the onDelete callback exactly once.
	effectFireOnDelete
)

func (e lifecycleEffect) String() string {
	switch e {
	case effectNone:
		return "none"
	case effectEnteredList:
		return "entered-list"
	case effectFireOnDelete:
		return "fire-on-delete"
	}
	return "unknown"
}

// promoteItem applies the promotion half of the lifecycle. A queued promotion
// can arrive late - after the item was deleted, cleared, or expired - and
// none of those may resurrect or admit the item.
func promoteItem[T any](list *List[T], item *Item[T], getsPerPromote int32) lifecycleEffect {
	switch item.state {
	case stateDeleted, stateAbandoned:
		// A late promotion must never resurrect a terminal item.
		return effectNone
	case statePromoted:
		if item.shouldPromote(getsPerPromote) {
			list.MoveToFront(item)
			item.promotions = 0
		}
		return effectNone
	default: // stateNew
		if item.Expired() {
			// An item that expired while its promotion was still queued is
			// never admitted into the recency list.
			item.state = stateAbandoned
			return effectNone
		}
		list.Insert(item)
		item.state = statePromoted
		return effectEnteredList
	}
}

// deleteItem applies the deletion half of the lifecycle. Only a Promoted
// item owns a callback, so only that transition reports effectFireOnDelete.
// Deletion is idempotent: repeated or drained deletes of a terminal item
// are no-ops.
func deleteItem[T any](list *List[T], item *Item[T]) lifecycleEffect {
	switch item.state {
	case statePromoted:
		list.Remove(item)
		item.state = stateDeleted
		return effectFireOnDelete
	case stateNew:
		// Deleted before its queued promotion was processed; no callback
		// was ever owned, so none fires.
		item.state = stateDeleted
		return effectNone
	default:
		return effectNone
	}
}

// abandonItem moves a live item to a terminal state without firing the
// onDelete callback. Used by Clear, which drops every item regardless of
// state, and by the stop/clear drains for items whose queued updates are
// discarded.
func abandonItem[T any](item *Item[T]) {
	switch item.state {
	case stateNew, statePromoted:
		item.state = stateAbandoned
	}
}
