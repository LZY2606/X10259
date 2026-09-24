package ccache

import "sync/atomic"

// Lifecycle of an item across the three places that jointly make up cache
// ownership:
//
//   - the owning bucket's lookup map (visible Get/Set/Replace/Delete state),
//   - the promotables queue (LRU insertion / re-ordering requests), and
//   - the deletables queue (release / onDelete ownership requests).
//
// Item ownership is a single state machine whose transitions are driven from
// the cache's worker goroutine. The states are expressed as an atomic int32 so
// that producers racing on the public API (Get/Set/Delete/Clear) only observe
// legal states and can never re-own an item that has already been released.
type itemState int32

const (
	// stateQueued: the item is owned by a bucket, but its LRU ownership has
	// not yet been taken. It may be present in the promotables and/or
	// deletables queues any number of times.
	stateQueued itemState = 0
	// statePromoted: the item is owned by a bucket and the worker-owned LRU
	// list, and contributes to the cache size.
	statePromoted itemState = 1
	// stateDead: ownership has been released at least once. No later queue
	// message may resurrect the item into the LRU list, the size counter or
	// the onDelete callback.
	stateDead itemState = 2
)

func (i *Item[T]) state() itemState {
	return itemState(atomic.LoadInt32(&i.lifecycle))
}

func (i *Item[T]) isDead() bool {
	return i.state() == stateDead
}

// markPromoted is the only legal Queued -> Promoted transition. It returns
// false when the item was released before a queued promotion was processed:
// a late promotion must never resurrect a deleted (or expired) item.
func (i *Item[T]) markPromoted() bool {
	return atomic.CompareAndSwapInt32(&i.lifecycle, int32(stateQueued), int32(statePromoted))
}

// kill permanently moves the item to stateDead from any live state. It returns
// true for exactly one concurrent caller, which both grants ownership of the
// onDelete callback and guarantees the callback fires at most once.
func (i *Item[T]) kill() bool {
	for {
		current := atomic.LoadInt32(&i.lifecycle)
		if current == int32(stateDead) {
			return false
		}
		if atomic.CompareAndSwapInt32(&i.lifecycle, current, int32(stateDead)) {
			return true
		}
	}
}
