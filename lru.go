package ccache

import "sync/atomic"

// lru owns the mutable, worker-only state shared by Cache and LayeredCache:
// the recency list, the tracked size and the dropped counter. All item state
// transitions (queued -> promoted -> deleted) are methods on lru and are
// invoked from exactly one place: the background worker loop in worker.go.
//
// Keeping the transitions here is what makes the lifecycle "a single
// inspectable state machine": neither the bucket methods nor any public API
// mutates list membership, size accounting or item.state directly.
type lru[T any] struct {
	list            *List[T]
	size            int64
	pruneTargetSize int64
	dropped         int
}

func newLRU[T any](maxSize, percentToPrune int64) lru[T] {
	return lru[T]{
		list:            NewList[T](),
		pruneTargetSize: pruneTarget(maxSize, percentToPrune),
	}
}

func pruneTarget(maxSize, percentToPrune int64) int64 {
	return maxSize - maxSize*percentToPrune/100
}

func (l *lru[T]) setMaxSize(maxSize, percentToPrune int64) {
	l.pruneTargetSize = pruneTarget(maxSize, percentToPrune)
}

// retire is the unique queued/promoted -> deleted transition. It is called
// for explicit deletes and for items displaced by an overwrite; eviction goes
// through evict below because it additionally unlinks the bucket occupant.
//
// The onDelete callback is invoked at most once per item, only when the item
// actually left the promoted set, and without any bucket or list lock held, so
// a callback that re-enters the cache cannot deadlock against itself.
func (l *lru[T]) retire(item *Item[T], onDelete func(*Item[T])) {
	switch item.state {
	case stateDeleted:
		return
	case statePromoted:
		l.size -= item.size
		l.list.Remove(item)
		if onDelete != nil {
			onDelete(item)
		}
	}
	item.markDeleted()
}

// promote is the unique queued -> promoted transition and the promoted-state
// reorder path. current reports whether the item is still the occupant of its
// bucket slot: an item displaced by an overwrite must never enter the list, and
// an item that expired while waiting in the queue must not be resurrected into
// the promoted set.
//
// It returns true when the promotion grew the promoted set (in which case the
// caller checks the size bound and may gc).
func (l *lru[T]) promote(item *Item[T], now int64, getsPerPromote int32, current bool) bool {
	switch item.state {
	case stateDeleted:
		return false
	case statePromoted:
		if item.shouldPromote(getsPerPromote) {
			l.list.MoveToFront(item)
			item.promotions = 0
		}
		return false
	}
	// stateQueued: a stale (overwritten) or expired queue entry is discarded
	// without entering the list and without an onDelete callback: it never
	// became part of the promoted set, and the item that displaced it owns the
	// eventual delete callback.
	if !current || item.expiredAt(now) {
		item.markDeleted()
		return false
	}
	l.size += item.size
	l.list.Insert(item)
	item.state = statePromoted
	return true
}

// evict unconditionally removes a promoted item from the set during gc. The
// caller is responsible for having verified the item is still the bucket
// occupant (identity guard) and is not tracked. Returns the removed size.
func (l *lru[T]) evict(item *Item[T], onDelete func(*Item[T])) int64 {
	if item.state != statePromoted {
		return 0
	}
	l.size -= item.size
	l.list.Remove(item)
	if onDelete != nil {
		onDelete(item)
	}
	item.markDeleted()
	return item.size
}

func (l *lru[T]) reset() {
	l.list = NewList[T]()
	atomic.StoreInt64(&l.size, 0)
}

// snapshotSize reads the worker-owned size; it is also reachable through the
// GetSize control command from other goroutines, so use an atomic load/store.
func (l *lru[T]) snapshotSize() int64 {
	return atomic.LoadInt64(&l.size)
}
