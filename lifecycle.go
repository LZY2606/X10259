package ccache

import (
	"sync/atomic"
)

// lifecycleHooks connect the single ownership state machine to the concrete
// bucket implementation of a plain Cache or a LayeredCache. The state machine
// itself is identical for both; only how an item is locked, cleared from and
// evicted out of its bucket differs.
type lifecycleHooks[T any] struct {
	// lockAll/unlockAll freeze every bucket, as required by a synchronous
	// Clear.
	lockAll   func()
	unlockAll func()
	// clearBuckets resets every bucket lookup. The implementation marks every
	// item still held by a bucket as stateDeleted.
	clearBuckets func()
	// evictFromBucket removes a promoted item from the bucket that owns it.
	// It is called only from the worker-side gc path.
	evictFromBucket func(item *Item[T])
}

// lifecycle owns the single state machine that moves an item between a bucket
// lookup, the promotables/deletables queues and the recency list.
//
// Both Cache and LayeredCache embed a lifecycle and route all asynchronous
// state changes through it, so there is exactly one implementation of
// promotion, deletion, expiry rejection, LRU eviction and clear/stop draining.
type lifecycle[T any] struct {
	cfg *Configuration[T]
	control
	list            *List[T]
	size            int64
	pruneTargetSize int64
	maxSize         int64
	promotables     chan *Item[T]
	deletables      chan *Item[T]
	hooks           lifecycleHooks[T]
}

func newLifecycle[T any](cfg *Configuration[T], hooks lifecycleHooks[T]) *lifecycle[T] {
	return &lifecycle[T]{
		cfg:             cfg,
		control:         newControl(),
		list:            NewList[T](),
		maxSize:         cfg.maxSize,
		pruneTargetSize: cfg.maxSize - cfg.maxSize*int64(cfg.percentToPrune)/100,
		promotables:     make(chan *Item[T], cfg.promoteBuffer),
		deletables:      make(chan *Item[T], cfg.deleteBuffer),
		hooks:           hooks,
	}
}

// enqueuePromote schedules a freshly inserted (or re-inserted) item for
// insertion into the recency list. Set uses the (unbuffered-fail style)
// non-blocking queue for read-driven promotions, but an insertion must always
// be accounted for, so this send blocks if the queue is full.
func (l *lifecycle[T]) enqueuePromote(item *Item[T]) {
	l.promotables <- item
}

// isFresh reports whether the item is non-expired against the cache clock.
// All public freshness decisions route through this one check, so a controllable
// test clock drives exactly the production expiry logic.
func (l *lifecycle[T]) isFresh(item *Item[T]) bool {
	return !item.expiredAt(l.cfg.now().UnixNano())
}

func (l *lifecycle[T]) halted(fn func()) {
	l.hooks.lockAll()
	defer l.hooks.unlockAll()
	fn()
}

// run is the single worker loop. Promotions, deletions and control commands are
// all serialized here, which is what makes the per-item state transitions atomic
// with respect to one another.
func (l *lifecycle[T]) run() {
	dropped := 0

	promoteItem := func(item *Item[T]) {
		if l.applyPromote(item) && l.size > l.maxSize {
			dropped += l.gc()
		}
	}

	for {
		// Test barrier: a no-op in production. When closed in tests the worker
		// parks without consuming anything and the test drives applyPromote /
		// applyDelete directly.
		l.cfg.workerBarrier.wait()

		select {
		case item := <-l.promotables:
			promoteItem(item)
		case item := <-l.deletables:
			l.applyDelete(item)
		case command := <-l.control:
			switch msg := command.(type) {
			case controlStop:
				goto drain
			case controlGetDropped:
				msg.res <- dropped
				dropped = 0
			case controlSetMaxSize:
				newMaxSize := msg.size
				l.maxSize = newMaxSize
				l.pruneTargetSize = newMaxSize - newMaxSize*int64(l.cfg.percentToPrune)/100
				if l.size > l.maxSize {
					dropped += l.gc()
				}
				msg.done <- struct{}{}
			case controlClear:
				l.halted(func() {
					drainChannel(l.promotables)
					drainChannel(l.deletables)
					l.hooks.clearBuckets()
					l.size = 0
					l.list = NewList[T]()
				})
				msg.done <- struct{}{}
			case controlGetSize:
				msg.res <- l.size
			case controlGC:
				dropped += l.gc()
				msg.done <- struct{}{}
			case controlSyncUpdates:
				flushPending(l.promotables, promoteItem, l.deletables, l.applyDelete)
				msg.done <- struct{}{}
			}
		}
	}

drain:
	// On stop, pending deletions are finalized but pending promotions are
	// abandoned: the cache is no longer usable and those items were never
	// accounted for in size or in the recency list.
	for {
		select {
		case item := <-l.deletables:
			l.applyDelete(item)
		default:
			return
		}
	}
}

func drainChannel[T any](ch <-chan *Item[T]) {
	for len(ch) > 0 {
		<-ch
	}
}

// flushPending processes as many items as can immediately be received from the
// two queues. Promotions are drained before deletions, matching the worker's
// select ordering. If another goroutine enqueues after this returns, that is
// fine: SyncUpdates only guarantees processing of values sent by the calling
// goroutine.
func flushPending[T any](
	promotables <-chan *Item[T],
	promoteFn func(*Item[T]),
	deletables <-chan *Item[T],
	deleteFn func(*Item[T]),
) {
drainPromotes:
	for {
		select {
		case item := <-promotables:
			promoteFn(item)
		default:
			break drainPromotes
		}
	}
drainDeletes:
	for {
		select {
		case item := <-deletables:
			deleteFn(item)
		default:
			break drainDeletes
		}
	}
}

// applyDelete is the queued -> deleted / promoted -> deleted transition.
// It returns true only when an item actually left the recency list. The
// onDelete callback fires at most once, only on the promoted -> deleted edge,
// and without any bucket lock held (deletions are dequeued on the worker).
func (l *lifecycle[T]) applyDelete(item *Item[T]) bool {
	switch item.ownerState() {
	case stateDeleted:
		// A deletion event that arrives after a prior deletion/eviction/clear
		// must not double-decrement size or double-fire the callback.
		return false
	case stateQueued:
		// The item was removed from its bucket before it ever reached the
		// recency list: discard it without touching size or the list.
		item.transitionTo(stateDeleted)
		return false
	}

	l.size -= item.size
	if l.cfg.onDelete != nil {
		l.cfg.onDelete(item)
	}
	l.list.Remove(item)
	item.transitionTo(stateDeleted)
	return true
}

// applyPromote is the queued -> promoted transition, plus promotion accounting
// for already-promoted items. It returns true when a new item entered the
// recency list (the caller checks the size limit and may trigger gc).
func (l *lifecycle[T]) applyPromote(item *Item[T]) bool {
	if item.ownerState() == stateDeleted {
		// Stale promotion for an item already deleted, overwritten, evicted or
		// cleared: it must never re-enter the list or be counted again.
		return false
	}

	if item.ownerState() == statePromoted {
		if item.shouldPromote(l.cfg.getsPerPromote) {
			l.list.MoveToFront(item)
			item.promotions = 0
		}
		return false
	}

	// An item that expired while sitting in the promotables queue can never be
	// resurrected by a late promotion: it is finalized instead of entering the
	// recency list.
	if item.expiredAt(l.cfg.now().UnixNano()) {
		item.transitionTo(stateDeleted)
		return false
	}

	item.transitionTo(statePromoted)
	l.size += item.size
	l.list.Insert(item)
	return true
}

// gc evicts the least recently used items until size is back down to the prune
// target. Each eviction is a promoted -> deleted state transition performed
// without re-touching queues, so evicted items are immune to stale promotions.
func (l *lifecycle[T]) gc() int {
	dropped := 0
	item := l.list.Tail

	prunedSize := int64(0)
	sizeToPrune := l.size - l.pruneTargetSize

	for prunedSize < sizeToPrune {
		if item == nil {
			return dropped
		}
		prev := item.prev
		if !l.cfg.tracking || atomic.LoadInt32(&item.refCount) == 0 {
			l.hooks.evictFromBucket(item)
			itemSize := item.size
			l.size -= itemSize
			prunedSize += itemSize

			l.list.Remove(item)
			if l.cfg.onDelete != nil {
				l.cfg.onDelete(item)
			}
			dropped++
			item.transitionTo(stateDeleted)
		}
		item = prev
	}
	return dropped
}
