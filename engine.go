package ccache

import "sync/atomic"

// engine owns the single item ownership state machine shared by Cache and
// LayeredCache. An item's lifecycle is described solely by its state field:
//
//	itemPending -> itemActive -> itemRemoved
//
// All transitions are executed by the single worker goroutine, serialized
// through the promotables/deletables queues and the control channel:
//
//   - promote (promotables): a pending item becomes active and is inserted in
//     the recency list; a late promotion of an expired item is rejected so an
//     expired item can never be revived or moved to the LRU head.
//   - remove (deletables): an active item is detached from the recency list,
//     its size released and the onDelete callback fires exactly once; a
//     pending item is simply marked removed.
//   - evict (gc): same terminal transition as remove, but the item is also
//     deleted from its bucket by identity, so an eviction can never remove a
//     successor value that replaced the stale list node.
//   - clear: all owned items transition to removed at once, under the bucket
//     locks, and queued transitions are drained so a stale queued message can
//     never touch the fresh generation.
//
// The callback is always invoked after bucket/list locks have been released,
// which lets callbacks re-enter the cache through its public API.

// bucketSet is the storage view the state machine drives. The two production
// implementations are flatBuckets and layeredBuckets.
type bucketSet[T any] interface {
	// evictItem removes item's bucket slot when the slot still belongs to the
	// item (identity comparison).
	evictItem(item *Item[T])
	// halt acquires every storage lock; unhalt releases them.
	halt()
	unhalt()
	// resetLocked clears every bucket and marks its items removed. The caller
	// owns the halt lock.
	resetLocked()
	// scan walks every stored item for invariant checks, on the same storage
	// the production path mutates.
	scan(fn func(item *Item[T]))
}

type engine[T any] struct {
	control
	config          *Configuration[T]
	buckets         bucketSet[T]
	list            *List[T]
	size            int64
	maxSize         int64
	pruneTargetSize int64
	deletables      chan *Item[T]
	promotables     chan *Item[T]
}

func newEngine[T any](config *Configuration[T], buckets bucketSet[T]) *engine[T] {
	e := &engine[T]{
		config:          config,
		buckets:         buckets,
		control:         newControl(),
		list:            NewList[T](),
		deletables:      make(chan *Item[T], config.deleteBuffer),
		promotables:     make(chan *Item[T], config.promoteBuffer),
		maxSize:         config.maxSize,
		pruneTargetSize: config.maxSize - config.maxSize*int64(config.percentToPrune)/100,
	}
	go e.worker()
	return e
}

// queuePromote enqueues a promotion request from a public entry point.
func (e *engine[T]) queuePromote(item *Item[T]) {
	e.promotables <- item
}

// tryQueuePromote enqueues a promotion request without blocking: when the
// promotion buffer is full the promotion is skipped, exactly as Get has
// always done (skipping a promotion is safe: the item stays pending/active).
func (e *engine[T]) tryQueuePromote(item *Item[T]) {
	select {
	case e.promotables <- item:
	default:
	}
}

// queueRemove enqueues an explicit delete from a public entry point.
func (e *engine[T]) queueRemove(item *Item[T]) {
	e.deletables <- item
}

// worker is the sole mutator of recency-list ownership.
func (e *engine[T]) worker() {
	dropped := 0
	cc := e.control

	promoteItem := func(item *Item[T]) {
		if e.promote(item) && e.size > e.maxSize {
			dropped += e.gc()
		}
	}

	for {
		select {
		case item := <-e.promotables:
			promoteItem(item)
		case item := <-e.deletables:
			e.remove(item)
		case msg := <-cc:
			switch m := msg.(type) {
			case controlStop:
				goto drain
			case controlGetDropped:
				m.res <- dropped
				dropped = 0
			case controlSetMaxSize:
				newMaxSize := m.size
				e.maxSize = newMaxSize
				e.pruneTargetSize = newMaxSize - newMaxSize*int64(e.config.percentToPrune)/100
				if e.size > e.maxSize {
					dropped += e.gc()
				}
				m.done <- struct{}{}
			case controlClear:
				e.clear()
				m.done <- struct{}{}
			case controlGetSize:
				m.res <- e.size
			case controlGC:
				dropped += e.gc()
				m.done <- struct{}{}
			case controlSyncUpdates:
				doAllPendingPromotesAndDeletes(e.promotables, promoteItem, e.deletables, e.remove)
				m.done <- struct{}{}
			}
		}
	}

drain:
	for {
		select {
		case item := <-e.deletables:
			e.remove(item)
		default:
			return
		}
	}
}

// clear makes the clear transition atomic with respect to every public
// operation: storage is locked while the queues are drained and every owned
// item is marked removed, so no transition queued before the clear can land on
// the new (empty) generation.
func (e *engine[T]) clear() {
	e.buckets.halt()
	drainQueued(e.promotables)
	drainQueued(e.deletables)
	e.buckets.resetLocked()
	e.size = 0
	e.list = NewList[T]()
	e.buckets.unhalt()
}

func drainQueued[T any](items <-chan *Item[T]) {
	for {
		select {
		case item := <-items:
			item.setState(itemRemoved)
		default:
			return
		}
	}
}

// promote applies a promotion event. It returns true when the item newly
// became active (and thus the caller may have to run eviction).
func (e *engine[T]) promote(item *Item[T]) bool {
	// A queued promotion that races with an explicit delete, an overwrite or a
	// clear is fenced off by the terminal state.
	if item.lifecycleState() == itemRemoved {
		return false
	}
	// A late promotion must never resurrect an expired item: it neither enters
	// the recency list nor is moved to the LRU head.
	if item.Expired() {
		return false
	}
	if item.lifecycleState() == itemActive {
		if item.shouldPromote(e.config.getsPerPromote) {
			e.list.MoveToFront(item)
			item.promotions = 0
		}
		return false
	}
	item.setState(itemActive)
	e.size += item.size
	e.list.Insert(item)
	return true
}

// remove applies an explicit delete event.
func (e *engine[T]) remove(item *Item[T]) {
	state := item.lifecycleState()
	if state == itemRemoved {
		return
	}
	item.setState(itemRemoved)
	if state != itemActive {
		// pending -> removed: the item never reached the recency list, was
		// never counted towards the size and no deletion callback fires.
		return
	}
	e.detachAndNotify(item)
}

// evict applies an eviction transition: the same terminal ownership release as
// remove, plus an identity deletion from the bucket.
func (e *engine[T]) evict(item *Item[T]) int {
	if item.lifecycleState() != itemActive {
		return 0
	}
	e.buckets.evictItem(item)
	item.setState(itemRemoved)
	e.detachAndNotify(item)
	return 1
}

// detachAndNotify releases list/size ownership and fires the callback exactly
// once. Storage and list locks are not held here.
func (e *engine[T]) detachAndNotify(item *Item[T]) {
	e.size -= item.size
	e.list.Remove(item)
	if e.config.onDelete != nil {
		e.config.onDelete(item)
	}
}

func (e *engine[T]) gc() int {
	dropped := 0
	item := e.list.Tail

	prunedSize := int64(0)
	sizeToPrune := e.size - e.pruneTargetSize

	for prunedSize < sizeToPrune {
		if item == nil {
			return dropped
		}
		prev := item.prev
		if !e.config.tracking || atomic.LoadInt32(&item.refCount) == 0 {
			dropped += e.evict(item)
			prunedSize += item.size
		}
		item = prev
	}
	return dropped
}

// doAllPendingPromotesAndDeletes processes everything already buffered on the
// two queues without blocking. It is used to implement SyncUpdates. If another
// goroutine sends an item after this function stops draining, that is fine:
// SyncUpdates only guarantees processing of values sent by the calling
// goroutine.
func doAllPendingPromotesAndDeletes[T any](
	promotables <-chan *Item[T],
	promoteFn func(*Item[T]),
	deletables <-chan *Item[T],
	deleteFn func(*Item[T]),
) {
doAllPromotes:
	for {
		select {
		case item := <-promotables:
			promoteFn(item)
		default:
			break doAllPromotes
		}
	}
doAllDeletes:
	for {
		select {
		case item := <-deletables:
			deleteFn(item)
		default:
			break doAllDeletes
		}
	}
}
