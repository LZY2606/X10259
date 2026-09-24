package ccache

import "sync/atomic"

// engine owns the asynchronous half of the cache: the promotables and
// deletables queues, the worker goroutine, the LRU list, the size counter and
// the onDelete callback. Cache and LayeredCache embed one engine and differ
// only in how their owning buckets are keyed; the item lifecycle state machine
// therefore exists exactly once and is shared by every public entry point.
type engine[T any] struct {
	*Configuration[T]
	control
	list            *List[T]
	size            int64
	pruneTargetSize int64
	promotables     chan *Item[T]
	deletables      chan *Item[T]
	flights         *flightGroup

	// lockAll locks every owning bucket; clearAll releases everything currently
	// owned; evict removes one key from its owning bucket. These hooks are the
	// only place the engine knows which concrete cache it serves.
	lockAll  func()
	unlockAll func()
	clearAll func()
	evict    func(item *Item[T])
}

func newEngine[T any](config *Configuration[T], hooks lifecycleHooks[T]) *engine[T] {
	e := &engine[T]{
		Configuration:   config,
		control:         newControl(),
		list:            NewList[T](),
		promotables:     make(chan *Item[T], config.promoteBuffer),
		deletables:      make(chan *Item[T], config.deleteBuffer),
		flights:         newFlightGroup(),
		pruneTargetSize: config.maxSize - config.maxSize*int64(config.percentToPrune)/100,
		lockAll:         hooks.lockAll,
		unlockAll:       hooks.unlockAll,
		clearAll:        hooks.clearAll,
		evict:           hooks.evict,
	}
	go e.run()
	return e
}

// lifecycleHooks connect the shared engine to a concrete cache's buckets.
type lifecycleHooks[T any] struct {
	lockAll   func()
	unlockAll func()
	clearAll  func()
	evict     func(item *Item[T])
}

func (e *engine[T]) queuePromote(item *Item[T]) {
	e.promotables <- item
}

// tryQueuePromote offers one promotion request without blocking: a full
// promotion buffer simply skips LRU bookkeeping for that read.
func (e *engine[T]) tryQueuePromote(item *Item[T]) {
	select {
	case e.promotables <- item:
	default:
	}
}

func (e *engine[T]) queueDelete(item *Item[T]) {
	e.deletables <- item
}

func (e *engine[T]) halted(fn func()) {
	e.lockAll()
	defer e.unlockAll()
	fn()
}

func (e *engine[T]) run() {
	dropped := 0
	cc := e.control

	promoteItem := func(item *Item[T]) {
		if e.doPromote(item) && e.size > e.maxSize {
			dropped += e.gc()
		}
	}

	for {
		select {
		case item := <-e.promotables:
			promoteItem(item)
		case item := <-e.deletables:
			e.doDelete(item)
		case control := <-cc:
			switch msg := control.(type) {
			case controlStop:
				goto drain
			case controlGetDropped:
				msg.res <- dropped
				dropped = 0
			case controlSetMaxSize:
				newMaxSize := msg.size
				e.maxSize = newMaxSize
				e.pruneTargetSize = newMaxSize - newMaxSize*int64(e.percentToPrune)/100
				if e.size > e.maxSize {
					dropped += e.gc()
				}
				msg.done <- struct{}{}
			case controlClear:
				e.halted(func() {
					promotables := e.promotables
					for len(promotables) > 0 {
						<-promotables
					}
					deletables := e.deletables
					for len(deletables) > 0 {
						<-deletables
					}
					e.clearAll()
					e.size = 0
					e.list = NewList[T]()
				})
				msg.done <- struct{}{}
			case controlGetSize:
				msg.res <- e.size
			case controlGC:
				dropped += e.gc()
				msg.done <- struct{}{}
			case controlSyncUpdates:
				doAllPendingPromotesAndDeletes(e.promotables, promoteItem, e.deletables, e.doDelete)
				msg.done <- struct{}{}
			}
		}
	}

drain:
	for {
		select {
		case item := <-e.deletables:
			e.doDelete(item)
		default:
			return
		}
	}
}

// This method is used to implement SyncUpdates. It simply receives and processes as many
// items as it can receive from the promotables and deletables channels immediately without
// blocking. If some other goroutine sends an item on either channel after this method has
// finished receiving, that's OK, because SyncUpdates only guarantees processing of values
// that were already sent by the same goroutine.
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

// doDelete is the Deleted terminal transition of the item state machine.
// The single kill() winner owns removal from the LRU list, the size delta and
// the onDelete callback; every late duplicate delete is a no-op.
func (e *engine[T]) doDelete(item *Item[T]) {
	if !item.kill() {
		return
	}
	if item.inList {
		e.size -= item.size
		e.list.Remove(item)
	}
	if e.onDelete != nil {
		// The callback receives released ownership. No bucket lock, list lock
		// or flight-group lock is held here, so re-entering the cache from the
		// callback cannot deadlock against a global lock.
		e.onDelete(item)
	}
}

// doPromote is the Promoted transition of the item state machine. It returns
// true only when the item newly gained LRU ownership and therefore changed the
// cache size, which is the sole condition under which pressure GC may run.
func (e *engine[T]) doPromote(item *Item[T]) bool {
	if item.isDead() {
		// already released (Delete / overwrite / Clear / eviction)
		return false
	}
	if item.inList {
		if item.shouldPromote(e.getsPerPromote) {
			e.list.MoveToFront(item)
			item.promotions = 0
		}
		return false
	}
	// Defense in depth against the promote/delete and promote/expiry races:
	// an item that expired while queued must not be resurrected into the LRU
	// list (where it would count toward size and resist eviction).
	if !item.fresh() {
		return false
	}
	if !item.markPromoted() {
		return false
	}
	e.size += item.size
	e.list.Insert(item)
	return true
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
		if !e.tracking || atomic.LoadInt32(&item.refCount) == 0 {
			if item.kill() {
				e.evict(item)
				e.size -= item.size
				prunedSize += item.size
				e.list.Remove(item)
				if e.onDelete != nil {
					e.onDelete(item)
				}
				dropped++
			}
		}
		item = prev
	}
	return dropped
}

