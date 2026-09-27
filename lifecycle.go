package ccache

import (
	"sync/atomic"
	"time"
)

// lifecycle owns the single item state machine shared by Cache and
// LayeredCache. Both cache shapes keep items in the same three places:
//
//   - a bucket lookup (owned directly by the cache's public methods)
//   - a promotion queue (promotables/refreshables)
//   - a deletion queue (deletables)
//
// and the worker goroutine is the unique owner of every transition between
// those places and the recency list. Centralizing the transitions here means
// the promotion, expiry and onDelete ownership rules exist exactly once and
// can be checked against one model instead of two near-identical copies.
type lifecycle[T any] struct {
	list            *List[T]
	size            int64
	maxSize         int64
	pruneTargetSize int64
	percentToPrune  int
	getsPerPromote  int32
	tracking        bool
	onDelete        func(item *Item[T])
	// evict removes the item's key from its bucket lookup while the cache is
	// under memory pressure. It carries no allocation: normal deletes keep
	// going through bucket.remove via the public Delete entry point.
	evict func(item *Item[T])

	promotables chan *Item[T]
	refreshables chan *Item[T]
	deletables  chan *Item[T]
	control     control

	// halt serializes Clear against concurrent bucket mutations. It is
	// provided by the embedding cache because only it knows its buckets.
	halt   func()
	unhalt func()
	// clearBuckets drops every lookup and marks the items found there dead.
	clearBuckets func()
}

func newLifecycle[T any](config *Configuration[T], control control, halt, unhalt func(), clearBuckets, evict func(*Item[T])) *lifecycle[T] {
	return &lifecycle[T]{
		list:            NewList[T](),
		control:         control,
		maxSize:         config.maxSize,
		pruneTargetSize: config.maxSize - config.maxSize*int64(config.percentToPrune)/100,
		percentToPrune:  config.percentToPrune,
		getsPerPromote:  config.getsPerPromote,
		tracking:        config.tracking,
		onDelete:        config.onDelete,
		promotables:     make(chan *Item[T], config.promoteBuffer),
		refreshables:    make(chan *Item[T], config.promoteBuffer),
		deletables:      make(chan *Item[T], config.deleteBuffer),
		halt:            halt,
		unhalt:          unhalt,
		clearBuckets:    clearBuckets,
		evict:           evict,
	}
}

// run is the worker loop. All lifecycle state is mutated from here.
func (l *lifecycle[T]) run() {
	dropped := 0

	promote := func(item *Item[T]) {
		if l.insert(item) && l.size > l.maxSize {
			dropped += l.gc()
		}
	}
	touch := func(item *Item[T]) {
		if l.access(item) && l.size > l.maxSize {
			dropped += l.gc()
		}
	}

	for {
		select {
		case item := <-l.promotables:
			promote(item)
		case item := <-l.refreshables:
			touch(item)
		case item := <-l.deletables:
			l.remove(item)
		case msg := <-l.control:
			switch m := msg.(type) {
			case controlStop:
				l.drain()
				return
			case controlGetDropped:
				m.res <- dropped
				dropped = 0
			case controlSetMaxSize:
				newMaxSize := m.size
				l.maxSize = newMaxSize
				l.pruneTargetSize = newMaxSize - newMaxSize*int64(l.percentToPrune)/100
				if l.size > l.maxSize {
					dropped += l.gc()
				}
				m.done <- struct{}{}
			case controlClear:
				l.reset()
				m.done <- struct{}{}
			case controlGetSize:
				m.res <- l.size
			case controlGC:
				dropped += l.gc()
				m.done <- struct{}{}
			case controlSyncUpdates:
				l.drainQueues(promote, touch)
				m.done <- struct{}{}
			}
		}
	}
}

// drain processes queued deletes during shutdown. Pending promotions are
// intentionally abandoned: a request that was still buffered never attached
// to the list and therefore has no size or callback ownership to release.
func (l *lifecycle[T]) drain() {
	for {
		select {
		case item := <-l.deletables:
			l.remove(item)
		default:
			return
		}
	}
}

// drainQueues processes everything that was buffered before the worker
// observed a controlSyncUpdates barrier, without blocking on new sends.
func (l *lifecycle[T]) drainQueues(promote, touch func(*Item[T])) {
drainPromotables:
	for {
		select {
		case item := <-l.promotables:
			promote(item)
		default:
			break drainPromotables
		}
	}
drainRefreshables:
	for {
		select {
		case item := <-l.refreshables:
			touch(item)
		default:
			break drainRefreshables
		}
	}
drainDeletables:
	for {
		select {
		case item := <-l.deletables:
			l.remove(item)
		default:
			break drainDeletables
		}
	}
}

// insert handles a Set-path promotion. It returns true if the item newly
// attached to the list and may have pushed the cache over maxSize.
func (l *lifecycle[T]) insert(item *Item[T]) bool {
	switch item.state {
	case stateNew:
		l.size += item.size
		item.state = stateActive
		l.list.Insert(item)
		return true
	case stateActive:
		l.list.MoveToFront(item)
		return false
	default: // stateDead: a delete, overwrite or clear won the race
		return false
	}
}

// access handles a Get-path promotion (LRU recency refresh). A late access
// must never resurrect an item that has since been deleted or overwritten,
// and an expired item which is merely queued is not attached.
func (l *lifecycle[T]) access(item *Item[T]) bool {
	switch item.state {
	case stateNew:
		if item.Expired() {
			return false
		}
		l.size += item.size
		item.state = stateActive
		l.list.Insert(item)
		return true
	case stateActive:
		if item.shouldPromote(l.getsPerPromote) {
			l.list.MoveToFront(item)
			item.promotions = 0
		}
		return false
	default: // stateDead
		return false
	}
}

// remove handles a Delete-path message. The onDelete callback is owned
// exactly here and exactly once per item: new items were never observable on
// the list, dead items were already released.
func (l *lifecycle[T]) remove(item *Item[T]) {
	switch item.state {
	case stateNew:
		item.state = stateDead
	case stateActive:
		l.size -= item.size
		if l.onDelete != nil {
			l.onDelete(item)
		}
		l.list.Remove(item)
		item.state = stateDead
	}
}

// reset clears all cache state. Everything that is discarded while
// detaching the list is marked dead first, so a delete or promotion that is
// already buffered cannot half-update the size or fire onDelete for a cache
// that has already been cleared.
func (l *lifecycle[T]) reset() {
	l.halt()
	defer l.unhalt()

	for len(l.promotables) > 0 {
		<-l.promotables
	}
	for len(l.refreshables) > 0 {
		<-l.refreshables
	}
	for len(l.deletables) > 0 {
		<-l.deletables
	}

	for item := l.list.Head; item != nil; item = item.next {
		item.state = stateDead
	}
	l.clearBuckets()
	l.size = 0
	l.list = NewList[T]()
}

// gc evicts from the tail of the recency list until the cache is back at its
// prune target. Every eviction is checked against the item's current bucket
// identity: an eviction message for a stale item must not delete the newer
// item that replaced it (the overwrite/eviction race).
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
		if !l.tracking || atomic.LoadInt32(&item.refCount) == 0 {
			if item.state != stateDead {
				l.evict(item)
				itemSize := item.size
				l.size -= itemSize
				prunedSize += itemSize

				l.list.Remove(item)
				if l.onDelete != nil {
					l.onDelete(item)
				}
				dropped += 1
				item.state = stateDead
			}
		}
		item = prev
	}
	return dropped
}

var _ = time.Now
