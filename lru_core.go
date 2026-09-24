// lruCore holds the single state machine shared by Cache and LayeredCache.
//
// An item has three places where ownership can reside:
//   - the bucket lookup (storage),
//   - the LRU recency list (eviction ownership),
//   - the promotables / deletables queues (pending ownership transfers).
//
// Before this type existed, the ownership rules were implicit: a magic
// promotions counter value (-2), the inList flag and ad-hoc ordering between
// the two queues were read independently from several places, and the result
// depended on which queue the worker drained first. All transitions now live in
// this file, go through the item's explicit lifecycle state and are safe to
// drive in either queue order.
package ccache

import (
	"sync"
	"sync/atomic"
	"time"
)

// lruHooks are the storage-specific operations the state machine needs. Cache
// and LayeredCache provide different implementations over the same bucket
// primitives; the state-machine logic itself is implemented exactly once.
type lruHooks[T any] struct {
	// evict removes item from its lookup only while the lookup still maps its
	// key to item. A false result means the key was overwritten after the item
	// was selected for eviction; the state machine leaves both the old item and
	// the replacement untouched and lets the pending delete message retire the
	// old item through the normal transition.
	evict func(item *Item[T]) bool
	// lockAll / unlockAll halt concurrent bucket mutation. Clear needs the
	// queues drained while lookups can neither insert nor remove items.
	lockAll   func()
	unlockAll func()
	// clearLookup drops every lookup entry. All bucket locks are held.
	clearLookup func()
}

type lruCore[T any] struct {
	config          *Configuration[T]
	control         control
	hooks           lruHooks[T]
	list            *List[T]
	size            int64
	pruneTargetSize int64
	deletables      chan *Item[T]
	promotables     chan *Item[T]

	workerDone sync.WaitGroup
}

func newLRUCore[T any](config *Configuration[T], cc control, hooks lruHooks[T]) *lruCore[T] {
	core := &lruCore[T]{
		config:          config,
		control:         cc,
		hooks:           hooks,
		list:            NewList[T](),
		deletables:      make(chan *Item[T], config.deleteBuffer),
		promotables:     make(chan *Item[T], config.promoteBuffer),
		pruneTargetSize: config.maxSize - config.maxSize*int64(config.percentToPrune)/100,
	}
	if !config.manual {
		core.workerDone.Add(1)
		go core.run()
	}
	return core
}

// now is the clock used by every state-machine expiration decision. Production
// configurations are wired to time.Now in New/Layered; tests substitute a fake
// clock and still execute this exact production code.
func (c *lruCore[T]) now() time.Time {
	if c.config.nowFn != nil {
		return c.config.nowFn()
	}
	return time.Now()
}

func (c *lruCore[T]) expired(item *Item[T]) bool {
	return atomic.LoadInt64(&item.expires) < c.now().UnixNano()
}

// offerPromote enqueues a Get-driven promotion hint without blocking. Expired
// items are not even queued: no late message can later promote them.
func (c *lruCore[T]) offerPromote(item *Item[T]) {
	if c.expired(item) {
		return
	}
	select {
	case c.promotables <- item:
	default:
	}
}

func (c *lruCore[T]) enqueuePromote(item *Item[T]) {
	c.promotables <- item
}

func (c *lruCore[T]) enqueueDelete(item *Item[T]) {
	c.deletables <- item
}

func (c *lruCore[T]) waitStopped() {
	c.workerDone.Wait()
}

// applyPromote is the single promotion transition. It returns true exactly
// when the item just entered the recency list, which is the only moment the
// size budget has to be rechecked.
func (c *lruCore[T]) applyPromote(item *Item[T]) bool {
	switch item.state {
	case lifecycleTerminal:
		// A delete, overwrite, eviction, clear or expiration-as-late-promotion
		// retired this item first. It must never come back.
		return false
	case lifecyclePromoted:
		if item.shouldPromote(c.config.getsPerPromote) {
			c.list.MoveToFront(item)
			item.promotions = 0
		}
		return false
	default:
		if c.expired(item) {
			// The item expired while it waited in the promotables queue.
			// Retire it without entering the recency list: a late promotion
			// must neither resurrect it nor let it displace live LRU entries.
			// It lingers in its lookup until overwrite/delete, exactly like an
			// expired item that never received a Get.
			item.retire()
			return false
		}
		item.state = lifecyclePromoted
		c.size += item.size
		c.list.Insert(item)
		return true
	}
}

// applyDelete is the single deletion transition. onDelete ownership and size
// accounting follow the item's current owner, so the callback fires at most
// once regardless of queue interleaving and no bucket or list lock is held
// while it runs.
func (c *lruCore[T]) applyDelete(item *Item[T]) {
	switch item.state {
	case lifecycleTerminal:
		return
	case lifecyclePromoted:
		c.size -= item.size
		c.list.Remove(item)
		item.retire()
		if c.config.onDelete != nil {
			c.config.onDelete(item)
		}
	default:
		// The item never reached the recency list (overwritten or removed
		// while still queued), so the LRU never owned its callback.
		item.retire()
	}
}

// applyGC evicts unreferenced tail items until the configured prune target is
// reached.
func (c *lruCore[T]) applyGC() int {
	dropped := 0
	item := c.list.Tail
	prunedSize := int64(0)
	sizeToPrune := c.size - c.pruneTargetSize

	for prunedSize < sizeToPrune {
		if item == nil {
			return dropped
		}
		prev := item.prev
		if !c.config.tracking || atomic.LoadInt32(&item.refCount) == 0 {
			if c.hooks.evict(item) {
				c.size -= item.size
				prunedSize += item.size
				c.list.Remove(item)
				item.retire()
				dropped++
				if c.config.onDelete != nil {
					c.config.onDelete(item)
				}
			}
		}
		item = prev
	}
	return dropped
}

// applyClear atomically discards queues, lookups and recency list. Buckets are
// halted for the whole transition, so a message drained here can never be
// re-queued against the new empty structures, and every reachable item is
// retired so a message already in flight when the locks release is a no-op.
func (c *lruCore[T]) applyClear() {
	c.hooks.lockAll()
	retired := make(map[*Item[T]]struct{})
	for {
		select {
		case item := <-c.promotables:
			retired[item] = struct{}{}
		case item := <-c.deletables:
			retired[item] = struct{}{}
		default:
			goto cleared
		}
	}
cleared:
	for item := c.list.Head; item != nil; item = item.next {
		retired[item] = struct{}{}
	}
	c.hooks.clearLookup()
	for item := range retired {
		item.retire()
	}
	c.size = 0
	c.list = NewList[T]()
	c.hooks.unlockAll()
}

func (c *lruCore[T]) run() {
	defer c.workerDone.Done()
	dropped := 0

	promoteItem := func(item *Item[T]) {
		if c.applyPromote(item) && c.size > c.config.maxSize {
			dropped += c.applyGC()
		}
	}

	for {
		select {
		case item := <-c.promotables:
			promoteItem(item)
		case item := <-c.deletables:
			c.applyDelete(item)
		case control := <-c.control:
			switch msg := control.(type) {
			case controlStop:
				goto drain
			case controlGetDropped:
				msg.res <- dropped
				dropped = 0
			case controlSetMaxSize:
				newMaxSize := msg.size
				c.config.maxSize = newMaxSize
				c.pruneTargetSize = newMaxSize - newMaxSize*int64(c.config.percentToPrune)/100
				if c.size > c.config.maxSize {
					dropped += c.applyGC()
				}
				msg.done <- struct{}{}
			case controlClear:
				c.applyClear()
				msg.done <- struct{}{}
			case controlGetSize:
				msg.res <- c.size
			case controlGC:
				dropped += c.applyGC()
				msg.done <- struct{}{}
			case controlSyncUpdates:
				doAllPendingPromotesAndDeletes(c.promotables, promoteItem, c.deletables, c.applyDelete)
				msg.done <- struct{}{}
			}
		}
	}

drain:
	for {
		select {
		case item := <-c.deletables:
			c.applyDelete(item)
		default:
			return
		}
	}
}

// doAllPendingPromotesAndDeletes drains as many items as the channels can
// provide immediately and feeds each one through the same state-machine
// transitions the worker uses. SyncUpdates relies on it; if another goroutine
// sends after it returns, that send is simply processed later.
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
