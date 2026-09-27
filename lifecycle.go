package ccache

// lifecycle owns the single item state machine shared by Cache and
// LayeredCache. Both caches have the same shape:
//
//	bucket lookup  (ownership of the key -> item mapping)
//	promotables    (FIFO messages asking the worker to admit/reorder an item)
//	deletables     (FIFO messages asking the worker to release list ownership)
//	LRU list       (ownership of size accounting, ordering and onDelete)
//
// An Item moves through exactly three states, see itemState* in item.go:
//
//	pending --admit--> listed --evict--> removed
//	   |                   ^
//	   +------retire-------+--------retire-----> removed
//
// All transitions are driven by the single worker goroutine (retire() itself
// is an atomic CAS and may also be observed from Delete/Clear), which is why
// the methods here touch no bucket locks and why onDelete callbacks are invoked
// while no cache lock is held.
type lifecycle[T any] struct {
	list            *List[T]
	size            *int64
	getsPerPromote  *int32
	onDelete        func(item *Item[T])
	removeFromIndex func(key string)
}

// applyPromote processes one promotables message.
//
// pending + still the key's current item: admitted to the list, size grows.
// listed:                    only the recency position may change, and only
//                            after getsPerPromote non-promoting gets.
// removed / superseded:      the message is dropped; a late promotion can never
//                            resurrect a deleted, evicted or cleared item.
//
// Returns true when the admission itself caused the cache to be over capacity
// and the caller should run the eviction sweep.
func (l *lifecycle[T]) applyPromote(item *Item[T]) bool {
	if item.itemState() == itemStateRemoved {
		return false
	}
	if item.inList {
		if item.shouldPromote(*l.getsPerPromote) {
			l.list.MoveToFront(item)
			item.promotions = 0
		}
		return false
	}
	if !item.admitToList() {
		// Delete/Clear/eviction won the race while the promotion was queued.
		return false
	}
	*l.size += item.size
	l.list.Insert(item)
	return true
}

// applyDelete processes one deletables message. List ownership - size, list
// membership and the onDelete callback - is released at most once per item,
// because evictFromList()/retire() are atomic state transitions.
func (l *lifecycle[T]) applyDelete(item *Item[T]) {
	switch item.itemState() {
	case itemStateListed:
		if !item.evictFromList() {
			return
		}
		l.releaseListed(item)
	case itemStatePending:
		item.retire()
	}
}

// evict removes an over-capacity or otherwise doomed item that is currently in
// the list. It also removes the item from its bucket lookup. It must only be
// called for listed items. Returns false if the item had already lost list
// ownership (a queued Delete/Clear won the race).
func (l *lifecycle[T]) evict(item *Item[T]) bool {
	if !item.evictFromList() {
		return false
	}
	l.removeFromIndex(item.key)
	l.releaseListed(item)
	return true
}

// releaseListed performs the once-only work that belongs to an item leaving the
// LRU list: size accounting, list unlinking and the ownership callback.
func (l *lifecycle[T]) releaseListed(item *Item[T]) {
	*l.size -= item.size
	l.list.Remove(item)
	if l.onDelete != nil {
		// No bucket lock or global cache lock is held here, so a callback is
		// free to re-enter any public cache method.
		l.onDelete(item)
	}
}
