package ccache

import (
	"strings"
	"sync"
	"time"
)

type bucket[T any] struct {
	sync.RWMutex
	lookup map[string]*Item[T]
	clock  func() time.Time
}

func (b *bucket[T]) now() time.Time {
	if b.clock != nil {
		return b.clock()
	}
	return time.Now()
}

func (b *bucket[T]) newItem(key string, value T, duration time.Duration, track bool) *Item[T] {
	item := newItem[T](key, value, b.now().Add(duration).UnixNano(), track)
	item.clock = b.clock
	return item
}

func (b *bucket[T]) itemCount() int {
	b.RLock()
	defer b.RUnlock()
	return len(b.lookup)
}

func (b *bucket[T]) forEachFunc(matches func(key string, item *Item[T]) bool) bool {
	lookup := b.lookup
	b.RLock()
	defer b.RUnlock()
	for key, item := range lookup {
		if !matches(key, item) {
			return false
		}
	}
	return true
}

func (b *bucket[T]) get(key string) *Item[T] {
	b.RLock()
	defer b.RUnlock()
	return b.lookup[key]
}

func (b *bucket[T]) setnx(key string, value T, duration time.Duration, track bool) *Item[T] {
	b.RLock()
	item := b.lookup[key]
	b.RUnlock()
	if item != nil {
		return item
	}

	newItem := b.newItem(key, value, duration, track)

	b.Lock()
	defer b.Unlock()

	// check again under write lock
	item = b.lookup[key]
	if item != nil {
		return item
	}

	b.lookup[key] = newItem
	return newItem
}

func (b *bucket[T]) setnx2(key string, f func() T, duration time.Duration, track bool) (*Item[T], bool) {
	b.RLock()
	item := b.lookup[key]
	b.RUnlock()
	if item != nil {
		return item, true
	}

	b.Lock()
	defer b.Unlock()

	// check again under write lock
	item = b.lookup[key]
	if item != nil {
		return item, true
	}

	newItem := b.newItem(key, f(), duration, track)
	b.lookup[key] = newItem
	return newItem, false
}

func (b *bucket[T]) set(key string, value T, duration time.Duration, track bool) (*Item[T], *Item[T]) {
	item := b.newItem(key, value, duration, track)
	b.Lock()
	existing := b.lookup[key]
	b.lookup[key] = item
	b.Unlock()
	return item, existing
}

func (b *bucket[T]) remove(key string) *Item[T] {
	b.Lock()
	item := b.lookup[key]
	delete(b.lookup, key)
	b.Unlock()
	return item
}

// removeItem deletes the mapping for item's key only when the value currently
// stored is item itself. This is used by the eviction path: an item that was
// replaced between the time it entered the recency list and the time it gets
// pruned must never remove its successor from the bucket.
// Returns true when the mapping was removed.
func (b *bucket[T]) removeItem(item *Item[T]) bool {
	b.Lock()
	defer b.Unlock()
	if b.lookup[item.key] != item {
		return false
	}
	delete(b.lookup, item.key)
	return true
}

// scan calls fn for every entry while holding the read lock. It exists for the
// ownership invariant check and walks the very same lookup that the production
// state machine mutates; it never fabricates state.
func (b *bucket[T]) scan(fn func(*Item[T])) {
	b.RLock()
	defer b.RUnlock()
	for _, item := range b.lookup {
		fn(item)
	}
}

func (b *bucket[T]) delete(key string) {
	b.Lock()
	delete(b.lookup, key)
	b.Unlock()
}

// This is an expensive operation, so we do what we can to optimize it and limit
// the impact on concurrent operations. Specifically, we:
// 1 - Do an initial iteration to collect matches. This allows us to do the
//     "expensive" prefix check (on all values) using only a read-lock
// 2 - Do a second iteration, under write lock, for the matched results to do
//     the actual deletion

// Also, this is the only place where the Bucket is aware of cache detail: the
// deletables channel. Passing it here lets us avoid iterating over matched items
// again in the cache. Further, we pass item to deletables BEFORE actually removing
// the item from the map. I'm pretty sure this is 100% fine, but it is unique.
// (We do this so that the write to the channel is under the read lock and not the
// write lock)
func (b *bucket[T]) deleteFunc(matches func(key string, item *Item[T]) bool, deletables chan *Item[T]) int {
	lookup := b.lookup
	items := make([]*Item[T], 0)

	b.RLock()
	for key, item := range lookup {
		if matches(key, item) {
			deletables <- item
			items = append(items, item)
		}
	}
	b.RUnlock()

	if len(items) == 0 {
		// avoid the write lock if we can
		return 0
	}

	b.Lock()
	for _, item := range items {
		delete(lookup, item.key)
	}
	b.Unlock()
	return len(items)
}

func (b *bucket[T]) deletePrefix(prefix string, deletables chan *Item[T]) int {
	return b.deleteFunc(func(key string, item *Item[T]) bool {
		return strings.HasPrefix(key, prefix)
	}, deletables)
}

// resetLocked replaces the lookup with an empty one and marks every previously
// owned item as removed. The caller must hold the bucket's write lock.
func (b *bucket[T]) resetLocked() {
	for _, item := range b.lookup {
		item.setState(itemRemoved)
	}
	b.lookup = make(map[string]*Item[T])
}
