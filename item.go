package ccache

import (
	"fmt"
	"sync/atomic"
	"time"
)

type Sized interface {
	Size() int64
}

type TrackedItem[T any] interface {
	Value() T
	Release()
	Expired() bool
	TTL() time.Duration
	Expires() time.Time
	Extend(duration time.Duration)
}

// Item lifecycle state. This is the single source of truth for where an item
// lives with respect to the three structures that together make up the cache:
// the bucket lookup, the promotables queue and the LRU list (with the
// deletables queue carrying transition messages). It replaces the previous
// "promotions == -2" sentinel, which conflated promotion counters with
// ownership.
const (
	// itemStatePending: the item exists in a bucket lookup but has never been
	// admitted into the LRU list. It may be waiting in the promotables queue.
	itemStatePending int32 = 0

	// itemStateListed: the item is owned by the LRU list and contributes to the
	// cache size. Promotion messages only reorder it within the list.
	itemStateListed int32 = 1

	// itemStateRemoved: the item has been deleted, evicted or swept by Clear.
	// It must never be admitted into (or resurrected within) the LRU list, and
	// it must contribute nothing to the cache size.
	itemStateRemoved int32 = 2
)

type Item[T any] struct {
	key        string
	group      string
	promotions int32
	refCount   int32
	state      int32
	expires    int64
	size       int64
	value      T
	now        clock
	next       *Item[T]
	prev       *Item[T]
	inList     bool
}

func newItem[T any](key string, value T, expires int64, track bool, now clock) *Item[T] {
	size := int64(1)
	if now == nil {
		now = realClock
	}

	// https://github.com/golang/go/issues/49206
	if sized, ok := (interface{})(value).(Sized); ok {
		size = sized.Size()
	}

	item := &Item[T]{
		key:        key,
		value:      value,
		promotions: 0,
		state:      itemStatePending,
		size:       size,
		expires:    expires,
		now:        now,
	}
	if track {
		item.refCount = 1
	}
	return item
}

// state loads the current lifecycle state.
func (i *Item[T]) itemState() int32 {
	return atomic.LoadInt32(&i.state)
}

// isRemoved reports whether the item has already left the cache for good.
func (i *Item[T]) isRemoved() bool {
	return i.itemState() == itemStateRemoved
}

// admitToList is the pending -> listed transition. It fails (returns false) if
// the item was removed before the worker processed its promotion, in which
// case a late promotion must not resurrect it.
func (i *Item[T]) admitToList() bool {
	return atomic.CompareAndSwapInt32(&i.state, itemStatePending, itemStateListed)
}

// evictFromList is the listed -> removed transition, used by the worker when it
// evicts an LRU-listed item.
func (i *Item[T]) evictFromList() bool {
	return atomic.CompareAndSwapInt32(&i.state, itemStateListed, itemStateRemoved)
}

// retire is the any-state -> removed transition, used by Delete and Clear.
// The returned bool is true exactly for the first retirement, so callers can
// rely on it to fire delete ownership (callbacks, size accounting) at most
// once.
func (i *Item[T]) retire() bool {
	for {
		state := i.itemState()
		if state == itemStateRemoved {
			return false
		}
		if atomic.CompareAndSwapInt32(&i.state, state, itemStateRemoved) {
			return true
		}
	}
}

func (i *Item[T]) shouldPromote(getsPerPromote int32) bool {
	i.promotions += 1
	return i.promotions == getsPerPromote
}

func (i *Item[T]) Key() string {
	return i.key
}

func (i *Item[T]) Value() T {
	return i.value
}

func (i *Item[T]) track() {
	atomic.AddInt32(&i.refCount, 1)
}

func (i *Item[T]) Release() {
	atomic.AddInt32(&i.refCount, -1)
}

func (i *Item[T]) Expired() bool {
	expires := atomic.LoadInt64(&i.expires)
	return expires < i.now.Now().UnixNano()
}

func (i *Item[T]) TTL() time.Duration {
	expires := atomic.LoadInt64(&i.expires)
	return time.Nanosecond * time.Duration(expires-i.now.Now().UnixNano())
}

func (i *Item[T]) Expires() time.Time {
	expires := atomic.LoadInt64(&i.expires)
	return time.Unix(0, expires)
}

func (i *Item[T]) Extend(duration time.Duration) {
	atomic.StoreInt64(&i.expires, i.now.Now().Add(duration).UnixNano())
}

// String returns a string representation of the Item. This includes the default string
// representation of its Value(), as implemented by fmt.Sprintf with "%v", but the exact
// format of the string should not be relied on; it is provided only for debugging
// purposes, and because otherwise including an Item in a call to fmt.Printf or
// fmt.Sprintf expression could cause fields of the Item to be read in a non-thread-safe
// way.
func (i *Item[T]) String() string {
	group := i.group
	if group == "" {
		return fmt.Sprintf("Item(%s:%v)", i.key, i.value)
	}

	return fmt.Sprintf("Item(%s:%s:%v)", group, i.key, i.value)
}
