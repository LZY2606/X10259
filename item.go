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

type Item[T any] struct {
	key        string
	group      string
	promotions int32
	refCount   int32
	lifecycle  int32
	expires    int64
	size       int64
	value      T
	clock      clock
	next       *Item[T]
	prev       *Item[T]
	inList     bool
}

func newItem[T any](key string, value T, expires int64, track bool) *Item[T] {
	return newItemWithClock[T](key, value, expires, track, systemClock{})
}

// newItemWithClock is the single item constructor used by every production
// path; newItem is its fixed-clock form (also used by existing unit tests).
// No test-only constructor exists: a cache configured with a fake clock
// creates its items through the exact same code as a production cache.
func newItemWithClock[T any](key string, value T, expires int64, track bool, cl clock) *Item[T] {
	size := int64(1)

	// https://github.com/golang/go/issues/49206
	if sized, ok := (interface{})(value).(Sized); ok {
		size = sized.Size()
	}

	item := &Item[T]{
		key:       key,
		value:     value,
		size:      size,
		expires:   expires,
		clock:     cl,
		lifecycle: int32(stateQueued),
	}
	if track {
		item.refCount = 1
	}
	return item
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

func (i *Item[T]) timeSource() clock {
	if i.clock == nil {
		return systemClock{}
	}
	return i.clock
}

// fresh reports whether the item is live at the cache's current time. It is
// the one freshness predicate shared by Get promotion gating and the worker's
// defense-in-depth re-check, so a queued promotion can never bring an expired
// item back into the LRU list.
func (i *Item[T]) fresh() bool {
	return atomic.LoadInt64(&i.expires) >= i.timeSource().nowNano()
}

func (i *Item[T]) Expired() bool {
	return !i.fresh()
}

func (i *Item[T]) TTL() time.Duration {
	expires := atomic.LoadInt64(&i.expires)
	return time.Nanosecond * time.Duration(expires-i.timeSource().nowNano())
}

func (i *Item[T]) Expires() time.Time {
	expires := atomic.LoadInt64(&i.expires)
	return time.Unix(0, expires)
}

func (i *Item[T]) Extend(duration time.Duration) {
	atomic.StoreInt64(&i.expires, i.timeSource().now().Add(duration).UnixNano())
}

// String returns a string representation of the Item. This includes the default string
// representation of its Value(), as implemented by fmt.Sprintf with "%v", but the exact
// format of the string should not be relied on; it is provided only for debugging
// purposes, and because otherwise including an Item in a call to fmt.Printf or
// fmt.Sprintf expression could cause fields of an Item to be read in a non-thread-safe
// way.
func (i *Item[T]) String() string {
	group := i.group
	if group == "" {
		return fmt.Sprintf("Item(%s:%v)", i.key, i.value)
	}
	return fmt.Sprintf("Item(%s:%s:%v)", group, i.key, i.value)
}
