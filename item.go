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

// Lifecycle states. An item's ownership of its slot in a bucket, its place on
// the promotables/deletables queues and its membership in the recency list is
// described by exactly this one state machine:
//
//	itemPending: the item is in a bucket, but has never been promoted into the
//	             recency list.
//	itemActive:  the item is in a bucket and in the recency list.
//	itemRemoved: ownership has been released. The item must never be inserted
//	             into the recency list or counted towards the size again.
//
// Only the worker goroutine moves an item between states. The state is kept in
// an atomic so that external readers (inspection in tests, the debug
// formatter) never observe a torn transition.
const (
	itemPending uint32 = iota
	itemActive
	itemRemoved
)

type Item[T any] struct {
	key        string
	group      string
	promotions int32
	refCount   int32
	expires    int64
	size       int64
	value      T
	next       *Item[T]
	prev       *Item[T]
	state      uint32
	clock      func() time.Time
}

func newItem[T any](key string, value T, expires int64, track bool) *Item[T] {
	size := int64(1)

	// https://github.com/golang/go/issues/49206
	if sized, ok := (interface{})(value).(Sized); ok {
		size = sized.Size()
	}

	item := &Item[T]{
		key:        key,
		value:      value,
		promotions: 0,
		size:       size,
		expires:    expires,
		state:      itemPending,
	}
	if track {
		item.refCount = 1
	}
	return item
}

func (i *Item[T]) lifecycleState() uint32 {
	return atomic.LoadUint32(&i.state)
}

func (i *Item[T]) setState(state uint32) {
	atomic.StoreUint32(&i.state, state)
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

func (i *Item[T]) now() time.Time {
	if i.clock != nil {
		return i.clock()
	}
	return time.Now()
}

func (i *Item[T]) Expired() bool {
	expires := atomic.LoadInt64(&i.expires)
	return expires < i.now().UnixNano()
}

func (i *Item[T]) TTL() time.Duration {
	expires := atomic.LoadInt64(&i.expires)
	return time.Nanosecond * time.Duration(expires-i.now().UnixNano())
}

func (i *Item[T]) Expires() time.Time {
	expires := atomic.LoadInt64(&i.expires)
	return time.Unix(0, expires)
}

func (i *Item[T]) Extend(duration time.Duration) {
	atomic.StoreInt64(&i.expires, i.now().Add(duration).UnixNano())
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
