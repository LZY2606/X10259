package ccache

import (
	"fmt"
	"sync/atomic"
	"time"
)

type Sized interface {
	Size() int64
}

// lifecycle is the single, authoritative state an item moves through. It records
// which owner currently holds the item:
//   - lifecycleQueued: the item exists in a bucket lookup, and may be queued in
//     the promotables channel, but it is not in the LRU list
//   - lifecyclePromoted: the item exists in a bucket lookup and in the LRU list
//   - lifecycleTerminal: the item has been deleted/evicted/expired-as-late-
//     promotion; a late promotables/deletables message must never revive it
//
// Every transition is performed by the single worker goroutine through the
// state-machine functions on lruCore, so the field itself needs no atomic
// protection. Callers only enqueue *Item messages; they never mutate state.
type lifecycle int8

const (
	lifecycleQueued lifecycle = iota
	lifecyclePromoted
	lifecycleTerminal
)

func (s lifecycle) String() string {
	switch s {
	case lifecycleQueued:
		return "queued"
	case lifecyclePromoted:
		return "promoted"
	default:
		return "terminal"
	}
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
	expires    int64
	size       int64
	value      T
	next       *Item[T]
	prev       *Item[T]
	inList     bool
	state      lifecycle
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

// isTerminal reports whether the item has already left the cache. It exists so
// that a message sitting in the promotables or deletables queues can be checked
// against the single state machine instead of relying on a magic counter value.
func (i *Item[T]) isTerminal() bool {
	return i.state == lifecycleTerminal
}

// retire is the only way to move an item to the terminal state. It is invoked
// solely from the worker-side state-machine transitions.
func (i *Item[T]) retire() {
	i.state = lifecycleTerminal
	i.promotions = -2
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
	return expires < time.Now().UnixNano()
}

func (i *Item[T]) TTL() time.Duration {
	expires := atomic.LoadInt64(&i.expires)
	return time.Nanosecond * time.Duration(expires-time.Now().UnixNano())
}

func (i *Item[T]) Expires() time.Time {
	expires := atomic.LoadInt64(&i.expires)
	return time.Unix(0, expires)
}

func (i *Item[T]) Extend(duration time.Duration) {
	atomic.StoreInt64(&i.expires, time.Now().Add(duration).UnixNano())
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
