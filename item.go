package ccache

import (
	"fmt"
	"sync/atomic"
	"time"
)

type Sized interface {
	Size() int64
}

// ItemState identifies which component of the cache currently owns an item.
// Items move through a single, inspectable state machine shared by the
// promotion, expiration and onDelete-callback paths:
//
//	StatePending  -- created by a bucket (Set/Setnx/Fetch), waiting in the
//	                 promotables queue; not yet owned by the LRU list.
//	StateListed   -- owned by the LRU list, which is owned by the worker
//	                 goroutine.
//	StateDeleted  -- terminal. Owned by no one. A deleted item must never
//	                 be resurrected by a late promotion, regardless of
//	                 expiration or goroutine scheduling.
//
// The only legal transitions are Pending->Listed, Pending->Deleted and
// Listed->Deleted. The transition into StateDeleted happens exactly once,
// which is what guarantees the onDelete callback fires at most once and
// that size accounting is released exactly once.
//
// States are mutated only by the worker goroutine, or under a bucket write
// lock (clear), so mutations need no atomic operations; concurrent readers
// should use Item.State.
type ItemState int32

const (
	StatePending ItemState = iota
	StateListed
	StateDeleted
)

func (s ItemState) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateListed:
		return "listed"
	case StateDeleted:
		return "deleted"
	}
	return "unknown"
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
	state      ItemState
	now        func() time.Time
	next       *Item[T]
	prev       *Item[T]
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

// State returns the item's current ownership state. It is safe to call
// concurrently with the cache's worker goroutine.
func (i *Item[T]) State() ItemState {
	return ItemState(atomic.LoadInt32((*int32)(&i.state)))
}

// markListed records the Pending->Listed transition. It is called by the
// worker goroutine when the item is inserted into the LRU list.
func (i *Item[T]) markListed() {
	i.state = StateListed
}

// markDeleted performs the terminal transition into StateDeleted and
// reports the state the item transitioned from, so the caller can run the
// exit actions (size accounting, list removal, onDelete callback) exactly
// once. A second call is a no-op that reports StateDeleted.
func (i *Item[T]) markDeleted() (previous ItemState) {
	previous = i.state
	i.state = StateDeleted
	return
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
	return expires < i.nowTime().UnixNano()
}

func (i *Item[T]) TTL() time.Duration {
	expires := atomic.LoadInt64(&i.expires)
	return time.Nanosecond * time.Duration(expires-i.nowTime().UnixNano())
}

func (i *Item[T]) Expires() time.Time {
	expires := atomic.LoadInt64(&i.expires)
	return time.Unix(0, expires)
}

func (i *Item[T]) Extend(duration time.Duration) {
	atomic.StoreInt64(&i.expires, i.nowTime().Add(duration).UnixNano())
}

// nowTime returns the current time from the clock the item was created
// with (see Configuration.Clock), falling back to the system clock for
// items constructed outside of a cache.
func (i *Item[T]) nowTime() time.Time {
	if i.now == nil {
		return time.Now()
	}
	return i.now()
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
