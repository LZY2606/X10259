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

// itemState is the lifecycle state of an Item.
//
// An item only ever flows forward through these states, and every transition
// is applied by exactly one owner: the cache's worker goroutine (via
// lifecycle). This makes the item's ownership across the bucket lookup, the
// promotion queue and the deletion queue a single, inspectable state machine:
//
//	stateNew    - the item exists in a bucket lookup and/or a promotion
//	              queue, but has never been attached to the recency list
//	stateActive - the item is attached to the recency list and counts
//	              towards the cache size
//	stateDead   - terminal. The item is detached from the recency list (or
//	              was never attached), no longer counts towards the size and
//	              must never be promoted or have its onDelete callback run
//	              again.
//
// All state writes happen on the worker goroutine; the only concurrent
// readers are callers holding a reference returned through the public API and
// tests synchronized via control.SyncUpdates().
type itemState uint8

const (
	stateNew itemState = iota
	stateActive
	stateDead
)

func (s itemState) String() string {
	switch s {
	case stateNew:
		return "new"
	case stateActive:
		return "active"
	case stateDead:
		return "dead"
	default:
		return fmt.Sprintf("invalid(%d)", uint8(s))
	}
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
	state      itemState
	// now is the clock the item evaluates its expiry against. Production
	// items are always given time.Now; it is an indirection only so that
	// tests can drive expiry deterministically through a fake clock. The
	// nil fallback keeps zero-value Items (used by unit tests) usable and
	// costs one predictable branch per Expired/TTL/Expires/Extend call.
	now func() time.Time
}

func newItem[T any](key string, value T, expires int64, track bool, now func() time.Time) *Item[T] {
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
		state:      stateNew,
		now:        now,
	}
	if track {
		item.refCount = 1
	}
	return item
}

func (i *Item[T]) nowTime() time.Time {
	if i.now != nil {
		return i.now()
	}
	return time.Now()
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

// lifecycleState reports the item's ownership state. It is intended for tests
// and diagnostics. In production the state machine is driven exclusively by
// the worker goroutine.
func (i *Item[T]) lifecycleState() itemState {
	return i.state
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
