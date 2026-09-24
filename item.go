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
	expires    int64
	size       int64
	value      T
	next       *Item[T]
	prev       *Item[T]
	inList     bool
	state      itemState
}

// itemState is the single source of truth for where an Item lives with
// respect to the cache's three ownership locations: the bucket lookup, the
// promotables queue (or the LRU list once promoted) and the deletables queue.
//
// Every transition is performed by the single background worker through
// (*lru).promote and (*lru).retire, which means the field does not need
// atomic access. Callers (Get/Set/Delete) only stage items into queues and
// never mutate the state themselves.
type itemState uint8

const (
	// stateQueued: the item is the current occupant of its bucket slot and is
	// either waiting in the promotables queue or has not been promoted yet. It
	// is not owned by the LRU list.
	stateQueued itemState = iota
	// statePromoted: the item is the current occupant of its bucket slot and is
	// owned by the LRU list, contributing to the cache's size.
	statePromoted
	// stateDeleted is terminal. The item no longer contributes to the cache: it
	// has been removed from the LRU list (if it ever reached it) and its
	// onDelete callback, if any, has already been invoked. A late promotion or
	// deletion message for this item is a no-op.
	stateDeleted
)

func (s itemState) String() string {
	switch s {
	case stateQueued:
		return "queued"
	case statePromoted:
		return "promoted"
	case stateDeleted:
		return "deleted"
	}
	return "unknown"
}

func newItem[T any](key string, value T, expires int64, track bool) *Item[T] {
	size := int64(1)

	// https://github.com/golang/go/issues/49206
	if sized, ok := (interface{})(value).(Sized); ok {
		size = sized.Size()
	}

	item := &Item[T]{
		key:     key,
		value:   value,
		size:    size,
		expires: expires,
		state:   stateQueued,
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

// markDeleted records the terminal transition. The promotions counter is also
// set to -2 for backwards-compatible observability (the previous implementation
// used -2 as its "deleted" sentinel); all new logic keys off item.state.
func (i *Item[T]) markDeleted() {
	i.state = stateDeleted
	i.promotions = -2
}

// expiredAt reports whether the item expired relative to now. All internal
// liveness decisions route through this method so tests can drive time
// deterministically with a fake clock.
func (i *Item[T]) expiredAt(now int64) bool {
	return atomic.LoadInt64(&i.expires) < now
}

// expireAbsolute computes the absolute expiration instant relative to clock.
func expireAbsolute(clock Clock, duration time.Duration) int64 {
	return clock.Now().Add(duration).UnixNano()
}
