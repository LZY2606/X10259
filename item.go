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

// Item lifecycle states. An item moves through a single ownership state
// machine as it travels between its bucket, the promotables queue, the LRU
// list and the delete queue:
//
//	live (unlisted) --promotables--> live (listed) --deletables--> dead
//
//   - An item is born "live" when Set/Setnx/Setnx2 stores it in a bucket.
//     It is owned by that bucket and is visible to Get.
//   - The worker's promote path moves a live item into the LRU list. An
//     item that expired (or died) while its promotion was still queued is
//     dropped instead: a late promotion never revives it.
//   - Delete, overwrite, GC and Clear transfer ownership to the delete
//     path. finalize performs the live->dead transition exactly once; the
//     winner alone accounts the size and runs the onDelete callback (for
//     listed items), so the callback fires at most once per item and never
//     while holding a bucket lock.
//   - A dead item can never re-enter the list: late promotions and duplicate
//     deletes are no-ops.
const (
	itemLive int32 = iota
	itemDead
)

type Item[T any] struct {
	key        string
	group      string
	promotions int32
	state      int32
	refCount   int32
	expires    int64
	size       int64
	value      T
	next       *Item[T]
	prev       *Item[T]
	inList     bool
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

// finalize atomically moves the item from live to dead, reporting whether
// the caller won the transition. Exactly one caller observes true, which is
// what guarantees exactly-once ownership of the delete path (size accounting
// and the onDelete callback).
func (i *Item[T]) finalize() bool {
	return atomic.CompareAndSwapInt32(&i.state, itemLive, itemDead)
}

func (i *Item[T]) isDead() bool {
	return atomic.LoadInt32(&i.state) == itemDead
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
	return expires < timeNow().UnixNano()
}

func (i *Item[T]) TTL() time.Duration {
	expires := atomic.LoadInt64(&i.expires)
	return time.Nanosecond * time.Duration(expires-timeNow().UnixNano())
}

func (i *Item[T]) Expires() time.Time {
	expires := atomic.LoadInt64(&i.expires)
	return time.Unix(0, expires)
}

func (i *Item[T]) Extend(duration time.Duration) {
	atomic.StoreInt64(&i.expires, timeNow().Add(duration).UnixNano())
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
