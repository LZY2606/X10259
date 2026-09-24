// An LRU cached aimed at high concurrency
package ccache

import (
	"errors"
	"hash/fnv"
	"sync"
	"time"
)

type Cache[T any] struct {
	*Configuration[T]
	control
	core *lruCore[T]

	buckets    []*bucket[T]
	bucketMask uint32

	// flights collapses concurrent Fetch misses for the same key into a single
	// fetch invocation. The hot (hit) path never touches it.
	flightsMu sync.Mutex
	flights   map[string]*fetchCall[T]
}

// Create a new cache with the specified configuration
// See ccache.Configure() for creating a configuration
func New[T any](config *Configuration[T]) *Cache[T] {
	buckets := make([]*bucket[T], config.buckets)
	for i := 0; i < config.buckets; i++ {
		buckets[i] = &bucket[T]{
			lookup: make(map[string]*Item[T]),
			nowFn:  config.nowFn,
		}
	}
	c := &Cache[T]{
		Configuration: config,
		control:       newControl(),
		bucketMask:    uint32(config.buckets) - 1,
		buckets:       buckets,
		flights:       make(map[string]*fetchCall[T]),
	}
	c.core = newLRUCore(config, c.control, lruHooks[T]{
		evict: func(item *Item[T]) bool {
			return c.bucket(item.key).deleteIf(item.key, item)
		},
		lockAll: func() {
			for _, bucket := range c.buckets {
				bucket.Lock()
			}
		},
		unlockAll: func() {
			for _, bucket := range c.buckets {
				bucket.Unlock()
			}
		},
		clearLookup: func() {
			for _, bucket := range c.buckets {
				bucket.clear()
			}
		},
	})
	return c
}

// list exposes the recency list owned by the state machine. It exists only to
// preserve the read-only field access used by callers; the state machine is the
// sole mutator.
func (c *Cache[T]) list() *List[T] {
	return c.core.list
}

func (c *Cache[T]) ItemCount() int {
	count := 0
	for _, b := range c.buckets {
		count += b.itemCount()
	}
	return count
}

func (c *Cache[T]) DeletePrefix(prefix string) int {
	count := 0
	for _, b := range c.buckets {
		count += b.deletePrefix(prefix, c.core.deletables)
	}
	return count
}

// Deletes all items that the matches func evaluates to true.
func (c *Cache[T]) DeleteFunc(matches func(key string, item *Item[T]) bool) int {
	count := 0
	for _, b := range c.buckets {
		count += b.deleteFunc(matches, c.core.deletables)
	}
	return count
}

func (c *Cache[T]) ForEachFunc(matches func(key string, item *Item[T]) bool) {
	for _, b := range c.buckets {
		if !b.forEachFunc(matches) {
			break
		}
	}
}

// Get an item from the cache. Returns nil if the item wasn't found.
// This can return an expired item. Use item.Expired() to see if the item
// is expired and item.TTL() to see how long until the item expires (which
// will be negative for an already expired item).
func (c *Cache[T]) Get(key string) *Item[T] {
	item := c.bucket(key).get(key)
	if item == nil {
		return nil
	}
	c.core.offerPromote(item)
	return item
}

// Same as Get but does not promote the value. This essentially circumvents the
// "least recently used" aspect of this cache. To some degree, it's akin to a
// "peak"
func (c *Cache[T]) GetWithoutPromote(key string) *Item[T] {
	return c.bucket(key).get(key)
}

// Used when the cache was created with the Track() configuration option.
// Avoid otherwise
func (c *Cache[T]) TrackingGet(key string) TrackedItem[T] {
	item := c.Get(key)
	if item == nil {
		return nil
	}
	item.track()
	return item
}

// Used when the cache was created with the Track() configuration option.
// Sets the item, and returns a tracked reference to it.
func (c *Cache[T]) TrackingSet(key string, value T, duration time.Duration) TrackedItem[T] {
	return c.set(key, value, duration, true)
}

// Set the value in the cache for the specified duration
func (c *Cache[T]) Set(key string, value T, duration time.Duration) {
	c.set(key, value, duration, false)
}

// Setnx set the value in the cache for the specified duration if not exists
func (c *Cache[T]) Setnx(key string, value T, duration time.Duration) {
	c.bucket(key).setnx(key, value, duration, false)
}

// Setnx2 set the value in the cache for the specified duration if not exists
func (c *Cache[T]) Setnx2(key string, f func() T, duration time.Duration) *Item[T] {
	item, existing := c.bucket(key).setnx2(key, f, duration, false)
	if existing {
		c.core.offerPromote(item)
	} else {
		c.core.enqueuePromote(item)
	}
	return item
}

// Replace the value if it exists, does not set if it doesn't.
// Returns true if the item existed an was replaced, false otherwise.
// Replace does not reset item's TTL
func (c *Cache[T]) Replace(key string, value T) bool {
	item := c.bucket(key).get(key)
	if item == nil {
		return false
	}
	c.Set(key, value, item.TTL())
	return true
}

// Extend the value if it exists, does not set if it doesn't exists.
// Returns true if the expire time of the item an was extended, false otherwise.
func (c *Cache[T]) Extend(key string, duration time.Duration) bool {
	item := c.bucket(key).get(key)
	if item == nil {
		return false
	}

	item.Extend(duration)
	return true
}

// ErrFetchRejected is returned to every waiter when the single leader of a
// concurrent Fetch group returns an error. It wraps the fetch error, so errors
// .Is(err, ErrFetchRejected) distinguishes a shared failure without hiding the
// original error layer.
var ErrFetchRejected = errors.New("ccache: fetch rejected by the shared loader")

// fetchCall is one in-flight single-flight load.
type fetchCall[T any] struct {
	done chan struct{}
	item *Item[T]
	err  error
}

// Attempts to get the value from the cache and calles fetch on a miss (missing
// or stale item). Concurrent calls for the same missing key collapse into a
// single fetch invocation: the leader calls fetch exactly once, and every
// waiter receives the same item or, on failure, the same wrapped error. If
// fetch returns an error, nothing is cached.
func (c *Cache[T]) Fetch(key string, duration time.Duration, fetch func() (T, error)) (*Item[T], error) {
	item := c.Get(key)
	if item != nil && !item.Expired() {
		return item, nil
	}

	c.flightsMu.Lock()
	if call := c.flights[key]; call != nil {
		c.flightsMu.Unlock()
		<-call.done
		// The leader's Set may have lost an overwrite race; read the
		// authoritative current value.
		if current := c.GetWithoutPromote(key); current != nil && !c.core.expired(current) {
			return current, nil
		}
		return nil, call.err
	}
	call := &fetchCall[T]{done: make(chan struct{})}
	c.flights[key] = call
	c.flightsMu.Unlock()

	defer func() {
		c.flightsMu.Lock()
		delete(c.flights, key)
		c.flightsMu.Unlock()
		close(call.done)
	}()

	value, err := fetch()
	if err != nil {
		call.err = fmtErrorf(err)
		return nil, call.err
	}
	item = c.set(key, value, duration, false)
	call.item = item
	return item, nil
}

func fmtErrorf(err error) error {
	return &fetchError{err: err}
}

type fetchError struct {
	err error
}

func (e *fetchError) Error() string { return ErrFetchRejected.Error() + ": " + e.err.Error() }
func (e *fetchError) Unwrap() error { return e.err }
func (e *fetchError) Is(target error) bool {
	return target == ErrFetchRejected || target == e.err
}

// Remove the item from the cache, return true if the item was present, false otherwise.
func (c *Cache[T]) Delete(key string) bool {
	item := c.bucket(key).remove(key)
	if item != nil {
		c.core.enqueueDelete(item)
		return true
	}
	return false
}

func (c *Cache[T]) set(key string, value T, duration time.Duration, track bool) *Item[T] {
	item, existing := c.bucket(key).set(key, value, duration, track)
	if existing != nil {
		c.core.enqueueDelete(existing)
	}
	c.core.enqueuePromote(item)
	return item
}

func (c *Cache[T]) bucket(key string) *bucket[T] {
	h := fnv.New32a()
	h.Write([]byte(key))
	return c.buckets[h.Sum32()&c.bucketMask]
}
