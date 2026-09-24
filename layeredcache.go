// An LRU cached aimed at high concurrency
package ccache

import (
	"hash/fnv"
	"sync"
	"time"
)

type LayeredCache[T any] struct {
	*Configuration[T]
	control
	core *lruCore[T]

	buckets    []*layeredBucket[T]
	bucketMask uint32

	// flights collapses concurrent Fetch misses for the same primary/secondary
	// key pair into a single fetch invocation. The hit path never touches it.
	flightsMu sync.Mutex
	flights   map[string]*fetchCall[T]
}

// Create a new layered cache with the specified configuration.
// A layered cache used a two keys to identify a value: a primary key
// and a secondary key. Get, Set and Delete require both a primary and
// secondary key. However, DeleteAll requires only a primary key, deleting
// all values that share the same primary key.

// Layered Cache is useful as an HTTP cache, for where an HTTP purge might
// delete multiple variants of the same resource:
// primary key = "user/44"
// secondary key 1 = ".json"
// secondary key 2 = ".xml"

// See ccache.Configure() for creating a configuration
func Layered[T any](config *Configuration[T]) *LayeredCache[T] {
	buckets := make([]*layeredBucket[T], config.buckets)
	for i := 0; i < config.buckets; i++ {
		buckets[i] = &layeredBucket[T]{
			buckets: make(map[string]*bucket[T]),
			nowFn:   config.nowFn,
		}
	}
	c := &LayeredCache[T]{
		Configuration: config,
		control:       newControl(),
		bucketMask:    uint32(config.buckets) - 1,
		buckets:       buckets,
		flights:       make(map[string]*fetchCall[T]),
	}
	c.core = newLRUCore(config, c.control, lruHooks[T]{
		evict: func(item *Item[T]) bool {
			return c.bucket(item.group).deleteIf(item.group, item.key, item)
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

func (c *LayeredCache[T]) ItemCount() int {
	count := 0
	for _, b := range c.buckets {
		count += b.itemCount()
	}
	return count
}

// Get an item from the cache. Returns nil if the item wasn't found.
// This can return an expired item. Use item.Expired() to see if the item
// is expired and item.TTL() to see how long until the item expires (which
// will be negative for an already expired item).
func (c *LayeredCache[T]) Get(primary, secondary string) *Item[T] {
	item := c.bucket(primary).get(primary, secondary)
	if item == nil {
		return nil
	}
	c.core.offerPromote(item)
	return item
}

// Same as Get but does not promote the value. This essentially circumvents the
// "least recently used" aspect of this cache. To some degree, it's akin to a
// "peak"
func (c *LayeredCache[T]) GetWithoutPromote(primary, secondary string) *Item[T] {
	return c.bucket(primary).get(primary, secondary)
}

func (c *LayeredCache[T]) ForEachFunc(primary string, matches func(key string, item *Item[T]) bool) {
	c.bucket(primary).forEachFunc(primary, matches)
}

// Get the secondary cache for a given primary key. This operation will
// never return nil. In the case where the primary key does not exist, a
// new, underlying, empty bucket will be created and returned.
func (c *LayeredCache[T]) GetOrCreateSecondaryCache(primary string) *SecondaryCache[T] {
	primaryBkt := c.bucket(primary)
	bkt := primaryBkt.getSecondaryBucket(primary)
	primaryBkt.Lock()
	if bkt == nil {
		bkt = &bucket[T]{lookup: make(map[string]*Item[T]), nowFn: c.nowFn}
		primaryBkt.buckets[primary] = bkt
	}
	primaryBkt.Unlock()
	return &SecondaryCache[T]{
		bucket: bkt,
		pCache: c,
	}
}

// Used when the cache was created with the Track() configuration option.
// Avoid otherwise
func (c *LayeredCache[T]) TrackingGet(primary, secondary string) TrackedItem[T] {
	item := c.Get(primary, secondary)
	if item == nil {
		return nil
	}
	item.track()
	return item
}

// Set the value in the cache for the specified duration
func (c *LayeredCache[T]) TrackingSet(primary, secondary string, value T, duration time.Duration) TrackedItem[T] {
	return c.set(primary, secondary, value, duration, true)
}

// Set the value in the cache for the specified duration
func (c *LayeredCache[T]) Set(primary, secondary string, value T, duration time.Duration) {
	c.set(primary, secondary, value, duration, false)
}

// Replace the value if it exists, does not set if it doesn't.
// Returns true if the item existed an was replaced, false otherwise.
// Replace does not reset item's TTL nor does it alter its position in the LRU
func (c *LayeredCache[T]) Replace(primary, secondary string, value T) bool {
	item := c.bucket(primary).get(primary, secondary)
	if item == nil {
		return false
	}
	c.Set(primary, secondary, value, item.TTL())
	return true
}

// Attempts to get the value from the cache and calles fetch on a miss.
// Concurrent calls for the same missing primary/secondary pair collapse into a
// single fetch invocation: the leader calls fetch exactly once and waiters
// receive the same item or wrapped error. If fetch returns an error, no value
// is cached and the error is returned back to the caller.
func (c *LayeredCache[T]) Fetch(primary, secondary string, duration time.Duration, fetch func() (T, error)) (*Item[T], error) {
	item := c.Get(primary, secondary)
	if item != nil && !item.Expired() {
		return item, nil
	}

	key := primary + "\x00" + secondary
	c.flightsMu.Lock()
	if call := c.flights[key]; call != nil {
		c.flightsMu.Unlock()
		<-call.done
		if current := c.GetWithoutPromote(primary, secondary); current != nil && !c.core.expired(current) {
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
	return c.set(primary, secondary, value, duration, false), nil
}

// Remove the item from the cache, return true if the item was present, false otherwise.
func (c *LayeredCache[T]) Delete(primary, secondary string) bool {
	item := c.bucket(primary).remove(primary, secondary)
	if item != nil {
		c.core.enqueueDelete(item)
		return true
	}
	return false
}

// Deletes all items that share the same primary key
func (c *LayeredCache[T]) DeleteAll(primary string) bool {
	return c.bucket(primary).deleteAll(primary, c.core.deletables)
}

// Deletes all items that share the same primary key and prefix.
func (c *LayeredCache[T]) DeletePrefix(primary, prefix string) int {
	return c.bucket(primary).deletePrefix(primary, prefix, c.core.deletables)
}

// Deletes all items that share the same primary key and where the matches func evaluates to true.
func (c *LayeredCache[T]) DeleteFunc(primary string, matches func(key string, item *Item[T]) bool) int {
	return c.bucket(primary).deleteFunc(primary, matches, c.core.deletables)
}

func (c *LayeredCache[T]) set(primary, secondary string, value T, duration time.Duration, track bool) *Item[T] {
	item, existing := c.bucket(primary).set(primary, secondary, value, duration, track)
	if existing != nil {
		c.core.enqueueDelete(existing)
	}
	c.core.enqueuePromote(item)
	return item
}

func (c *LayeredCache[T]) bucket(key string) *layeredBucket[T] {
	h := fnv.New32a()
	h.Write([]byte(key))
	return c.buckets[h.Sum32()&c.bucketMask]
}
