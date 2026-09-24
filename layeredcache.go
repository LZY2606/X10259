// An LRU cached aimed at high concurrency
package ccache

import (
	"hash/fnv"
	"time"
)

type LayeredCache[T any] struct {
	*Configuration[T]
	*engine[T]
	buckets    []*layeredBucket[T]
	bucketMask uint32
}

// layeredBuckets adapts the two-level bucket layout to the state machine.
type layeredBuckets[T any] struct {
	buckets []*layeredBucket[T]
	mask    uint32
}

func (b layeredBuckets[T]) bucketFor(key string) *layeredBucket[T] {
	h := fnv.New32a()
	h.Write([]byte(key))
	return b.buckets[h.Sum32()&b.mask]
}

func (b layeredBuckets[T]) evictItem(item *Item[T]) {
	b.bucketFor(item.group).removeItem(item)
}

func (b layeredBuckets[T]) halt() {
	for _, bucket := range b.buckets {
		bucket.halt()
	}
}

func (b layeredBuckets[T]) unhalt() {
	for _, bucket := range b.buckets {
		bucket.unhalt()
	}
}

func (b layeredBuckets[T]) resetLocked() {
	for _, bucket := range b.buckets {
		bucket.resetLocked()
	}
}

func (b layeredBuckets[T]) scan(fn func(item *Item[T])) {
	for _, bucket := range b.buckets {
		bucket.scan(fn)
	}
}

// Create a new layered cache with the specified configuration.
// A layered cache used a two keys to identify a value: a primary key
// and a secondary key. Get, Set and Delete require both a primary and
// secondary key. However, DeleteAll requires only a primary key, deleting
// all values that share the same primary key.

// Layered Cache is useful as an HTTP cache, where an HTTP purge might
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
			clock:  config.nowFn(),
		}
	}
	storage := layeredBuckets[T]{buckets: buckets, mask: uint32(config.buckets) - 1}
	c := &LayeredCache[T]{
		Configuration: config,
		engine:        newEngine(config, bucketSet[T](storage)),
		buckets:       buckets,
		bucketMask:    storage.mask,
	}
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
	if !item.Expired() {
		c.tryQueuePromote(item)
	}
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
	primaryBkt.Lock()
	bkt, exists := primaryBkt.buckets[primary]
	if !exists {
		bkt = primaryBkt.inner()
		primaryBkt.buckets[primary] = bkt
	}
	primaryBkt.Unlock()
	return &SecondaryCache[T]{
		group:  primary,
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

// Attempts to get the value from the cache and calles fetch on a miss (missing
// or stale item). If fetch returns an error, no value is cached and the error
// is returned back to the caller.
// Note that Fetch merely calls the public Get and Set functions. If you want
// a different Fetch behavior, such as thundering herd protection or returning
// expired items, implement it in your application.
func (c *LayeredCache[T]) Fetch(primary, secondary string, duration time.Duration, fetch func() (T, error)) (*Item[T], error) {
	item := c.Get(primary, secondary)
	if item != nil && !item.Expired() {
		return item, nil
	}
	value, err := fetch()
	if err != nil {
		return nil, err
	}
	return c.set(primary, secondary, value, duration, false), nil
}

// Remove the item from the cache, return true if the item was present, false otherwise.
func (c *LayeredCache[T]) Delete(primary, secondary string) bool {
	item := c.bucket(primary).remove(primary, secondary)
	if item != nil {
		c.queueRemove(item)
		return true
	}
	return false
}

// Deletes all items that share the same primary key
func (c *LayeredCache[T]) DeleteAll(primary string) bool {
	return c.bucket(primary).deleteAll(primary, c.deletables)
}

// Deletes all items that share the same primary key and prefix.
func (c *LayeredCache[T]) DeletePrefix(primary, prefix string) int {
	return c.bucket(primary).deletePrefix(primary, prefix, c.deletables)
}

// Deletes all items that share the same primary key and where the matches func evaluates to true.
func (c *LayeredCache[T]) DeleteFunc(primary string, matches func(key string, item *Item[T]) bool) int {
	return c.bucket(primary).deleteFunc(primary, matches, c.deletables)
}

func (c *LayeredCache[T]) set(primary, secondary string, value T, duration time.Duration, track bool) *Item[T] {
	item, existing := c.bucket(primary).set(primary, secondary, value, duration, track)
	if existing != nil {
		c.queueRemove(existing)
	}
	c.queuePromote(item)
	return item
}

func (c *LayeredCache[T]) bucket(key string) *layeredBucket[T] {
	h := fnv.New32a()
	h.Write([]byte(key))
	return c.buckets[h.Sum32()&c.bucketMask]
}
