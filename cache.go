// An LRU cached aimed at high concurrency
package ccache

import (
	"hash/fnv"
	"time"
)

type Cache[T any] struct {
	*Configuration[T]
	*engine[T]
	buckets    []*bucket[T]
	bucketMask uint32
}

// flatBuckets adapts the flat bucket layout to the state machine's bucketSet.
type flatBuckets[T any] struct {
	buckets []*bucket[T]
	mask    uint32
}

func (b flatBuckets[T]) evictItem(item *Item[T]) {
	b.bucketFor(item.key).removeItem(item)
}

func (b flatBuckets[T]) halt() {
	for _, bucket := range b.buckets {
		bucket.Lock()
	}
}

func (b flatBuckets[T]) unhalt() {
	for _, bucket := range b.buckets {
		bucket.Unlock()
	}
}

func (b flatBuckets[T]) resetLocked() {
	for _, bucket := range b.buckets {
		bucket.resetLocked()
	}
}

func (b flatBuckets[T]) scan(fn func(item *Item[T])) {
	for _, bucket := range b.buckets {
		bucket.scan(fn)
	}
}

func (b flatBuckets[T]) bucketFor(key string) *bucket[T] {
	h := fnv.New32a()
	h.Write([]byte(key))
	return b.buckets[h.Sum32()&b.mask]
}

// Create a new cache with the specified configuration
// See ccache.Configure() for creating a configuration
func New[T any](config *Configuration[T]) *Cache[T] {
	buckets := make([]*bucket[T], config.buckets)
	for i := 0; i < config.buckets; i++ {
		buckets[i] = &bucket[T]{
			lookup: make(map[string]*Item[T]),
			clock:  config.nowFn(),
		}
	}
	storage := flatBuckets[T]{buckets: buckets, mask: uint32(config.buckets) - 1}
	c := &Cache[T]{
		Configuration: config,
		engine:        newEngine(config, bucketSet[T](storage)),
	}
	c.buckets = buckets
	c.bucketMask = storage.mask
	return c
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
		count += b.deletePrefix(prefix, c.deletables)
	}
	return count
}

// Deletes all items that the matches func evaluates to true.
func (c *Cache[T]) DeleteFunc(matches func(key string, item *Item[T]) bool) int {
	count := 0
	for _, b := range c.buckets {
		count += b.deleteFunc(matches, c.deletables)
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
	if !item.Expired() {
		c.tryQueuePromote(item)
	}
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
	if existing && !item.Expired() {
		c.tryQueuePromote(item)
	} else if !existing {
		c.queuePromote(item)
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

// Attempts to get the value from the cache and calles fetch on a miss (missing
// or stale item). If fetch returns an error, no value is cached and the error
// is returned back to the caller.
// Note that Fetch merely calls the public Get and Set functions. If you want
// a different Fetch behavior, such as thundering herd protection or returning
// expired items, implement it in your application.
func (c *Cache[T]) Fetch(key string, duration time.Duration, fetch func() (T, error)) (*Item[T], error) {
	item := c.Get(key)
	if item != nil && !item.Expired() {
		return item, nil
	}
	value, err := fetch()
	if err != nil {
		return nil, err
	}
	return c.set(key, value, duration, false), nil
}

// Remove the item from the cache, return true if the item was present, false otherwise.
func (c *Cache[T]) Delete(key string) bool {
	item := c.bucket(key).remove(key)
	if item != nil {
		c.queueRemove(item)
		return true
	}
	return false
}

func (c *Cache[T]) set(key string, value T, duration time.Duration, track bool) *Item[T] {
	item, existing := c.bucket(key).set(key, value, duration, track)
	if existing != nil {
		c.queueRemove(existing)
	}
	c.queuePromote(item)
	return item
}

func (c *Cache[T]) bucket(key string) *bucket[T] {
	h := fnv.New32a()
	h.Write([]byte(key))
	return c.buckets[h.Sum32()&c.bucketMask]
}
