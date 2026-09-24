// An LRU cached aimed at high concurrency
package ccache

import (
	"hash/fnv"
	"time"
)

type Cache[T any] struct {
	*engine[T]
	buckets    []*bucket[T]
	bucketMask uint32
}

// Create a new cache with the specified configuration
// See ccache.Configure() for creating a configuration
func New[T any](config *Configuration[T]) *Cache[T] {
	c := &Cache[T]{
		bucketMask: uint32(config.buckets) - 1,
		buckets:    make([]*bucket[T], config.buckets),
	}
	hooks := lifecycleHooks[T]{
		lockAll: func() {
			for _, b := range c.buckets {
				b.Lock()
			}
		},
		unlockAll: func() {
			for _, b := range c.buckets {
				b.Unlock()
			}
		},
		clearAll: func() {
			for _, b := range c.buckets {
				b.clear()
			}
		},
		evict: func(item *Item[T]) {
			c.bucket(item.key).delete(item.key)
		},
	}
	c.engine = newEngine(config, hooks)
	for i := 0; i < config.buckets; i++ {
		c.buckets[i] = &bucket[T]{
			lookup: make(map[string]*Item[T]),
			clock:  config.clock,
		}
	}
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
	if item.fresh() {
		c.tryQueuePromote(item)
	}
	return item
}

// Same as Get but does not promote the value. This essentially circumvents the
// "least recently used" aspect of this cache. To some degree, it's akin to a "peak"
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
	// consistent with Get
	if existing && item.fresh() {
		c.tryQueuePromote(item)
		// consistent with set
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

// Attempts to get the value from the cache and calls fetch on a miss (missing
// or stale item). Concurrent Fetch calls for the same missing key share a
// single fetch invocation (single flight). If fetch returns an error, no value
// is cached, no coordination state is retained, and the error is returned
// unchanged back to every waiting caller.
func (c *Cache[T]) Fetch(key string, duration time.Duration, fetch func() (T, error)) (*Item[T], error) {
	item := c.Get(key)
	if item != nil && !item.Expired() {
		return item, nil
	}

	for {
		wait, leader := c.flights.acquire(key)
		if !leader {
			<-wait
			// The leader either populated the cache (common path) or failed
			// without leaving any state behind. Re-check through the normal
			// public read path; on a miss the follower joins the next single
			// flight as its own leader.
			if existing := c.GetWithoutPromote(key); existing != nil && !existing.Expired() {
				return existing, nil
			}
			continue
		}

		value, err := fetch()
		if err != nil {
			c.flights.finish(key)
			return nil, err
		}
		c.flights.finish(key)
		return c.set(key, value, duration, false), nil
	}
}

// Remove the item from the cache, return true if the item was present, false otherwise.
func (c *Cache[T]) Delete(key string) bool {
	item := c.bucket(key).remove(key)
	if item != nil {
		c.queueDelete(item)
		return true
	}
	return false
}

func (c *Cache[T]) set(key string, value T, duration time.Duration, track bool) *Item[T] {
	item, existing := c.bucket(key).set(key, value, duration, track)
	if existing != nil {
		c.queueDelete(existing)
	}
	c.queuePromote(item)
	return item
}

func (c *Cache[T]) bucket(key string) *bucket[T] {
	h := fnv.New32a()
	h.Write([]byte(key))
	return c.buckets[h.Sum32()&c.bucketMask]
}

