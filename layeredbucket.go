package ccache

import (
	"sync"
	"time"
)

type layeredBucket[T any] struct {
	sync.RWMutex
	buckets map[string]*bucket[T]
	clock   func() time.Time
}

func (b *layeredBucket[T]) inner() *bucket[T] {
	return &bucket[T]{lookup: make(map[string]*Item[T]), clock: b.clock}
}

func (b *layeredBucket[T]) itemCount() int {
	count := 0
	b.RLock()
	defer b.RUnlock()
	for _, b := range b.buckets {
		count += b.itemCount()
	}
	return count
}

func (b *layeredBucket[T]) get(primary, secondary string) *Item[T] {
	bucket := b.getSecondaryBucket(primary)
	if bucket == nil {
		return nil
	}
	return bucket.get(secondary)
}

func (b *layeredBucket[T]) getSecondaryBucket(primary string) *bucket[T] {
	b.RLock()
	bucket, exists := b.buckets[primary]
	b.RUnlock()
	if !exists {
		return nil
	}
	return bucket
}

func (b *layeredBucket[T]) set(primary, secondary string, value T, duration time.Duration, track bool) (*Item[T], *Item[T]) {
	b.Lock()
	bkt, exists := b.buckets[primary]
	if !exists {
		bkt = b.inner()
		b.buckets[primary] = bkt
	}
	b.Unlock()
	item, existing := bkt.set(secondary, value, duration, track)
	item.group = primary
	return item, existing
}

func (b *layeredBucket[T]) remove(primary, secondary string) *Item[T] {
	b.RLock()
	bucket, exists := b.buckets[primary]
	b.RUnlock()
	if !exists {
		return nil
	}
	return bucket.remove(secondary)
}

// removeItem deletes the secondary mapping only when it still belongs to item.
func (b *layeredBucket[T]) removeItem(item *Item[T]) {
	b.RLock()
	bucket, exists := b.buckets[item.group]
	b.RUnlock()
	if !exists {
		return
	}
	bucket.removeItem(item)
}

func (b *layeredBucket[T]) delete(primary, secondary string) {
	b.RLock()
	bucket, exists := b.buckets[primary]
	b.RUnlock()
	if !exists {
		return
	}
	bucket.delete(secondary)
}

func (b *layeredBucket[T]) deletePrefix(primary string, prefix string, deletables chan *Item[T]) int {
	b.RLock()
	bucket, exists := b.buckets[primary]
	b.RUnlock()
	if !exists {
		return 0
	}
	return bucket.deletePrefix(prefix, deletables)
}

func (b *layeredBucket[T]) deleteFunc(primary string, matches func(key string, item *Item[T]) bool, deletables chan *Item[T]) int {
	b.RLock()
	bucket, exists := b.buckets[primary]
	b.RUnlock()
	if !exists {
		return 0
	}
	return bucket.deleteFunc(matches, deletables)
}

func (b *layeredBucket[T]) deleteAll(primary string, deletables chan *Item[T]) bool {
	b.RLock()
	bucket, exists := b.buckets[primary]
	b.RUnlock()
	if !exists {
		return false
	}

	bucket.Lock()
	defer bucket.Unlock()

	if l := len(bucket.lookup); l == 0 {
		return false
	}
	for key, item := range bucket.lookup {
		delete(bucket.lookup, key)
		deletables <- item
	}
	return true
}

func (b *layeredBucket[T]) forEachFunc(primary string, matches func(key string, item *Item[T]) bool) {
	b.RLock()
	bucket, exists := b.buckets[primary]
	b.RUnlock()
	if exists {
		bucket.forEachFunc(matches)
	}
}

func (b *layeredBucket[T]) scan(fn func(item *Item[T])) {
	b.RLock()
	inner := b.buckets
	b.RUnlock()
	for _, bucket := range inner {
		bucket.scan(fn)
	}
}

// halt locks every secondary bucket. It is the layered counterpart of
// flatBuckets.halt and must be paired with resetLocked/unhalt.
func (b *layeredBucket[T]) halt() {
	b.RLock()
	inner := b.buckets
	b.RUnlock()
	for _, bucket := range inner {
		bucket.Lock()
	}
}

func (b *layeredBucket[T]) unhalt() {
	b.RLock()
	inner := b.buckets
	b.RUnlock()
	for _, bucket := range inner {
		bucket.Unlock()
	}
}

// resetLocked marks every owned item removed and drops every secondary bucket.
// Every secondary bucket that existed at halt time must be held. The outer map
// lock is taken only for the swap, never while an inner lock is needed.
func (b *layeredBucket[T]) resetLocked() {
	b.RLock()
	inner := b.buckets
	b.RUnlock()
	for _, bucket := range inner {
		bucket.resetLocked()
	}
	b.Lock()
	b.buckets = make(map[string]*bucket[T])
	b.Unlock()
}
