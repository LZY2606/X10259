package ccache

import "sync"

// flightGroup deduplicates concurrent Fetch calls for the same key so that
// only one goroutine runs the user-supplied fetch function (single-flight /
// thundering-herd protection). It is shared by Cache.Fetch and
// LayeredCache.Fetch so both public entry points execute the exact same
// production logic.
//
// The zero value is ready to use; the map is allocated lazily on the first
// miss so that caches which never call Fetch keep their previous allocation
// profile.
type flightGroup[K comparable, T any] struct {
	mu      sync.Mutex
	flights map[K]*flight[T]
}

type flight[T any] struct {
	done chan struct{}
	item *Item[T]
	err  error
}

// do runs fn under single-flight semantics: the first caller for a key
// becomes the leader and executes fn; concurrent callers for the same key
// wait for the leader and share its result (value or error). No cache lock
// is held while fn runs, so fn may freely call other cache methods (it must
// not, however, recursively Fetch the same key, which would deadlock - the
// same limitation as golang.org/x/sync/singleflight).
func (g *flightGroup[K, T]) do(key K, fn func() (*Item[T], error)) (*Item[T], error) {
	g.mu.Lock()
	if g.flights == nil {
		g.flights = make(map[K]*flight[T])
	}
	if f, ok := g.flights[key]; ok {
		g.mu.Unlock()
		<-f.done
		return f.item, f.err
	}
	f := &flight[T]{done: make(chan struct{})}
	g.flights[key] = f
	g.mu.Unlock()

	f.item, f.err = fn()

	g.mu.Lock()
	delete(g.flights, key)
	g.mu.Unlock()
	close(f.done)
	return f.item, f.err
}
