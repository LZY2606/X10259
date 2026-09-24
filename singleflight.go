package ccache

import "sync"

// fetchGroup deduplicates concurrent Fetch calls for the same key ("fetch
// singleflight"): while a fetch function is running, additional callers wait
// for its outcome and share the same result instead of invoking fetch again.
//
// It is a generic, self-contained primitive used by the production Fetch path
// of both Cache and LayeredCache; tests drive it only through the public Fetch
// entry points. On error every waiter receives the same error and the call
// leaves no group, cache entry, counter or other state behind, so an identical
// retry starts from a clean slate.
type fetchGroup[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*fetchCall[V]
}

type fetchCall[V any] struct {
	wg  sync.WaitGroup
	val V
	err error
}

func (g *fetchGroup[K, V]) Do(key K, fn func() (V, error)) (V, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[K]*fetchCall[V])
	}
	if call, ok := g.m[key]; ok {
		g.mu.Unlock()
		call.wg.Wait()
		return call.val, call.err
	}
	call := &fetchCall[V]{}
	call.wg.Add(1)
	g.m[key] = call
	g.mu.Unlock()

	call.val, call.err = fn()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	call.wg.Done()
	return call.val, call.err
}

// inflight reports whether a fetch for key is currently running. It exists for
// diagnostics/assertions and takes the same lock as Do.
func (g *fetchGroup[K, V]) inflight(key K) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, ok := g.m[key]
	return ok
}
