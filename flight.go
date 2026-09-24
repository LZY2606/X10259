package ccache

import "sync"

// flightGroup coalesces concurrent fetches for the same key into a single
// invocation of the user-provided fetch function. It exists to uphold the
// Fetch contract under concurrency:
//
//   - exactly one fetch function runs per concurrently-missing key (single
//     flight / thundering-herd protection),
//   - a failed fetch leaves nothing behind in the group, so a retry is a
//     fresh single flight (failure atomicity),
//   - the group's mutex is held only over map bookkeeping, never while the
//     user fetch runs and never alongside a bucket lock, so a fetch callback
//     may safely re-enter the cache.
type flightGroup struct {
	mu     sync.Mutex
	active map[string]*flightCall
}

type flightCall struct {
	done chan struct{}
}

func newFlightGroup() *flightGroup {
	return &flightGroup{active: make(map[string]*flightCall)}
}

// acquire reserves the key for the caller. The leader gets ok=true and must
// invoke finish exactly once; followers block on the returned channel until
// the leader finishes.
func (g *flightGroup) acquire(key string) (wait <-chan struct{}, ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if call, exists := g.active[key]; exists {
		return call.done, false
	}
	call := &flightCall{done: make(chan struct{})}
	g.active[key] = call
	return nil, true
}

func (g *flightGroup) finish(key string) {
	g.mu.Lock()
	call := g.active[key]
	delete(g.active, key)
	g.mu.Unlock()
	if call != nil {
		close(call.done)
	}
}
