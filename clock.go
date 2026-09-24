package ccache

import (
	"sync"
	"time"
)

// Clock abstracts time for the cache. Production caches always use
// systemClock; tests inject a fake clock through an internal configuration
// setter so that expiration behavior can be driven step by step through the
// public API without real sleeps.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}

// fakeClock is a manually advanced monotonic clock. It is concurrency-safe so
// that race-mode tests can advance it while workers run.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_700_000_000, 0)}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance moves the clock forward by d.
func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// Set moves the clock to an explicit instant (used to push time backwards when
// a test needs to simulate a reordered scheduling condition).
func (f *fakeClock) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t
}
