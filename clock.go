package ccache

import (
	"sync/atomic"
	"time"
)

// clock is the single time source used by the cache. Production code always
// uses systemClock; a fakeClock can be attached through a Configuration to
// make expiration-driven state transitions deterministic in tests. Expiration
// math is expressed in nanoseconds everywhere, matching time.Time.UnixNano.
type clock interface {
	now() time.Time
	nowNano() int64
}

type systemClock struct{}

func (systemClock) now() time.Time {
	return time.Now()
}

func (systemClock) nowNano() int64 {
	return time.Now().UnixNano()
}

// fakeClock is a monotonically advanced, concurrency-safe clock. It starts at
// a fixed epoch so that raw expiration values never depend on the host clock.
a const testEpochNanos int64 = 946684800000000000 // 2000-01-01T00:00:00Z

type fakeClock struct {
	ns atomic.Int64
}

func newFakeClock() *fakeClock {
	cl := &fakeClock{}
	cl.ns.Store(testEpochNanos)
	return cl
}

func (c *fakeClock) now() time.Time {
	return time.Unix(0, c.ns.Load())
}

func (c *fakeClock) nowNano() int64 {
	return c.ns.Load()
}

func (c *fakeClock) add(d time.Duration) int64 {
	return c.ns.Add(int64(d))
}

func (c *fakeClock) set(ns int64) {
	c.ns.Store(ns)
}
