package ccache

import (
	"sync/atomic"
	"time"
)

// clock is the single time source for every expiry computation in the cache:
// item creation (bucket.set/setnx/setnx2), Item.Expired, Item.TTL, Item.Extend
// and the worker's promotion path all read the clock through timeNow. There
// are no test-only branches in that logic; tests in this package may swap the
// source via setClockForTest and therefore exercise the exact production code
// path with a fake clock.
//
// The value is stored atomically so that swapping the clock never races with
// the background worker goroutine of any live cache.
var clock atomic.Value // stores func() time.Time

func init() {
	clock.Store(func() time.Time { return time.Now() })
}

func timeNow() time.Time {
	return clock.Load().(func() time.Time)()
}

// setClockForTest replaces the cache-wide clock and returns a function that
// restores the previous one. It is unexported and only used by white-box
// tests in this package; production code always observes the real clock
// installed by init.
func setClockForTest(now func() time.Time) func() {
	previous := clock.Load().(func() time.Time)
	clock.Store(now)
	return func() { clock.Store(previous) }
}
