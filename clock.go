package ccache

import "time"

// clock abstracts the source of time used for item expiration. Production
// caches always run on realClock. Tests install a fake clock through
// Configuration.withClock so that expiration can be driven deterministically;
// the fake clock flows through the exact same production code paths
// (bucket.set, bucket.setnx, Item.Expired, Item.TTL, Item.Extend and the
// LayeredCache.Get freshness check), there is no test-only branch.
type clock interface {
	now() time.Time
}

type realClock struct{}

func (realClock) now() time.Time {
	return time.Now()
}

// resolveClock returns c, or the real clock when c is nil. The nil case exists
// so that zero-value buckets and items (built directly by older tests) keep
// working against wall time.
func resolveClock(c clock) clock {
	if c == nil {
		return realClock{}
	}
	return c
}
