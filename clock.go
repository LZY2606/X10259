package ccache

import "time"

// clock is the single seam through which time-dependent state (item expiry)
// reads "now". Production caches are always wired to systemClock; the fake
// clock used by deterministic tests is package-private and shares exactly this
// interface with the production path - there is no test-only branching inside
// the cache or state machine.
type clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}

var realClock clock = systemClock{}
