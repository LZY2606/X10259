package ccache

import "time"

// clock is the single seam through which cache internals observe the current
// time. Production caches use the system clock; tests can inject a fake clock
// to advance time deterministically. The fake clock shares the exact same
// expiration/promotion code paths as production code.
type clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}

func resolveClock(c *Configuration) clock {
	if c.clock != nil {
		return c.clock
	}
	return systemClock{}
}
