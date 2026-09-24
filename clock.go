package ccache

import "time"

// clock is the single seam used for every time-sensitive decision made by the
// cache (item expiry, TTL computations, promotion of stale items). Production
// code is always driven by systemClock; the package's tests inject a
// controllable fake clock through a test-only constructor so that expiry and
// "late promotion" races can be reproduced without sleeping or relying on the
// scheduler.
type clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}

func nowOf(c clock) time.Time {
	if c == nil {
		return time.Now()
	}
	return c.Now()
}
