package ccache

import "time"

// This file contains package-internal seams used to make the cache's
// time-dependent and asynchronously-scheduled behavior deterministically
// testable. Nothing here is exported.
//
// Both seams are passive in production:
//   - "now" defaults to time.Now and is read through the exact same code path
//     that the public methods use. It does not fork expiry semantics; a cached
//     value is evaluated against one clock in exactly one place.
//   - "workerBarrier" defaults to a permanently-open barrier, so the worker
//     goroutine runs exactly as it does without the seam. When a test closes
//     the barrier, the worker parks instead of draining the queues; the test
//     then drives the *same* lifecycle.applyPromote/applyDelete methods that the
//     worker would call. There is no test-only copy of the algorithm.

// clockFunc returns the cache's notion of the current time.
type clockFunc func() time.Time

// workerGate is a synchronization barrier for the background worker.
type workerGate interface {
	// wait blocks while the barrier is closed. A closed gate parks the worker
	// before its next select, with no queue item consumed.
	wait()
}

// openGate is the production default: it never blocks.
type openGate struct{}

func (openGate) wait() {}

// withClock overrides the clock used for item expiry. It is intended for
// in-package tests which need a controllable, monotonic clock; production code
// uses the time.Now default installed by Configure.
func (c *Configuration[T]) withClock(now func() time.Time) *Configuration[T] {
	c.now = now
	return c
}

// withWorkerGate installs a synchronization barrier for the background worker.
// See workerGate for the contract.
func (c *Configuration[T]) withWorkerGate(gate workerGate) *Configuration[T] {
	c.workerBarrier = gate
	return c
}

// blockingGate is a reusable close/open barrier for worker scheduling in tests.
type blockingGate struct {
	resume chan struct{}
}

func newBlockingGate() *blockingGate {
	g := &blockingGate{resume: make(chan struct{})}
	g.open()
	return g
}

// open releases any parked (or future) worker wait permanently.
func (g *blockingGate) open() {
	select {
	case <-g.resume:
		// already open
	default:
		close(g.resume)
	}
}

// block installs a fresh closed barrier.
func (g *blockingGate) block() {
	g.resume = make(chan struct{})
}

func (g *blockingGate) wait() {
	<-g.resume
}
