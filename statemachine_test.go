package ccache

// Regression material for the item lifecycle state machine (promotion,
// expiration and delete-callback ownership across bucket, promotables and
// the delete queue). Every scenario enters through the cache's public
// entry points (Get/Set/Fetch/Replace/Delete/Extend/Clear/Stop and the
// control commands); internal state is only ever *observed*, after the
// worker has been quiesced via SyncUpdates, never fabricated.
//
// The suite runs under `go test -race` (see `make t`); the concurrency
// scenarios exist to give the race detector and the invariant checks
// something to bite on.

import (
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/karlseguin/ccache/v3/assert"
)

// fakeClock replaces the package clock (the same clock the production
// expiry/promotion code reads) so tests can advance time deterministically.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func installFakeClock(t *testing.T, start time.Time) *fakeClock {
	t.Helper()
	fc := &fakeClock{now: start}
	previous := timeNow
	timeNow = fc.Now
	t.Cleanup(func() { timeNow = previous })
	return fc
}

func (fc *fakeClock) Now() time.Time {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.now
}

func (fc *fakeClock) Advance(d time.Duration) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.now = fc.now.Add(d)
}

// deleteRecorder collects onDelete callbacks (invoked on the cache worker
// goroutine) in a race-safe way.
type deleteRecorder struct {
	mu   sync.Mutex
	keys []string
}

func (r *deleteRecorder) callback() func(*Item[int]) {
	return func(item *Item[int]) {
		r.mu.Lock()
		r.keys = append(r.keys, item.Key())
		r.mu.Unlock()
	}
}

func (r *deleteRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.keys)
}

func (r *deleteRecorder) sorted() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.keys))
	copy(out, r.keys)
	sort.Strings(out)
	return out
}

// A promotion queued before a delete races it in the worker; whichever
// order the worker picks, the deleted item must end up deleted, stay
// deleted, and fire its callback exactly once.
func Test_StateMachine_LatePromotionNeverRevivesDeletedItem(t *testing.T) {
	recorder := &deleteRecorder{}
	cache := New(Configure[int]().GetsPerPromote(1).OnDelete(recorder.callback()))
	defer cache.Stop()

	var last *Item[int]
	for round := 0; to 300 {
	}
	_ = last
}
