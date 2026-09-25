package ccache

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file holds the fixed-input contract materials for the ccache item
// lifecycle state machine (promotion / expiration / onDelete ownership):
// promote/delete races, overwrite ownership, fetch concurrency, callback
// reentrancy, clear/stop drains, a mid-sequence recoverable failure and the
// allocation baselines. Every case enters through public methods and drives
// determinism with a fake clock and synchronization barriers (no sleeps, no
// scheduler dependence). Failure messages identify the first violated
// contract with the item/key/state as evidence.

func lifecycleConfig(clock func() time.Time, recorder *lifecycleRecorder[string]) *Configuration[string] {
	return Configure[string]().
		MaxSize(5000).
		PromoteBuffer(1024).
		DeleteBuffer(1024).
		OnDelete(recorder.callback()).
		withClock(clock)
}

// The item moves New -> Promoted -> Deleted through public entries, and the
// state machine state is observable at each settling point.
func Test_Contract_LifecycleStateTransitions(t *testing.T) {
	clock := newFakeClock()
	recorder := newLifecycleRecorder[string]()
	cache := New(lifecycleConfig(clock.Now, recorder))
	defer cache.Stop()

	// Setnx is the public entry that installs an item without enqueuing a
	// promotion, so its New state is observable without racing the worker.
	cache.Setnx("k", "v1", time.Minute)
	item := itemPointer(cache, "k")
	if item.state != stateNew {
		t.Fatalf("first violation: after Setnx item state=%s, want new", item.state)
	}

	// Get enqueues a promotion; when it settles: New -> Promoted.
	cache.Get("k")
	cache.SyncUpdates()
	if item.state != statePromoted {
		t.Fatalf("first violation: after SyncUpdates item state=%s, want promoted", item.state)
	}
	verifyLifecycleInvariants(t, cache, "after-promote", recorder)

	// Overwrite sends the previous incarnation to the delete queue:
	// Promoted -> Deleted with exactly one callback; the new item is New.
	cache.Set("k", "v2", time.Minute)
	second := itemPointer(cache, "k")
	if second == item {
		t.Fatalf("first violation: overwrite reused the same item pointer")
	}
	cache.SyncUpdates()
	if item.state != stateDeleted {
		t.Fatalf("first violation: overwritten item state=%s, want deleted", item.state)
	}
	if got := recorder.countFor(item); got != 1 {
		t.Fatalf("first violation: overwritten item callbacks=%d, want exactly 1", got)
	}
	if second.state != statePromoted {
		t.Fatalf("first violation: replacement item state=%s, want promoted", second.state)
	}
	verifyLifecycleInvariants(t, cache, "after-overwrite", recorder)

	// Explicit delete of a listed item: Promoted -> Deleted.
	cache.Delete("k")
	cache.SyncUpdates()
	if second.state != stateDeleted {
		t.Fatalf("first violation: deleted replacement state=%s, want deleted", second.state)
	}
	if got := recorder.countFor(second); got != 1 {
		t.Fatalf("first violation: deleted item callbacks=%d, want exactly 1", got)
	}
	if cache.GetWithoutPromote("k") != nil {
		t.Fatalf("first violation: deleted key still resolvable")
	}
	verifyLifecycleInvariants(t, cache, "after-delete", recorder)
}

// An item deleted before its first promotion settles owns no callback:
// New -> Deleted (a queued promotion would later no-op on the terminal
// state). Setnx deterministically installs an item without enqueuing any
// promotion (existing public behavior), so the New -> Deleted path is
// observable without depending on worker select ordering.
func Test_Contract_DeleteBeforeFirstPromotion(t *testing.T) {
	clock := newFakeClock()
	recorder := newLifecycleRecorder[string]()
	cache := New(lifecycleConfig(clock.Now, recorder))
	defer cache.Stop()

	cache.Setnx("k", "v", time.Minute)
	if got := cache.Delete("k"); got != true {
		t.Fatalf("first violation: Delete returned %v, want true", got)
	}
	cache.SyncUpdates()
	if recorder.callbackCount() != 0 {
		t.Fatalf("first violation: callbacks for never-promoted item=%d, want 0 (it owned no callback)", recorder.callbackCount())
	}
	if cache.GetSize() != 0 || cache.ItemCount() != 0 {
		t.Fatalf("first violation: GetSize=%d ItemCount=%d, want 0/0 after delete-before-promote", cache.GetSize(), cache.ItemCount())
	}
	verifyLifecycleInvariants(t, cache, "delete-before-promote", recorder)
}

// A promoted item raced with Delete from the public Get (promote) and Delete
// entries. Either ordering of the queued events is legal, but both must
// converge: key gone, size zero, exactly one callback, invariants hold.
func Test_Contract_PromoteDeleteRaceFromBarrier(t *testing.T) {
	iterations := 2000
	if raceDetectorEnabled {
		iterations = 500
	}
	for iter := 0; iter < iterations; iter++ {
		clock := newFakeClock()
		recorder := newLifecycleRecorder[string]()
		cache := New(lifecycleConfig(clock.Now, recorder))

		// Existing promoted item owns exactly one callback.
		cache.Set("k", "v", time.Minute)
		cache.SyncUpdates()
		existing := itemPointer(cache, "k")

		var wg sync.WaitGroup
		wg.Add(2)
		gate := newSyncGate(2)
		go func() {
			defer wg.Done()
			gate.arrive()
			cache.Get("k") // enqueues a promotion if not expired
		}()
		go func() {
			defer wg.Done()
			gate.arrive()
			cache.Delete("k")
		}()
		wg.Wait()
		cache.SyncUpdates()

		if cache.GetWithoutPromote("k") != nil {
			t.Fatalf("iter %d: first violation: key still resolvable after promote/delete race", iter)
		}
		if cache.GetSize() != 0 {
			t.Fatalf("iter %d: first violation: GetSize=%d after race, want 0", iter, cache.GetSize())
		}
		if got := recorder.countFor(existing); got != 1 {
			t.Fatalf("iter %d: first violation: raced item callbacks=%d, want exactly 1", iter, got)
		}
		verifyLifecycleInvariants(t, cache, phasef(iter, "promote/delete race"), recorder)
		cache.Stop()
		verifySettledLifecycleInvariants(t, cache, phasef(iter, "promote/delete race settled"), recorder)
	}
}

// Overwriting a key: only the surviving incarnation is listed/sized, every
// replaced incarnation fires its callback exactly once and never returns
// through public entries afterwards.
func Test_Contract_OverwriteOwnership(t *testing.T) {
	clock := newFakeClock()
	recorder := newLifecycleRecorder[string]()
	cache := New(lifecycleConfig(clock.Now, recorder))
	defer cache.Stop()

	var versions []*Item[string]
	for i := 0; i < 4; i++ {
		cache.Set("k", string(rune('a'+i)), time.Minute)
		versions = append(versions, itemPointer(cache, "k"))
		cache.SyncUpdates()
	}
	verifyLifecycleInvariants(t, cache, "four overwrites", recorder)

	for i, item := range versions[:3] {
		if item.state != stateDeleted {
			t.Fatalf("first violation: overwritten version %d state=%s, want deleted", i, item.state)
		}
		if got := recorder.countFor(item); got != 1 {
			t.Fatalf("first violation: overwritten version %d callbacks=%d, want exactly 1", i, got)
		}
	}
	if got := cache.Get("k").Value(); got != "d" {
		t.Fatalf("first violation: public Get returned %q, want surviving value %q", got, "d")
	}
	if cache.ItemCount() != 1 || cache.GetSize() != 1 {
		t.Fatalf("first violation: ItemCount=%d GetSize=%d, want 1/1 after overwrites", cache.ItemCount(), cache.GetSize())
	}

	cache.Delete("k")
	cache.SyncUpdates()
	if recorder.callbackCount() != 4 {
		t.Fatalf("first violation: total callbacks=%d, want exactly 4 (3 overwrites + 1 delete)", recorder.callbackCount())
	}
	verifyLifecycleInvariants(t, cache, "after final delete", recorder)
}

// An item that expires while its very first promotion is still queued must
// never be admitted to the recency list, and no late promotion can resurrect
// it afterwards.
func Test_Contract_ExpiredItemNotResurrectedByLatePromotion(t *testing.T) {
	clock := newFakeClock()
	recorder := newLifecycleRecorder[string]()
	cache := New(lifecycleConfig(clock.Now, recorder))
	defer cache.Stop()

	// Promotion pending; the fake clock advances past TTL before the worker
	// settles it.
	cache.Set("late", "v", time.Minute)
	pending := itemPointer(cache, "late")
	clock.Advance(2 * time.Minute)
	cache.SyncUpdates()

	if pending.state != stateAbandoned {
		t.Fatalf("first violation: late-promoted expired item state=%s, want abandoned", pending.state)
	}
	if cache.GetSize() != 0 {
		t.Fatalf("first violation: GetSize=%d after late promotion of expired item, want 0", cache.GetSize())
	}
	verifyLifecycleInvariants(t, cache, "late promotion of expired item", recorder)

	// A later Get on the expired item returns it (documented Get semantics)
	// but enqueues no promotion: it cannot come back.
	item := cache.Get("late")
	if item == nil || !item.Expired() {
		t.Fatalf("first violation: expired Get returned expired=%v, want the stale item marked expired", item == nil)
	}
	cache.SyncUpdates()
	if pending.state != stateAbandoned || cache.GetSize() != 0 {
		t.Fatalf("first violation: expired item was resurrected: state=%s GetSize=%d", pending.state, cache.GetSize())
	}

	// Fetch replaces it with a fresh incarnation; the abandoned incarnation
	// never owned a callback.
	out, err := cache.Fetch("late", time.Minute, func() (string, error) { return "fresh", nil })
	if err != nil || out.Value() != "fresh" {
		t.Fatalf("first violation: Fetch replacement returned value=%q err=%v", out, err)
	}
	cache.SyncUpdates()
	if cache.GetSize() != 1 || cache.ItemCount() != 1 {
		t.Fatalf("first violation: after Fetch ItemCount=%d GetSize=%d, want 1/1", cache.ItemCount(), cache.GetSize())
	}
	if recorder.callbackCount() != 0 {
		t.Fatalf("first violation: abandoned incarnation produced %d callbacks, want 0", recorder.callbackCount())
	}
	verifyLifecycleInvariants(t, cache, "after fetch replacement", recorder)
}

// A promoted item that expires is not re-promoted by Get; Fetch replaces it
// and the old incarnation settles its single callback.
func Test_Contract_ExpiredPromotedItemSettlesOnce(t *testing.T) {
	clock := newFakeClock()
	recorder := newLifecycleRecorder[string]()
	cache := New(lifecycleConfig(clock.Now, recorder).GetsPerPromote(1))
	defer cache.Stop()

	cache.Set("k", "old", time.Minute)
	cache.SyncUpdates()
	old := itemPointer(cache, "k")
	clock.Advance(2 * time.Minute)

	// Gets on an expired item must not move it to the front / resurrect it.
	for i := 0; i < 5; i++ {
		cache.Get("k")
	}
	cache.SyncUpdates()
	if old.state != statePromoted || cache.GetSize() != 1 {
		t.Fatalf("first violation: expired promoted item moved: state=%s size=%d", old.state, cache.GetSize())
	}

	out, err := cache.Fetch("k", time.Minute, func() (string, error) { return "new", nil })
	if err != nil || out.Value() != "new" {
		t.Fatalf("first violation: Fetch returned value=%q err=%v", out, err)
	}
	cache.SyncUpdates()
	if old.state != stateDeleted {
		t.Fatalf("first violation: expired overwritten item state=%s, want deleted", old.state)
	}
	if got := recorder.countFor(old); got != 1 {
		t.Fatalf("first violation: expired overwritten item callbacks=%d, want exactly 1", got)
	}
	verifyLifecycleInvariants(t, cache, "expired overwrite", recorder)
}

// Many callers Fetch the same missing key behind a single barrier. Fetch has
// documented "no thundering-herd dedup" behavior; what must hold regardless
// of scheduling is: every caller succeeds, exactly one incarnation survives
// in the bucket/list, and replaced incarnations each fire at most once.
func Test_Contract_FetchSingleFlightConsistency(t *testing.T) {
	const callers = 32
	for iter := 0; iter < 50; iter++ {
		clock := newFakeClock()
		recorder := newLifecycleRecorder[string]()
		cache := New(lifecycleConfig(clock.Now, recorder))

		var fetchCalls int32
		gate := newSyncGate(callers)
		var wg sync.WaitGroup
		errs := make(chan error, callers)
		values := make(chan string, callers)
		wg.Add(callers)
		for i := 0; i < callers; i++ {
			go func() {
				defer wg.Done()
				gate.arrive()
				item, err := cache.Fetch("k", time.Minute, func() (string, error) {
					atomic.AddInt32(&fetchCalls, 1)
					return "value", nil
				})
				if err != nil {
					errs <- err
					return
				}
				values <- item.Value()
			}()
		}
		wg.Wait()
		close(errs)
		close(values)
		for err := range errs {
			t.Fatalf("iter %d: first violation: concurrent Fetch returned %v", iter, err)
		}
		for value := range values {
			if value != "value" {
				t.Fatalf("iter %d: first violation: concurrent Fetch returned %q, want %q", iter, value, "value")
			}
		}

		// All caller-visible reads finish before the final settle, so that
		// no Get can enqueue a promotion that races the settled inspection.
		if cache.Get("k").Value() != "value" {
			t.Fatalf("iter %d: first violation: surviving item has wrong value", iter)
		}
		cache.SyncUpdates()
		if cache.ItemCount() != 1 || cache.GetSize() != 1 {
			t.Fatalf("iter %d: first violation: after concurrent Fetch ItemCount=%d GetSize=%d, want 1/1",
				iter, cache.ItemCount(), cache.GetSize())
		}
		verifyLifecycleInvariants(t, cache, phasef(iter, "concurrent fetch"), recorder)
		cache.Stop()
		verifySettledLifecycleInvariants(t, cache, phasef(iter, "concurrent fetch settled"), recorder)
	}
}

// The onDelete callback runs on the worker while holding no bucket/global
// lock: re-entrant Get/Set/Delete/Fetch/Replace calls from inside the
// callback must complete, not deadlock, and callbacks stay at-most-once.
func Test_Contract_OnDeleteCallbackReentrant(t *testing.T) {
	clock := newFakeClock()
	recorder := newLifecycleRecorder[string]()
	var cache *Cache[string]
	cache = New(lifecycleConfig(clock.Now, recorder))
	defer cache.Stop()

	cache.Set("a", "1", time.Minute)
	cache.Set("b", "2", time.Minute)
	cache.Set("trigger", "x", time.Minute)
	cache.SyncUpdates()

	// Swap in a re-entrant callback (same configuration seam, still public
	// entries from within the worker goroutine).
	cache.Configuration.onDelete = func(item *Item[string]) {
		recorder.callback()(item)
		if item.key != "trigger" {
			return
		}
		mustComplete(t, "reentrant Get", 2*time.Second, func() {
			if got := cache.Get("a"); got == nil || got.Value() != "1" {
				t.Errorf("reentrant Get saw unexpected state")
			}
		})
		mustComplete(t, "reentrant Set", 2*time.Second, func() {
			cache.Set("c", "3", time.Minute)
		})
		mustComplete(t, "reentrant Delete", 2*time.Second, func() {
			cache.Delete("b")
		})
		mustComplete(t, "reentrant Fetch", 2*time.Second, func() {
			if _, err := cache.Fetch("d", time.Minute, func() (string, error) { return "4", nil }); err != nil {
				t.Errorf("reentrant Fetch returned %v", err)
			}
		})
		mustComplete(t, "reentrant Replace", 2*time.Second, func() {
			cache.Replace("a", "11")
		})
	}

	mustComplete(t, "Delete trigger", 2*time.Second, func() {
		cache.Delete("trigger")
	})
	cache.SyncUpdates()
	verifyLifecycleInvariants(t, cache, "after reentrant callback", recorder)
	if cache.Get("a").Value() != "11" {
		t.Fatalf("first violation: reentrant Replace did not take effect")
	}
}

// Clear empties buckets, queues, list and size, abandons every item (no
// resurrection by queued promotions), and the cache stays usable afterwards.
// Stop drains already-issued deletes so their callbacks settle.
func Test_Contract_ClearAndStopDrain(t *testing.T) {
	clock := newFakeClock()
	recorder := newLifecycleRecorder[string]()
	cache := New(lifecycleConfig(clock.Now, recorder))

	cache.Set("a", "1", time.Minute)
	cache.Set("b", "2", time.Minute)
	cache.Set("c", "3", -time.Minute) // expired before its promotion settles
	cache.SyncUpdates()
	if cache.GetSize() != 2 {
		t.Fatalf("first violation: GetSize=%d before Clear, want 2 (expired item not admitted)", cache.GetSize())
	}

	cache.Clear()
	if cache.ItemCount() != 0 || cache.GetSize() != 0 {
		t.Fatalf("first violation: after Clear ItemCount=%d GetSize=%d, want 0/0", cache.ItemCount(), cache.GetSize())
	}
	verifyLifecycleInvariants(t, cache, "after clear", recorder)
	if cache.Get("a") != nil {
		t.Fatalf("first violation: cleared key resolvable")
	}

	// Usable after clear; newly set items behave normally.
	cache.Set("n", "x", time.Minute)
	cache.SyncUpdates()
	if cache.ItemCount() != 1 || cache.GetSize() != 1 {
		t.Fatalf("first violation: after Clear+Set ItemCount=%d GetSize=%d, want 1/1", cache.ItemCount(), cache.GetSize())
	}
	// Explicit delete after clear drains on Stop and fires exactly once.
	cache.Delete("n")
	cache.Stop()
	if recorder.callbackCount() != 1 {
		t.Fatalf("first violation: callbacks after Stop drain=%d, want exactly 1 (cleared items own none)", recorder.callbackCount())
	}
}

var errInject = errors.New("injected recoverable fetch failure")

// Mid-acceptance failure story: a Fetch failure (the public error layer) must
// leave no value, count, callback or temporary state behind; the exact same
// inputs then replay deterministically, including ordering changes that are
// semantically irrelevant.
func Test_Contract_RecoverableFailureAndReplayStability(t *testing.T) {
	keys := []string{"a", "b", "c", "miss", "expired"}
	script := func(run string) cacheSnapshot {
		clock := newFakeClock()
		recorder := newLifecycleRecorder[string]()
		cache := New(lifecycleConfig(clock.Now, recorder))
		defer cache.Stop()

		fetches := 0
		fetcher := func() (string, error) {
			fetches++
			// The first two fetches (missing key, then expired key) fail
			// through the public error result; later calls succeed.
			if fetches <= 2 {
				return "", errInject
			}
			return "loaded", nil
		}

		// Stage 1: establish shared/persistent state.
		cache.Set("a", "1", time.Minute)
		cache.Set("b", "2", time.Minute)
		cache.Set("expired", "old", time.Minute)
		cache.SyncUpdates()
		clock.Advance(2 * time.Minute)

		// Stage 2 (mid-sequence injected, recoverable failure): Fetch a
		// missing key fails through the public error result.
		if _, err := cache.Fetch("miss", time.Minute, fetcher); !errors.Is(err, errInject) {
			t.Fatalf("%s: first violation at failure injection: err=%v, want errInject", run, err)
		}
		if cache.GetWithoutPromote("miss") != nil {
			t.Fatalf("%s: first violation: failed Fetch left a cached value", run)
		}
		if cache.GetSize() != 3 || cache.ItemCount() != 3 {
			t.Fatalf("%s: first violation: failed Fetch mutated cache counters: GetSize=%d ItemCount=%d, want 3/3",
				run, cache.GetSize(), cache.ItemCount())
		}
		verifyLifecycleInvariants(t, cache, run+" failed fetch", recorder)

		// Failure on an existing expired key returns the same error layer and
		// leaves the stale item in place (documented Fetch behavior).
		before := cache.GetWithoutPromote("expired")
		if _, err := cache.Fetch("expired", time.Minute, fetcher); !errors.Is(err, errInject) {
			t.Fatalf("%s: first violation at expired-key failure: err=%v, want errInject", run, err)
		}
		after := cache.GetWithoutPromote("expired")
		if before != after || after.Value() != "old" {
			t.Fatalf("%s: first violation: failed Fetch over an expired key mutated the stored item", run)
		}
		verifyLifecycleInvariants(t, cache, run+" expired failed fetch", recorder)

		// Stage 3: recovery with the exact same inputs now succeeds.
		out, err := cache.Fetch("miss", time.Minute, fetcher)
		if err != nil || out.Value() != "loaded" {
			t.Fatalf("%s: first violation at recovery: value=%v err=%v", run, out, err)
		}
		cache.Set("c", "3", time.Minute)
		if ok := cache.Replace("a", "11"); !ok {
			t.Fatalf("%s: first violation: Replace returned false for existing key", run)
		}
		cache.Delete("b")
		cache.SyncUpdates()
		verifyLifecycleInvariants(t, cache, run+" recovered sequence", recorder)
		return takeSnapshot(cache, recorder, keys)
	}

	first := script("first run")
	replay := script("identical replay")
	assertSnapshotsEqual(t, "failure/replay", first, replay, sortedKeys(keys), true)
}

// The end-to-end acceptance story: one run walks every contract stage through
// the public entries while recording caller-visible results and internal
// invariants at each stage; a second run replays the same logical inputs with
// reordered commutative stages and different scheduling, and must produce the
// same observable outcome.
func Test_Contract_AcceptanceStory(t *testing.T) {
	keys := []string{"keep", "race", "hot", "gone", "late", "flaky"}

	story := func(run string, reorder bool) cacheSnapshot {
		clock := newFakeClock()
		recorder := newLifecycleRecorder[string]()
		cache := New(lifecycleConfig(clock.Now, recorder).GetsPerPromote(1))
		stage := 0
		checkpoint := func() {
			stage++
			cache.SyncUpdates()
			verifyLifecycleInvariants(t, cache, phasef(stage, run), recorder)
		}

		// Stage: seed state, including a delete-before-promote and an
		// overwrite (ownership transfers).
		cache.Set("keep", "v", time.Hour)
		cache.Set("gone", "x", time.Hour)
		cache.SyncUpdates() // settle promotions so ownership outcomes are order-independent
		cache.Delete("gone")
		cache.Set("keep", "v2", time.Hour)
		checkpoint()

		// Stage: promote/delete race on a promoted item, from a barrier.
		cache.Set("race", "r", time.Hour)
		cache.SyncUpdates()
		gate := newSyncGate(2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); gate.arrive(); cache.Get("race") }()
		go func() { defer wg.Done(); gate.arrive(); cache.Delete("race") }()
		wg.Wait()
		checkpoint()
		if cache.Get("race") != nil {
			t.Fatalf("%s: first violation: raced key survived delete", run)
		}

		// Stage: late promotion of an expired item must not resurrect it.
		cache.Set("late", "l", time.Minute)
		clock.Advance(2 * time.Minute)
		checkpoint()
		if cache.GetSize() != 1 {
			t.Fatalf("%s: first violation: GetSize=%d, want 1 (only keep survives)", run, cache.GetSize())
		}

		// Stage: concurrent fetch of one hot key from a barrier.
		const callers = 8
		hotGate := newSyncGate(callers)
		wg.Add(callers)
		for i := 0; i < callers; i++ {
			go func() {
				defer wg.Done()
				hotGate.arrive()
				if _, err := cache.Fetch("hot", time.Hour, func() (string, error) { return "h", nil }); err != nil {
					t.Errorf("%s: concurrent fetch failed: %v", run, err)
				}
			}()
		}
		wg.Wait()
		checkpoint()

		// Stage: injected recoverable failure, then identical retry.
		attempts := 0
		flaky := func() (string, error) {
			attempts++
			if attempts == 1 {
				return "", errInject
			}
			return "f", nil
		}
		if _, err := cache.Fetch("flaky", time.Hour, flaky); !errors.Is(err, errInject) {
			t.Fatalf("%s: first violation: injected failure err=%v, want errInject", run, err)
		}
		if cache.GetWithoutPromote("flaky") != nil {
			t.Fatalf("%s: first violation: failed fetch left cached state", run)
		}
		item, err := cache.Fetch("flaky", time.Hour, flaky)
		if err != nil || item.Value() != "f" {
			t.Fatalf("%s: first violation: retry after failure returned value=%v err=%v", run, item, err)
		}
		checkpoint()

		// Stage: reorder-sensitive commutative tail. In the reordered run the
		// delete of "keep" happens before the extra set; both orders commute.
		if reorder {
			cache.Delete("keep")
			cache.Set("extra", "e", time.Hour)
		} else {
			cache.Set("extra", "e", time.Hour)
			cache.Delete("keep")
		}
		checkpoint()

		snap := takeSnapshot(cache, recorder, append(append([]string(nil), keys...), "extra"))
		cache.Stop()
		verifySettledLifecycleInvariants(t, cache, run+" settled", recorder)
		return snap
	}

	first := story("first pass", false)
	second := story("reordered pass", true)
	allKeys := append(append([]string(nil), keys...), "extra")
	// The concurrent-fetch stage legitimately races promotions against
	// overwrite deletes, so its total callback count is scheduling dependent;
	// every stage still proves callbacks fire at most once per item. The
	// caller-visible values, ItemCount and GetSize must be replay-stable.
	assertSnapshotsEqual(t, "acceptance story", first, second, sortedKeys(allKeys), false)
}

// Allocation baselines for the hot public paths. Measured on the pre-change
// implementation: Get hit 0, Set 1, Delete miss 0, Fetch hit 0 allocations.
// Budgets leave headroom of one allocation; the lifecycle refactor must not
// regress the performance level of the public entries. Skipped under -race
// because the detector instruments allocations.
func Test_Contract_AllocationBaseline(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("allocation baseline is measured without the race detector")
	}
	cache := New(Configure[string]())
	defer cache.Stop()
	cache.Set("k", "v", time.Minute)
	cache.SyncUpdates()

	assertBudget := func(name string, budget float64, fn func()) {
		t.Helper()
		if got := testing.AllocsPerRun(200, fn); got > budget {
			t.Fatalf("first allocation regression: %s allocates %.0f/op, budget %.0f", name, got, budget)
		}
	}
	assertBudget("Get hit", 1, func() { cache.Get("k") })
	assertBudget("Get miss", 1, func() { cache.Get("absent") })
	assertBudget("Set", 2, func() { cache.Set("k", "v", time.Minute) })
	assertBudget("Delete miss", 1, func() { cache.Delete("absent") })
	assertBudget("Fetch hit", 1, func() {
		if _, err := cache.Fetch("k", time.Minute, func() (string, error) { return "v", nil }); err != nil {
			t.Fatalf("fetch failed: %v", err)
		}
	})
	assertBudget("Replace", 2, func() { cache.Replace("k", "v") })
}
