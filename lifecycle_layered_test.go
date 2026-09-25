package ccache

import (
	"sync"
	"testing"
	"time"
)

// The LayeredCache drives items through the exact same lifecycle state
// machine and fake-clock seam as the Cache. These cases cover the shared
// machinery through its second public entry point.
func Test_Contract_LayeredExpiredNotResurrectedAndOwnership(t *testing.T) {
	clock := newFakeClock()
	recorder := newLifecycleRecorder[string]()
	cache := Layered(Configure[string]().OnDelete(recorder.callback()).withClock(clock.Now))
	defer cache.Stop()

	cache.Set("p", "s1", "a", time.Minute)
	cache.SyncUpdates()
	cache.Set("p", "s2", "b", time.Minute)
	cache.SyncUpdates()

	// Pending promotion that arrives after expiration is abandoned, not
	// admitted into the recency list.
	cache.Set("p", "s3", "c", time.Minute)
	clock.Advance(2 * time.Minute)
	cache.SyncUpdates()
	if cache.GetSize() != 2 {
		t.Fatalf("first violation: layered GetSize=%d, want 2 (expired late item not admitted)", cache.GetSize())
	}

	// Overwrite a promoted secondary key: exactly one callback for the old
	// incarnation.
	old := cache.GetWithoutPromote("p", "s1")
	cache.Set("p", "s1", "a2", time.Hour)
	cache.SyncUpdates()
	if old.state != stateDeleted || recorder.countFor(old) != 1 {
		t.Fatalf("first violation: layered overwrite state=%s callbacks=%d", old.state, recorder.countFor(old))
	}
	if got := cache.Get("p", "s1").Value(); got != "a2" {
		t.Fatalf("first violation: layered overwrite visible value=%q, want a2", got)
	}

	// DeleteAll removes the whole primary group.
	if cache.DeleteAll("p") != true {
		t.Fatalf("first violation: DeleteAll returned false")
	}
	cache.SyncUpdates()
	if cache.GetSize() != 0 || cache.ItemCount() != 0 {
		t.Fatalf("first violation: after DeleteAll GetSize=%d ItemCount=%d, want 0/0", cache.GetSize(), cache.ItemCount())
	}
	recorder.assertAtMostOnce(t, "layered delete-all")
}

func Test_Contract_LayeredPromoteDeleteRace(t *testing.T) {
	iterations := 300
	if raceDetectorEnabled {
		iterations = 100
	}
	for iter := 0; iter < iterations; iter++ {
		clock := newFakeClock()
		recorder := newLifecycleRecorder[string]()
		cache := Layered(Configure[string]().OnDelete(recorder.callback()).withClock(clock.Now))

		cache.Set("p", "s", "v", time.Minute)
		cache.SyncUpdates()
		existing := cache.GetWithoutPromote("p", "s")

		gate := newSyncGate(2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); gate.arrive(); cache.Get("p", "s") }()
		go func() { defer wg.Done(); gate.arrive(); cache.Delete("p", "s") }()
		wg.Wait()
		cache.SyncUpdates()

		if cache.GetWithoutPromote("p", "s") != nil || cache.GetSize() != 0 {
			t.Fatalf("iter %d: first violation: layered race left item behind", iter)
		}
		if recorder.countFor(existing) != 1 {
			t.Fatalf("iter %d: first violation: layered raced item callbacks=%d, want 1", iter, recorder.countFor(existing))
		}
		cache.Stop()
	}
}

func Test_Contract_LayeredClearUsableAfterwards(t *testing.T) {
	clock := newFakeClock()
	recorder := newLifecycleRecorder[string]()
	cache := Layered(Configure[string]().OnDelete(recorder.callback()).withClock(clock.Now))

	cache.Set("p", "s1", "1", time.Minute)
	cache.Set("p", "s2", "2", time.Minute)
	cache.Set("q", "s1", "3", -time.Minute)
	cache.SyncUpdates()
	if cache.GetSize() != 2 {
		t.Fatalf("first violation: layered GetSize=%d before clear, want 2", cache.GetSize())
	}

	cache.Clear()
	if cache.ItemCount() != 0 || cache.GetSize() != 0 {
		t.Fatalf("first violation: after layered Clear ItemCount=%d GetSize=%d, want 0/0", cache.ItemCount(), cache.GetSize())
	}
	cache.Set("r", "s", "4", time.Minute)
	cache.SyncUpdates()
	if cache.ItemCount() != 1 || cache.GetSize() != 1 {
		t.Fatalf("first violation: layered cache unusable after Clear: ItemCount=%d GetSize=%d", cache.ItemCount(), cache.GetSize())
	}
	cache.Stop()
}
