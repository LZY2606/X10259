package ccache

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

// Two complementary groups live here:
//
//  1. A fixed-seed, budgeted property test that runs a scripted mix of public
//     operations against the cache and against an independent reference model
//     (a fake clock makes expiration deterministic). The first diverging step
//     is reported with the operation, key and expected/observed state.
//  2. Order-stability runs that only change result-irrelevant enumeration and
//     scheduling conditions (bucket count, op permutation, goroutine
//     scheduling of disjoint-key writes) and assert identical outcomes.
//
// Neither group relies on map iteration order, real scheduler timing or local
// machine configuration.

// modelIncarnation is the model's view of one physical Item incarnation.
type modelIncarnation struct {
	value   string
	expires int64 // nanos on the fake clock
	decided bool  // a SyncUpdates has settled this incarnation's promotion
	listed  bool  // admitted to the recency list at settlement
}

type lifecycleModel struct {
	entries map[string]*modelIncarnation
	count   int // listed incarnations = expected GetSize
}

func newLifecycleModel() *lifecycleModel {
	return &lifecycleModel{entries: make(map[string]*modelIncarnation)}
}

func (m *lifecycleModel) settle(nowNanos int64) {
	for key, inc := range m.entries {
		if inc.decided {
			continue
		}
		inc.decided = true
		if inc.expires > nowNanos {
			inc.listed = true
			m.count++
		} else {
			inc.listed = false
		}
		_ = key
	}
}

func (m *lifecycleModel) set(key, value string, expires int64) {
	if old := m.entries[key]; old != nil && old.listed {
		m.count--
	}
	m.entries[key] = &modelIncarnation{value: value, expires: expires}
}

func (m *lifecycleModel) delete(key string) {
	if inc := m.entries[key]; inc != nil {
		if inc.listed {
			m.count--
		}
		delete(m.entries, key)
	}
}

func (m *lifecycleModel) clear() {
	m.entries = make(map[string]*modelIncarnation)
	m.count = 0
}

// recordedOp keeps enough context to point at the first divergence.
type recordedOp struct {
	step int
	name string
	key  string
}

func (o recordedOp) String() string {
	return fmt.Sprintf("step %d %s(%q)", o.step, o.name, o.key)
}

// Fixed seed and explicit scale budget: the run is reproducible on any
// machine and bounded in work.
const (
	modelSeed int64 = 20260919
	modelOps        = 3000
	modelKeys       = 64
)

func Test_Contract_PropertyModelConformance(t *testing.T) {
	clock := newFakeClock()
	recorder := newLifecycleRecorder[string]()
	cache := New(Configure[string]().
		MaxSize(1_000_000). // no pressure eviction: model owns that axis
		OnDelete(recorder.callback()).
		withClock(clock.Now))
	defer cache.Stop()

	model := newLifecycleModel()
	rng := rand.New(rand.NewSource(modelSeed))
	recent := make([]recordedOp, 0, 16)

	checkAgainstModel := func(op recordedOp) {
		t.Helper()
		// Fixed key list, never map enumeration.
		for ki := 0; ki < modelKeys; ki++ {
			key := fmt.Sprintf("k%02d", ki)
			item := cache.GetWithoutPromote(key)
			inc, wantPresent := model.entries[key]
			if wantPresent != (item != nil) {
				t.Fatalf("first divergence at %s: key %q present model=%v cache=%v; recent ops=%v",
					op, key, wantPresent, item != nil, recent)
			}
			if wantPresent && item.Value() != inc.value {
				t.Fatalf("first divergence at %s: key %q value model=%q cache=%q; recent ops=%v",
					op, key, inc.value, item.Value(), recent)
			}
			if item != nil {
				if got, want := item.Expired(), item.Expires().Before(clock.Now()); got != want ||
					(inc.expires <= clock.Now().UnixNano()) != got {
					t.Fatalf("first divergence at %s: key %q Expired()=%v expires=%s now=%s",
						op, key, got, item.Expires(), clock.Now())
				}
			}
		}
		if got := cache.ItemCount(); got != len(model.entries) {
			t.Fatalf("first divergence at %s: ItemCount cache=%d model=%d; recent ops=%v",
				op, got, len(model.entries), recent)
		}
		if got := cache.GetSize(); got != int64(model.count) {
			t.Fatalf("first divergence at %s: GetSize cache=%d model=%d; recent ops=%v",
				op, got, model.count, recent)
		}
		recorder.assertAtMostOnce(t, op.String())
	}

	ttls := []time.Duration{time.Minute, 5 * time.Minute, time.Hour, -time.Minute}
	for step := 0; step < modelOps; step++ {
		key := fmt.Sprintf("k%02d", rng.Intn(modelKeys))
		op := recordedOp{step: step, key: key}

		// The clock is only advanced with everything settled, so the
		// model's promotion decision uses the same instant as the worker.
		switch n := rng.Intn(100); {
		case n < 30:
			op.name = "Set"
			value := fmt.Sprintf("v%d", step)
			ttl := ttls[rng.Intn(len(ttls))]
			cache.Set(key, value, ttl)
			cache.SyncUpdates()
			model.settle(clock.Now().UnixNano())
			model.set(key, value, clock.Now().Add(ttl).UnixNano())
			model.settle(clock.Now().UnixNano())
		case n < 60:
			op.name = "Get"
			cache.Get(key)
		case n < 75:
			op.name = "Fetch"
			value := fmt.Sprintf("f%d", step)
			inc, present := model.entries[key]
			nowNanos := clock.Now().UnixNano()
			willFetch := !present || inc.expires <= nowNanos
			var fetchErr error
			if willFetch && rng.Intn(7) == 0 {
				fetchErr = errInject
			}
			item, err := cache.Fetch(key, time.Minute, func() (string, error) {
				if fetchErr != nil {
					return "", fetchErr
				}
				return value, nil
			})
			cache.SyncUpdates()
			model.settle(clock.Now().UnixNano())
			if willFetch && fetchErr == nil {
				model.set(key, value, clock.Now().Add(time.Minute).UnixNano())
				model.settle(clock.Now().UnixNano())
			} else if willFetch && fetchErr != nil {
				if err != errInject || item != nil {
					t.Fatalf("first divergence at %s: failed fetch returned item=%v err=%v, want nil/%v", op, item, err, fetchErr)
				}
			} else if err != nil {
				t.Fatalf("first divergence at %s: cache-hit fetch returned err=%v", op, err)
			}
		case n < 85:
			op.name = "Replace"
			value := fmt.Sprintf("r%d", step)
			cache.Replace(key, value)
			cache.SyncUpdates()
			model.settle(clock.Now().UnixNano())
			if inc, present := model.entries[key]; present {
				// Replace preserves the absolute expiry (TTL at replace time).
				cache.SyncUpdates()
				model.set(key, value, inc.expires)
				model.settle(clock.Now().UnixNano())
			}
		case n < 95:
			op.name = "Delete"
			cache.Delete(key)
			cache.SyncUpdates()
			model.settle(clock.Now().UnixNano())
			model.delete(key)
		case n < 98:
			op.name = "AdvanceClock"
			cache.SyncUpdates()
			clock.Advance(time.Duration(30+rng.Intn(150)) * time.Second)
			model.settle(clock.Now().UnixNano())
		default:
			op.name = "Clear"
			key = ""
			cache.Clear()
			model.clear()
		}

		recent = append(recent, op)
		if len(recent) > 16 {
			recent = recent[1:]
		}
		if step%32 == 0 || step == modelOps-1 {
			cache.SyncUpdates()
			model.settle(clock.Now().UnixNano())
			checkAgainstModel(op)
		}
	}
}

// The same logical input multiset applied with result-irrelevant enumeration
// and scheduling variations must converge on identical caller-visible state.
func Test_Contract_OrderStabilityAcrossEnumerationAndScheduling(t *testing.T) {
	const keyCount = 96
	makeKeys := func() []string {
		keys := make([]string, 0, keyCount)
		for i := 0; i < keyCount; i++ {
			keys = append(keys, fmt.Sprintf("ord%03d", i))
		}
		return keys
	}

	run := func(seed int64, buckets uint32, concurrent bool) cacheSnapshot {
		clock := newFakeClock()
		recorder := newLifecycleRecorder[string]()
		cache := New(Configure[string]().
			MaxSize(1_000_000).
			Buckets(buckets).
			OnDelete(recorder.callback()).
			withClock(clock.Now))
		defer cache.Stop()

		keys := makeKeys()
		rng := rand.New(rand.NewSource(seed))
		order := rng.Perm(keyCount)

		setKey := func(i int) {
			ki := order[i]
			cache.Set(keys[ki], fmt.Sprintf("v%d", ki), time.Hour)
		}
		if concurrent {
			// Barrier-release all writers: scheduling is arbitrary, but the
			// keys are disjoint, so the outcome must not depend on it.
			gate := newSyncGate(keyCount)
			var wg sync.WaitGroup
			wg.Add(keyCount)
			for i := 0; i < keyCount; i++ {
				go func(i int) {
					defer wg.Done()
					gate.arrive()
					setKey(i)
				}(i)
			}
			wg.Wait()
		} else {
			for i := 0; i < keyCount; i++ {
				setKey(i)
			}
		}
		cache.SyncUpdates()

		// Deletes commute across disjoint keys; try them in reverse order in
		// one of the runs.
		if seed == modelSeed+2 {
			for i := keyCount - 2; i >= 0; i -= 2 {
				cache.Delete(keys[i])
			}
		} else {
			for i := 0; i < keyCount; i += 2 {
				cache.Delete(keys[i])
			}
		}
		cache.SyncUpdates()
		snap := takeSnapshot(cache, recorder, keys)
		// Callback total is deterministic here (every promotion settles
		// before the matching delete); pin it explicitly.
		if snap.callbacks != keyCount/2 {
			t.Fatalf("seed %d buckets %d concurrent=%v: callbacks=%d, want %d (exactly one per deleted, fully-promoted item)",
				seed, buckets, concurrent, snap.callbacks, keyCount/2)
		}
		return snap
	}

	// Only vary things irrelevant to results: seed (input permutation),
	// bucket count (hash enumeration), and concurrent scheduling.
	variants := []struct {
		seed       int64
		buckets    uint32
		concurrent bool
	}{
		{modelSeed + 1, 16, false},
		{modelSeed + 2, 16, false}, // reversed delete order
		{modelSeed + 1, 1, false},  // single bucket
		{modelSeed + 1, 64, false},
		{modelSeed + 1, 16, true}, // barrier-scheduled concurrent writes
		{modelSeed + 3, 32, true},
	}
	keys := makeKeys()
	sort.Strings(keys)
	baseline := run(variants[0].seed, variants[0].buckets, variants[0].concurrent)
	for _, v := range variants[1:] {
		got := run(v.seed, v.buckets, v.concurrent)
		phase := fmt.Sprintf("seed=%d buckets=%d concurrent=%v", v.seed, v.buckets, v.concurrent)
		assertSnapshotsEqual(t, phase, baseline, got, keys, true)
	}
}
