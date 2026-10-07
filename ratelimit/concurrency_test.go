package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Concurrent contention: many instances (limiters) share one store. A burst of
// N simultaneous requests against a token bucket of capacity B must allow
// exactly B (no over-issue), regardless of CAS conflicts/retries.
func TestConcurrentNoOverIssue(t *testing.T) {
	store := NewMemoryStore()
	c := NewFakeClock()
	const burst = 50
	const instances = 4
	const perInstance = 200

	policy := func() Policy {
		return Policy{ID: "p", Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
			Key: "*", Algorithm: AlgTokenBucket, Rate: 0.0001, Burst: burst}
	}

	var allowed, rejected atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < instances; i++ {
		m, err := NewPolicyManager(store, c, []Policy{policy()})
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perInstance; j++ {
				d, err := m.Check(t.Context(), Request{CallerID: "shared", Key: "k"})
				if err != nil {
					t.Error(err)
					return
				}
				if d.Allowed {
					allowed.Add(1)
				} else {
					rejected.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if got := allowed.Load(); got != burst {
		t.Fatalf("allowed=%d want exactly %d (no over-issue)", got, burst)
	}
	if got := rejected.Load(); got != int64(instances*perInstance-burst) {
		t.Fatalf("rejected=%d want %d", got, instances*perInstance-burst)
	}
}

// Concurrent contention across algorithms: leaky bucket and sliding window
// also never over-issue under a burst.
func TestConcurrentNoOverIssueAllAlgorithms(t *testing.T) {
	store := NewMemoryStore()
	c := NewFakeClock()
	const burst = 30
	const goroutines = 8
	const each = 100

	algos := []struct {
		name string
		alg  AlgorithmType
		rate float64
		win  time.Duration
	}{
		{"leaky", AlgLeakyBucket, 0.0001, 0},
		// Sliding window caps per-window throughput at min(Burst,
		// ceil(Rate*Window)); rate 1/s over a 1-minute window allows 60, so
		// the effective cap here is exactly Burst.
		{"sliding", AlgSlidingWindow, 1, time.Minute},
	}
	for _, tc := range algos {
		store.Restore(t.Context(), nil)
		p := Policy{ID: tc.name, Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
			Key: "*", Algorithm: tc.alg, Rate: tc.rate, Burst: burst, Window: tc.win}
		m, err := NewPolicyManager(store, c, []Policy{p})
		if err != nil {
			t.Fatal(err)
		}
		var allowed atomic.Int64
		var wg sync.WaitGroup
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < each; j++ {
					d, err := m.Check(t.Context(), Request{CallerID: "c", Key: "k"})
					if err != nil {
						t.Error(err)
						return
					}
					if d.Allowed {
						allowed.Add(1)
					}
				}
			}()
		}
		wg.Wait()
		if got := allowed.Load(); got != burst {
			t.Fatalf("%s: allowed=%d want %d", tc.name, got, burst)
		}
	}
}

// Retry under contention: a store that always conflicts a few times before
// committing must still converge without over-issuing.
func TestRetryConverges(t *testing.T) {
	inner := NewMemoryStore()
	store := &conflictStore{Store: inner, conflicts: 3}
	c := NewFakeClock()
	p := Policy{ID: "p", Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
		Key: "*", Algorithm: AlgTokenBucket, Rate: 1, Burst: 10}
	m, err := NewPolicyManager(store, c, []Policy{p})
	if err != nil {
		t.Fatal(err)
	}
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				d, err := m.Check(t.Context(), Request{CallerID: "c", Key: "k"})
				if err == nil && d.Allowed {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != 10 {
		t.Fatalf("allowed=%d want 10 (retries must converge, no over-issue)", got)
	}
}

// conflictStore wraps a Store and forces the first N CAS attempts to conflict.
type conflictStore struct {
	Store
	mu        sync.Mutex
	conflicts int
	seen      int
}

func (s *conflictStore) CAS(ctx context.Context, key string, oldVal, newVal []byte) (bool, error) {
	s.mu.Lock()
	if s.seen < s.conflicts {
		s.seen++
		s.mu.Unlock()
		return false, nil // force a retry
	}
	s.mu.Unlock()
	return s.Store.CAS(ctx, key, oldVal, newVal)
}
