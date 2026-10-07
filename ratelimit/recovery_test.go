package ratelimit

import (
	"path/filepath"
	"testing"
	"time"
)

// restart opens a fresh FileStore backed by the same file and builds a new
// manager with a reset monotonic clock (as a real process restart would).
func restart(t *testing.T, path string, policies []Policy) (*PolicyManager, *FakeClock) {
	t.Helper()
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	c := NewFakeClock() // monotonic clock resets on process restart
	m, err := NewPolicyManager(store, c, policies)
	if err != nil {
		t.Fatal(err)
	}
	return m, c
}

// restartLikeReal mirrors a real process restart more closely: the new
// process monotonic clock resets to mono0, while the wall clock is continuous
// and reads wall0 at the moment of restart (usually "later" than the old
// process wall time). This is the scenario that exposed the long-term
// rejection bug: persisted monotonic timestamps outlive their clock lineage.
func restartLikeReal(t *testing.T, path string, mono0, wall0 time.Duration, policies []Policy) (*PolicyManager, *FakeClock) {
	t.Helper()
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	c := NewFakeClock()
	c.SetMono(mono0)
	c.SetNow(time.Unix(0, wall0.Nanoseconds()))
	m, err := NewPolicyManager(store, c, policies)
	if err != nil {
		t.Fatal(err)
	}
	return m, c
}

// Restart recovery: token-bucket balance survives; used quota is not cleared.
func TestRestartRecoveryTokenBucket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rl.json")
	p := Policy{ID: "p", Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
		Key: "*", Algorithm: AlgTokenBucket, Rate: 1, Burst: 10}

	m, c := restart(t, path, []Policy{p})
	req := Request{CallerID: "a", Key: "k"}
	for i := 0; i < 5; i++ {
		if d, _ := m.Check(t.Context(), req); !d.Allowed {
			t.Fatalf("pre-restart request %d rejected", i)
		}
	}
	c.Advance(2 * time.Second) // 2 tokens refill, 7 available
	m.Check(t.Context(), req)  // consume one -> 6
	_ = m

	// Restart.
	m2, c2 := restart(t, path, []Policy{p})
	// 6 tokens must remain: used quota is not cleared by the restart.
	for i := 0; i < 6; i++ {
		if d, _ := m2.Check(t.Context(), req); !d.Allowed {
			t.Fatalf("post-restart request %d rejected: used quota was cleared", i)
		}
	}
	if d, _ := m2.Check(t.Context(), req); d.Allowed {
		t.Fatal("post-restart allowed more than the persisted balance")
	}

	// The limiter is not stuck: refill accrues after restart.
	c2.Advance(1 * time.Second)
	if d, _ := m2.Check(t.Context(), req); !d.Allowed {
		t.Fatal("limiter stuck after restart")
	}
}

// Restart recovery: sliding-window usage survives; in-window counts are not
// zeroed.
func TestRestartRecoverySlidingWindow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rl.json")
	p := Policy{ID: "p", Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
		Key: "*", Algorithm: AlgSlidingWindow, Rate: 1, Burst: 5, Window: 10 * time.Second}

	m, _ := restart(t, path, []Policy{p})
	req := Request{CallerID: "a", Key: "k"}
	for i := 0; i < 3; i++ {
		if d, _ := m.Check(t.Context(), req); !d.Allowed {
			t.Fatalf("pre-restart request %d rejected", i)
		}
	}

	// Restart: 3 used in the 10s window, 2 must remain.
	m2, c2 := restart(t, path, []Policy{p})
	for i := 0; i < 2; i++ {
		if d, _ := m2.Check(t.Context(), req); !d.Allowed {
			t.Fatalf("post-restart request %d rejected: window usage was cleared", i)
		}
	}
	if d, _ := m2.Check(t.Context(), req); d.Allowed {
		t.Fatal("post-restart window allowed more than remaining quota")
	}

	// After the window expires, quota frees normally (not permanently locked).
	c2.Advance(11 * time.Second)
	for i := 0; i < 5; i++ {
		if d, _ := m2.Check(t.Context(), req); !d.Allowed {
			t.Fatalf("post-window request %d rejected", i)
		}
	}
}

// Restart recovery: leaky-bucket level survives.
func TestRestartRecoveryLeakyBucket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rl.json")
	p := Policy{ID: "p", Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
		Key: "*", Algorithm: AlgLeakyBucket, Rate: 1, Burst: 4}

	m, _ := restart(t, path, []Policy{p})
	req := Request{CallerID: "a", Key: "k"}
	for i := 0; i < 4; i++ {
		m.Check(t.Context(), req)
	}
	if d, _ := m.Check(t.Context(), req); d.Allowed {
		t.Fatal("pre-restart bucket should be full")
	}

	m2, _ := restart(t, path, []Policy{p})
	if d, _ := m2.Check(t.Context(), req); d.Allowed {
		t.Fatal("post-restart bucket lost its queued level")
	}
}
