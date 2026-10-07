package ratelimit

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// restartAt simulates a process restart: a fresh FileStore over the same file
// and a fresh process lifetime (new BootID, mono reset to zero) whose wall
// clock reads wall — i.e. the process came back up at that physical time.
func restartAt(t *testing.T, path string, policies []Policy, wall time.Time) (*PolicyManager, *FakeClock) {
	t.Helper()
	m, c := restart(t, path, policies)
	c.SetNow(wall)
	return m, c
}

func swRestartPolicy() Policy {
	return Policy{ID: "sw", Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
		Key: "*", Algorithm: AlgSlidingWindow, Rate: 100, Burst: 3, Window: 10 * time.Second}
}

// Restart aging: usage recorded before a restart ages by real physical time
// while the process is down. Once the downtime exceeds the window, the old
// usage no longer affects decisions.
func TestSlidingWindowRestartAgesByPhysicalTime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rl.json")
	p := swRestartPolicy()
	req := Request{CallerID: "a", Key: "k"}

	m, c := restart(t, path, []Policy{p})
	wallDown := c.Now()
	for i := 0; i < 3; i++ {
		if d, _ := m.Check(t.Context(), req); !d.Allowed {
			t.Fatalf("pre-restart request %d rejected", i)
		}
	}
	if d, _ := m.Check(t.Context(), req); d.Allowed {
		t.Fatal("pre-restart window should be full")
	}

	// The process stays down for 11s (> 10s window), then comes back up.
	m2, c2 := restartAt(t, path, []Policy{p}, wallDown.Add(11*time.Second))

	// This test is only meaningful if physical time really advanced across
	// the restart while the monotonic clock did not.
	if !c2.Now().After(wallDown.Add(10 * time.Second)) {
		t.Fatalf("wall clock did not advance past the window: %v -> %v", wallDown, c2.Now())
	}
	if c2.Mono() != 0 {
		t.Fatalf("monotonic clock must reset on restart, got %v", c2.Mono())
	}
	if c2.BootID() == c.BootID() {
		t.Fatal("restart must produce a new process lifetime")
	}

	// All pre-restart usage is older than the window: the full quota is back.
	for i := 0; i < 3; i++ {
		if d, _ := m2.Check(t.Context(), req); !d.Allowed {
			t.Fatalf("post-restart request %d rejected: pre-restart usage did not age out", i)
		}
	}
	if d, _ := m2.Check(t.Context(), req); d.Allowed {
		t.Fatal("post-restart window allowed more than the configured quota")
	}
}

// Partial aging across a restart: downtime shorter than the window keeps the
// usage counted (quota is not cleared), and the remaining aging accrues
// normally after the restart, so the slot frees exactly when the total real
// elapsed time crosses the window.
func TestSlidingWindowRestartPartialAging(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rl.json")
	p := swRestartPolicy()
	req := Request{CallerID: "a", Key: "k"}

	m, c := restart(t, path, []Policy{p})
	wallDown := c.Now()
	for i := 0; i < 3; i++ {
		m.Check(t.Context(), req)
	}

	// Down for 6s of the 10s window: all 3 are still in-window.
	m2, c2 := restartAt(t, path, []Policy{p}, wallDown.Add(6*time.Second))
	if !c2.Now().Equal(wallDown.Add(6 * time.Second)) {
		t.Fatal("physical time did not advance by exactly the downtime")
	}
	if d, _ := m2.Check(t.Context(), req); d.Allowed {
		t.Fatal("restart cleared in-window usage: quota must survive restarts")
	}

	// 4 more seconds of uptime: total real elapsed = 10s, the boundary is
	// crossed and the quota frees. Not locked, not released early.
	c2.Advance(3999 * time.Millisecond)
	if d, _ := m2.Check(t.Context(), req); d.Allowed {
		t.Fatal("usage aged out before the real window elapsed")
	}
	c2.Advance(1 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if d, _ := m2.Check(t.Context(), req); !d.Allowed {
			t.Fatalf("request %d rejected after the window fully elapsed", i)
		}
	}
}

// Restart with the wall clock rolled back: no quota is created out of thin
// air, and the limiter is not locked forever — normal uptime aging resumes.
func TestSlidingWindowRestartWallRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rl.json")
	p := swRestartPolicy()
	req := Request{CallerID: "a", Key: "k"}

	m, c := restart(t, path, []Policy{p})
	wallDown := c.Now()
	for i := 0; i < 3; i++ {
		m.Check(t.Context(), req)
	}

	// The machine's wall clock was set 1h back while the process was down.
	m2, c2 := restartAt(t, path, []Policy{p}, wallDown.Add(-1*time.Hour))
	if !c2.Now().Before(wallDown) {
		t.Fatal("wall clock must read earlier than before the restart")
	}
	// No quota appears: the window is still full.
	for i := 0; i < 3; i++ {
		if d, _ := m2.Check(t.Context(), req); d.Allowed {
			t.Fatalf("wall rollback created quota (request %d allowed)", i)
		}
	}
	// Not locked forever: once the new process's own uptime crosses the
	// window, the usage expires.
	c2.Advance(11 * time.Second)
	for i := 0; i < 3; i++ {
		if d, _ := m2.Check(t.Context(), req); !d.Allowed {
			t.Fatalf("request %d rejected: limiter locked after wall rollback", i)
		}
	}
}

// Rate constraint: the per-window cap is min(Burst, ceil(Rate*Window)), so a
// large Burst cannot exceed the rate-derived throughput.
func TestSlidingWindowRateConstraint(t *testing.T) {
	c := NewFakeClock()
	l := NewLimiter(NewMemoryStore(), c)
	// Burst would allow 100, but Rate 2/s over a 1s window caps it at 2.
	p := swPolicy(2, 100, time.Second)
	ctx := t.Context()
	req := Request{CallerID: "a", Key: "k"}

	for i := 0; i < 2; i++ {
		if d, _ := l.Consume(ctx, p, req); !d.Allowed {
			t.Fatalf("request %d rejected below the rate-derived cap", i)
		}
	}
	if d, _ := l.Consume(ctx, p, req); d.Allowed {
		t.Fatal("allowed beyond ceil(Rate*Window): rate does not constrain the window")
	}

	// Sustained throughput: each elapsed second frees exactly the 2 requests
	// counted one second ago, never more.
	for round := 0; round < 5; round++ {
		c.Advance(time.Second)
		for i := 0; i < 2; i++ {
			if d, _ := l.Consume(ctx, p, req); !d.Allowed {
				t.Fatalf("round %d request %d rejected: window did not slide", round, i)
			}
		}
		if d, _ := l.Consume(ctx, p, req); d.Allowed {
			t.Fatalf("round %d allowed more than Rate per second", round)
		}
	}
}

// Rate hot update: changing Rate in a policy update changes decisions
// immediately, without waiting for the window to drain.
func TestSlidingWindowRateHotUpdate(t *testing.T) {
	store := NewMemoryStore()
	c := NewFakeClock()
	p := swRestartPolicy()
	p.Rate = 2 // cap = min(3, ceil(2*10)) = 3 (unchanged); use a tight window instead
	p.Window = time.Second
	p.Burst = 100
	m, err := NewPolicyManager(store, c, []Policy{p})
	if err != nil {
		t.Fatal(err)
	}
	req := Request{CallerID: "a", Key: "k"}

	for i := 0; i < 2; i++ {
		if d, _ := m.Check(t.Context(), req); !d.Allowed {
			t.Fatalf("request %d rejected below cap", i)
		}
	}
	if d, _ := m.Check(t.Context(), req); d.Allowed {
		t.Fatal("allowed beyond the rate-derived cap")
	}

	// Raise the rate: the cap grows from 2 to 10 and decisions change at once.
	p.Rate = 10
	ver, err := m.Update([]Policy{p})
	if err != nil {
		t.Fatal(err)
	}
	d, _ := m.Check(t.Context(), req)
	if !d.Allowed {
		t.Fatal("decision did not track the updated rate")
	}
	if d.Version != ver {
		t.Fatalf("decision version=%d want %d", d.Version, ver)
	}
}

// Legacy state: a state persisted by the previous version (bucket timestamps
// on a foreign pre-restart clock, no anchor fields) must neither be cleared
// nor lock the quota forever — it ages out within one window.
func TestSlidingWindowLegacyStateRebased(t *testing.T) {
	store := NewMemoryStore()
	c := NewFakeClock()
	p := swRestartPolicy()
	req := Request{CallerID: "a", Key: "k"}
	key := buildStoreKey(p, req)

	// Foreign-clock timestamp (e.g. 500s of uptime of the dead process),
	// far beyond anything the new process's monotonic clock will read soon.
	legacy := []byte(`{"b":[{"t":500000000000,"c":3}]}`)
	if ok, err := store.CAS(t.Context(), key, nil, legacy); err != nil || !ok {
		t.Fatalf("seed legacy state: ok=%v err=%v", ok, err)
	}

	l := NewLimiter(store, c)
	// Usage is kept, not cleared: the window is full (3 of 3).
	if d, _ := l.Consume(t.Context(), p, req); d.Allowed {
		t.Fatal("legacy in-window usage was cleared")
	}
	// But it is not locked forever: it ages out within one window.
	c.Advance(10 * time.Second)
	for i := 0; i < 3; i++ {
		if d, _ := l.Consume(t.Context(), p, req); !d.Allowed {
			t.Fatalf("request %d rejected: legacy usage never aged out", i)
		}
	}
}

// Restart + aging + concurrent decisions: when the window expires exactly at
// a restart and many goroutines race to consume, the freed quota is issued
// exactly once — no transient over-issue, no stuck quota.
func TestSlidingWindowRestartConcurrentNoOverIssue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rl.json")
	p := swRestartPolicy() // limit = min(3, ceil(100*10)) = 3
	req := Request{CallerID: "a", Key: "k"}

	m, c := restart(t, path, []Policy{p})
	wallDown := c.Now()
	for i := 0; i < 3; i++ {
		m.Check(t.Context(), req)
	}

	// Come back up after the window has fully elapsed.
	m2, c2 := restartAt(t, path, []Policy{p}, wallDown.Add(11*time.Second))
	if c2.Mono() != 0 || !c2.Now().After(wallDown.Add(10*time.Second)) {
		t.Fatal("restart did not reset mono / advance wall as required")
	}

	const goroutines = 8
	const each = 10
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < each; j++ {
				if d, err := m2.Check(t.Context(), req); err == nil && d.Allowed {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != 3 {
		t.Fatalf("allowed=%d want exactly 3 (aged-out quota issued exactly once)", got)
	}
}
