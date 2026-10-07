package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSlidingWindowRestartAgingBeyondWindow is the bug reproduction: after a
// process restart the monotonic clock resets, but real wall time has advanced
// beyond the window. Usage older than the window MUST be released on the very
// first post-restart decision instead of pinning the quota for a long time.
//
// The test asserts that time genuinely advanced (both mono and wall deltas
// exceed the window) before the restart; it cannot pass with a frozen clock.
func TestSlidingWindowRestartAgingBeyondWindow(t *testing.T) {
	path := t.TempDir() + "/rl.json"
	p := Policy{ID: "p", Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
		Key: "*", Algorithm: AlgSlidingWindow, Rate: 10, Burst: 3, Window: time.Second}

	m1, c1 := restart(t, path, []Policy{p})
	req := Request{CallerID: "a", Key: "k"}
	wallBefore := c1.WallNano()
	monoBefore := int64(c1.Mono())
	for i := 0; i < 3; i++ {
		if d, _ := m1.Check(context.Background(), req); !d.Allowed {
			t.Fatalf("pre-restart request %d rejected", i)
		}
		c1.Advance(50 * time.Millisecond)
	}
	if d, _ := m1.Check(context.Background(), req); d.Allowed {
		t.Fatal("pre-restart window should be full")
	}

	// Real downtime longer than the window: both clocks genuinely advance.
	c1.Advance(2 * time.Second)
	wallGap := c1.WallNano() - wallBefore
	monoGap := int64(c1.Mono()) - monoBefore
	if wallGap <= int64(p.Window) || monoGap <= int64(p.Window) {
		t.Fatalf("test setup: time did not really advance past the window")
	}

	// Restart: monotonic clock resets to zero, wall clock stays continuous.
	m2, _ := restartLikeReal(t, path, 0, time.Duration(c1.WallNano()), []Policy{p})

	// The first decision after restart must already see the history expired.
	d, _ := m2.Check(context.Background(), req)
	if !d.Allowed {
		t.Fatalf("usage older than window still counted after restart: %+v", d)
	}
	if d.Remaining != 2 {
		t.Fatalf("remaining=%d want 2 (full window freed, one just used)", d.Remaining)
	}
	for i := 0; i < 2; i++ {
		if d, _ := m2.Check(context.Background(), req); !d.Allowed {
			t.Fatalf("post-restart request %d rejected: quota was not fully restored", i)
		}
	}
	if d, _ := m2.Check(context.Background(), req); d.Allowed {
		t.Fatal("post-restart window over-issued beyond its limit")
	}
}

// TestSlidingWindowRestartShortDowntime verifies aging is exact, not
// all-or-nothing: a restart after a short downtime keeps in-window usage;
// quota frees precisely when true elapsed time crosses the window boundary.
func TestSlidingWindowRestartShortDowntime(t *testing.T) {
	path := t.TempDir() + "/rl.json"
	win := 10 * time.Second
	p := Policy{ID: "p", Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
		Key: "*", Algorithm: AlgSlidingWindow, Rate: 10, Burst: 5, Window: win}

	m1, c1 := restart(t, path, []Policy{p})
	req := Request{CallerID: "a", Key: "k"}
	for i := 0; i < 5; i++ {
		m1.Check(context.Background(), req)
	}

	// Downtime shorter than the window.
	c1.Advance(3 * time.Second)
	m2, c2 := restartLikeReal(t, path, 0, time.Duration(c1.WallNano()), []Policy{p})
	if d, _ := m2.Check(context.Background(), req); d.Allowed {
		t.Fatal("in-window usage was dropped immediately after a short restart")
	}

	// 6.9s of additional uptime: 9.9s total < window, still blocked.
	c2.Advance(6900 * time.Millisecond)
	if d, _ := m2.Check(context.Background(), req); d.Allowed {
		t.Fatal("quota freed before the real elapsed time reached the window")
	}

	// 7.0s total uptime after a 3s downtime = exactly the window: freed.
	c2.Advance(100 * time.Millisecond)
	if d, _ := m2.Check(context.Background(), req); !d.Allowed {
		t.Fatal("quota stayed locked once real elapsed time reached the window")
	}
}

// TestSlidingWindowRateConstraint verifies Rate actually bounds the sliding
// window: limit = min(Burst, ceil(Rate*Window)). Time is advanced at every
// step, so a frozen clock could not satisfy the expectations.
func TestSlidingWindowRateConstraint(t *testing.T) {
	store := NewMemoryStore()
	c := NewFakeClock()
	p := Policy{ID: "p", Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
		Key: "*", Algorithm: AlgSlidingWindow, Rate: 10, Burst: 100, Window: time.Second}
	m, err := NewPolicyManager(store, c, []Policy{p})
	if err != nil {
		t.Fatal(err)
	}
	req := Request{CallerID: "a", Key: "k"}
	ctx := context.Background()

	// Instantaneous burst is capped by Rate*Window = 10, not Burst = 100.
	for i := 0; i < 10; i++ {
		if d, _ := m.Check(ctx, req); !d.Allowed {
			t.Fatalf("request %d within rate cap rejected", i)
		}
	}
	if d, _ := m.Check(ctx, req); d.Allowed {
		t.Fatal("sliding window admitted more than Rate*Window inside one window")
	}

	// Half a second later nothing has aged out (all requests sit at t=0).
	c.Advance(500 * time.Millisecond)
	if d, _ := m.Check(ctx, req); d.Allowed {
		t.Fatal("rate quota recovered before a window elapsed")
	}

	// One second later the full rate-limited window is available again.
	c.Advance(500 * time.Millisecond)
	for i := 0; i < 10; i++ {
		if d, _ := m.Check(ctx, req); !d.Allowed {
			t.Fatalf("second-window request %d rejected", i)
		}
	}
	if d, _ := m.Check(ctx, req); d.Allowed {
		t.Fatal("second window over-issued beyond the rate cap")
	}

	// Sustained throughput over several windows: exactly Rate per window.
	c.Advance(time.Second)
	var admitted int64
	for cycle := 0; cycle < 3; cycle++ {
		for i := 0; i < 10; i++ {
			if d, _ := m.Check(ctx, req); d.Allowed {
				admitted++
			}
		}
		if d, _ := m.Check(ctx, req); d.Allowed {
			t.Fatal("sustained traffic exceeded 10/s")
		}
		c.Advance(time.Second)
	}
	if admitted != 30 {
		t.Fatalf("admitted=%d want 30 over 3 windows (10/s)", admitted)
	}
}

// TestSlidingWindowRateHotUpdate verifies a Rate change takes effect on the
// next decision without clearing existing usage or migrating state.
func TestSlidingWindowRateHotUpdate(t *testing.T) {
	store := NewMemoryStore()
	c := NewFakeClock()
	mk := func(rate float64, burst int64) Policy {
		return Policy{ID: "p", Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
			Key: "*", Algorithm: AlgSlidingWindow, Rate: rate, Burst: burst, Window: time.Second}
	}
	m, err := NewPolicyManager(store, c, []Policy{mk(10, 100)})
	if err != nil {
		t.Fatal(err)
	}
	req := Request{CallerID: "a", Key: "k"}
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		m.Check(ctx, req)
	}
	c.Advance(time.Second + time.Millisecond)

	// Raise the rate to 20/s: 20 requests are now admissible in the window.
	if _, err := m.Update([]Policy{mk(20, 100)}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if d, _ := m.Check(ctx, req); !d.Allowed {
			t.Fatalf("after raising rate, request %d rejected", i)
		}
	}
	if d, _ := m.Check(ctx, req); d.Allowed {
		t.Fatal("raised rate did not bind the window")
	}
	c.Advance(time.Second + time.Millisecond)

	// Lower the rate to 5/s: at most 5 in the new window.
	if _, err := m.Update([]Policy{mk(5, 100)}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if d, _ := m.Check(ctx, req); !d.Allowed {
			t.Fatalf("after lowering rate, request %d rejected", i)
		}
	}
	if d, _ := m.Check(ctx, req); d.Allowed {
		t.Fatal("lowered rate did not tighten the window")
	}
}

// TestSlidingWindowLegacyStateAging covers on-disk state written by the
// pre-anchor version (bare monotonic timestamps). It must not lock the quota:
// usage is conservatively retained at restart but ages out within one window
// of real forward time.
func TestSlidingWindowLegacyStateAging(t *testing.T) {
	p := Policy{ID: "sw", Algorithm: AlgSlidingWindow, Rate: 10, Burst: 5, Window: 10 * time.Second}
	// Timestamp 5s in the future relative to a freshly reset mono clock, which
	// is exactly the pre-restart failure shape.
	raw := []byte(`{"b":[{"t":5000000000,"c":5}]}`)

	d := slidingWindowAllow(raw, 100*time.Millisecond, time.Unix(1_700_000_000, 0).UnixNano(), 999, p)
	if d.allowed {
		t.Fatal("legacy in-window usage was dropped on first load")
	}

	// Once real time advances by the window from the conservative rebase
	// point, the legacy usage expires.
	d = slidingWindowAllow(d.newState, 10*time.Second+100*time.Millisecond,
		time.Unix(1_700_000_010, 0).UnixNano(), 999, p)
	if !d.allowed {
		t.Fatal("legacy usage locked the window beyond one window span")
	}
}

// TestSlidingWindowRestartWallRollback: wall time moving backwards across a
// restart must create no quota (no over-issue) and must not lock the quota;
// forward movement restores capacity normally.
func TestSlidingWindowRestartWallRollback(t *testing.T) {
	path := t.TempDir() + "/rl.json"
	p := Policy{ID: "p", Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
		Key: "*", Algorithm: AlgSlidingWindow, Rate: 10, Burst: 3, Window: time.Second}

	m1, c1 := restart(t, path, []Policy{p})
	req := Request{CallerID: "a", Key: "k"}
	for i := 0; i < 3; i++ {
		m1.Check(context.Background(), req)
	}

	// Restart with the wall clock stepped backwards by 100s, mono reset.
	back := time.Duration(c1.WallNano()) - 100*time.Second
	m2, c2 := restartLikeReal(t, path, 0, back, []Policy{p})
	if d, _ := m2.Check(context.Background(), req); d.Allowed {
		t.Fatal("wall rollback across restart created quota")
	}

	// Forward by the window: capacity returns, quota is not locked.
	c2.Advance(time.Second + time.Millisecond)
	if d, _ := m2.Check(context.Background(), req); !d.Allowed {
		t.Fatal("window stayed locked after the clock moved forward again")
	}
}

// TestSlidingWindowConcurrentAcrossRestartLineages hammers the shared store
// from two limiter instances living in different monotonic lineages (as a new
// process replacing an old one) at overlapping wall times. CAS commits must
// never over-issue, and after real time passes the window both lineages must
// recover their full quota.
func TestSlidingWindowConcurrentAcrossRestartLineages(t *testing.T) {
	store := NewMemoryStore()
	win := 10 * time.Second
	p := Policy{ID: "p", Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
		Key: "*", Algorithm: AlgSlidingWindow, Rate: 10, Burst: 20, Window: win}

	// Old lineage: some usage committed 2s ago in wall time.
	cOld := NewFakeClock()
	mOld, err := NewPolicyManager(store, cOld, []Policy{p})
	if err != nil {
		t.Fatal(err)
	}
	req := Request{CallerID: "a", Key: "k"}
	for i := 0; i < 5; i++ {
		mOld.Check(context.Background(), req)
	}
	cOld.Advance(2 * time.Second)

	// New lineage: mono reset to zero, wall continuous with the old process.
	cNew := NewFakeClock()
	cNew.SetNow(time.Unix(0, cOld.WallNano()))
	mNew, err := NewPolicyManager(store, cNew, []Policy{p})
	if err != nil {
		t.Fatal(err)
	}

	var allowed atomic.Int64
	var wg sync.WaitGroup
	hammer := func(m *PolicyManager, n int) {
		defer wg.Done()
		for i := 0; i < n; i++ {
			d, _ := m.Check(context.Background(), req)
			if d.Allowed {
				allowed.Add(1)
			}
		}
	}
	wg.Add(2)
	go hammer(mOld, 200)
	go hammer(mNew, 200)
	wg.Wait()

	// 5 already counted in-window; total admissions across both lineages
	// cannot exceed the window limit of 20.
	if got := allowed.Load(); got != 15 {
		t.Fatalf("cross-lineage admissions=%d want 15 (no over-issue)", got)
	}

	// After real time crosses the window in both lineages, quota recovers.
	cOld.Advance(win + time.Second)
	cNew.Advance(win + time.Second)
	allowed.Store(0)
	wg.Add(2)
	go hammer(mOld, 50)
	go hammer(mNew, 50)
	wg.Wait()
	if got := allowed.Load(); got != 20 {
		t.Fatalf("post-window admissions=%d want 20 (quota locked after restart contention)", got)
	}
}

// TestSlidingWindowCorruptState recovers from undecodable persisted bytes
// instead of locking the key.
func TestSlidingWindowCorruptState(t *testing.T) {
	p := Policy{ID: "p", Algorithm: AlgSlidingWindow, Rate: 10, Burst: 3, Window: time.Second}
	d := slidingWindowAllow([]byte("{not json"), 0, 0, 1, p)
	if !d.allowed {
		t.Fatal("corrupt state locked the sliding window")
	}
}
