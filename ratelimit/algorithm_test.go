package ratelimit

import (
	"testing"
	"time"
)

func runAlgo(t *testing.T, v limitView, clk *ManualClock, n int64) bool {
	t.Helper()
	return v.withState(v.state).tryAcquire(clk.Mono(), clk.Now().UnixNano(), n)
}

func TestTokenBucketBurstAndRefill(t *testing.T) {
	clk := NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	st := newState(AlgoTokenBucket)
	v := limitView{algo: AlgoTokenBucket, rate: 1, burst: 3, state: st}

	// Cold start: full bucket -> exactly Burst admitted, (Burst+1)th denied.
	for i := 0; i < 3; i++ {
		if !runAlgo(t, v, clk, 1) {
			t.Fatalf("burst request %d should pass", i)
		}
	}
	if runAlgo(t, v, clk, 1) {
		t.Fatal("4th request at same instant must be rejected (boundary deterministic)")
	}

	// 1 token/s: after 1s exactly one token exists -> one pass, one deny.
	clk.Advance(time.Second)
	if !runAlgo(t, v, clk, 1) {
		t.Fatal("after 1s, one refilled token should pass")
	}
	if runAlgo(t, v, clk, 1) {
		t.Fatal("no second token yet")
	}

	// Refill never exceeds burst.
	clk.Advance(time.Hour)
	got := 0
	for runAlgo(t, v, clk, 1) {
		got++
	}
	if got != 3 {
		t.Fatalf("refill cap violated: got %d want 3", got)
	}
}

func TestTokenBucketWallClockRollbackNoQuota(t *testing.T) {
	clk := NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	st := newState(AlgoTokenBucket)
	v := limitView{algo: AlgoTokenBucket, rate: 10, burst: 2, state: st}
	if !runAlgo(t, v, clk, 1) || !runAlgo(t, v, clk, 1) {
		t.Fatal("burst should pass")
	}
	if runAlgo(t, v, clk, 1) {
		t.Fatal("bucket should be empty")
	}
	// Move the WALL clock backwards; mono untouched. No tokens may appear.
	clk.SetWall(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	if runAlgo(t, v, clk, 1) {
		t.Fatal("wall clock rollback must not fabricate tokens")
	}
	// Mono still advances normally: recovery, not a permanent deny.
	clk.Advance(100 * time.Millisecond) // rate 10/s -> 1 token
	if !runAlgo(t, v, clk, 1) {
		t.Fatal("monotonic advance should refill; limiter must not be wedged")
	}
}

func TestLeakyBucketShape(t *testing.T) {
	clk := NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	st := newState(AlgoLeakyBucket)
	v := limitView{algo: AlgoLeakyBucket, rate: 2, burst: 3, state: st}
	for i := 0; i < 3; i++ {
		if !runAlgo(t, v, clk, 1) {
			t.Fatalf("leaky burst %d", i)
		}
	}
	if runAlgo(t, v, clk, 1) {
		t.Fatal("leaky bucket full -> reject")
	}
	// After 500ms exactly 1 unit leaked -> one admit.
	clk.Advance(500 * time.Millisecond)
	if !runAlgo(t, v, clk, 1) {
		t.Fatal("one slot should have drained")
	}
	if runAlgo(t, v, clk, 1) {
		t.Fatal("still full after one admit")
	}
	// Rejected requests do not add water: wait 1.5s, then exactly 3 fit.
	clk.Advance(1500 * time.Millisecond)
	got := 0
	for runAlgo(t, v, clk, 1) {
		got++
	}
	if got != 3 {
		t.Fatalf("leaky drain got %d want 3", got)
	}
}

func TestSlidingWindowBoundaries(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := NewManualClock(start)
	st := newState(AlgoSlidingWindow)
	// window 10s, 10 sub-buckets => 1s buckets; limit 3 per window.
	v := limitView{algo: AlgoSlidingWindow, rate: 3, window: int64(10 * time.Second), subs: 10, state: st}

	for i := 0; i < 3; i++ {
		if !runAlgo(t, v, clk, 1) {
			t.Fatalf("window admit %d", i)
		}
	}
	if runAlgo(t, v, clk, 1) {
		t.Fatal("window full -> reject")
	}
	// 9.5s later: bucket at t=0 (start 0) satisfies b <= now-W?
	// now=9.5e9, trailing=-0.5e9; bucket start 0 > trailing -> still counts
	// (conservative). Must remain rejected.
	clk.Advance(9500 * time.Millisecond)
	if runAlgo(t, v, clk, 1) {
		t.Fatal("at 9.5s, t=0 bucket is still inside the half-open window")
	}
	// At exactly 10s, trailing=0, bucket start 0 <= trailing -> dropped.
	clk.Advance(500 * time.Millisecond)
	if !runAlgo(t, v, clk, 1) {
		t.Fatal("at exactly window width, old usage must have aged out")
	}
}

func TestSlidingWindowRolling(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := NewManualClock(start)
	st := newState(AlgoSlidingWindow)
	v := limitView{algo: AlgoSlidingWindow, rate: 3, window: int64(10 * time.Second), subs: 10, state: st}
	// 2 at t=0, 2 at t=9 -> at t=9 sum=4? 2 old still counted: only 1 fits.
	if !runAlgo(t, v, clk, 1) || !runAlgo(t, v, clk, 1) {
		t.Fatal("first two")
	}
	clk.Advance(9 * time.Second)
	if !runAlgo(t, v, clk, 1) {
		t.Fatal("third should fit (2 old + 1 = 3)")
	}
	if runAlgo(t, v, clk, 1) {
		t.Fatal("fourth within window must reject (2 old fully counted)")
	}
	clk.Advance(time.Second) // t=10: t=0 bucket drops
	if !runAlgo(t, v, clk, 1) {
		t.Fatal("after trailing edge passes, only t=9 bucket remains")
	}
}
