package ratelimit

import (
	"context"
	"testing"
	"time"
)

// TestSlidingWindowRealClockPhysicalAging uses the real clock and really
// sleeps, proving aging is driven by physical elapsed time and cannot pass on
// a frozen clock. The window is kept short to stay CI-friendly.
func TestSlidingWindowRealClockPhysicalAging(t *testing.T) {
	store := NewMemoryStore()
	l := NewLimiter(store, NewRealClock())
	p := Policy{ID: "p", Algorithm: AlgSlidingWindow, Rate: 1000, Burst: 2, Window: 60 * time.Millisecond}
	req := Request{CallerID: "a", Key: "k"}
	ctx := context.Background()

	if d, _ := l.Consume(ctx, p, req); !d.Allowed {
		t.Fatal("first request rejected")
	}
	if d, _ := l.Consume(ctx, p, req); !d.Allowed {
		t.Fatal("second request rejected")
	}
	if d, _ := l.Consume(ctx, p, req); d.Allowed {
		t.Fatal("third request should be rate limited within the window")
	}

	time.Sleep(20 * time.Millisecond)
	if d, _ := l.Consume(ctx, p, req); d.Allowed {
		t.Fatal("usage aged out before the window physically elapsed")
	}

	// Physically wait beyond the window: quota must recover.
	time.Sleep(60 * time.Millisecond)
	if d, _ := l.Consume(ctx, p, req); !d.Allowed {
		t.Fatal("usage did not age out after the physical window elapsed")
	}
}

// TestSlidingWindowLimitFormula pins the joint rate/burst semantics.
func TestSlidingWindowLimitFormula(t *testing.T) {
	cases := []struct {
		rate   float64
		burst  int64
		window time.Duration
		want   int64
	}{
		{10, 100, time.Second, 10},           // rate binds
		{10, 3, time.Second, 3},              // burst binds
		{0.5, 100, 2 * time.Second, 1},       // ceil(1)=1
		{1, 100, 1500 * time.Millisecond, 2}, // ceil(1.5)=2
		{100, 5, time.Minute, 5},             // burst binds under high rate
	}
	for _, tc := range cases {
		p := Policy{Rate: tc.rate, Burst: tc.burst, Window: tc.window}
		if got := slidingWindowLimit(p); got != tc.want {
			t.Errorf("rate=%v burst=%d window=%s: limit=%d want %d",
				tc.rate, tc.burst, tc.window, got, tc.want)
		}
	}
}
