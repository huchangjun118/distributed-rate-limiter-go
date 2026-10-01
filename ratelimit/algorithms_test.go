package ratelimit

import (
	"testing"
	"time"
)

func tbPolicy(rate float64, burst int64) Policy {
	return Policy{ID: "tb", Algorithm: AlgTokenBucket, Rate: rate, Burst: burst}
}

func lbPolicy(rate float64, burst int64) Policy {
	return Policy{ID: "lb", Algorithm: AlgLeakyBucket, Rate: rate, Burst: burst}
}

func swPolicy(rate float64, burst int64, window time.Duration) Policy {
	return Policy{ID: "sw", Algorithm: AlgSlidingWindow, Rate: rate, Burst: burst, Window: window}
}

// Token bucket: burst allows exactly Burst instant requests, then rejects;
// refill accrues over time and is capped at Burst.
func TestTokenBucketBurstAndRefill(t *testing.T) {
	c := NewFakeClock()
	l := NewLimiter(NewMemoryStore(), c)
	p := tbPolicy(1, 5)
	ctx := t.Context()

	for i := 0; i < 5; i++ {
		d, err := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"})
		if err != nil || !d.Allowed {
			t.Fatalf("request %d: allowed=%v err=%v", i, d.Allowed, err)
		}
	}
	d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"})
	if d.Allowed || d.Reason != ReasonRejected {
		t.Fatalf("expected rejection after burst, got %+v", d)
	}
	if d.Remaining != 0 {
		t.Fatalf("remaining=%d want 0", d.Remaining)
	}

	// Refill: 1 token per second.
	c.Advance(500 * time.Millisecond)
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); d.Allowed {
		t.Fatal("refilled after only 500ms")
	}
	c.Advance(500 * time.Millisecond)
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); !d.Allowed {
		t.Fatal("expected allow after 1s refill")
	}

	// Long idle must not exceed burst capacity.
	c.Advance(1 * time.Hour)
	for i := 0; i < 5; i++ {
		if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); !d.Allowed {
			t.Fatalf("idle refill request %d rejected", i)
		}
	}
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); d.Allowed {
		t.Fatal("bucket overfilled beyond burst")
	}
}

// Token bucket: a monotonic clock rollback creates no quota and never
// permanently rejects.
func TestTokenBucketRollback(t *testing.T) {
	c := NewFakeClock()
	l := NewLimiter(NewMemoryStore(), c)
	p := tbPolicy(1, 5)
	ctx := t.Context()

	for i := 0; i < 5; i++ {
		l.Consume(ctx, p, Request{CallerID: "a", Key: "k"})
	}
	// All quota used.
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); d.Allowed {
		t.Fatal("bucket should be empty")
	}

	// Time jumps forward 10s: bucket refills to full.
	c.Advance(10 * time.Second)
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); !d.Allowed {
		t.Fatal("expected full bucket after advance")
	}
	// Consumed one, 4 left. Roll back 10s: no extra quota appears.
	c.SetMono(c.Mono() - 10*time.Second)
	for i := 0; i < 4; i++ {
		if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); !d.Allowed {
			t.Fatalf("request %d should be allowed from pre-rollback quota", i)
		}
	}
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); d.Allowed {
		t.Fatal("rollback created extra quota")
	}

	// Rolling back must not permanently lock the limiter: when the clock
	// returns to the same metering point, quota is available again.
	c.SetMono(c.Mono() + 10*time.Second)
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); !d.Allowed {
		t.Fatal("limiter stayed rejected after clock caught up")
	}
}

// Leaky bucket: capacity burst, overflow rejected, leak frees room.
func TestLeakyBucketOverflowAndLeak(t *testing.T) {
	c := NewFakeClock()
	l := NewLimiter(NewMemoryStore(), c)
	p := lbPolicy(1, 3)
	ctx := t.Context()

	for i := 0; i < 3; i++ {
		if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); !d.Allowed {
			t.Fatalf("request %d rejected within capacity", i)
		}
	}
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); d.Allowed {
		t.Fatal("expected overflow rejection")
	}

	// Leak half a second: no room yet.
	c.Advance(500 * time.Millisecond)
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); d.Allowed {
		t.Fatal("leaked room after only 500ms")
	}
	// Leak a full second: one slot frees.
	c.Advance(500 * time.Millisecond)
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); !d.Allowed {
		t.Fatal("expected allow after 1s leak")
	}
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); d.Allowed {
		t.Fatal("second request should still overflow")
	}

	// Sustained rate: after a long idle the bucket is empty, not over-full.
	c.Advance(1 * time.Hour)
	for i := 0; i < 3; i++ {
		if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); !d.Allowed {
			t.Fatalf("idle request %d rejected", i)
		}
	}
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); d.Allowed {
		t.Fatal("leaky bucket accepted more than capacity after idle")
	}
}

// Sliding window: boundary behavior is deterministic. Requests are spread
// across sub-buckets so they expire one sub-bucket at a time.
func TestSlidingWindowBoundary(t *testing.T) {
	c := NewFakeClock()
	l := NewLimiter(NewMemoryStore(), c)
	p := swPolicy(10, 3, time.Second)
	ctx := t.Context()

	// Three requests, one per 100ms sub-bucket.
	for i := 0; i < 3; i++ {
		if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); !d.Allowed {
			t.Fatalf("request %d rejected within window quota", i)
		}
		c.Advance(100 * time.Millisecond)
	}
	// t=300ms: window is full.
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); d.Allowed {
		t.Fatal("expected rejection at full window")
	}

	// t=999ms: the oldest sub-bucket (t=0) is 999ms old, still in-window.
	c.Advance(699 * time.Millisecond)
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); d.Allowed {
		t.Fatal("sliding window allowed before boundary")
	}

	// t=1000ms: the t=0 sub-bucket expires (now - bucketStart >= window).
	c.Advance(1 * time.Millisecond)
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); !d.Allowed {
		t.Fatal("sliding window did not free the oldest slot at boundary")
	}
	// The t=100ms and t=200ms sub-buckets are still in-window: only one slot
	// freed, so the next request is rejected.
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); d.Allowed {
		t.Fatal("sliding window freed more than one slot at boundary")
	}

	// t=1100ms: the t=100ms sub-bucket expires, freeing one more slot.
	c.Advance(100 * time.Millisecond)
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); !d.Allowed {
		t.Fatal("sliding window did not free the second slot")
	}
}

// Sliding window: a rollback does not resurrect expired quota or drop
// in-use quota.
func TestSlidingWindowRollback(t *testing.T) {
	c := NewFakeClock()
	l := NewLimiter(NewMemoryStore(), c)
	p := swPolicy(10, 2, time.Second)
	ctx := t.Context()

	l.Consume(ctx, p, Request{CallerID: "a", Key: "k"})
	l.Consume(ctx, p, Request{CallerID: "a", Key: "k"})
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); d.Allowed {
		t.Fatal("window should be full")
	}

	// Rollback: counts stay (no quota created, no quota dropped).
	c.SetMono(c.Mono() - 5*time.Second)
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); d.Allowed {
		t.Fatal("rollback dropped in-use quota")
	}

	// Forward past the original boundary: quota frees normally.
	c.SetMono(c.Mono() + 6*time.Second)
	if d, _ := l.Consume(ctx, p, Request{CallerID: "a", Key: "k"}); !d.Allowed {
		t.Fatal("window did not free after clock advanced past boundary")
	}
}
