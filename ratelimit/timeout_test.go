package ratelimit

import (
	"context"
	"testing"
	"time"
)

// A deadline that expires while lock-free algorithm math is running must
// abort the commit: no state lands, and the counter stays usable.
func TestUpdateDeadlineAfterMathAbortsCommit(t *testing.T) {
	c, _ := NewLocalCounter(DefaultClock(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	time.Sleep(10 * time.Millisecond) // already expired when Update runs
	_, err := c.Update(ctx, "k", func(st *algoState, found bool) (bool, *algoState, error) {
		if !found {
			st = newState(AlgoTokenBucket)
		}
		// Math "succeeds" on the private clone...
		v := limitView{algo: AlgoTokenBucket, rate: 1, burst: 5, state: st}
		ok := v.tryAcquire(1, 1, 1)
		return ok, st, nil
	})
	if err == nil {
		t.Fatal("expired ctx must abort before commit")
	}
	if _, exists := c.snapshot()["k"]; exists {
		t.Fatal("expired transaction must not leave committed state")
	}
	ok, err := c.Update(context.Background(), "k2", windowTxn(1, 1))
	if err != nil || !ok {
		t.Fatalf("counter remains usable: ok=%v err=%v", ok, err)
	}
}

// Fail-open engine reports the degraded timeout reason distinctly and still
// does not commit when the parent context is already canceled.
func TestFailOpenCanceledReasonDistinct(t *testing.T) {
	c, _ := NewLocalCounter(DefaultClock(), nil)
	cfg := PolicyConfig{Version: 1, Rules: []Rule{{
		ID: "r", Algo: AlgoTokenBucket, Scope: ScopeServer, Key: "k",
		Rate: 1, Burst: 1, Fail: FailOpen,
	}}}
	e, err := NewEngine(Config{
		Counters:       Counters{Caller: c, Server: c},
		Clock:          DefaultClock(),
		CounterTimeout: time.Second,
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := e.Allow(ctx, &Request{Key: "k"})
	if !d.Allowed || !d.Degraded {
		t.Fatalf("fail-open must allow on cancellation: %+v", d)
	}
	if d.Reasons["r"] != ReasonDegradedCanceled {
		t.Fatalf("want degraded_canceled, got %v", d.Reasons)
	}
}
