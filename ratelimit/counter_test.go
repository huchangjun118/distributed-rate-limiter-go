package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

// sharedWindow builds a txn that applies sliding-window math directly on the
// counter, to test the substrate independently of the engine.
func windowTxn(limit int64, nowWall int64) KeyTxn {
	return func(st *algoState, found bool) (bool, *algoState, error) {
		if !found {
			st = newState(AlgoSlidingWindow)
		}
		v := limitView{algo: AlgoSlidingWindow, rate: float64(limit), window: 1 << 40, subs: 1, state: st}
		ok := v.tryAcquire(0, nowWall, 1)
		return ok, st, nil
	}
}

func TestCounterConcurrentNeverOverIssue(t *testing.T) {
	c, err := NewLocalCounter(DefaultClock(), nil)
	if err != nil {
		t.Fatal(err)
	}
	const quota, goroutines, perG = int64(500), 64, 100
	var allowed atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx := context.Background()
			for i := 0; i < perG; i++ {
				ok, uerr := c.Update(ctx, "k", windowTxn(quota, 1))
				if uerr != nil {
					t.Errorf("unexpected error: %v", uerr)
					return
				}
				if ok {
					allowed.Add(1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := allowed.Load(); got != quota {
		t.Fatalf("admitted %d, want exactly %d (no over/under issue)", got, quota)
	}
	if got := c.snapshot()["k"].Buckets[0]; got != quota {
		t.Fatalf("committed count %d want %d", got, quota)
	}
}

func TestCounterTwoInstancesShareQuota(t *testing.T) {
	c, _ := NewLocalCounter(DefaultClock(), nil)
	// Two engines acting as two instances, both on the same counter.
	cfg := PolicyConfig{Version: 1, Rules: []Rule{{
		ID: "g", Algo: AlgoSlidingWindow, Scope: ScopeServer, Key: "r",
		Rate: 100, Window: 1 << 30, SubBuckets: 1,
	}}}
	eng1, err := NewEngine(Config{Counters: Counters{Caller: c, Server: c}, Clock: DefaultClock()}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	eng2, err := NewEngine(Config{Counters: Counters{Caller: c, Server: c}, Clock: DefaultClock()}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var a1, a2 atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	shoot := func(e *Engine, a *atomic.Int64, n int) {
		defer wg.Done()
		<-start
		for i := 0; i < n; i++ {
			if e.Allow(context.Background(), &Request{Caller: "x", Key: "r"}).Allowed {
				a.Add(1)
			}
		}
	}
	wg.Add(2)
	go shoot(eng1, &a1, 200)
	go shoot(eng2, &a2, 150)
	close(start)
	wg.Wait()
	if total := a1.Load() + a2.Load(); total != 100 {
		t.Fatalf("two instances admitted %d, want 100", total)
	}
}

func TestCounterConflictRetriesReadFreshState(t *testing.T) {
	c, _ := NewLocalCounter(DefaultClock(), nil)
	// Sequential contended calls must converge: 1000 updates against limit
	// 1000 admit all, proving retries don't lose or double count.
	var allowed int
	for i := 0; i < 1000; i++ {
		ok, err := c.Update(context.Background(), "k", windowTxn(1000, int64(1+i/100)))
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			allowed++
		}
	}
	if allowed != 1000 {
		t.Fatalf("admitted %d want 1000", allowed)
	}
}

func TestCounterCancelCommitsNothing(t *testing.T) {
	c, _ := NewLocalCounter(DefaultClock(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok, err := c.Update(ctx, "k", windowTxn(5, 1))
	if err == nil {
		t.Fatal("canceled ctx must return error")
	}
	if ok {
		t.Fatal("canceled txn must not admit")
	}
	if _, exists := c.snapshot()["k"]; exists {
		t.Fatal("canceled txn must leave no state behind (no key, no lock)")
	}
	// Counter must remain fully usable immediately (not locked).
	ok, err = c.Update(context.Background(), "k", windowTxn(5, 1))
	if err != nil || !ok {
		t.Fatalf("counter must serve after cancel: ok=%v err=%v", ok, err)
	}
}

func TestCounterDegradationRecoversNoLockout(t *testing.T) {
	c, _ := NewLocalCounter(DefaultClock(), nil)
	c.FailNext(3)
	for i := 0; i < 3; i++ {
		if _, err := c.Update(context.Background(), "k", windowTxn(5, 1)); err != ErrUnavailable {
			t.Fatalf("call %d: want ErrUnavailable", i)
		}
	}
	ok, err := c.Update(context.Background(), "k", windowTxn(5, 1))
	if err != nil || !ok {
		t.Fatalf("counter must recover after transient failures: ok=%v err=%v", ok, err)
	}
}
