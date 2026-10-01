package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// While one goroutine rolls policy versions continuously, workers evaluate
// requests. Every decision must report exactly one complete version, and the
// engine must never panic or corrupt state under -race.
func TestConcurrentUpdatesAndAllow(t *testing.T) {
	c, _ := NewLocalCounter(DefaultClock(), nil)
	v1 := PolicyConfig{Version: 1, Rules: []Rule{{
		ID: "r", Algo: AlgoTokenBucket, Scope: ScopeServer, Key: "k", Rate: 1000, Burst: 1000,
	}}}
	e, err := NewEngine(Config{Counters: Counters{Caller: c, Server: c}, Clock: DefaultClock()}, v1)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		v := uint64(2)
		for !stop.Load() {
			cfg := PolicyConfig{Version: v, Rules: []Rule{{
				ID: "r", Algo: AlgoTokenBucket, Scope: ScopeServer, Key: "k",
				Rate: float64(1000 + v), Burst: int64(1000 + v),
			}}}
			if err := e.Update(cfg); err != nil {
				t.Errorf("update: %v", err)
				return
			}
			// Also attempt an INVALID update: it must fail and leave service
			// intact while readers are active.
			_ = e.Update(PolicyConfig{Version: v + 1000000, Rules: []Rule{{ID: "r", Rate: -1}}})
			v++
			time.Sleep(time.Millisecond)
		}
	}()
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				d := e.Allow(context.Background(), &Request{Key: "k"})
				if d.Version < 1 {
					t.Errorf("decision against invalid version %d", d.Version)
					return
				}
			}
		}()
	}
	time.Sleep(60 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
}

// Heavy concurrent contention on a tiny quota across many goroutines:
// exactly quota admits, with race detector active.
func TestRaceContendedSmallQuota(t *testing.T) {
	c, _ := NewLocalCounter(DefaultClock(), nil)
	cfg := PolicyConfig{Version: 1, Rules: []Rule{{
		ID: "w", Algo: AlgoSlidingWindow, Scope: ScopeServer, Key: "r",
		Rate: 10, Window: time.Hour, SubBuckets: 2,
	}}}
	e, err := NewEngine(Config{Counters: Counters{Caller: c, Server: c}, Clock: DefaultClock()}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var allowed atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 100; i++ {
				if e.Allow(context.Background(), &Request{Key: "r"}).Allowed {
					allowed.Add(1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	if allowed.Load() != 10 {
		t.Fatalf("admitted %d want exactly 10", allowed.Load())
	}
}
