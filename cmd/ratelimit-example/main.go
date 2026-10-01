// Command ratelimit-example demonstrates: mixed policies with stable deny
// reasons, two service instances sharing one quota (never over-issuing),
// fail-open/fail-closed degradation, and snapshot restart recovery.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	rl "github.com/huchangjun118/distributed-rate-limiter-go/ratelimit"
)

func main() {
	ctx := context.Background()

	// Phase 1: per-caller burst isolation + distinguishable deny reason.
	cfg := rl.PolicyConfig{
		Version: 1,
		Rules: []rl.Rule{
			{ID: "per-caller-tb", Algo: rl.AlgoTokenBucket, Scope: rl.ScopeCaller,
				Key: "/api", Rate: 2, Burst: 3, Fail: rl.FailClosed, Match: rl.MatchKeys("/api")},
			{ID: "global-window", Algo: rl.AlgoSlidingWindow, Scope: rl.ScopeServer,
				Key: "/api", Rate: 100, Window: time.Second, SubBuckets: 10, Fail: rl.FailOpen,
				Match: rl.MatchKeys("/api")},
		},
	}
	shared, err := rl.NewLocalCounter(rl.DefaultClock(), nil)
	must(err)
	eng1, err := rl.NewEngine(rl.Config{
		Counters: rl.Counters{Caller: shared, Server: shared},
		Clock:    rl.DefaultClock(),
	}, cfg)
	must(err)

	fmt.Println("== burst of 5 from caller A, then 5 from caller B (per-caller burst=3) ==")
	for _, caller := range []string{"A", "B"} {
		for i := 0; i < 5; i++ {
			d := eng1.Allow(ctx, &rl.Request{Caller: caller, Key: "/api"})
			why := ""
			if !d.Allowed {
				why = fmt.Sprintf(" reasons=%v", d.Reasons)
			}
			fmt.Printf("caller=%s req=%d allowed=%v deniedBy=%v%s\n", caller, i, d.Allowed, d.RejectedBy, why)
		}
	}

	// Phase 2: two engines on ONE shared counter = two instances sharing one
	// global quota. A fresh counter/key keeps the number independent of the
	// first phase. The server window is 100 per hour; 200 unique callers each
	// with a huge per-caller bucket hammer it concurrently. Exactly 100 may
	// pass, regardless of goroutine scheduling.
	demoShared, err := rl.NewLocalCounter(rl.DefaultClock(), nil)
	must(err)
	demoCfg := rl.PolicyConfig{Version: 1, Rules: []rl.Rule{
		{ID: "per-caller-tb", Algo: rl.AlgoTokenBucket, Scope: rl.ScopeCaller,
			Key: "/demo", Rate: 1000, Burst: 1000, Match: rl.MatchKeys("/demo")},
		{ID: "global-window", Algo: rl.AlgoSlidingWindow, Scope: rl.ScopeServer,
			Key: "/demo", Rate: 100, Window: time.Hour, SubBuckets: 10,
			Match: rl.MatchKeys("/demo")},
	}}
	inst1, err := rl.NewEngine(rl.Config{
		Counters: rl.Counters{Caller: demoShared, Server: demoShared},
		Clock:    rl.DefaultClock(),
	}, demoCfg)
	must(err)
	inst2, err := rl.NewEngine(rl.Config{
		Counters: rl.Counters{Caller: demoShared, Server: demoShared},
		Clock:    rl.DefaultClock(),
	}, demoCfg)
	must(err)

	fmt.Println("== 200 concurrent clients across 2 instances; shared window = 100 ==")
	var wg sync.WaitGroup
	var allowed int64
	var mu sync.Mutex
	start := make(chan struct{})
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			e := inst1
			if i%2 == 0 {
				e = inst2
			}
			if e.Allow(ctx, &rl.Request{Caller: fmt.Sprintf("c%d", i), Key: "/demo"}).Allowed {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}(i)
	}
	close(start)
	wg.Wait()
	fmt.Printf("allowed=%d (must be exactly 100: no over-issue under contention)\n", allowed)

	// Phase 3: snapshot restart recovery for sliding-window usage.
	dir, err := os.MkdirTemp("", "ratelimit-demo-")
	must(err)
	defer os.RemoveAll(dir)
	store := rl.NewFileStore(filepath.Join(dir, "snap.json"))
	clock := rl.NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	winCfg := rl.PolicyConfig{Version: 1, Rules: []rl.Rule{{
		ID: "w", Algo: rl.AlgoSlidingWindow, Scope: rl.ScopeServer,
		Key: "k", Rate: 5, Window: 10 * time.Second, SubBuckets: 10, Match: rl.MatchKeys("k"),
	}}}
	eA, err := rl.NewEngine(rl.Config{Clock: clock, Store: store, PersistInterval: time.Hour}, winCfg)
	must(err)
	for i := 0; i < 5; i++ {
		if !eA.Allow(ctx, &rl.Request{Key: "k"}).Allowed {
			panic("window should admit 5")
		}
	}
	must(eA.Persist(ctx))
	must(eA.Close())

	// "Restart" 2s later (inside the 10s window) with a fresh clock object at
	// the matching wall time: in-window usage must survive and deny the 6th.
	clock2 := rl.NewManualClock(clock.Now().Add(2 * time.Second))
	eB, err := rl.NewEngine(rl.Config{Clock: clock2, Store: store, PersistInterval: time.Hour}, winCfg)
	must(err)
	defer eB.Close()
	d := eB.Allow(ctx, &rl.Request{Key: "k"})
	fmt.Printf("after restart, 6th request within window allowed=%v (want false; in-window quota restored)\n", d.Allowed)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
