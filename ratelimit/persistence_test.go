package ratelimit

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func windowBurstCfg() PolicyConfig {
	return PolicyConfig{Version: 1, Rules: []Rule{{
		ID: "w", Algo: AlgoSlidingWindow, Scope: ScopeServer, Key: "k",
		Rate: 5, Window: 10 * time.Second, SubBuckets: 10,
	}}}
}

func TestRestartWindowUsageNotZeroed(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(filepath.Join(dir, "snap.json"))
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := NewManualClock(t0)

	e1, err := NewEngine(Config{Clock: clk, Store: store, PersistInterval: time.Hour}, windowBurstCfg())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if !e1.Allow(context.Background(), &Request{Key: "k"}).Allowed {
			t.Fatal("window admits 5")
		}
	}
	if err := e1.Persist(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate restart 2s later (still inside the 10s window) with a brand
	// new clock object at the matching wall time.
	clk2 := NewManualClock(t0.Add(2 * time.Second))
	e2, err := NewEngine(Config{Clock: clk2, Store: store, PersistInterval: time.Hour}, windowBurstCfg())
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	d := e2.Allow(context.Background(), &Request{Key: "k"})
	if d.Allowed {
		t.Fatal("in-window usage must survive restart: 6th request must be denied")
	}
	if d.Reasons["w"] != ReasonWindowExceeded {
		t.Fatalf("want window reason, got %v", d.Reasons)
	}

	// After the window fully elapses, capacity returns normally.
	clk2.Advance(9 * time.Second)
	if !e2.Allow(context.Background(), &Request{Key: "k"}).Allowed {
		t.Fatal("usage must age out by wall time after restart")
	}
}

func TestRestartTokenBucketShutdownGapRefill(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(filepath.Join(dir, "snap.json"))
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := NewManualClock(t0)
	cfg := PolicyConfig{Version: 1, Rules: []Rule{{
		ID: "tb", Algo: AlgoTokenBucket, Scope: ScopeServer, Key: "k", Rate: 1, Burst: 5,
	}}}
	e1, err := NewEngine(Config{Clock: clk, Store: store, PersistInterval: time.Hour}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		e1.Allow(context.Background(), &Request{Key: "k"})
	}
	if err := e1.Persist(context.Background()); err != nil {
		t.Fatal(err)
	}
	e1.Close()

	// Restart 3s later: at 1 token/s exactly 3 tokens should be available
	// after rebase (no more — shutdown refill follows the same cap/schedule).
	clk2 := NewManualClock(t0.Add(3 * time.Second))
	e2, err := NewEngine(Config{Clock: clk2, Store: store, PersistInterval: time.Hour}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	got := 0
	for e2.Allow(context.Background(), &Request{Key: "k"}).Allowed {
		got++
	}
	if got != 3 {
		t.Fatalf("after 3s shutdown want exactly 3 refilled, got %d", got)
	}
}

func TestRestartWallRollbackFabricatesNoQuota(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(filepath.Join(dir, "snap.json"))
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := NewManualClock(t0)
	cfg := PolicyConfig{Version: 1, Rules: []Rule{{
		ID: "tb", Algo: AlgoTokenBucket, Scope: ScopeServer, Key: "k", Rate: 1, Burst: 5,
	}}}
	e1, _ := NewEngine(Config{Clock: clk, Store: store, PersistInterval: time.Hour}, cfg)
	for i := 0; i < 5; i++ {
		e1.Allow(context.Background(), &Request{Key: "k"})
	}
	_ = e1.Persist(context.Background())
	e1.Close()

	// Restart with the wall clock set BACK by an hour: gap is clamped at 0,
	// mono also starts at 0 -> no refill may appear.
	clk2 := NewManualClock(t0.Add(-time.Hour))
	e2, err := NewEngine(Config{Clock: clk2, Store: store, PersistInterval: time.Hour}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if e2.Allow(context.Background(), &Request{Key: "k"}).Allowed {
		t.Fatal("backwards wall clock at restart must not create tokens")
	}
	// Forward monotonic progress still heals the limiter.
	clk2.Advance(time.Second)
	if !e2.Allow(context.Background(), &Request{Key: "k"}).Allowed {
		t.Fatal("monotonic advance post-restart must refill normally")
	}
}

func TestFileStoreAtomicRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "snap.json")
	s := NewFileStore(path)
	ctx := context.Background()

	loaded, meta, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 0 || meta != nil {
		t.Fatal("fresh store must load empty snapshot")
	}
	states := map[string]*algoState{
		"s:w|k": {Kind: AlgoTokenBucket, Init: true, LastMono: 123, Tokens: 2},
	}
	if err := s.Save(ctx, states, &SnapshotMeta{WallNanos: 9, MonoNanos: 8}); err != nil {
		t.Fatal(err)
	}
	got, m, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if m.WallNanos != 9 || m.MonoNanos != 8 {
		t.Fatalf("meta lost: %+v", m)
	}
	if got["s:w|k"].Tokens != 2 || got["s:w|k"].LastMono != 123 {
		t.Fatalf("state lost: %+v", got["s:w|k"])
	}
}
