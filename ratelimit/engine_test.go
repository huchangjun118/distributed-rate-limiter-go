package ratelimit

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func newTestEngine(t *testing.T, cfg PolicyConfig, c Counter) *Engine {
	t.Helper()
	if c == nil {
		var err error
		c, err = NewLocalCounter(DefaultClock(), nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	e, err := NewEngine(Config{
		Counters:       Counters{Caller: c, Server: c},
		Clock:          DefaultClock(),
		CounterTimeout: 200 * time.Millisecond,
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func TestMixedPoliciesDenySetStable(t *testing.T) {
	// Two rules both deny at the same time. Declare them in two orders in
	// separate configs; the output must be identical (sorted by rule ID).
	build := func(ids []string) PolicyConfig {
		rules := []Rule{
			{ID: ids[0], Algo: AlgoTokenBucket, Scope: ScopeCaller, Key: "k", Rate: 1, Burst: 1},
			{ID: ids[1], Algo: AlgoTokenBucket, Scope: ScopeServer, Key: "k", Rate: 1, Burst: 1},
		}
		return PolicyConfig{Version: 1, Rules: rules}
	}
	run := func(order []string) Decision {
		e := newTestEngine(t, build(order), nil)
		d1 := e.Allow(context.Background(), &Request{Caller: "A", Key: "k"})
		if !d1.Allowed {
			t.Fatal("first request should pass both")
		}
		return e.Allow(context.Background(), &Request{Caller: "A", Key: "k"})
	}
	a := run([]string{"aaa-rule", "zzz-rule"})
	b := run([]string{"zzz-rule", "aaa-rule"})
	if fmt.Sprint(a.RejectedBy) != "[aaa-rule zzz-rule]" {
		t.Fatalf("unexpected stable deny set: %v", a.RejectedBy)
	}
	if fmt.Sprint(a.RejectedBy) != fmt.Sprint(b.RejectedBy) {
		t.Fatalf("deny set changed with rule order: %v vs %v", a.RejectedBy, b.RejectedBy)
	}
	if a.Reasons["aaa-rule"] != ReasonTokenBucketEmpty || a.Reasons["zzz-rule"] != ReasonTokenBucketEmpty {
		t.Fatalf("reasons not distinguishable per rule: %v", a.Reasons)
	}
}

func TestScopeIsolation(t *testing.T) {
	c, _ := NewLocalCounter(DefaultClock(), nil)
	cfg := PolicyConfig{Version: 1, Rules: []Rule{
		{ID: "percaller", Algo: AlgoTokenBucket, Scope: ScopeCaller, Key: "k", Rate: 1, Burst: 1},
		{ID: "global", Algo: AlgoTokenBucket, Scope: ScopeServer, Key: "k", Rate: 1, Burst: 5},
	}}
	e := newTestEngine(t, cfg, c)
	// 10 distinct callers: each gets its own burst-1, global admits up to 5.
	var allowed, perCallerDenies, globalDenies int
	for i := 0; i < 10; i++ {
		d := e.Allow(context.Background(), &Request{Caller: fmt.Sprintf("u%d", i), Key: "k"})
		if d.Allowed {
			allowed++
			continue
		}
		if _, ok := d.Reasons["percaller"]; ok {
			perCallerDenies++
		}
		if _, ok := d.Reasons["global"]; ok {
			globalDenies++
		}
	}
	if allowed != 5 {
		t.Fatalf("expected exactly 5 global admits, got %d", allowed)
	}
	if perCallerDenies != 0 {
		t.Fatalf("distinct callers must not share caller-scoped quota, denies=%d", perCallerDenies)
	}
	if globalDenies != 5 {
		t.Fatalf("remaining 5 should hit the global rule, got %d", globalDenies)
	}

	// Same caller twice: second request denied by the caller rule, proving
	// identical logical keys for different callers do not crosstalk (u0 and
	// u1 each had 1 independently), and one caller's burst stays local.
	d := e.Allow(context.Background(), &Request{Caller: "u0", Key: "k"})
	if d.Allowed || d.Reasons["percaller"] != ReasonTokenBucketEmpty {
		t.Fatalf("caller u0 second burst must be denied by caller rule: %+v", d)
	}
}

func TestOneKeyBurstCannotStarveOthers(t *testing.T) {
	cfg := PolicyConfig{Version: 1, Rules: []Rule{{
		ID: "tb-k1", Algo: AlgoTokenBucket, Scope: ScopeServer, Key: "k1", Rate: 1, Burst: 100, Match: MatchKeys("k1"),
	}, {
		ID: "tb-k2", Algo: AlgoTokenBucket, Scope: ScopeServer, Key: "k2", Rate: 1, Burst: 1, Match: MatchKeys("k2"),
	}}}
	e := newTestEngine(t, cfg, nil)
	for i := 0; i < 100; i++ {
		if !e.Allow(context.Background(), &Request{Key: "k1"}).Allowed {
			t.Fatal("k1 burst")
		}
	}
	if !e.Allow(context.Background(), &Request{Key: "k2"}).Allowed {
		t.Fatal("k2 must be unaffected by k1's burst (independent physical counters)")
	}
}

func TestHotUpdateFailureKeepsOldVersion(t *testing.T) {
	cfg := PolicyConfig{Version: 1, Rules: []Rule{{
		ID: "r1", Algo: AlgoTokenBucket, Scope: ScopeServer, Key: "k", Rate: 1, Burst: 1,
	}}}
	e := newTestEngine(t, cfg, nil)
	if e.CurrentVersion() != 1 {
		t.Fatal("version 1")
	}

	bad := PolicyConfig{Version: 2, Rules: []Rule{{
		ID: "r1", Algo: Algorithm("nonsense"), Scope: ScopeServer, Key: "k", Rate: 1, Burst: 1,
	}}}
	if err := e.Update(bad); err == nil {
		t.Fatal("invalid update must fail")
	}
	if e.CurrentVersion() != 1 {
		t.Fatalf("failed update must keep old version, got %d", e.CurrentVersion())
	}

	bad2 := PolicyConfig{Version: 3, Rules: []Rule{{ID: "dup"}, {ID: "dup"}}}
	if err := e.Update(bad2); err == nil {
		t.Fatal("duplicate IDs must fail")
	}
	if e.CurrentVersion() != 1 {
		t.Fatal("old version still required after second failure")
	}

	// Valid update then takes effect.
	good := PolicyConfig{Version: 4, Rules: []Rule{{
		ID: "r1", Algo: AlgoTokenBucket, Scope: ScopeServer, Key: "k", Rate: 10, Burst: 10,
	}}}
	if err := e.Update(good); err != nil {
		t.Fatal(err)
	}
	if e.CurrentVersion() != 4 {
		t.Fatal("version 4 expected")
	}
}

func TestDegradationModes(t *testing.T) {
	c, _ := NewLocalCounter(DefaultClock(), nil)
	cfg := PolicyConfig{Version: 1, Rules: []Rule{
		{ID: "open-rule", Algo: AlgoTokenBucket, Scope: ScopeServer, Key: "ko", Rate: 1, Burst: 1, Fail: FailOpen, Match: MatchKeys("ko")},
		{ID: "closed-rule", Algo: AlgoTokenBucket, Scope: ScopeServer, Key: "kc", Rate: 1, Burst: 1, Fail: FailClosed, Match: MatchKeys("kc")},
	}}
	e := newTestEngine(t, cfg, c)
	c.FailNext(2)

	d := e.Allow(context.Background(), &Request{Key: "ko"})
	if !d.Allowed || !d.Degraded {
		t.Fatalf("fail-open must allow: %+v", d)
	}
	if d.Reasons["open-rule"] != ReasonDegradedAllow {
		t.Fatalf("want degraded_allow, got %v", d.Reasons)
	}
	d = e.Allow(context.Background(), &Request{Key: "kc"})
	if d.Allowed {
		t.Fatal("fail-closed must deny")
	}
	if d.Reasons["closed-rule"] != ReasonCounterUnavail {
		t.Fatalf("want counter_unavailable, got %v", d.Reasons)
	}
	// Recovery: serving resumes with no lockout.
	if !e.Allow(context.Background(), &Request{Key: "ko"}).Allowed {
		t.Fatal("service must resume after counter recovery")
	}
}

func TestDegradedTimeoutReason(t *testing.T) {
	c, _ := NewLocalCounter(DefaultClock(), nil)
	cfg := PolicyConfig{Version: 1, Rules: []Rule{{
		ID: "r", Algo: AlgoTokenBucket, Scope: ScopeServer, Key: "k", Rate: 1, Burst: 1, Fail: FailClosed,
	}}}
	e, err := NewEngine(Config{
		Counters:       Counters{Caller: c, Server: c},
		Clock:          DefaultClock(),
		CounterTimeout: time.Nanosecond,
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Expire the context before evaluation.
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond)
	d := e.Allow(ctx, &Request{Key: "k"})
	if d.Allowed {
		t.Fatal("deadline must deny under fail-closed")
	}
	if d.Reasons["r"] != ReasonCounterTimeout && d.Reasons["r"] != ReasonCounterCanceled {
		t.Fatalf("want timeout/canceled reason, got %v", d.Reasons)
	}
}

func TestDecisionVersionPinned(t *testing.T) {
	cfg := PolicyConfig{Version: 1, Rules: []Rule{{
		ID: "r", Algo: AlgoTokenBucket, Scope: ScopeServer, Key: "k", Rate: 1, Burst: 1,
	}}}
	e := newTestEngine(t, cfg, nil)
	d := e.Allow(context.Background(), &Request{Key: "k"})
	if d.Version != 1 {
		t.Fatalf("decision must carry version 1, got %d", d.Version)
	}
}

func TestMatcherSelectsRules(t *testing.T) {
	cfg := PolicyConfig{Version: 1, Rules: []Rule{{
		ID: "only-v1", Algo: AlgoTokenBucket, Scope: ScopeServer, Key: "k",
		Rate: 1, Burst: 1, Match: MatchKeys("/v1"),
	}}}
	e := newTestEngine(t, cfg, nil)
	if !e.Allow(context.Background(), &Request{Key: "/v1"}).Allowed {
		t.Fatal("first /v1")
	}
	if e.Allow(context.Background(), &Request{Key: "/v1"}).Allowed {
		t.Fatal("/v1 limited to 1")
	}
	// Unmatched key is not constrained.
	for i := 0; i < 5; i++ {
		if !e.Allow(context.Background(), &Request{Key: "/v2"}).Allowed {
			t.Fatal("non-matching key must not be limited")
		}
	}
}
