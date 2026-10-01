package ratelimit

import (
	"testing"
)

// Caller-scoped counters are isolated per caller: the same resource key for
// caller A and caller B does not share quota.
func TestCallerScopeIsolation(t *testing.T) {
	c := NewFakeClock()
	l := NewLimiter(NewMemoryStore(), c)
	p := Policy{ID: "per-caller", Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
		Key: "api", Algorithm: AlgTokenBucket, Rate: 1, Burst: 2}
	ctx := t.Context()

	for _, caller := range []string{"A", "B"} {
		for i := 0; i < 2; i++ {
			if d, _ := l.Consume(ctx, p, Request{CallerID: caller, ServiceID: "s", Key: "api"}); !d.Allowed {
				t.Fatalf("caller %s request %d rejected", caller, i)
			}
		}
		if d, _ := l.Consume(ctx, p, Request{CallerID: "A", ServiceID: "s", Key: "api"}); d.Allowed {
			t.Fatal("caller A exceeded quota")
		}
	}
	// Caller B still has its own quota.
	if d, _ := l.Consume(ctx, p, Request{CallerID: "B", ServiceID: "s", Key: "api"}); d.Allowed {
		t.Fatal("caller B quota should be independent but exhausted too")
	}
	// Different resource keys of the same caller do not interfere.
	if d, _ := l.Consume(ctx, p, Request{CallerID: "A", ServiceID: "s", Key: "other"}); !d.Allowed {
		t.Fatal("different key should have independent quota")
	}
}

// Service-scoped counters are shared across all callers of the service.
func TestServiceScopeShared(t *testing.T) {
	c := NewFakeClock()
	l := NewLimiter(NewMemoryStore(), c)
	p := Policy{ID: "per-service", Scope: ScopeService, CallerID: "*", ServiceID: "*",
		Key: "*", Algorithm: AlgTokenBucket, Rate: 1, Burst: 3}
	ctx := t.Context()

	for i := 0; i < 3; i++ {
		caller := []string{"A", "B", "C"}[i]
		if d, _ := l.Consume(ctx, p, Request{CallerID: caller, ServiceID: "svc", Key: "k"}); !d.Allowed {
			t.Fatalf("request %d (caller %s) rejected within shared quota", i, caller)
		}
	}
	if d, _ := l.Consume(ctx, p, Request{CallerID: "D", ServiceID: "svc", Key: "k"}); d.Allowed {
		t.Fatal("shared service quota exceeded")
	}
	// A different service has its own quota.
	if d, _ := l.Consume(ctx, p, Request{CallerID: "A", ServiceID: "other-svc", Key: "k"}); !d.Allowed {
		t.Fatal("different service should have independent quota")
	}
}

// A single key's burst must not crowd out other keys: counters are per
// (policy, scope identity, resource key).
func TestSingleKeyDoesNotCrowdOthers(t *testing.T) {
	c := NewFakeClock()
	l := NewLimiter(NewMemoryStore(), c)
	p := Policy{ID: "p", Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
		Key: "*", Algorithm: AlgTokenBucket, Rate: 1, Burst: 1}
	ctx := t.Context()

	if d, _ := l.Consume(ctx, p, Request{CallerID: "A", ServiceID: "s", Key: "hot"}); !d.Allowed {
		t.Fatal("first request rejected")
	}
	if d, _ := l.Consume(ctx, p, Request{CallerID: "A", ServiceID: "s", Key: "hot"}); d.Allowed {
		t.Fatal("hot key should be rate limited")
	}
	// Another key of the same caller is unaffected.
	if d, _ := l.Consume(ctx, p, Request{CallerID: "A", ServiceID: "s", Key: "cold"}); !d.Allowed {
		t.Fatal("cold key should not be crowded out by hot key")
	}
}

// Multi-policy priority: when several policies match, the highest-priority
// rejection wins, and the result is independent of declaration order.
func TestPolicyPriorityStable(t *testing.T) {
	store := NewMemoryStore()
	c := NewFakeClock()

	low := Policy{ID: "low", Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
		Key: "*", Algorithm: AlgTokenBucket, Rate: 1, Burst: 1, Priority: 1}
	high := Policy{ID: "high", Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
		Key: "*", Algorithm: AlgTokenBucket, Rate: 1, Burst: 1, Priority: 10}

	for _, order := range [][]Policy{{low, high}, {high, low}} {
		store.Restore(t.Context(), nil)
		m, err := NewPolicyManager(store, c, order)
		if err != nil {
			t.Fatal(err)
		}
		req := Request{CallerID: "A", ServiceID: "s", Key: "k"}
		if d, err := m.Check(t.Context(), req); err != nil || !d.Allowed {
			t.Fatalf("first request allowed=%v err=%v", d.Allowed, err)
		}
		d, _ := m.Check(t.Context(), req)
		if d.Allowed {
			t.Fatal("expected rejection")
		}
		if d.PolicyID != "high" {
			t.Fatalf("rejection reason should come from high-priority policy, got %q", d.PolicyID)
		}
		if d.Reason != ReasonRejected {
			t.Fatalf("reason=%q want %q", d.Reason, ReasonRejected)
		}
	}
}

// A request matching multiple policies must pass all of them; the rejecting
// policy is identified.
func TestMultiPolicyAllMustPass(t *testing.T) {
	store := NewMemoryStore()
	c := NewFakeClock()
	caller := Policy{ID: "caller", Scope: ScopeCaller, CallerID: "A", ServiceID: "*",
		Key: "*", Algorithm: AlgTokenBucket, Rate: 1, Burst: 5, Priority: 1}
	service := Policy{ID: "service", Scope: ScopeService, CallerID: "*", ServiceID: "svc",
		Key: "*", Algorithm: AlgTokenBucket, Rate: 1, Burst: 1, Priority: 2}
	m, err := NewPolicyManager(store, c, []Policy{caller, service})
	if err != nil {
		t.Fatal(err)
	}
	req := Request{CallerID: "A", ServiceID: "svc", Key: "k"}

	if d, _ := m.Check(t.Context(), req); !d.Allowed {
		t.Fatalf("first request rejected: %+v", d)
	}
	// Service quota (burst 1) is exhausted; caller quota still has room.
	d, _ := m.Check(t.Context(), req)
	if d.Allowed {
		t.Fatal("service quota should reject")
	}
	if d.PolicyID != "service" {
		t.Fatalf("rejection should identify service policy, got %q", d.PolicyID)
	}
}

// Wildcard matching semantics.
func TestKeyWildcardMatching(t *testing.T) {
	p := Policy{Key: "api.*"}
	cases := []struct {
		key  string
		want bool
	}{
		{"api.users", true},
		{"api.orders", true},
		{"api", false},
		{"other", false},
	}
	for _, tc := range cases {
		if got := p.matches(Request{Key: tc.key}); got != tc.want {
			t.Errorf("keyMatch(%q)=%v want %v", tc.key, got, tc.want)
		}
	}
	if !keyMatch("*", "anything") {
		t.Error("* should match everything")
	}
	if !idMatch("*", "anyone") {
		t.Error("caller * should match everyone")
	}
}

// Decision version pinning: a decision reports the snapshot version it used.
func TestDecisionReportsVersion(t *testing.T) {
	store := NewMemoryStore()
	c := NewFakeClock()
	m, err := NewPolicyManager(store, c, []Policy{
		{ID: "p", Scope: ScopeCaller, CallerID: "*", ServiceID: "*", Key: "*",
			Algorithm: AlgTokenBucket, Rate: 1, Burst: 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	d, _ := m.Check(t.Context(), Request{CallerID: "A", Key: "k"})
	if d.Version != 1 {
		t.Fatalf("version=%d want 1", d.Version)
	}
	v, err := m.Update([]Policy{
		{ID: "p", Scope: ScopeCaller, CallerID: "*", ServiceID: "*", Key: "*",
			Algorithm: AlgTokenBucket, Rate: 1, Burst: 10},
	})
	if err != nil || v != 2 {
		t.Fatalf("update v=%d err=%v", v, err)
	}
	d, _ = m.Check(t.Context(), Request{CallerID: "A", Key: "k"})
	if d.Version != 2 {
		t.Fatalf("version=%d want 2", d.Version)
	}
}
