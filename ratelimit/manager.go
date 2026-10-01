package ratelimit

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

// Snapshot is a complete, immutable policy version. In-flight decisions pin
// the snapshot they started with, so a decision never mixes two versions.
type Snapshot struct {
	Version  int64
	Policies []Policy
}

// Stats are aggregate counters exposed for observability and tests.
type Stats struct {
	Allowed          int64
	Rejected         int64
	DegradedAllowed  int64
	DegradedRejected int64
}

// PolicyManager holds the current policy snapshot and serves decisions.
type PolicyManager struct {
	limiter *Limiter

	updateMu sync.Mutex
	current  atomic.Pointer[Snapshot]

	allowed          atomic.Int64
	rejected         atomic.Int64
	degradedAllowed  atomic.Int64
	degradedRejected atomic.Int64
}

// NewPolicyManager creates a manager with an initial set of policies.
func NewPolicyManager(store Store, clock Clock, policies []Policy) (*PolicyManager, error) {
	m := &PolicyManager{limiter: NewLimiter(store, clock)}
	if _, err := m.Update(policies); err != nil {
		return nil, err
	}
	return m, nil
}

// Update atomically replaces the policy set with policies. The new set is
// fully validated before it is committed; on error the previous snapshot
// keeps serving.
func (m *PolicyManager) Update(policies []Policy) (int64, error) {
	cp := make([]Policy, len(policies))
	copy(cp, policies)

	seen := make(map[string]struct{}, len(cp))
	for i := range cp {
		if err := cp[i].validate(); err != nil {
			return 0, err
		}
		if _, dup := seen[cp[i].ID]; dup {
			return 0, fmt.Errorf("policy: duplicate id %q", cp[i].ID)
		}
		seen[cp[i].ID] = struct{}{}
	}

	m.updateMu.Lock()
	defer m.updateMu.Unlock()

	ver := int64(1)
	if cur := m.current.Load(); cur != nil {
		ver = cur.Version + 1
	}
	for i := range cp {
		cp[i].Version = ver
	}
	m.current.Store(&Snapshot{Version: ver, Policies: cp})
	return ver, nil
}

// Check evaluates all matching policies for req.
//
// Matching policies are evaluated in a deterministic order: higher priority
// first, then policy id. The first rejection wins, so the reported reason is
// stable and independent of the order policies were declared. All matching
// policies must allow for the request to pass.
//
// The decision pins one snapshot version: policies are loaded once and never
// re-read mid-decision, so a hot update cannot cause a decision to observe a
// mix of two versions.
func (m *PolicyManager) Check(ctx context.Context, req Request) (Decision, error) {
	snap := m.current.Load()
	if snap == nil {
		return Decision{}, fmt.Errorf("manager: no policy snapshot")
	}

	matches := make([]Policy, 0, len(snap.Policies))
	for _, p := range snap.Policies {
		if p.matches(req) {
			matches = append(matches, p)
		}
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Priority != matches[j].Priority {
			return matches[i].Priority > matches[j].Priority
		}
		return matches[i].ID < matches[j].ID
	})

	var last Decision
	matched := false
	for _, p := range matches {
		dec, err := m.limiter.Consume(ctx, p, req)
		if err != nil {
			return Decision{}, err
		}
		m.record(dec)
		if !dec.Allowed {
			return dec, nil
		}
		// Remember the decision: a fail-open degradation must keep its
		// "degraded_allow" reason instead of being rewritten to "allowed".
		last = dec
		matched = true
	}

	if matched {
		return last, nil
	}
	dec := Decision{Allowed: true, Reason: ReasonAllowed, Version: snap.Version}
	m.record(dec)
	return dec, nil
}

func (m *PolicyManager) record(d Decision) {
	switch d.Reason {
	case ReasonAllowed:
		m.allowed.Add(1)
	case ReasonRejected:
		m.rejected.Add(1)
	case ReasonDegradedAllow:
		m.degradedAllowed.Add(1)
	case ReasonDegradedReject:
		m.degradedRejected.Add(1)
	}
}

// Stats returns a point-in-time copy of the aggregate counters.
func (m *PolicyManager) Stats() Stats {
	return Stats{
		Allowed:          m.allowed.Load(),
		Rejected:         m.rejected.Load(),
		DegradedAllowed:  m.degradedAllowed.Load(),
		DegradedRejected: m.degradedRejected.Load(),
	}
}

// SnapshotVersion returns the currently committed policy version.
func (m *PolicyManager) SnapshotVersion() int64 {
	if s := m.current.Load(); s != nil {
		return s.Version
	}
	return 0
}
