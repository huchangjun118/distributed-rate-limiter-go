package ratelimit

import (
	"fmt"
	"strings"
	"time"
)

// Scope defines whose quota a policy counts against.
type Scope int

const (
	// ScopeCaller counts against the caller's own quota (per callerID).
	ScopeCaller Scope = iota
	// ScopeService counts against the service's shared quota (per serviceID),
	// i.e. all callers of the service share one counter.
	ScopeService
)

// AlgorithmType identifies the metering algorithm.
type AlgorithmType int

const (
	AlgTokenBucket AlgorithmType = iota
	AlgLeakyBucket
	AlgSlidingWindow
)

// DegradeMode decides what a decision does when the shared counter is
// unavailable (store error, timeout or cancellation).
type DegradeMode int

const (
	// DegradeReject fails closed: the request is rejected.
	DegradeReject DegradeMode = iota
	// DegradeAllow fails open: the request is allowed through.
	DegradeAllow
)

// Stable decision reasons. They are part of the observable contract:
// callers can distinguish a normal rejection from a degradation rejection.
const (
	ReasonAllowed        = "allowed"
	ReasonRejected       = "rate_limited"
	ReasonDegradedAllow  = "degraded_allow"
	ReasonDegradedReject = "degraded_reject"
)

// Policy is a single rate-limit rule.
type Policy struct {
	// ID is stable across hot updates; it identifies the policy in decisions.
	ID string
	// Scope selects caller-level or service-level quota isolation.
	Scope Scope
	// CallerID matches the caller; "*" matches every caller.
	CallerID string
	// ServiceID matches the service; "*" matches every service.
	ServiceID string
	// Key matches the request resource key. "*" matches everything,
	// "prefix.*" matches a prefix, otherwise exact match.
	Key string

	Algorithm AlgorithmType
	// Rate is the refill/leak rate in units per second. For the sliding
	// window it jointly bounds the window: at most ceil(Rate*Window) requests
	// are admitted per window span (alongside the Burst hard cap).
	Rate float64
	// Burst is the token-bucket capacity / leaky-bucket queue capacity /
	// sliding-window hard instantaneous cap. For the sliding window the
	// effective window cap is min(Burst, ceil(Rate*Window)).
	Burst int64
	// Window is the sliding-window span (only used by AlgSlidingWindow).
	Window time.Duration

	// Priority decides which policy's rejection wins when several policies
	// match the same request: larger value = higher priority. Ties are broken
	// by policy ID, so the result is independent of rule iteration order.
	Priority int
	// Degrade decides fail-open vs fail-closed when the store is unavailable.
	Degrade DegradeMode

	// Version is filled by the PolicyManager when a snapshot is committed.
	Version int64
}

// Request is the input to a rate-limit decision.
type Request struct {
	CallerID  string
	ServiceID string
	Key       string
}

// Decision is the result of a rate-limit check.
type Decision struct {
	Allowed   bool
	Reason    string
	PolicyID  string
	Algorithm AlgorithmType
	Version   int64
	// Remaining is the number of requests still available to this policy/key
	// after the decision (best effort, not meaningful under degradation).
	Remaining int64
}

// validate checks the policy parameters. It is called on every update before
// the new snapshot is committed, so an invalid update can never replace a
// serving snapshot.
func (p Policy) validate() error {
	if strings.TrimSpace(p.ID) == "" {
		return fmt.Errorf("policy: id is required")
	}
	if p.Scope != ScopeCaller && p.Scope != ScopeService {
		return fmt.Errorf("policy %s: invalid scope %d", p.ID, p.Scope)
	}
	if p.CallerID == "" {
		return fmt.Errorf("policy %s: callerId is required (use \"*\" to match all)", p.ID)
	}
	if p.ServiceID == "" {
		return fmt.Errorf("policy %s: serviceId is required (use \"*\" to match all)", p.ID)
	}
	if p.Key == "" {
		return fmt.Errorf("policy %s: key is required (use \"*\" to match all)", p.ID)
	}
	if p.Rate <= 0 {
		return fmt.Errorf("policy %s: rate must be positive, got %v", p.ID, p.Rate)
	}
	if p.Burst <= 0 {
		return fmt.Errorf("policy %s: burst must be positive, got %d", p.ID, p.Burst)
	}
	switch p.Algorithm {
	case AlgTokenBucket, AlgLeakyBucket:
	case AlgSlidingWindow:
		if p.Window <= 0 {
			return fmt.Errorf("policy %s: sliding window requires a positive window", p.ID)
		}
		if p.Window < 10*time.Millisecond {
			return fmt.Errorf("policy %s: window must be >= 10ms, got %s", p.ID, p.Window)
		}
	default:
		return fmt.Errorf("policy %s: unknown algorithm %d", p.ID, p.Algorithm)
	}
	if p.Degrade != DegradeReject && p.Degrade != DegradeAllow {
		return fmt.Errorf("policy %s: invalid degrade mode %d", p.ID, p.Degrade)
	}
	return nil
}

// matches reports whether the policy applies to req.
func (p Policy) matches(req Request) bool {
	switch p.Scope {
	case ScopeCaller:
		if !idMatch(p.CallerID, req.CallerID) {
			return false
		}
	case ScopeService:
		if !idMatch(p.ServiceID, req.ServiceID) {
			return false
		}
	}
	return keyMatch(p.Key, req.Key)
}

// idMatch supports exact match and "*" wildcard.
func idMatch(pattern, id string) bool {
	if pattern == "*" {
		return true
	}
	return pattern == id
}

// keyMatch supports "*" (all), "prefix.*" (prefix) and exact match.
func keyMatch(pattern, key string) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, ".*") {
		prefix := strings.TrimSuffix(pattern, ".*")
		return strings.HasPrefix(key, prefix+".")
	}
	return pattern == key
}
