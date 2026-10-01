package ratelimit

import (
	"context"
	"fmt"
	"net/url"
)

// Limiter evaluates one policy against the shared counter store.
// Multiple Limiter instances sharing one Store represent multiple service
// instances sharing one quota.
type Limiter struct {
	store Store
	clock Clock
}

func NewLimiter(store Store, clock Clock) *Limiter {
	return &Limiter{store: store, clock: clock}
}

// Consume evaluates policy p for req. It never returns an error for store
// failures: those are turned into a degraded decision per p.Degrade. The only
// error returned is a nil-store programming error.
//
// Concurrency safety: every state change is a single atomic CAS. On conflict
// the decision is recomputed from the latest committed state, so a decision is
// always based on a complete committed count and no retry can over-issue. A
// timeout or cancellation never writes a partial state and never holds a lock,
// so the quota can never get stuck.
func (l *Limiter) Consume(ctx context.Context, p Policy, req Request) (Decision, error) {
	if l.store == nil {
		return Decision{}, fmt.Errorf("limiter: store is nil")
	}
	key := buildStoreKey(p, req)

	for {
		if err := ctx.Err(); err != nil {
			// Timed out or canceled before we could commit: no state touched.
			return l.degrade(p, err)
		}

		oldRaw, err := l.store.Get(ctx, key)
		if err != nil {
			return l.degrade(p, err)
		}

		res := dispatch(p.Algorithm, oldRaw, l.clock.Mono(), p)
		if !res.dirty {
			return decisionFrom(p, res), nil
		}

		swapped, err := l.store.CAS(ctx, key, oldRaw, res.newState)
		if err != nil {
			return l.degrade(p, err)
		}
		if swapped {
			return decisionFrom(p, res), nil
		}
		// Conflict: another instance committed first. Retry on the latest
		// committed state. This is safe because each attempt re-reads the
		// full count; no attempt ever commits on top of a stale value.
	}
}

func decisionFrom(p Policy, res algoResult) Decision {
	d := Decision{
		PolicyID:  p.ID,
		Algorithm: p.Algorithm,
		Version:   p.Version,
		Remaining: res.remaining,
	}
	if res.allowed {
		d.Allowed = true
		d.Reason = ReasonAllowed
	} else {
		d.Allowed = false
		d.Reason = ReasonRejected
	}
	return d
}

func (l *Limiter) degrade(p Policy, cause error) (Decision, error) {
	_ = cause // reason is encoded in the decision, not the error
	d := Decision{
		PolicyID:  p.ID,
		Algorithm: p.Algorithm,
		Version:   p.Version,
	}
	if p.Degrade == DegradeAllow {
		d.Allowed = true
		d.Reason = ReasonDegradedAllow
	} else {
		d.Allowed = false
		d.Reason = ReasonDegradedReject
	}
	return d, nil
}

// buildStoreKey derives the isolated counter key for a policy/request pair.
// Caller-scoped counters include the caller id; service-scoped counters
// include the service id. Different callers therefore never share a counter,
// and different keys never share one either.
func buildStoreKey(p Policy, req Request) string {
	var scopeID string
	switch p.Scope {
	case ScopeService:
		scopeID = req.ServiceID
	default:
		scopeID = req.CallerID
	}
	return fmt.Sprintf("rl:%d:%s:%s:%s",
		p.Scope,
		url.QueryEscape(p.ID),
		url.QueryEscape(scopeID),
		url.QueryEscape(req.Key),
	)
}
