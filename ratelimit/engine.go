package ratelimit

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

// Physical key layout. Prefixes guarantee:
//   - caller and server scopes never collide for the same logical key;
//   - distinct rule IDs are independent counters, so one rule's burst can
//     never consume another rule's quota;
//   - one caller's burst for a key never consumes another caller's.
const (
	prefixCaller = "c:"
	prefixServer = "s:"
)

type compiledRule struct {
	rule Rule
	view limitView
}

type compiledRules struct {
	version uint64
	byID    map[string]compiledRule
	// sorted IDs: evaluation order is independent of configuration order,
	// which makes the reported deny set stable.
	order []string
}

func compile(p PolicyConfig) *compiledRules {
	cr := &compiledRules{version: p.Version, byID: map[string]compiledRule{}}
	for _, r := range p.Rules {
		v := limitView{algo: r.Algo, rate: r.Rate, burst: r.Burst, subs: r.SubBuckets}
		if r.Window > 0 {
			v.window = r.Window.Nanoseconds()
		}
		cr.byID[r.ID] = compiledRule{rule: r, view: v}
		cr.order = append(cr.order, r.ID)
	}
	sort.Strings(cr.order)
	return cr
}

// physicalKey returns the counter key for a rule/request pair.
func physicalKey(r Rule, req *Request) string {
	var b strings.Builder
	if r.Scope == ScopeServer {
		b.WriteString(prefixServer)
		b.WriteString(r.ID)
		b.WriteByte('|')
		b.WriteString(req.Key)
		return b.String()
	}
	b.WriteString(prefixCaller)
	b.WriteString(r.ID)
	b.WriteByte('|')
	b.WriteString(req.Caller)
	b.WriteByte('|')
	b.WriteString(req.Key)
	return b.String()
}

// Engine is the service entry point.
type Engine struct {
	cfg Config

	mu       sync.RWMutex
	current  *PolicyConfig
	compiled *compiledRules

	// optional persistence coordination (only when the engine itself owns
	// local counters restored from a Store)
	persistCounter *LocalCounter
	stop           chan struct{}
	done           chan struct{}
	closeOnce      sync.Once
}

// NewEngine builds an engine. If cfg.Store != nil, a single shared
// *LocalCounter* is created, restored from the snapshot (state rebased onto
// cfg.Clock across the restart), wrapped into both scopes with distinct
// prefixes, and background persistence is started. Otherwise cfg.Counters
// must be fully provided.
func NewEngine(cfg Config, initial PolicyConfig) (*Engine, error) {
	norm, err := initial.Validate()
	if err != nil {
		return nil, err
	}
	if cfg.Clock == nil {
		cfg.Clock = DefaultClock()
	}
	if cfg.CounterTimeout <= 0 {
		cfg.CounterTimeout = defaultCounterTimeout
	}
	if cfg.PersistInterval <= 0 {
		cfg.PersistInterval = defaultPersistEvery
	}

	e := &Engine{
		cfg:      cfg,
		current:  &norm,
		compiled: compile(norm),
	}

	if cfg.Store != nil {
		lc, lerr := NewLocalCounter(cfg.Clock, cfg.Store)
		if lerr != nil {
			return nil, lerr
		}
		e.persistCounter = lc
		// Both scopes share the physical map but never collide because keys
		// carry distinct c:/s: prefixes.
		e.cfg.Counters = Counters{Caller: lc, Server: lc}
		e.stop = make(chan struct{})
		e.done = make(chan struct{})
		go e.persistLoop()
	}
	if e.cfg.Counters.Caller == nil || e.cfg.Counters.Server == nil {
		return nil, ErrInvalidConfig
	}
	return e, nil
}

// Allow evaluates one request against every matching rule of the CURRENT
// pinned version:
//
//   - Matching rules are processed in ascending rule-ID order regardless of
//     configuration order; every matching rule is evaluated and reports
//     independently, so the set of denying rules is stable.
//   - A request is Allowed only if every matching rule admits it.
//     Admitted rules have consumed their quota even if a later rule denies:
//     this is the documented "no global rollback" semantics — quota
//     consumption mirrors what happened on the shared counter and avoids
//     rollback races between instances. All denying rule IDs with reasons
//     are returned sorted.
//   - Any per-rule counter error (unavailable / deadline / canceled) is
//     handled by that rule's FailMode: FailOpen admits with a distinguishable
//     degraded reason; FailClosed denies with a distinct reason per cause.
//     Degraded rules never commit partial state.
func (e *Engine) Allow(ctx context.Context, req *Request) Decision {
	cr := e.pin()
	d := Decision{Allowed: true, Version: cr.version, Reasons: map[string]Reason{}}
	if req.Tokens <= 0 {
		req.Tokens = 1
	}

	nowMono := e.cfg.Clock.Mono()
	nowWall := e.cfg.Clock.Now().UnixNano()
	n := req.Tokens

	for _, id := range cr.order {
		c := cr.byID[id]
		if c.rule.Match != nil && !c.rule.Match.Match(req) {
			continue
		}
		counter := e.cfg.Counters.Server
		if c.rule.Scope == ScopeCaller {
			counter = e.cfg.Counters.Caller
		}
		pkey := physicalKey(c.rule, req)
		view := c.view

		tctx, cancel := context.WithTimeout(ctx, e.cfg.CounterTimeout)
		admit, err := counter.Update(tctx, pkey, func(st *algoState, found bool) (bool, *algoState, error) {
			if found && st.Kind != view.algo {
				// Same physical key was created by a previous version with a
				// different algorithm (rule ID reused). Treat its state as
				// fresh rather than mis-interpreting bytes.
				st = newState(view.algo)
				found = false
			}
			if !found {
				st = newState(view.algo)
			}
			ok := view.withState(st).tryAcquire(nowMono, nowWall, n)
			return ok, st, nil
		})
		cancel()

		if err != nil {
			reason := degradeReason(err)
			if c.rule.Fail == FailOpen {
				d.Degraded = true
				d.Reasons[id] = openReason(reason)
				continue // nothing committed: no partial state, no locked quota
			}
			d.Allowed = false
			d.RejectedBy = append(d.RejectedBy, id)
			d.Reasons[id] = reason
			continue
		}
		if !admit {
			d.Allowed = false
			d.RejectedBy = append(d.RejectedBy, id)
			d.Reasons[id] = algoDenyReason(c.rule.Algo)
		}
	}
	sort.Strings(d.RejectedBy)
	return d
}

func (e *Engine) pin() *compiledRules {
	e.mu.RLock()
	cr := e.compiled
	e.mu.RUnlock()
	return cr
}

// Update validates and atomically installs a new policy version. Evaluation
// pins one immutable pointer, so an in-flight Allow observes either the old
// or the new version completely. On validation failure the previous version
// keeps serving unchanged.
func (e *Engine) Update(cfg PolicyConfig) error {
	norm, err := cfg.Validate()
	if err != nil {
		return err
	}
	e.mu.Lock()
	e.current = &norm
	e.compiled = compile(norm)
	e.mu.Unlock()
	return nil
}

// CurrentVersion returns the installed version number.
func (e *Engine) CurrentVersion() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.current.Version
}

// CurrentRules returns a copy of the installed rules (inspection).
func (e *Engine) CurrentRules() []Rule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]Rule, len(e.current.Rules))
	copy(out, e.current.Rules)
	return out
}

// Persist forces one snapshot flush. No-op without an owned Store.
func (e *Engine) Persist(ctx context.Context) error {
	if e.persistCounter == nil || e.cfg.Store == nil {
		return nil
	}
	return e.persistCounter.persist(ctx, e.cfg.Store, e.cfg.Clock)
}

func (e *Engine) persistLoop() {
	t := time.NewTicker(e.cfg.PersistInterval)
	defer t.Stop()
	for {
		select {
		case <-e.stop:
			close(e.done)
			return
		case <-t.C:
			// Persistence failure must not affect serving: the last good
			// snapshot simply remains, and the next tick retries.
			_ = e.Persist(context.Background())
		}
	}
}

// Close flushes a final snapshot (best effort), stops background work and
// closes the Store. It is idempotent.
func (e *Engine) Close() error {
	if e.stop == nil {
		return nil
	}
	e.closeOnce.Do(func() {
		close(e.stop)
		<-e.done
		_ = e.Persist(context.Background())
	})
	return e.cfg.Store.Close()
}

// withState returns a copy of the parameter view bound to st.
func (v limitView) withState(st *algoState) limitView {
	v.state = st
	return v
}

func algoDenyReason(a Algorithm) Reason {
	switch a {
	case AlgoTokenBucket:
		return ReasonTokenBucketEmpty
	case AlgoLeakyBucket:
		return ReasonLeakyBucketFull
	case AlgoSlidingWindow:
		return ReasonWindowExceeded
	default:
		return ReasonCounterUnavail
	}
}

// degradeReason maps a counter-layer error to a distinct fail-closed reason.
func degradeReason(err error) Reason {
	switch {
	case isDeadline(err):
		return ReasonCounterTimeout
	case isCanceled(err):
		return ReasonCounterCanceled
	default:
		return ReasonCounterUnavail
	}
}

func openReason(r Reason) Reason {
	switch r {
	case ReasonCounterTimeout:
		return ReasonDegradedTimeout
	case ReasonCounterCanceled:
		return ReasonDegradedCanceled
	default:
		return ReasonDegradedAllow
	}
}

func isDeadline(err error) bool { return errors.Is(err, context.DeadlineExceeded) }
func isCanceled(err error) bool { return errors.Is(err, context.Canceled) }
