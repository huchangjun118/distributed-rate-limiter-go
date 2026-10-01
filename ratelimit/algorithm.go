package ratelimit

// This file contains the deterministic state machines of the three
// algorithms. Every function is pure math over (now, params, state): the same
// inputs always produce the same decision and next state, so behavior is
// reproducible and independent of goroutine scheduling.
//
// Concurrency safety does not live here: callers (Counter) invoke this math
// inside a transaction against one committed snapshot and commit the result
// atomically, so admitted totals can never exceed capacity.

// algoState is the per-key mutable state held inside the shared counter.
// Different rules map to different physical keys, and creation validates
// that one physical key keeps one algorithm; therefore Kind alone selects
// which fields are meaningful. The shape is JSON-persisted.
type algoState struct {
	Kind Algorithm `json:"kind"`
	// Init marks a continuous-algorithm key as initialized; it removes the
	// ambiguity of LastMono legitimately being 0 on a restored snapshot.
	Init bool `json:"init,omitempty"`

	// Continuous algorithms (token bucket / leaky bucket).
	LastMono int64   `json:"last_mono"` // monotonic ns of last update
	Tokens   float64 `json:"tokens"`    // tb: available tokens; lb: queued requests

	// Parameters are snapshotted alongside state so a restart can mature
	// refill/drain for the shutdown gap before the first request arrives,
	// without needing the rule catalog.
	SavedRate  float64 `json:"saved_rate,omitempty"`
	SavedBurst int64   `json:"saved_burst,omitempty"`

	// Sliding window: counts keyed by sub-bucket start in WALL nanoseconds.
	// Wall alignment is what makes in-window usage meaningful across a
	// process restart (monotonic origins are per-process).
	Buckets map[int64]int64 `json:"buckets,omitempty"`
}

func newState(kind Algorithm) *algoState {
	st := &algoState{Kind: kind}
	if kind == AlgoSlidingWindow {
		st.Buckets = map[int64]int64{}
	}
	return st
}

// clone returns a deep-enough copy for optimistic concurrency.
func (s *algoState) clone() *algoState {
	cp := *s
	if s.Buckets != nil {
		cp.Buckets = make(map[int64]int64, len(s.Buckets))
		for b, n := range s.Buckets {
			cp.Buckets[b] = n
		}
	}
	return &cp
}

// limitView bundles one rule's algorithm parameters.
type limitView struct {
	algo   Algorithm
	rate   float64
	burst  int64
	window int64 // ns
	subs   int
	state  *algoState
}

// tryAcquire advances the state in place and reports whether n units fit.
//
// nowMono: current monotonic reading (ns) — drives tb refill / lb drain.
// nowWall: current wall time in ns since epoch — aligns window buckets.
func (v limitView) tryAcquire(nowMono, nowWall, n int64) bool {
	switch v.algo {
	case AlgoTokenBucket:
		return v.tokenBucket(nowMono, n)
	case AlgoLeakyBucket:
		return v.leakyBucket(nowMono, n)
	case AlgoSlidingWindow:
		return v.slidingWindow(nowWall, n)
	default:
		return false
	}
}

// tokenBucket implements the classic token bucket.
//
// Boundary / burst semantics (deterministic):
//   - Capacity is Burst. A brand-new key STARTS FULL (cold-start burst), so
//     the first Burst units are admitted immediately.
//   - Tokens refill continuously at Rate tokens/s starting from the last
//     update, capped at Burst (no unbounded accumulation).
//   - Elapsed time is computed from the MONOTONIC clock and clamped at >= 0,
//     so a negative/zero interval adds exactly zero tokens. A monotonic
//     clock cannot advance by more than real time, hence wall-clock changes
//     can neither fabricate tokens nor wedge the limiter closed.
//   - Admission is "available >= requested": rejected requests consume
//     nothing; the refill timestamp is still advanced to now so no interval
//     is counted twice (and no interval is skipped).
func (v limitView) tokenBucket(nowMono, n int64) bool {
	st := v.state
	cap := float64(v.burst)
	if !st.Init {
		st.Init = true
		st.LastMono, st.Tokens = nowMono, cap
	}
	st.SavedRate, st.SavedBurst = v.rate, v.burst
	if elapsed := nowMono - st.LastMono; elapsed > 0 {
		st.Tokens += v.rate * float64(elapsed) / 1e9
		if st.Tokens > cap {
			st.Tokens = cap
		}
	}
	st.LastMono = nowMono
	if st.Tokens+1e-9 < float64(n) {
		return false
	}
	st.Tokens -= float64(n)
	return true
}

// leakyBucket implements the leaky bucket as a meter with constant outflow.
//
// Boundary / burst semantics (deterministic):
//   - Capacity is Burst queued requests; a brand-new key STARTS EMPTY.
//   - The water level drains continuously at Rate requests/s, never below
//     zero, based on the MONOTONIC clock (interval clamped at >= 0).
//   - Admission: admitted iff the post-leak level plus n stays <= Burst;
//     otherwise rejected and the level is unchanged. Accepted traffic can
//     burst up to Burst and is thereafter shaped to exactly Rate/s.
func (v limitView) leakyBucket(nowMono, n int64) bool {
	st := v.state
	cap := float64(v.burst)
	if !st.Init {
		st.Init = true
		st.LastMono, st.Tokens = nowMono, 0
	}
	st.SavedRate, st.SavedBurst = v.rate, v.burst
	if elapsed := nowMono - st.LastMono; elapsed > 0 {
		st.Tokens -= v.rate * float64(elapsed) / 1e9
		if st.Tokens < 0 {
			st.Tokens = 0
		}
	}
	st.LastMono = nowMono
	if st.Tokens+float64(n) > cap {
		return false
	}
	st.Tokens += float64(n)
	return true
}

// slidingWindow implements a sliding window over fixed sub-buckets.
//
// Geometry (deterministic):
//   - The window spans Window; time is partitioned into sub-buckets of
//     width Window/SubBuckets aligned to the epoch on the WALL clock.
//   - A bucket starting at b is "in window" at now iff
//     b <= now < b+Window  (half-open [b, b+Window)),
//     i.e. retained buckets satisfy now-Window < b <= now (with now rounded
//     to the current bucket start). Buckets at or older than the trailing
//     edge are dropped, so usage is never released an instant early and
//     only events strictly inside the last Window duration count.
//   - The oldest retained bucket overlaps the trailing edge and is counted
//     FULLY (a deterministic, conservative approximation of a smooth sliding
//     log): this can only over-count, never over-admit.
//   - Admission: admitted iff sum(retained buckets)+n <= limit, where the
//     limit is Rate interpreted as requests per Window; Burst > 0 overrides
//     it. This makes "rate per window" and "burst cap" both explicit.
//   - Bucket keys use wall-clock starts, so a snapshot restored after a
//     restart ages out by wall time and the unexpired part of the window is
//     retained instead of being zeroed.
func (v limitView) slidingWindow(nowWall, n int64) bool {
	st := v.state
	if st.Buckets == nil {
		st.Buckets = map[int64]int64{}
	}
	width := v.window / int64(v.subs)
	if width <= 0 {
		width = 1
	}
	curStart := nowWall - (nowWall % width)
	trailing := nowWall - v.window
	var sum int64
	for b, cnt := range st.Buckets {
		if b <= trailing || b > curStart {
			delete(st.Buckets, b)
			continue
		}
		sum += cnt
	}
	limit := int64(v.rate)
	if v.burst > 0 {
		limit = v.burst
	}
	if sum+n > limit {
		return false
	}
	st.Buckets[curStart] += n
	return true
}
