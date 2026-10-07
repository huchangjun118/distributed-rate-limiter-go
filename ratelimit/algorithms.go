package ratelimit

import (
	"encoding/json"
	"math"
	"time"
)

// algoResult is the outcome of a pure algorithm evaluation against a
// committed state.
type algoResult struct {
	allowed   bool
	remaining int64
	// newState is the state to commit via CAS.
	newState []byte
	// dirty reports whether the state changed and needs committing.
	dirty bool
}

// micro is the fixed-point scale: 1 token = 1_000_000 micro-units. Integer
// math avoids float drift across many refills.
const micro = int64(1_000_000)

func rateMicroPerSec(p Policy) int64 {
	r := p.Rate * float64(micro)
	if r > float64(1<<62) {
		return 1 << 62
	}
	return int64(r)
}

func clampElapsed(now, last int64) int64 {
	if now <= last {
		// Monotonic clock rolled back (or did not advance): no quota accrues.
		return 0
	}
	return now - last
}

// meteringElapsed returns the non-negative elapsed metering time. On a
// backwards jump it re-bases the last-seen marker to the authoritative clock.
// Re-basing never touches the stored balance, so it neither creates quota
// (refill is capped at burst anyway) nor withholds it indefinitely: future
// refill accrues from the current clock reading instead of waiting for the
// clock to pass a future-dated marker.
func meteringElapsed(nowNano int64, last *int64) int64 {
	elapsed := nowNano - *last
	if elapsed < 0 {
		*last = nowNano
		return 0
	}
	return elapsed
}

// tokenState is the persisted token-bucket state.
type tokenState struct {
	Tokens int64 `json:"t"` // micro-tokens
	Last   int64 `json:"l"` // last metering time, mono nanoseconds
}

// tokenBucketAllow implements the token-bucket algorithm:
//   - The bucket starts full (Burst tokens) and refills at Rate tokens/second.
//   - A request consumes one token; it is rejected when the bucket is empty.
//   - Burst allows up to Burst concurrent/instant requests; refills never
//     exceed Burst.
//   - A clock rollback (elapsed < 0) accrues nothing and does not move Last
//     backwards, so no quota is created out of thin air.
func tokenBucketAllow(raw []byte, now time.Duration, p Policy) algoResult {
	cap := p.Burst * micro
	nowNano := int64(now)

	var st tokenState
	if len(raw) == 0 {
		st = tokenState{Tokens: cap, Last: nowNano}
	} else if err := json.Unmarshal(raw, &st); err != nil {
		// Corrupt state: start full rather than locking the quota.
		st = tokenState{Tokens: cap, Last: nowNano}
	}
	if st.Tokens > cap {
		st.Tokens = cap
	}

	elapsed := meteringElapsed(nowNano, &st.Last)
	if elapsed > 0 {
		add := elapsed * rateMicroPerSec(p) / int64(time.Second)
		st.Tokens += add
		if st.Tokens > cap {
			st.Tokens = cap
		}
		st.Last = nowNano
	}

	allowed := st.Tokens >= micro
	if allowed {
		st.Tokens -= micro
	}
	out, _ := json.Marshal(st)
	return algoResult{allowed: allowed, remaining: st.Tokens / micro, newState: out, dirty: true}
}

// leakyState is the persisted leaky-bucket state.
type leakyState struct {
	Level int64 `json:"v"` // micro-units of queued work
	Last  int64 `json:"l"` // last metering time, mono nanoseconds
}

// leakyBucketAllow implements the leaky-bucket algorithm:
//   - The bucket has capacity Burst and leaks at Rate units/second.
//   - A request adds one unit when the bucket has room; overflow is rejected.
//   - Output is smooth: at most Rate requests/second sustained, with room for
//     Burst queued requests.
func leakyBucketAllow(raw []byte, now time.Duration, p Policy) algoResult {
	cap := p.Burst * micro
	nowNano := int64(now)

	var st leakyState
	if len(raw) == 0 {
		st = leakyState{Level: 0, Last: nowNano}
	} else if err := json.Unmarshal(raw, &st); err != nil {
		st = leakyState{Level: 0, Last: nowNano}
	}

	elapsed := meteringElapsed(nowNano, &st.Last)
	if elapsed > 0 {
		leaked := elapsed * rateMicroPerSec(p) / int64(time.Second)
		st.Level -= leaked
		if st.Level < 0 {
			st.Level = 0
		}
		st.Last = nowNano
	}

	allowed := st.Level+micro <= cap
	if allowed {
		st.Level += micro
	}
	remaining := (cap - st.Level) / micro
	if remaining < 0 {
		remaining = 0
	}
	out, _ := json.Marshal(st)
	return algoResult{allowed: allowed, remaining: remaining, newState: out, dirty: true}
}

// windowSlices is the number of sub-buckets a sliding window is divided into.
const windowSlices = 10

// slidingState is the persisted sliding-window state.
//
// Bucket timestamps and expiry are expressed on Meter, a virtual metering
// clock persisted alongside the raw clock readings. Meter advances with the
// monotonic clock while the reading comes from the same process lifetime
// (same BootID) and is monotonic; across a restart (different BootID) or a
// monotonic rollback it advances by the real physical (wall-clock) time
// elapsed since the last commit instead. Usage therefore ages by real
// elapsed time across restarts, while a wall-clock rollback can never create
// quota (negative deltas are clamped to zero).
type slidingState struct {
	Buckets []slidingBucket `json:"b"`
	Meter   int64           `json:"m"`  // virtual metering clock, nanoseconds
	Mono    int64           `json:"mo"` // mono reading at last commit
	Wall    int64           `json:"w"`  // wall reading at last commit, unix nanoseconds
	Boot    int64           `json:"bt"` // boot id of the last committer
}

type slidingBucket struct {
	T int64 `json:"t"` // sub-bucket start, meter nanoseconds
	C int64 `json:"c"` // request count in this sub-bucket
}

// advance brings st.Meter up to date from the raw clock readings and records
// them as the new anchor.
func (st *slidingState) advance(nowMono, nowWall, nowBoot int64) {
	switch {
	case st.Wall == 0:
		// No anchor yet: fresh state, or a legacy state persisted before the
		// anchor fields existed. Legacy bucket timestamps were recorded on a
		// foreign (pre-restart) clock and can never expire on the new one;
		// rebase Meter to the newest bucket so the recorded usage is kept but
		// ages out within one window from now.
		for _, b := range st.Buckets {
			if b.T > st.Meter {
				st.Meter = b.T
			}
		}
	case st.Boot == nowBoot && nowMono >= st.Mono:
		// Same process lifetime and the monotonic clock is monotonic: trust
		// it, so wall-clock adjustments (NTP steps, manual changes) cannot
		// affect aging.
		st.Meter += nowMono - st.Mono
	default:
		// A different process lifetime (restart) or a monotonic rollback:
		// mono readings are incomparable. Age by real physical elapsed time
		// so usage older than the window is released; a wall rollback clamps
		// to zero and never creates quota.
		if d := nowWall - st.Wall; d > 0 {
			st.Meter += d
		}
	}
	st.Mono = nowMono
	st.Wall = nowWall
	st.Boot = nowBoot
}

// slidingWindowLimit is the per-window request cap, jointly determined by
// Burst and Rate: at most Burst requests per window, and at most
// ceil(Rate * Window) so the sustained rate never exceeds Rate per second.
// The cap is at least 1 so a tiny Rate*Window can never lock the key out
// permanently.
func slidingWindowLimit(p Policy) int64 {
	limit := p.Burst
	if r := int64(math.Ceil(p.Rate * p.Window.Seconds())); r < limit {
		limit = r
	}
	if limit < 1 {
		limit = 1
	}
	return limit
}

// slidingWindowAllow implements the sliding-window algorithm:
//   - The window spans Window and holds at most slidingWindowLimit(p)
//     requests (see above: Burst capped by Rate).
//   - Time is divided into windowSlices fixed sub-buckets; sub-buckets older
//     than Window drop off, so the boundary is continuous (no fixed-window
//     reset burst).
//   - A request is allowed while the sum of in-window counts is below the
//     limit.
//   - The boundary is deterministic: at meter time t, any bucket with
//     t - bucketStart >= Window is expired.
//   - Aging uses the persisted metering clock (see slidingState), so usage
//     ages by real elapsed time across process restarts and never resurrects
//     on a clock rollback.
func slidingWindowAllow(raw []byte, mono time.Duration, wall time.Time, boot int64, p Policy) algoResult {
	nowMono := int64(mono)
	nowWall := wall.UnixNano()
	bucketSize := int64(p.Window) / windowSlices
	if bucketSize <= 0 {
		bucketSize = 1
	}

	var st slidingState
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &st)
	}
	st.advance(nowMono, nowWall, boot)
	nowNano := st.Meter

	// Expire buckets older than the window.
	cutoff := nowNano - int64(p.Window)
	kept := st.Buckets[:0]
	sum := int64(0)
	for _, b := range st.Buckets {
		if b.T > cutoff {
			kept = append(kept, b)
			sum += b.C
		}
	}
	st.Buckets = kept

	limit := slidingWindowLimit(p)
	allowed := sum < limit
	if allowed {
		cur := nowNano - (nowNano % bucketSize)
		if n := len(st.Buckets); n > 0 && st.Buckets[n-1].T == cur {
			st.Buckets[n-1].C++
		} else {
			st.Buckets = append(st.Buckets, slidingBucket{T: cur, C: 1})
		}
		sum++
	}

	remaining := limit - sum
	if remaining < 0 {
		remaining = 0
	}
	out, _ := json.Marshal(st)
	return algoResult{allowed: allowed, remaining: remaining, newState: out, dirty: true}
}

func dispatch(alg AlgorithmType, raw []byte, mono time.Duration, wall time.Time, boot int64, p Policy) algoResult {
	switch alg {
	case AlgTokenBucket:
		return tokenBucketAllow(raw, mono, p)
	case AlgLeakyBucket:
		return leakyBucketAllow(raw, mono, p)
	case AlgSlidingWindow:
		return slidingWindowAllow(raw, mono, wall, boot, p)
	default:
		return tokenBucketAllow(raw, mono, p)
	}
}
