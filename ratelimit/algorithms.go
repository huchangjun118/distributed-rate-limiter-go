package ratelimit

import (
	"encoding/json"
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
type slidingState struct {
	Buckets []slidingBucket `json:"b"`
}

type slidingBucket struct {
	T int64 `json:"t"` // sub-bucket start, mono nanoseconds
	C int64 `json:"c"` // request count in this sub-bucket
}

// slidingWindowAllow implements the sliding-window algorithm:
//   - The window spans Window and holds at most Burst requests.
//   - Time is divided into windowSlices fixed sub-buckets; sub-buckets older
//     than Window drop off, so the boundary is continuous (no fixed-window
//     reset burst).
//   - A request is allowed while the sum of in-window counts is below Burst.
//   - The boundary is deterministic: at time t, any bucket with
//     t - bucketStart >= Window is expired.
func slidingWindowAllow(raw []byte, now time.Duration, p Policy) algoResult {
	nowNano := int64(now)
	bucketSize := int64(p.Window) / windowSlices
	if bucketSize <= 0 {
		bucketSize = 1
	}

	var st slidingState
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &st)
	}

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

	allowed := sum < p.Burst
	if allowed {
		cur := nowNano - (nowNano % bucketSize)
		if n := len(st.Buckets); n > 0 && st.Buckets[n-1].T == cur {
			st.Buckets[n-1].C++
		} else {
			st.Buckets = append(st.Buckets, slidingBucket{T: cur, C: 1})
		}
		sum++
	}

	remaining := p.Burst - sum
	if remaining < 0 {
		remaining = 0
	}
	out, _ := json.Marshal(st)
	return algoResult{allowed: allowed, remaining: remaining, newState: out, dirty: true}
}

func dispatch(alg AlgorithmType, raw []byte, now time.Duration, p Policy) algoResult {
	switch alg {
	case AlgTokenBucket:
		return tokenBucketAllow(raw, now, p)
	case AlgLeakyBucket:
		return leakyBucketAllow(raw, now, p)
	case AlgSlidingWindow:
		return slidingWindowAllow(raw, now, p)
	default:
		return tokenBucketAllow(raw, now, p)
	}
}
