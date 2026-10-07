package ratelimit

import (
	"encoding/json"
	"math"
	"sort"
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
// Time framing: the monotonic clock resets to ~0 when a process restarts, so
// bucket timestamps cannot be stored as bare monotonic readings (they would
// become far-future timestamps after a restart and never expire). Instead the
// state pins an anchor pair (AM, AW): the monotonic reading and the wall-clock
// reading at the same instant in the current time lineage. Bucket starts are
// stored as offsets T from AM; a bucket's absolute monotonic time in the
// current lineage is AM+T.
//
// When a later evaluation sees a different lineage (a process restart), or a
// same-lineage clock rollback that crosses the anchor (nowMono < AM), the
// physical time that elapsed across the break is known from the wall clock,
// gap = nowWall-AW, so existing usage ages by REAL elapsed time: bucket
// offsets are shifted by -gap and the anchor re-pinned. Wall backwards jumps
// are treated as zero elapsed and never make offsets positive, so no quota is
// ever created by an adjustment; offsets never point into the future, so usage
// is not held longer than the evidence warrants once the clock moves forward.
type slidingState struct {
	// V is the state format version. V==0 is the pre-anchor on-disk format
	// where bucket T values are absolute monotonic nanoseconds.
	V int `json:"v,omitempty"`
	// L is the clock lineage the frame belongs to. A change of lineage marks
	// a process restart even when both Mono readings happen to be ~0.
	L int64 `json:"l,omitempty"`
	// AM/AW pin one instant in the current monotonic lineage.
	AM int64 `json:"am,omitempty"`
	AW int64 `json:"aw,omitempty"`
	// Buckets holds sub-bucket starts as offsets from AM (mono nanoseconds).
	Buckets []slidingBucket `json:"b"`
}

type slidingBucket struct {
	T int64 `json:"t"` // sub-bucket start as an offset from AM, mono nanoseconds
	C int64 `json:"c"` // request count in this sub-bucket
}

// rebaseFrame detects a monotonic-lineage reset and maps every persisted
// bucket into the new lineage using the real wall-clock elapsed time. It must
// be called before expiry/insertion.
func (st *slidingState) rebaseFrame(nowMono, nowWall, lineage int64) {
	if st.V < 1 {
		// Legacy format: T values are absolute monotonic readings from a
		// previous process/version and no wall correspondence was recorded.
		// Conservatively map the newest bucket to "now" (preserving relative
		// spacing): nothing is released without time evidence, but all usage
		// expires within at most one Window of real forward time and can never
		// lock the quota permanently.
		maxT := int64(0)
		for _, b := range st.Buckets {
			if b.T > maxT {
				maxT = b.T
			}
		}
		for i := range st.Buckets {
			st.Buckets[i].T -= maxT
		}
		st.V = 1
		st.L = lineage
		st.AM = nowMono
		st.AW = nowWall
		return
	}

	if st.L == lineage && nowMono >= st.AM {
		// Same lineage, monotonic reading not behind the anchor: ordinary
		// expiry against the existing frame is exactly right.
		return
	}

	// The lineage changed (process restart) or a within-lineage rollback
	// crossed the anchor. Physical time across the break is taken from the
	// wall clock, gap = nowWall-AW, so existing usage ages by REAL elapsed
	// time: bucket offsets are shifted by -gap and the anchor re-pinned.
	gap := nowWall - st.AW
	maxT := int64(0)
	for _, b := range st.Buckets {
		if b.T > maxT {
			maxT = b.T
		}
	}
	// A bucket's age after the break is gap - T. Two safety clamps:
	//   - gap < 0 (wall moved backwards): no elapsed time is credited, so no
	//     quota is ever created by a clock adjustment;
	//   - gap < maxT (clocks inconsistent / wall shorter than mono history):
	//     raise gap so the newest bucket maps to the present rather than into
	//     the future; this only withholds quota, never releases extra.
	// A genuinely long downtime leaves gap large and offsets deeply negative,
	// so usage older than Window expires immediately on the first post-restart
	// evaluation.
	if gap < maxT {
		gap = maxT
	}
	for i := range st.Buckets {
		st.Buckets[i].T -= gap
	}
	st.L = lineage
	st.AM = nowMono
	st.AW = nowWall
}

// slidingWindowLimit returns the maximum number of requests allowed inside
// one window: the burst cap jointly constrained by the rate. The rate buys at
// most Rate*Window requests per window span (rounded up per window boundary),
// while Burst is the hard instantaneous cap.
func slidingWindowLimit(p Policy) int64 {
	rateCap := int64(math.Ceil(p.Rate * p.Window.Seconds()))
	if rateCap < 1 {
		rateCap = 1
	}
	if rateCap < p.Burst {
		return rateCap
	}
	return p.Burst
}

// slidingWindowAllow implements the sliding-window algorithm:
//   - The window spans Window and holds at most min(Burst, ceil(Rate*Window))
//     requests, so both the burst cap and the configured rate constrain the
//     admitted count; changing Rate changes the decision immediately.
//   - Time is divided into windowSlices fixed sub-buckets; sub-buckets older
//     than Window drop off, so the boundary is continuous (no fixed-window
//     reset burst).
//   - Bucket timestamps live in a pinned (mono, wall) time frame, so usage
//     ages by real physical time across a process restart and is released as
//     soon as the true elapsed time crosses Window; a quota can never be
//     locked by a process lifecycle change.
//   - A request is allowed while the sum of in-window counts is below the
//     limit. Wall/mono clock adjustments create no quota and cannot lock the
//     window: a backwards jump accrues no elapsed time, and forward movement
//     frees the expired buckets normally.
func slidingWindowAllow(raw []byte, now time.Duration, wallNano, lineage int64, p Policy) algoResult {
	nowNano := int64(now)
	bucketSize := int64(p.Window) / windowSlices
	if bucketSize <= 0 {
		bucketSize = 1
	}
	limit := slidingWindowLimit(p)

	var st slidingState
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &st)
	}
	if len(raw) == 0 {
		// First-ever state: pin the frame at the current clock/lineage.
		st = slidingState{V: 1, L: lineage, AM: nowNano, AW: wallNano}
	}
	st.rebaseFrame(nowNano, wallNano, lineage)

	// Expire buckets older than the window. A bucket offset T places it at
	// AM+T; the cutoff in offset coordinates is (nowNano-Window)-AM.
	cutoff := nowNano - int64(p.Window) - st.AM
	kept := st.Buckets[:0]
	sum := int64(0)
	for _, b := range st.Buckets {
		if b.T > cutoff {
			kept = append(kept, b)
			sum += b.C
		}
	}
	st.Buckets = kept

	allowed := sum < limit
	if allowed {
		cur := nowNano - (nowNano % bucketSize)
		curOff := cur - st.AM
		merged := false
		for i := range st.Buckets {
			if st.Buckets[i].T == curOff {
				st.Buckets[i].C++
				merged = true
				break
			}
		}
		if !merged {
			st.Buckets = append(st.Buckets, slidingBucket{T: curOff, C: 1})
			// A clock step within a lineage can make the new sub-bucket start
			// order non-monotonic; keep starts sorted for stable expiry.
			sort.Slice(st.Buckets, func(i, j int) bool {
				return st.Buckets[i].T < st.Buckets[j].T
			})
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

func dispatch(alg AlgorithmType, raw []byte, now time.Duration, wallNano, lineage int64, p Policy) algoResult {
	switch alg {
	case AlgTokenBucket:
		return tokenBucketAllow(raw, now, p)
	case AlgLeakyBucket:
		return leakyBucketAllow(raw, now, p)
	case AlgSlidingWindow:
		return slidingWindowAllow(raw, now, wallNano, lineage, p)
	default:
		return tokenBucketAllow(raw, now, p)
	}
}
