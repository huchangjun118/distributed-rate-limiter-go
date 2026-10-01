package ratelimit

import "time"

// Clock abstracts time sources used by rate limiting.
//
// Monotonic time (Mono) is the ONLY clock used to refill token buckets or
// drain leaky buckets: it is immune to wall-clock adjustments (NTP step,
// manual set, rollback). Wall time (Now) is recorded for two purposes only:
//
//   - aligning sliding-window buckets with absolute, restart-independent
//     timestamps, so in-window usage survives restarts;
//   - bridging one process restart (the monotonic origin is per-process):
//     the shutdown gap is measured on the wall clock and clamped at zero,
//     i.e. a backwards wall clock can never fabricate elapsed time.
//
// Design contract: callers MUST treat Mono deltas as the source of truth for
// quota accrual. A wall-clock step backwards MUST NOT create quota and MUST
// NOT cause permanent denials; a wall-clock step forwards ages/expires
// state by real elapsed time only through the clamped restart gap.
type Clock interface {
	// Mono returns a monotonically non-decreasing reading in nanoseconds.
	// Only differences between readings (within one process) are meaningful.
	Mono() int64
	// Now returns the current wall-clock time. It may jump in either
	// direction; code must never derive quota from its live delta.
	Now() time.Time
}

// procStart is fixed once per process so every realClock instance in the
// process (e.g. multiple "instances" of the limiter sharing one counter)
// agrees on the monotonic origin.
var procStart = time.Now()

// realClock uses Go's runtime monotonic clock. time.Time values captured in
// this process carry a monotonic extent, and subtracting two such values
// ignores the wall component: NTP steps / manual wall changes cannot make
// Mono go backwards.
type realClock struct{}

// Mono implements Clock: nanoseconds since process start, strictly driven by
// the runtime monotonic clock.
func (realClock) Mono() int64 { return time.Since(procStart).Nanoseconds() }

// Now implements Clock.
func (realClock) Now() time.Time { return time.Now() }

// DefaultClock is the production clock.
func DefaultClock() Clock { return realClock{} }

// ManualClock is a deterministic clock for tests and reproducible local
// demos: both readings are fully controllable, and it is impossible to move
// Mono backwards via SetWall.
type ManualClock struct {
	mono int64
	wall time.Time
}

// NewManualClock creates a ManualClock anchored at the given wall time with
// monotonic reading 0.
func NewManualClock(wall time.Time) *ManualClock {
	return &ManualClock{wall: wall}
}

// Mono implements Clock.
func (c *ManualClock) Mono() int64 { return c.mono }

// Now implements Clock.
func (c *ManualClock) Now() time.Time { return c.wall }

// Advance moves BOTH readings forward by d. d must be >= 0 (monotonic time
// cannot go backwards).
func (c *ManualClock) Advance(d time.Duration) {
	if d < 0 {
		panic("ratelimit: ManualClock.Advance with negative duration")
	}
	c.mono += int64(d)
	c.wall = c.wall.Add(d)
}

// SetWall sets the wall clock to any t (including in the past). The
// monotonic reading is deliberately untouched: this simulates an NTP /
// manual wall adjustment and proves refill math does not depend on it.
func (c *ManualClock) SetWall(t time.Time) { c.wall = t }
