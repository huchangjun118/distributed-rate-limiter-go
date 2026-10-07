package ratelimit

import (
	"sync"
	"sync/atomic"
	"time"
)

// Clock abstracts the metering clock. Mono must be monotonic within a process:
// it never goes backwards. Now is the wall clock used for display/persistence.
type Clock interface {
	Now() time.Time
	Mono() time.Duration
}

// BootClock is an optional extension a Clock may implement. BootID identifies
// the process lifetime (monotonic-clock epoch): two readings with the same
// BootID are comparable via Mono; readings from different BootIDs belong to
// different process lifetimes and their Mono values are unrelated. The
// sliding window uses it to detect restarts and age usage by real physical
// (wall-clock) time across them. Clocks that do not implement it are treated
// as a single lifetime (mono-only aging, the historical behavior).
type BootClock interface {
	BootID() int64
}

// bootOf returns the boot identifier of c, or 0 if c does not expose one.
func bootOf(c Clock) int64 {
	if bc, ok := c.(BootClock); ok {
		return bc.BootID()
	}
	return 0
}

// realClock uses the system monotonic clock (time.Since is based on it).
type realClock struct{}

var processStart = time.Now()

func NewRealClock() Clock { return realClock{} }

func (realClock) Now() time.Time      { return time.Now() }
func (realClock) Mono() time.Duration { return time.Since(processStart) }

// BootID identifies this process lifetime: it changes on every process start.
func (realClock) BootID() int64 { return processStart.UnixNano() }

// FakeClock is a manually advanced clock for tests. All methods are safe for
// concurrent use. Each FakeClock is a distinct process lifetime (distinct
// BootID); creating a new one simulates a process restart.
type FakeClock struct {
	mu   sync.Mutex
	now  time.Time
	mono time.Duration
	boot int64
}

var fakeBoot atomic.Int64

func NewFakeClock() *FakeClock {
	return &FakeClock{now: time.Unix(1_700_000_000, 0), boot: fakeBoot.Add(1)}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) Mono() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mono
}

// BootID identifies this fake process lifetime.
func (c *FakeClock) BootID() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.boot
}

// Advance moves both clocks forward by d.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	c.mono += d
}

// SetMono sets the monotonic reading to an arbitrary value (including a value
// smaller than the previous one) to simulate a system clock rollback. The
// limiter clamps negative elapsed time, so a rollback never creates quota.
func (c *FakeClock) SetMono(m time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mono = m
}

// SetNow sets the wall clock.
func (c *FakeClock) SetNow(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}
