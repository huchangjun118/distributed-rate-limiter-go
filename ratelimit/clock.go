package ratelimit

import (
	"sync"
	"sync/atomic"
	"time"
)

// Clock abstracts the metering clock. Mono must be monotonic within a process:
// it never goes backwards. Now is the wall clock used for display/persistence.
// WallNano is the wall-clock reading in nanoseconds. Wall time may jump
// (NTP step, process restart); it is only used to bridge Mono resets across
// process restarts, never to accrue quota by itself.
//
// Lineage returns a process-lifetime identifier: it is constant within one
// running process and changes when the process restarts. Mono resets at a
// restart, so a Mono reading alone cannot always tell a fresh process from a
// long-lived one (both can read ~0); Lineage disambiguates them.
type Clock interface {
	Now() time.Time
	Mono() time.Duration
	WallNano() int64
	Lineage() int64
}

// realClock uses the system monotonic clock (time.Since is based on it).
type realClock struct{}

var processStart = time.Now()

func NewRealClock() Clock { return realClock{} }

func (realClock) Now() time.Time      { return time.Now() }
func (realClock) Mono() time.Duration { return time.Since(processStart) }
func (realClock) WallNano() int64     { return time.Now().UnixNano() }
func (realClock) Lineage() int64      { return processStart.UnixNano() }

// FakeClock is a manually advanced clock for tests. All methods are safe for
// concurrent use.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	mono    time.Duration
	lineage int64
}

// fakeLineageSeq hands every FakeClock a distinct lineage, so a freshly
// constructed FakeClock models a freshly started process.
var fakeLineageSeq atomic.Int64

func NewFakeClock() *FakeClock {
	return &FakeClock{
		now:     time.Unix(1_700_000_000, 0),
		lineage: 1_700_000_000_000_000_000 + fakeLineageSeq.Add(1),
	}
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

// WallNano returns the wall clock in absolute nanoseconds (UnixNano).
func (c *FakeClock) WallNano() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now.UnixNano()
}

// Lineage returns this clock's process-lifetime identifier.
func (c *FakeClock) Lineage() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lineage
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
