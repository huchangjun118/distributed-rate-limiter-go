package ratelimit

import (
	"sync"
	"time"
)

// Clock abstracts the metering clock. Mono must be monotonic within a process:
// it never goes backwards. Now is the wall clock used for display/persistence.
type Clock interface {
	Now() time.Time
	Mono() time.Duration
}

// realClock uses the system monotonic clock (time.Since is based on it).
type realClock struct{}

var processStart = time.Now()

func NewRealClock() Clock { return realClock{} }

func (realClock) Now() time.Time      { return time.Now() }
func (realClock) Mono() time.Duration { return time.Since(processStart) }

// FakeClock is a manually advanced clock for tests. All methods are safe for
// concurrent use.
type FakeClock struct {
	mu   sync.Mutex
	now  time.Time
	mono time.Duration
}

func NewFakeClock() *FakeClock {
	return &FakeClock{now: time.Unix(1_700_000_000, 0)}
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
