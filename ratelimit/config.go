package ratelimit

import (
	"errors"
	"fmt"
	"time"
)

// PolicyConfig is one complete, versioned rule set. Every successful hot
// update replaces the previous version atomically; an in-flight Allow runs
// entirely against either the old or the new version, never a mixture
// (evaluation pins the immutable compiled pointer once). A failed update
// leaves the previous good version serving unchanged.
type PolicyConfig struct {
	Version uint64 `json:"version"`
	Rules   []Rule `json:"rules"`
}

// Default tuning values.
const (
	defaultSubBuckets     = 10
	defaultCounterTimeout = 200 * time.Millisecond
	defaultPersistEvery   = 5 * time.Second
	defaultMaxRetry       = 8
)

// Validate checks semantic validity and fills defaults on a COPY returned as
// normalized; the receiver is never mutated.
func (p PolicyConfig) Validate() (PolicyConfig, error) {
	out := p
	seen := map[string]struct{}{}
	for i := range out.Rules {
		r := &out.Rules[i]
		if r.ID == "" {
			return out, fmt.Errorf("%w: rule at index %d has empty ID", errInvalidConfig, i)
		}
		if _, dup := seen[r.ID]; dup {
			return out, fmt.Errorf("%w: duplicate rule ID %q", errInvalidConfig, r.ID)
		}
		seen[r.ID] = struct{}{}
		switch r.Algo {
		case AlgoTokenBucket, AlgoLeakyBucket, AlgoSlidingWindow:
		default:
			return out, fmt.Errorf("%w: rule %q: unknown algorithm %q", errInvalidConfig, r.ID, r.Algo)
		}
		switch r.Scope {
		case ScopeCaller, ScopeServer:
		case "":
			r.Scope = ScopeCaller
		default:
			return out, fmt.Errorf("%w: rule %q: unknown scope %q", errInvalidConfig, r.ID, r.Scope)
		}
		if r.Key == "" {
			return out, fmt.Errorf("%w: rule %q: empty key", errInvalidConfig, r.ID)
		}
		if r.Rate <= 0 {
			return out, fmt.Errorf("%w: rule %q: rate must be > 0", errInvalidConfig, r.ID)
		}
		switch r.Algo {
		case AlgoTokenBucket, AlgoLeakyBucket:
			if r.Burst <= 0 {
				return out, fmt.Errorf("%w: rule %q: burst must be > 0", errInvalidConfig, r.ID)
			}
		case AlgoSlidingWindow:
			if r.Window <= 0 {
				return out, fmt.Errorf("%w: rule %q: window must be > 0", errInvalidConfig, r.ID)
			}
			if r.SubBuckets <= 0 {
				r.SubBuckets = defaultSubBuckets
			}
			if int64(r.SubBuckets) > r.Window.Nanoseconds() {
				return out, fmt.Errorf("%w: rule %q: sub-buckets (%d) finer than 1ns resolution", errInvalidConfig, r.ID, r.SubBuckets)
			}
			// Limit = Rate requests per window, optionally capped by Burst.
			if r.Rate < 1 && r.Burst <= 0 {
				return out, fmt.Errorf("%w: rule %q: sliding window needs rate >= 1 req/window or burst > 0", errInvalidConfig, r.ID)
			}
		}
		switch r.Fail {
		case FailOpen, FailClosed:
		case "":
			r.Fail = FailClosed
		default:
			return out, fmt.Errorf("%w: rule %q: unknown fail mode %q", errInvalidConfig, r.ID, r.Fail)
		}
	}
	return out, nil
}

// ErrInvalidConfig is returned for malformed policy versions.
var ErrInvalidConfig = errors.New("ratelimit: invalid config")

// errInvalidConfig is kept unexported as an alias sentinel for internal use.
var errInvalidConfig = ErrInvalidConfig

// Config configures an Engine.
type Config struct {
	Counters Counters
	Clock    Clock
	Store    Store // optional; nil disables snapshot persistence
	// PersistInterval is the steady snapshot interval (0 = default 5s).
	PersistInterval time.Duration
	// CounterTimeout bounds a single counter transaction (0 = 200ms).
	CounterTimeout time.Duration
}
