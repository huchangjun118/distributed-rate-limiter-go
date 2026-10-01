package ratelimit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// Errors returned through the Counter boundary.
var (
	// ErrUnavailable means the backing store cannot currently serve a
	// transaction; the engine applies the matching rule's FailMode.
	ErrUnavailable = errors.New("ratelimit: counter unavailable")
	// ErrConflict is the internal optimistic-concurrency retry signal. It
	// never escapes to callers.
	ErrConflict = errors.New("ratelimit: counter conflict")
)

// KeyTxn mutates a single key's state against one committed snapshot.
//
// found reports whether the key existed in the committed map. st is a
// PRIVATE CLONE of the committed state (or a fresh zero state when !found):
// mutating it cannot affect any other transaction. The implementation
// applies the deterministic algorithm math and returns admit plus the state
// to commit. Returning an error aborts the transaction with NO state change
// — in particular context cancellation/deadline never leaves a half-written
// or locked key, because nothing is locked across user math and the commit
// is a single map assignment under the counter lock.
type KeyTxn func(st *algoState, found bool) (admit bool, next *algoState, err error)

// Counter is the shared counting substrate all instances contend on.
//
// Contract:
//   - every successful Update commits against exactly one complete prior
//     version, atomically;
//   - under concurrency the sum of admitted units never exceeds capacity,
//     because math runs on a snapshot and the whole key commits at once —
//     there is no read-modify-write window observable between instances;
//   - contention causes retries against FRESH committed state, never double
//     accounting;
//   - timeout/cancellation returns without committing anything and without
//     holding locks, so quota is never permanently locked.
type Counter interface {
	Update(ctx context.Context, key string, fn KeyTxn) (bool, error)
}

// Counters groups the two physical stores used by an engine. Keeping them
// separate guarantees caller and server scopes can never alias even for
// identical keys; in the local implementation both may wrap the same shared
// underlying map (with a scope prefix) to emulate multiple instances.
type Counters struct {
	Caller Counter
	Server Counter
}

// LocalCounter is an in-process, concurrency-safe Counter backed by a map
// and optionally restored from / persisted to a Store. Several engines
// ("instances") sharing one *LocalCounter* or one backing map with distinct
// scope prefixes emulate a distributed deployment fully reproducibly.
//
// Concurrency strategy: short critical sections only. Each attempt clones
// state OUTSIDE the lock, runs the deterministic math lock-free, then takes
// the lock to verify the key version and commit. On version mismatch it
// retries; the number of retries is bounded only by actual contention and
// each retry re-reads committed state.
type LocalCounter struct {
	clock Clock

	mu     sync.Mutex
	states map[string]*algoState

	// fault injection (tests)
	failLeft atomic.Int32 // inject N ErrUnavailable, then recover
}

// NewLocalCounter creates a counter. When store != nil the latest snapshot
// is loaded immediately and continuous-algorithm states are rebased onto the
// given clock: monotonic timestamps are shifted by the wall-clock shutdown
// gap clamped at zero (negative wall movement never fabricates quota).
func NewLocalCounter(clock Clock, store Store) (*LocalCounter, error) {
	c := &LocalCounter{clock: clock, states: map[string]*algoState{}}
	if store == nil {
		return c, nil
	}
	states, meta, err := store.Load(context.Background())
	if err != nil {
		return nil, err
	}
	if meta != nil {
		c.rebase(states, *meta)
	}
	c.states = states
	return c, nil
}

// rebase bridges exactly one restart and MATURES continuous-algorithm state
// to the restore instant.
//
// Monotonic origins are per-process, so the shutdown duration is expressed in
// wall time (saved wall -> boot wall) and clamped at >= 0: a backwards wall
// movement contributes zero elapsed time and cannot fabricate quota. Each
// restored token bucket then refills for that duration (capped at Burst) and
// each leaky bucket drains (floored at zero); LastMono is translated to the
// new process's mono coordinate. Sliding-window buckets keep wall-clock keys
// and simply age out at their natural trailing edge on first access.
func (c *LocalCounter) rebase(states map[string]*algoState, meta SnapshotMeta) {
	nowWall := c.clock.Now().UnixNano()
	gap := nowWall - meta.WallNanos
	if gap < 0 {
		gap = 0
	}
	nowMono := c.clock.Mono()
	for _, st := range states {
		switch st.Kind {
		case AlgoTokenBucket:
			if st.Init {
				st.Tokens += st.SavedRate * float64(gap) / 1e9
				if st.SavedBurst > 0 && st.Tokens > float64(st.SavedBurst) {
					st.Tokens = float64(st.SavedBurst)
				}
			}
			st.LastMono = nowMono
		case AlgoLeakyBucket:
			if st.Init {
				st.Tokens -= st.SavedRate * float64(gap) / 1e9
				if st.Tokens < 0 {
					st.Tokens = 0
				}
			}
			st.LastMono = nowMono
		}
	}
}

// Update implements Counter with optimistic retries: contention (and only
// contention) causes a fresh re-read; timeouts/cancelation abort cleanly.
func (c *LocalCounter) Update(ctx context.Context, key string, fn KeyTxn) (bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if n := c.failLeft.Load(); n > 0 {
			if c.failLeft.CompareAndSwap(n, n-1) {
				return false, ErrUnavailable
			}
			continue
		}

		// Read a committed snapshot under a short lock.
		c.mu.Lock()
		cur, found := c.states[key]
		var work *algoState
		if found {
			work = cur.clone()
		}
		c.mu.Unlock()

		// Deterministic math on the private clone, with no lock held: a slow
		// caller can never lock quota for others.
		admit, next, err := fn(work, found)
		if err != nil {
			return false, err
		}
		if next == nil {
			next = work
		}

		// The deadline may have fired while the (lock-free) math was running.
		// Re-check before commit so a timed-out/canceled transaction never
		// lands a write: the attempt is simply discarded and retried by the
		// caller or treated as degradation by the engine. Nothing is left
		// behind because the work was private and no lock is held.
		if err := ctx.Err(); err != nil {
			return false, err
		}

		// Commit atomically. Pointer identity of the cloned source detects
		// exactly whether another transaction committed this key meanwhile.
		c.mu.Lock()
		cur2, found2 := c.states[key]
		conflict := found2 != found || (found && cur2 != cur)
		if !conflict {
			c.states[key] = next
		}
		c.mu.Unlock()

		if !conflict {
			return admit, nil
		}
		// Lost the race: discard work, re-read fresh committed state. No
		// quota changed hands because the losing commit is discarded.
	}
}

// snapshot returns a point-in-time deep copy of all key states.
func (c *LocalCounter) snapshot() map[string]*algoState {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]*algoState, len(c.states))
	for k, v := range c.states {
		out[k] = v.clone()
	}
	return out
}

// FailNext injects n transient ErrUnavailable results for tests, after which
// the counter recovers automatically (proving traffic resumes and no key is
// permanently locked).
func (c *LocalCounter) FailNext(n int32) { c.failLeft.Store(n) }

// persist flushes a consistent deep snapshot plus timing meta to store.
// Called by the owning engine periodically, on close, or on demand.
func (c *LocalCounter) persist(ctx context.Context, store Store, clock Clock) error {
	meta := &SnapshotMeta{
		WallNanos: clock.Now().UnixNano(),
		MonoNanos: clock.Mono(),
	}
	return store.Save(ctx, c.snapshot(), meta)
}
