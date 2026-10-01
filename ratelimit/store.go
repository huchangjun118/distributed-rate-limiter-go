package ratelimit

import (
	"bytes"
	"context"
	"sync"
)

// Store is the shared counter backend.
//
// Every state change is performed as a single atomic Compare-And-Swap:
// a decision reads a fully committed state (Get), computes the next state,
// and commits it only if nobody else changed it in between (CAS). A failed
// CAS simply retries on the latest committed state, so a decision is always
// based on a complete committed count and no retry can over-issue. No locks
// are held across requests, so a timeout or cancellation can never leave a
// count stuck in the middle or lock the quota.
type Store interface {
	// Get returns the raw state bytes for key. A missing key returns nil, nil.
	Get(ctx context.Context, key string) ([]byte, error)
	// CAS atomically sets key to newVal iff its current value equals oldVal.
	// oldVal == nil means the key must not exist. It returns swapped=false on
	// a conflict; the caller is expected to retry on the latest state.
	CAS(ctx context.Context, key string, oldVal, newVal []byte) (swapped bool, err error)
	// Snapshot returns an immutable point-in-time copy of all key/value pairs.
	Snapshot(ctx context.Context) (map[string][]byte, error)
	// Restore replaces the whole state with data (used for recovery tests).
	Restore(ctx context.Context, data map[string][]byte) error
	Close() error
}

// MemoryStore is an in-process Store. Multiple limiter instances sharing one
// MemoryStore behave like multiple service instances sharing one quota.
type MemoryStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[string][]byte)}
}

func (s *MemoryStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.data[key]
	if v == nil {
		return nil, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

func (s *MemoryStore) CAS(_ context.Context, key string, oldVal, newVal []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.data[key]
	if !bytes.Equal(cur, oldVal) {
		return false, nil
	}
	cp := make([]byte, len(newVal))
	copy(cp, newVal)
	s.data[key] = cp
	return true, nil
}

func (s *MemoryStore) Snapshot(_ context.Context) (map[string][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]byte, len(s.data))
	for k, v := range s.data {
		cp := make([]byte, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out, nil
}

func (s *MemoryStore) Restore(_ context.Context, data map[string][]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	fresh := make(map[string][]byte, len(data))
	for k, v := range data {
		cp := make([]byte, len(v))
		copy(cp, v)
		fresh[k] = cp
	}
	s.data = fresh
	return nil
}

func (s *MemoryStore) Close() error { return nil }
