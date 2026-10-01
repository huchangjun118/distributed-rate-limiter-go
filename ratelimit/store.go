package ratelimit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Store persists counter snapshots so that after a restart the quota already
// consumed inside the current window is NOT reset. Implementations must make
// Save atomic: a crash/power loss exposes the previous snapshot or the new
// one, never a torn/partial file.
type Store interface {
	io.Closer
	// Load returns the last saved snapshot. When nothing has been persisted
	// yet it returns a fresh empty map, nil meta and no error.
	Load(ctx context.Context) (map[string]*algoState, *SnapshotMeta, error)
	// Save atomically replaces the persisted snapshot.
	Save(ctx context.Context, states map[string]*algoState, meta *SnapshotMeta) error
}

// SnapshotMeta records timing provenance of a snapshot. Restart bridging is
// computed purely from WallNanos (the shutdown gap, clamped at >= 0), since
// monotonic origins are per-process; MonoNanos is retained for diagnostics.
type SnapshotMeta struct {
	WallNanos int64 `json:"wall_nanos"`
	MonoNanos int64 `json:"mono_nanos,omitempty"`
}

// ErrStoreUnavailable wraps persistence IO failures.
var ErrStoreUnavailable = errors.New("ratelimit: store unavailable")

// snapshotEnvelope is the on-disk JSON shape.
type snapshotEnvelope struct {
	Meta   SnapshotMeta          `json:"meta"`
	States map[string]*algoState `json:"states"`
}

// FileStore is an atomic JSON-file Store: Save writes a sibling temp file,
// fsyncs it and renames it over the target. Rename is atomic on POSIX, so a
// crash can never leave a half-written snapshot.
type FileStore struct {
	path string
}

// NewFileStore opens (or creates) a store at path. It does not perform the
// initial Load itself: pass the returned *FileStore to NewLocalCounter, which
// loads and rebases state; or call Load directly.
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

// Load implements Store.
func (s *FileStore) Load(ctx context.Context) (map[string]*algoState, *SnapshotMeta, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]*algoState{}, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("%w: read: %v", ErrStoreUnavailable, err)
	}
	var env snapshotEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, nil, fmt.Errorf("%w: decode: %v", ErrStoreUnavailable, err)
	}
	if env.States == nil {
		env.States = map[string]*algoState{}
	}
	return env.States, &env.Meta, nil
}

// Save implements Store.
func (s *FileStore) Save(ctx context.Context, states map[string]*algoState, meta *SnapshotMeta) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	env := snapshotEnvelope{Meta: *meta, States: states}
	data, err := json.MarshalIndent(&env, "", "  ")
	if err != nil {
		return fmt.Errorf("%w: encode: %v", ErrStoreUnavailable, err)
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("%w: mkdir: %v", ErrStoreUnavailable, err)
	}
	tmp, err := os.CreateTemp(dir, ".ratelimit-snap-*")
	if err != nil {
		return fmt.Errorf("%w: temp: %v", ErrStoreUnavailable, err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := io.Copy(tmp, bytes.NewReader(data)); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("%w: write: %v", ErrStoreUnavailable, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("%w: fsync: %v", ErrStoreUnavailable, err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("%w: close: %v", ErrStoreUnavailable, err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		cleanup()
		return fmt.Errorf("%w: rename: %v", ErrStoreUnavailable, err)
	}
	return nil
}

// Close implements Store.
func (s *FileStore) Close() error { return nil }

// MemoryStore is an in-memory Store for tests and local simulations. It is
// safe for concurrent use. Fault hooks let tests simulate persistence
// failures (hot-update/control-plane failure is exercised separately, but
// snapshot failures are also part of the degradation contract).
type MemoryStore struct {
	mu    sync.Mutex
	saved *snapshotEnvelope
	failN int // inject N failing Saves, then recover (0 = disabled)
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

// Load implements Store.
func (s *MemoryStore) Load(ctx context.Context) (map[string]*algoState, *SnapshotMeta, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saved == nil {
		return map[string]*algoState{}, nil, nil
	}
	out := make(map[string]*algoState, len(s.saved.States))
	for k, v := range s.saved.States {
		out[k] = v.clone()
	}
	meta := s.saved.Meta
	return out, &meta, nil
}

// Save implements Store.
func (s *MemoryStore) Save(ctx context.Context, states map[string]*algoState, meta *SnapshotMeta) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failN > 0 {
		s.failN--
		return ErrStoreUnavailable
	}
	cp := make(map[string]*algoState, len(states))
	for k, v := range states {
		cp[k] = v.clone()
	}
	s.saved = &snapshotEnvelope{Meta: *meta, States: cp}
	return nil
}

// Close implements Store.
func (s *MemoryStore) Close() error { return nil }

// FailNextSchedules injects n transient Save failures (test helper).
func (s *MemoryStore) FailNextSaves(n int) {
	s.mu.Lock()
	s.failN = n
	s.mu.Unlock()
}

// HasSnapshot reports whether at least one successful Save happened.
func (s *MemoryStore) HasSnapshot() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saved != nil
}
