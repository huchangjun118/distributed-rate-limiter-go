package ratelimit

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileStore is a Store that persists every committed state to a file on disk,
// so counts survive a process restart. It is intended for local/reproducible
// deployments and tests; multi-process deployments should plug in a shared
// Store (e.g. Redis) instead.
//
// Persistence is atomic: the whole state is written to a temp file, fsynced
// and renamed over the target, so a crash never leaves a half-written count.
type FileStore struct {
	mu   sync.Mutex
	path string
	data map[string][]byte
}

// NewFileStore opens (or creates) the store backed by path.
func NewFileStore(path string) (*FileStore, error) {
	s := &FileStore{path: path, data: make(map[string][]byte)}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, fmt.Errorf("filestore: read %s: %w", path, err)
	}
	if err := s.decode(raw); err != nil {
		return nil, fmt.Errorf("filestore: decode %s: %w", path, err)
	}
	return s, nil
}

func (s *FileStore) decode(raw []byte) error {
	var encoded map[string]string
	if err := json.Unmarshal(raw, &encoded); err != nil {
		return err
	}
	data := make(map[string][]byte, len(encoded))
	for k, v := range encoded {
		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return err
		}
		data[k] = b
	}
	s.data = data
	return nil
}

// persistLocked writes the whole state atomically. Caller holds s.mu.
func (s *FileStore) persistLocked() error {
	encoded := make(map[string]string, len(s.data))
	for k, v := range s.data {
		encoded[k] = base64.StdEncoding.EncodeToString(v)
	}
	raw, err := json.Marshal(encoded)
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".ratelimit-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		cleanup()
		return err
	}
	return nil
}

func (s *FileStore) Get(_ context.Context, key string) ([]byte, error) {
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

func (s *FileStore) CAS(_ context.Context, key string, oldVal, newVal []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.data[key]
	if !bytesEqual(cur, oldVal) {
		return false, nil
	}
	next := make(map[string][]byte, len(s.data)+1)
	for k, v := range s.data {
		cp := make([]byte, len(v))
		copy(cp, v)
		next[k] = cp
	}
	cp := make([]byte, len(newVal))
	copy(cp, newVal)
	next[key] = cp
	old := s.data
	s.data = next
	if err := s.persistLocked(); err != nil {
		// Persistence failed: roll back the in-memory state so no count is
		// half-written or locked.
		s.data = old
		return false, fmt.Errorf("filestore: persist: %w", err)
	}
	return true, nil
}

func (s *FileStore) Snapshot(_ context.Context) (map[string][]byte, error) {
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

func (s *FileStore) Restore(_ context.Context, data map[string][]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[string][]byte, len(data))
	for k, v := range data {
		cp := make([]byte, len(v))
		copy(cp, v)
		next[k] = cp
	}
	s.data = next
	if err := s.persistLocked(); err != nil {
		return fmt.Errorf("filestore: persist: %w", err)
	}
	return nil
}

func (s *FileStore) Close() error { return nil }

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
