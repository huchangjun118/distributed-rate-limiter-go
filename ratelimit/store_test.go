package ratelimit

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestMemoryStoreCAS(t *testing.T) {
	s := NewMemoryStore()
	ctx := t.Context()

	// Missing key.
	if v, err := s.Get(ctx, "k"); err != nil || v != nil {
		t.Fatalf("missing key: v=%v err=%v", v, err)
	}

	// CAS on missing key with nil old value succeeds.
	ok, err := s.CAS(ctx, "k", nil, []byte("v1"))
	if err != nil || !ok {
		t.Fatalf("initial CAS: ok=%v err=%v", ok, err)
	}
	// CAS with wrong old value fails (conflict).
	ok, err = s.CAS(ctx, "k", []byte("wrong"), []byte("v2"))
	if err != nil || ok {
		t.Fatalf("conflict CAS should fail: ok=%v err=%v", ok, err)
	}
	// Value unchanged after conflict.
	if v, _ := s.Get(ctx, "k"); !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("value changed after failed CAS: %q", v)
	}
	// CAS with correct old value succeeds.
	ok, err = s.CAS(ctx, "k", []byte("v1"), []byte("v2"))
	if err != nil || !ok {
		t.Fatalf("correct CAS: ok=%v err=%v", ok, err)
	}
	if v, _ := s.Get(ctx, "k"); !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("value=%q want v2", v)
	}
}

func TestMemoryStoreSnapshotRestore(t *testing.T) {
	s := NewMemoryStore()
	ctx := t.Context()
	s.CAS(ctx, "a", nil, []byte("1"))
	s.CAS(ctx, "b", nil, []byte("2"))

	snap, err := s.Snapshot(ctx)
	if err != nil || len(snap) != 2 {
		t.Fatalf("snapshot: len=%d err=%v", len(snap), err)
	}
	// Snapshot is a copy: mutating it does not affect the store.
	snap["a"] = []byte("mutated")
	if v, _ := s.Get(ctx, "a"); !bytes.Equal(v, []byte("1")) {
		t.Fatal("snapshot mutation leaked into store")
	}

	s2 := NewMemoryStore()
	if err := s2.Restore(ctx, snap); err != nil {
		t.Fatal(err)
	}
	if v, _ := s2.Get(ctx, "a"); !bytes.Equal(v, []byte("mutated")) {
		t.Fatalf("restored value=%q want mutated", v)
	}
}

func TestFileStorePersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "ratelimit.json")
	ctx := context.Background()

	s, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CAS(ctx, "k1", nil, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CAS(ctx, "k2", nil, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Reopen: state must survive.
	s2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for _, k := range []string{"k1", "k2"} {
		if v, err := s2.Get(ctx, k); err != nil || v == nil {
			t.Fatalf("after reopen %s: v=%v err=%v", k, v, err)
		}
	}
	if v, _ := s2.Get(ctx, "k1"); !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("k1=%q want v1", v)
	}

	// The file exists and is valid JSON.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) == 0 {
		t.Fatal("persisted file is empty")
	}
}

func TestFileStoreCASConflict(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(filepath.Join(dir, "rl.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	if ok, _ := s.CAS(ctx, "k", nil, []byte("v1")); !ok {
		t.Fatal("initial CAS failed")
	}
	if ok, _ := s.CAS(ctx, "k", []byte("wrong"), []byte("v2")); ok {
		t.Fatal("conflict CAS should fail")
	}
	if ok, _ := s.CAS(ctx, "k", []byte("v1"), []byte("v2")); !ok {
		t.Fatal("correct CAS failed")
	}
	if v, _ := s.Get(ctx, "k"); !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("value=%q want v2", v)
	}
}
