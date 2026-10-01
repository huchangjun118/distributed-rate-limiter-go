package ratelimit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

var errStoreDown = errors.New("store down")

// faultyStore injects failures/delays into a wrapped Store.
type faultyStore struct {
	Store
	mu      sync.Mutex
	failGet int
	failCAS int
	delay   time.Duration
	failErr error
}

func (f *faultyStore) wait(ctx context.Context) error {
	if f.delay <= 0 {
		return nil
	}
	select {
	case <-time.After(f.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *faultyStore) Get(ctx context.Context, key string) ([]byte, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	if f.failGet > 0 {
		f.failGet--
		f.mu.Unlock()
		return nil, f.failErr
	}
	f.mu.Unlock()
	return f.Store.Get(ctx, key)
}

func (f *faultyStore) CAS(ctx context.Context, key string, oldVal, newVal []byte) (bool, error) {
	if err := f.wait(ctx); err != nil {
		return false, err
	}
	f.mu.Lock()
	if f.failCAS > 0 {
		f.failCAS--
		f.mu.Unlock()
		return false, f.failErr
	}
	f.mu.Unlock()
	return f.Store.CAS(ctx, key, oldVal, newVal)
}

func basePolicy(id string, degrade DegradeMode) Policy {
	return Policy{ID: id, Scope: ScopeCaller, CallerID: "*", ServiceID: "*",
		Key: "*", Algorithm: AlgTokenBucket, Rate: 1, Burst: 5, Degrade: degrade}
}

// Degradation: when the store is down, fail-open and fail-closed policies
// produce distinguishable reasons; after recovery normal service resumes.
func TestDegradation(t *testing.T) {
	inner := NewMemoryStore()
	store := &faultyStore{Store: inner, failErr: errStoreDown}
	c := NewFakeClock()

	rejectPolicy := basePolicy("reject", DegradeReject)
	req := Request{CallerID: "a", Key: "k"}

	// Fail-open policy alone: degradation allows.
	store.failGet = 10
	mAllow, err := NewPolicyManager(store, c, []Policy{basePolicy("allow", DegradeAllow)})
	if err != nil {
		t.Fatal(err)
	}
	d, err := mAllow.Check(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Allowed || d.Reason != ReasonDegradedAllow {
		t.Fatalf("fail-open degradation: allowed=%v reason=%q", d.Allowed, d.Reason)
	}

	// Fail-closed policy alone: degradation rejects.
	store.failGet = 10
	mReject, err := NewPolicyManager(store, c, []Policy{rejectPolicy})
	if err != nil {
		t.Fatal(err)
	}
	d, _ = mReject.Check(t.Context(), req)
	if d.Allowed || d.Reason != ReasonDegradedReject {
		t.Fatalf("fail-closed degradation: allowed=%v reason=%q", d.Allowed, d.Reason)
	}

	// Mixing fail-open and fail-closed: all policies must allow, so the
	// fail-closed policy still rejects even though the other fails open.
	store.failGet = 10
	mMix, err := NewPolicyManager(store, c, []Policy{
		basePolicy("allow", DegradeAllow), rejectPolicy})
	if err != nil {
		t.Fatal(err)
	}
	d, _ = mMix.Check(t.Context(), req)
	if d.Allowed || d.Reason != ReasonDegradedReject {
		t.Fatalf("mixed degradation: allowed=%v reason=%q", d.Allowed, d.Reason)
	}

	// Recovery: store heals, normal decisions resume.
	store.failGet = 0
	d, _ = mReject.Check(t.Context(), req)
	if !d.Allowed || d.Reason != ReasonAllowed {
		t.Fatalf("after recovery: allowed=%v reason=%q", d.Allowed, d.Reason)
	}

	st := mReject.Stats()
	if st.DegradedRejected != 1 || st.Allowed != 1 {
		t.Fatalf("stats=%+v", st)
	}
}

// Timeout: a store that blocks past the ctx deadline degrades; no partial
// state is written and the quota is not locked (subsequent calls work).
func TestTimeoutNoLock(t *testing.T) {
	inner := NewMemoryStore()
	store := &faultyStore{Store: inner, delay: 200 * time.Millisecond}
	c := NewFakeClock()
	m, err := NewPolicyManager(store, c, []Policy{basePolicy("p", DegradeReject)})
	if err != nil {
		t.Fatal(err)
	}
	req := Request{CallerID: "a", Key: "k"}

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	d, err := m.Check(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed || d.Reason != ReasonDegradedReject {
		t.Fatalf("timeout should degrade reject, got allowed=%v reason=%q", d.Allowed, d.Reason)
	}

	// The store never committed anything: state is empty.
	snap, _ := inner.Snapshot(t.Context())
	if len(snap) != 0 {
		t.Fatalf("timeout left partial state: %v", snap)
	}

	// Quota not locked: with a working store the same key allows normally.
	store.delay = 0
	if d, _ := m.Check(t.Context(), req); !d.Allowed {
		t.Fatalf("quota locked after timeout: %+v", d)
	}
}

// Hot update: decisions concurrent with an update pin exactly one version;
// an invalid update fails and the previous version keeps serving.
func TestHotUpdate(t *testing.T) {
	store := NewMemoryStore()
	c := NewFakeClock()
	m, err := NewPolicyManager(store, c, []Policy{basePolicy("p", DegradeReject)})
	if err != nil {
		t.Fatal(err)
	}
	req := Request{CallerID: "a", Key: "k"}

	var wg sync.WaitGroup
	versions := make(chan int64, 200)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				d, err := m.Check(t.Context(), req)
				if err != nil {
					t.Error(err)
					return
				}
				versions <- d.Version
			}
		}()
	}
	// Update concurrently.
	v, err := m.Update([]Policy{basePolicy("p", DegradeReject)})
	if err != nil {
		t.Fatal(err)
	}
	if v != 2 {
		t.Fatalf("new version=%d want 2", v)
	}
	wg.Wait()
	close(versions)

	for ver := range versions {
		if ver != 1 && ver != 2 {
			t.Fatalf("decision observed version %d (mixed/unknown)", ver)
		}
	}
	if got := m.SnapshotVersion(); got != 2 {
		t.Fatalf("snapshot version=%d want 2", got)
	}

	// Invalid update: zero rate is rejected; v2 keeps serving.
	bad := basePolicy("p", DegradeReject)
	bad.Rate = 0
	if _, err := m.Update([]Policy{bad}); err == nil {
		t.Fatal("invalid update should fail")
	}
	if got := m.SnapshotVersion(); got != 2 {
		t.Fatalf("after failed update version=%d want 2", got)
	}
	d, _ := m.Check(t.Context(), req)
	if d.Version != 2 {
		t.Fatalf("decision after failed update used version %d", d.Version)
	}
}

// Hot update validation: duplicate ids and bad params are rejected.
func TestUpdateValidation(t *testing.T) {
	store := NewMemoryStore()
	c := NewFakeClock()
	m, err := NewPolicyManager(store, c, []Policy{basePolicy("p", DegradeReject)})
	if err != nil {
		t.Fatal(err)
	}

	dups := []Policy{basePolicy("p", DegradeReject), basePolicy("p", DegradeAllow)}
	if _, err := m.Update(dups); err == nil {
		t.Fatal("duplicate ids should be rejected")
	}
	if got := m.SnapshotVersion(); got != 1 {
		t.Fatalf("version=%d want 1 after rejected update", got)
	}

	bad := basePolicy("p", DegradeReject)
	bad.Window = 0
	bad.Algorithm = AlgSlidingWindow
	if _, err := m.Update([]Policy{bad}); err == nil {
		t.Fatal("sliding window without window should be rejected")
	}
}
