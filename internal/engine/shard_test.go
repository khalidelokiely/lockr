package engine

import (
	"context"
	"errors"
	"lockr/internal/primitive/lock"
	"testing"
	"time"
)

func newTestShard(t *testing.T) (*Shard, *ExpiryWheel) {
	t.Helper()
	w := NewExpiryWheel(2*time.Millisecond, 64, nil)
	s := NewShard(16, w)
	w.Start()
	t.Cleanup(w.Stop)
	return s, w
}

func acquire(t *testing.T, s *Shard, key string, typ lock.GrantType, ttl time.Duration) lock.Handle {
	t.Helper()
	h, err := s.Acquire(context.Background(), &AcquireRequest{Type: typ, TTL: ttl, Key: key, MustLock: true})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestShardLeaseExpiresAndHandsOffToWaiter(t *testing.T) {
	s, w := newTestShard(t)

	first := acquire(t, s, "k", lock.TypeWrite, 30*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	second, err := s.Acquire(ctx, &AcquireRequest{Type: lock.TypeWrite, TTL: 20 * time.Millisecond, Key: "k", MustLock: true})
	if err != nil {
		t.Fatal(err)
	}

	if waited := time.Since(start); waited < 30*time.Millisecond {
		t.Fatalf("waiter got the lock after %v, before the first lease's TTL", waited)
	}
	if second.FencingToken <= first.FencingToken {
		t.Fatal("fencing token must increase across the handoff")
	}

	// the stale holder is rejected, not silently accepted
	if err := s.Release("k", first.FencingToken, first.ReleaseSecret); err == nil {
		t.Fatal("expired holder released successfully")
	}

	// the waiter's own lease was armed at grant time and expires too
	deadline := time.Now().Add(time.Second)
	for w.Pending() != 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if w.Pending() != 0 {
		t.Fatalf("pending=%d, waiter's lease never expired", w.Pending())
	}
	s.mu.Lock()
	n := len(s.active)
	s.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d idle locks leaked in the shard map", n)
	}
}

func TestShardTokensSurviveIdleCleanup(t *testing.T) {
	s, _ := newTestShard(t)
	var last uint64
	for i := 0; i < 5; i++ {
		h := acquire(t, s, "k", lock.TypeWrite, time.Second)
		if h.FencingToken <= last {
			t.Fatalf("token went backwards across lock re-creation: %d <= %d", h.FencingToken, last)
		}
		last = h.FencingToken
		if err := s.Release("k", h.FencingToken, h.ReleaseSecret); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		if len(s.active) != 0 {
			t.Fatal("idle lock not cleaned up")
		}
		s.mu.Unlock()
	}
}

func TestShardCancelledWaiterIsRemoved(t *testing.T) {
	s, _ := newTestShard(t)
	held := acquire(t, s, "k", lock.TypeWrite, time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := s.Acquire(ctx, &AcquireRequest{Type: lock.TypeRead, TTL: time.Second, Key: "k", MustLock: true})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}

	s.mu.Lock()
	waiters := len(s.active["k"].Waiters)
	s.mu.Unlock()
	if waiters != 0 {
		t.Fatalf("%d abandoned waiters left behind", waiters)
	}
	_ = s.Release("k", held.FencingToken, held.ReleaseSecret)
}

func TestShardTryAcquireOnBusyKeyDoesNotLeak(t *testing.T) {
	s, _ := newTestShard(t)
	h := acquire(t, s, "k", lock.TypeWrite, time.Minute)
	_, err := s.Acquire(context.Background(), &AcquireRequest{Type: lock.TypeRead, TTL: time.Second, Key: "k"})
	if !errors.Is(err, lock.ErrLockBusy) {
		t.Fatalf("got %v", err)
	}
	_ = s.Release("k", h.FencingToken, h.ReleaseSecret)
}
