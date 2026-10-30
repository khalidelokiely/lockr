package lock

import (
	"errors"
	"testing"
	"time"
)

func mustGrant(t *testing.T, l *Lock, typ GrantType) Handle {
	t.Helper()
	r, err := l.TryAcquire(typ, time.Second, false)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	return r.Handle
}

func TestReadAfterWriteIsNotBlocked(t *testing.T) {
	l := NewLock("k", 0)
	w := mustGrant(t, l, TypeWrite)
	if err := l.Release(w.FencingToken, w.ReleaseSecret); err != nil {
		t.Fatal(err)
	}
	mustGrant(t, l, TypeRead)
}

func TestEachReaderReleasesWithItsOwnToken(t *testing.T) {
	l := NewLock("k", 0)
	hs := []Handle{mustGrant(t, l, TypeRead), mustGrant(t, l, TypeRead), mustGrant(t, l, TypeRead)}
	for _, h := range hs { // oldest first: used to fail with ErrStaleFencingToken
		if err := l.Release(h.FencingToken, h.ReleaseSecret); err != nil {
			t.Fatalf("release: %v", err)
		}
	}
	if len(l.Grants) != 0 {
		t.Fatal("grants left over")
	}
}

func TestReleaseRejectsWrongTokenAndSecret(t *testing.T) {
	l := NewLock("k", 0)
	h := mustGrant(t, l, TypeRead)
	if err := l.Release(h.FencingToken+1, h.ReleaseSecret); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("got %v", err)
	}
	if err := l.Release(h.FencingToken, "nope"); !errors.Is(err, ErrInvalidReleaseSecret) {
		t.Fatalf("got %v", err)
	}
}

// The anti-starvation rule: one queued writer shuts the door on NEW readers,
// existing readers drain, the writer goes next, then the readers behind it.
func TestWaitingWriterStopsNewReaders(t *testing.T) {
	l := NewLock("k", 0)
	r1 := mustGrant(t, l, TypeRead)
	r2 := mustGrant(t, l, TypeRead)

	wr, _ := l.TryAcquire(TypeWrite, time.Second, true) // queued
	if wr.IsGranted() {
		t.Fatal("writer must wait for readers")
	}

	if _, err := l.TryAcquire(TypeRead, time.Second, false); !errors.Is(err, ErrLockBusy) {
		t.Fatalf("new reader should be refused while a writer waits, got %v", err)
	}
	lateReader, _ := l.TryAcquire(TypeRead, time.Second, true) // queued behind the writer

	_ = l.Release(r1.FencingToken, r1.ReleaseSecret)
	select {
	case <-wr.WaitChan:
		t.Fatal("writer granted while a reader is still inside")
	default:
	}

	_ = l.Release(r2.FencingToken, r2.ReleaseSecret)
	var wh Handle
	select {
	case wh = <-wr.WaitChan:
	default:
		t.Fatal("writer should be granted once readers drained")
	}
	select {
	case <-lateReader.WaitChan:
		t.Fatal("reader jumped the writer")
	default:
	}

	_ = l.Release(wh.FencingToken, wh.ReleaseSecret)
	select {
	case <-lateReader.WaitChan:
	default:
		t.Fatal("reader should follow the writer")
	}
}

func TestFencingTokensStrictlyIncrease(t *testing.T) {
	l := NewLock("k", 0)
	var last uint64
	for i := 0; i < 5; i++ {
		typ := GrantType(i % 2)
		h := mustGrant(t, l, typ)
		if h.FencingToken <= last {
			t.Fatalf("token %d not > %d", h.FencingToken, last)
		}
		last = h.FencingToken
		_ = l.Release(h.FencingToken, h.ReleaseSecret)
	}
}

func TestExpireAfterReleaseIsNoop(t *testing.T) {
	l := NewLock("k", 0)
	h := mustGrant(t, l, TypeWrite)
	_ = l.Release(h.FencingToken, h.ReleaseSecret)
	if l.ExpireGrant(h.ReleaseSecret) {
		t.Fatal("expiry of an already released grant must be a no-op")
	}
}
