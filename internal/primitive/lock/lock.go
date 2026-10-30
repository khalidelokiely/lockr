package lock

import "time"

type Lock struct {
	Key          string
	FencingToken uint64 // last token issued on this key
	IsExclusive  bool
	Grants       []Grant
	Waiters      []Waiter
	NextWaiterID uint64

	// nextToken issues fencing tokens. The shard supplies a shard-wide monotonic
	// counter so tokens for a key keep increasing even if the idle Lock object is
	// deleted and recreated. Nil falls back to a per-lock counter (tests).
	nextToken func() uint64
	// onGrant fires for EVERY grant (immediate or handed off to a waiter), while
	// the caller's lock is held. The shard uses it to arm the expiry wheel, so a
	// lease's TTL starts at the instant it is granted, not when a woken goroutine
	// happens to get scheduled.
	onGrant func(Handle, time.Duration)
}

func NewLock(key string, fencingToken uint64) *Lock {
	return NewLockWithHooks(key, fencingToken, nil, nil)
}

func NewLockWithHooks(key string, fencingToken uint64, nextToken func() uint64, onGrant func(Handle, time.Duration)) *Lock {
	return &Lock{
		Key:          key,
		FencingToken: fencingToken,
		Grants:       make([]Grant, 0, 2),
		Waiters:      make([]Waiter, 0, 4),
		nextToken:    nextToken,
		onGrant:      onGrant,
	}
}

func (l *Lock) TryAcquire(grantType GrantType, ttl time.Duration, mustLock bool) (AcquireResult, error) {
	// Writer-preference: if anyone is queued, newcomers queue behind them. A single
	// waiting writer therefore stops new readers (no writer starvation) while the
	// readers already inside are left alone.
	if len(l.Waiters) == 0 {
		if grantType == TypeWrite {
			if len(l.Grants) == 0 {
				l.IsExclusive = true
				return AcquireResult{Handle: l.Grant(ttl)}, nil
			}
		} else if !l.IsExclusive {
			return AcquireResult{Handle: l.Grant(ttl)}, nil
		}
	}

	if !mustLock {
		return AcquireResult{}, ErrLockBusy
	}

	waiterID, ch := l.QueueWaiter(grantType, ttl)

	return AcquireResult{
		WaiterID: waiterID,
		WaitChan: ch,
	}, nil
}

func (l *Lock) Grant(ttl time.Duration) Handle {
	if l.nextToken != nil {
		l.FencingToken = l.nextToken()
	} else {
		l.FencingToken++
	}

	secret := generateSecret()
	expiresAt := time.Now().Add(ttl).UnixNano()

	l.Grants = append(l.Grants, Grant{
		ExpiresAt:     expiresAt,
		ReleaseSecret: secret,
		FencingToken:  l.FencingToken,
		IsActive:      true,
	})

	h := Handle{
		Key:           l.Key,
		FencingToken:  l.FencingToken,
		ReleaseSecret: secret,
		ExpiresAt:     expiresAt,
	}

	if l.onGrant != nil {
		l.onGrant(h, ttl)
	}

	return h
}

func (l *Lock) QueueWaiter(grantType GrantType, ttl time.Duration) (uint64, chan Handle) {
	l.NextWaiterID++
	ch := GetChan()

	l.Waiters = append(l.Waiters, Waiter{
		ID:      l.NextWaiterID,
		Type:    grantType,
		TTL:     ttl,
		OutChan: ch,
	})

	return l.NextWaiterID, ch
}

// Release is the explicit, client-driven release. The secret proves ownership and
// the token must be the one issued to that same grant.
func (l *Lock) Release(fencingToken uint64, releaseSecret string) error {
	idx := l.findGrant(releaseSecret)
	if idx == -1 {
		return ErrInvalidReleaseSecret
	}

	if l.Grants[idx].FencingToken != fencingToken {
		return ErrStaleFencingToken
	}

	l.removeGrant(idx)
	return nil
}

// ExpireGrant removes an active grant by secret (called by the expiry wheel).
// Returns false if it was already released: expiry after release is a no-op.
func (l *Lock) ExpireGrant(releaseSecret string) bool {
	idx := l.findGrant(releaseSecret)
	if idx == -1 {
		return false
	}

	l.removeGrant(idx)
	return true
}

func (l *Lock) findGrant(secret string) int {
	for i := range l.Grants {
		if l.Grants[i].IsActive && l.Grants[i].ReleaseSecret == secret {
			return i
		}
	}
	return -1
}

func (l *Lock) removeGrant(idx int) {
	last := len(l.Grants) - 1
	l.Grants[idx] = l.Grants[last]
	l.Grants[last] = Grant{} // drop references for GC
	l.Grants = l.Grants[:last]

	// An empty lock is no longer exclusive. Forgetting this left the lock
	// "write-held" forever after the first writer and blocked every later reader.
	if len(l.Grants) == 0 {
		l.IsExclusive = false
	}

	l.handoff()
}

func (l *Lock) handoff() {
	if len(l.Waiters) == 0 {
		return
	}

	if l.Waiters[0].Type == TypeWrite {
		if len(l.Grants) == 0 {
			w := l.popWaiter()
			l.IsExclusive = true
			w.OutChan <- l.Grant(w.TTL)
		}
		return
	}

	// Head is a reader: admit the whole run of consecutive readers.
	if !l.IsExclusive {
		for len(l.Waiters) > 0 && l.Waiters[0].Type == TypeRead {
			w := l.popWaiter()
			w.OutChan <- l.Grant(w.TTL)
		}
	}
}

func (l *Lock) popWaiter() Waiter {
	w := l.Waiters[0]
	l.Waiters[0] = Waiter{}
	l.Waiters = l.Waiters[1:]
	return w
}

func (l *Lock) RemoveWaiter(id uint64) bool {
	for i, w := range l.Waiters {
		if w.ID == id {
			copy(l.Waiters[i:], l.Waiters[i+1:])
			l.Waiters[len(l.Waiters)-1] = Waiter{}
			l.Waiters = l.Waiters[:len(l.Waiters)-1]

			if i == 0 {
				l.handoff() // a departing head writer may unblock the readers behind it
			}
			return true
		}
	}
	return false
}
