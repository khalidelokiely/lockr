package engine

import (
	"context"
	"errors"
	"lockr/internal/primitive/lock"
	"sync"
	"time"
)

var ErrWrongOwner = errors.New("shard not owned by this node")

type ShardState uint32

const (
	ShardNotOwned ShardState = iota // zero value: an empty shell
	ShardActive
	ShardFrozen // migrating out: no grants, releases or expiries touch it
)

type Shard struct {
	ID           uint32
	state        ShardState
	epoch        uint64
	mu           sync.RWMutex
	active       map[string]*lock.Lock
	draining     map[string]*lock.Lock
	peakSize     int
	tokenCounter uint64
	wheel        *ExpiryWheel
}

// NewShard wires the wheel's expiries back into this shard. Call it before
// wheel.Start(); a wheel serves exactly one shard.
func NewShard(shardID uint32, peakSize int, wheel *ExpiryWheel) *Shard {
	s := &Shard{
		peakSize:     peakSize,
		tokenCounter: uint64(time.Now().UnixNano()),
		wheel:        wheel,
	}

	return s
}

func (s *Shard) Activate(epoch uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == nil {
		s.active = make(map[string]*lock.Lock, s.peakSize)
	}
	s.epoch = epoch
	s.state = ShardActive
}

func (s *Shard) Freeze() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == ShardActive {
		s.state = ShardFrozen
	}
}

// Purge drops all state and returns the shard to an empty shell.
func (s *Shard) Purge() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = ShardNotOwned
	s.active = nil
}

func (s *Shard) State() ShardState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

type AcquireRequest struct {
	Type     lock.GrantType
	TTL      time.Duration
	Key      string
	MustLock bool
}

// nextToken must be called with s.mu held. One counter for the whole shard makes
// every fencing token on a key strictly increasing even across Lock re-creation.
func (s *Shard) nextToken() uint64 {
	s.tokenCounter++
	return s.tokenCounter
}

// onGrant must be called with s.mu held (every Lock method is).
func (s *Shard) onGrant(h lock.Handle, ttl time.Duration) {
	s.ScheduleExpiry(h.Key, h.ReleaseSecret, ttl)
}

func (s *Shard) Acquire(ctx context.Context, req *AcquireRequest) (lock.Handle, error) {
	s.mu.Lock()

	if s.state != ShardActive {
		s.mu.Unlock()
		return lock.Handle{}, ErrWrongOwner
	}

	l, exists := s.active[req.Key]
	if !exists {
		l = lock.NewLockWithHooks(req.Key, s.tokenCounter, s.nextToken, s.onGrant)
		s.active[req.Key] = l
	}

	res, err := l.TryAcquire(req.Type, req.TTL, req.MustLock)
	if err != nil {
		s.cleanUpIfIdle(req.Key, l)
		s.mu.Unlock()
		return lock.Handle{}, err
	}

	if res.IsGranted() {
		s.mu.Unlock()
		return res.Handle, nil
	}

	s.mu.Unlock()
	defer res.Free()

	select {
	case handle := <-res.WaitChan:
		return handle, nil
	case <-ctx.Done():
		s.mu.Lock()
		removed := l.RemoveWaiter(res.WaiterID)
		s.cleanUpIfIdle(req.Key, l)
		s.mu.Unlock()

		if !removed {
			// Granted in the same instant the context fired. The grant exists
			// (and its expiry is armed), so hand it over instead of leaking it.
			return <-res.WaitChan, nil
		}
		return lock.Handle{}, ctx.Err()
	}
}

func (s *Shard) Release(key string, fencingToken uint64, releaseSecret string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != ShardActive {
		return ErrWrongOwner
	}

	l, exists := s.active[key]
	if !exists {
		return lock.ErrNoLockForKey
	}

	if err := l.Release(fencingToken, releaseSecret); err != nil {
		return err
	}

	s.cleanUpIfIdle(key, l)
	return nil
}

func (s *Shard) ScheduleExpiry(key, releaseSecret string, ttl time.Duration) {
	if s.wheel != nil {
		s.wheel.Schedule(s.ID, key, releaseSecret, ttl)
	}
}

// ExpireGrant is the wheel's callback.
func (s *Shard) ExpireGrant(key, releaseSecret string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != ShardActive {
		return
	}

	if l, exists := s.active[key]; exists {
		l.ExpireGrant(releaseSecret)
		s.cleanUpIfIdle(key, l)
	}
}

// cleanUpIfIdle must be called with s.mu held. Safe w.r.t. fencing because tokens
// come from the shard counter, not from the Lock object.
func (s *Shard) cleanUpIfIdle(key string, l *lock.Lock) {
	if len(l.Grants) == 0 && len(l.Waiters) == 0 && s.active[key] == l {
		delete(s.active, key)
	}
}
