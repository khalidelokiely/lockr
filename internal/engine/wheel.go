package engine

import (
	"sync"
	"sync/atomic"
	"time"
)

// ExpirationTask is one pending lease expiry (an intrusive singly linked list node).
type ExpirationTask struct {
	ShardID       uint32
	Key           string
	ReleaseSecret string
	TargetTick    uint64
	Next          *ExpirationTask
}

func (e *ExpirationTask) Zero() *ExpirationTask {
	e.Next = nil
	e.ShardID = 0
	e.Key = ""
	e.ReleaseSecret = ""
	e.TargetTick = 0
	return e
}

type Slot struct {
	mu   sync.Mutex
	Head *ExpirationTask
}

func (s *Slot) Lock()   { s.mu.Lock() }
func (s *Slot) Unlock() { s.mu.Unlock() }

// ExpirationRouter receives expirations. It is called from the wheel goroutine,
// OUTSIDE any slot lock, so it may take the shard lock. It must not call
// ExpiryWheel.Stop.
type ExpirationRouter interface {
	ExpireGrant(shardID uint32, key, releaseSecret string)
}

// ExpiryWheel is a hashed timing wheel (Varghese & Lauck, scheme 6).
//
//	tick = elapsed / tickDuration          (monotonic, never wraps in practice)
//	slot = targetTick % numSlots           (which bucket the task lives in)
//
// A bucket may hold tasks from several rotations of the wheel, so every task
// carries its absolute TargetTick. When the hand reaches a slot, tasks whose
// TargetTick has arrived fire and the rest stay for the next rotation.
//
// Guarantees (these are what a lease system needs):
//   - NEVER early: a task fires at or after now+ttl (target tick is rounded up).
//   - Late by at most one tick plus callback/scheduling jitter.
//   - If the tick goroutine stalls, it catches up on every missed tick using the
//     wall clock, so lease time can't silently drift behind real time.
type ExpiryWheel struct {
	tickDuration time.Duration
	numSlots     uint64
	currentTick  atomic.Uint64 // last tick fully claimed by the hand
	slots        []Slot
	taskPool     sync.Pool
	router       ExpirationRouter
	pending      atomic.Int64

	epoch   time.Time
	elapsed func() time.Duration // time since epoch; swappable in tests

	lifecycle sync.Mutex
	stop      chan struct{}
	done      chan struct{}
}

func NewExpiryWheel(tickDuration time.Duration, numSlots uint64, router ExpirationRouter) *ExpiryWheel {
	if tickDuration <= 0 || numSlots == 0 {
		panic("engine: ExpiryWheel needs tickDuration > 0 and numSlots > 0")
	}

	w := &ExpiryWheel{
		tickDuration: tickDuration,
		numSlots:     numSlots,
		slots:        make([]Slot, numSlots),
		router:       router,
		epoch:        time.Now(), // carries a monotonic reading
	}
	w.elapsed = func() time.Duration { return time.Since(w.epoch) }
	w.taskPool.New = func() any { return &ExpirationTask{} }
	return w
}

// SetRouter must be called before Start.
func (w *ExpiryWheel) SetRouter(r ExpirationRouter) { w.router = r }

// Pending returns the number of tasks still waiting in the wheel.
func (w *ExpiryWheel) Pending() int64 { return w.pending.Load() }

// targetTick rounds UP so a task can never fire before its TTL elapsed.
func (w *ExpiryWheel) targetTick(ttl time.Duration) uint64 {
	if ttl < 0 {
		ttl = 0
	}
	due := w.elapsed() + ttl
	return uint64((due + w.tickDuration - 1) / w.tickDuration)
}

// Schedule never calls the router (callers hold the shard lock), so it cannot deadlock.
func (w *ExpiryWheel) Schedule(shardID uint32, key, releaseSecret string, ttl time.Duration) {
	task := w.taskPool.Get().(*ExpirationTask)
	task.ShardID, task.Key, task.ReleaseSecret = shardID, key, releaseSecret
	target := w.targetTick(ttl)

	w.pending.Add(1)
	for {
		if cur := w.currentTick.Load(); target <= cur {
			target = cur + 1 // already due: fire on the very next tick
		}

		slot := &w.slots[target%w.numSlots]
		slot.Lock()
		// Re-check under the slot lock. tick() advances currentTick BEFORE it
		// locks the slot, so if target > cur here the hand has not yet drained
		// this slot for `target`. Without this check a task could land in a slot
		// that was just drained and wait a whole extra rotation.
		if target <= w.currentTick.Load() {
			slot.Unlock()
			continue
		}
		task.TargetTick = target
		task.Next = slot.Head
		slot.Head = task
		slot.Unlock()
		return
	}
}

func (w *ExpiryWheel) Start() {
	w.lifecycle.Lock()
	defer w.lifecycle.Unlock()
	if w.stop != nil {
		return
	}
	w.stop, w.done = make(chan struct{}), make(chan struct{})
	go w.spin(w.stop, w.done)
}

// Stop halts the hand and waits for the goroutine to exit. Time keeps flowing
// while stopped, so a later Start fires everything that became due.
func (w *ExpiryWheel) Stop() {
	w.lifecycle.Lock()
	defer w.lifecycle.Unlock()
	if w.stop == nil {
		return
	}
	close(w.stop)
	<-w.done
	w.stop, w.done = nil, nil
}

func (w *ExpiryWheel) spin(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	ticker := time.NewTicker(w.tickDuration)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			w.catchUp()
		}
	}
}

// catchUp advances the hand to where the wall clock says it should be.
// time.Ticker drops ticks for slow receivers; deriving the target from the
// clock instead of counting ticker events keeps leases honest.
func (w *ExpiryWheel) catchUp() {
	want := uint64(w.elapsed() / w.tickDuration)
	for w.currentTick.Load() < want {
		w.tick()
	}
}

// tick advances the hand by one slot. Only one goroutine may call it at a time.
func (w *ExpiryWheel) tick() {
	cur := w.currentTick.Add(1)
	slot := &w.slots[cur%w.numSlots]

	slot.Lock()
	head := slot.Head
	slot.Head = nil
	slot.Unlock()

	var keepHead, keepTail *ExpirationTask // tasks for a later rotation, order preserved

	for t := head; t != nil; {
		next := t.Next

		if t.TargetTick <= cur {
			if w.router != nil {
				w.router.ExpireGrant(t.ShardID, t.Key, t.ReleaseSecret)
			}
			w.pending.Add(-1)
			w.taskPool.Put(t.Zero())
		} else {
			t.Next = nil
			if keepTail == nil {
				keepHead = t
			} else {
				keepTail.Next = t
			}
			keepTail = t
		}
		t = next
	}

	if keepHead != nil {
		slot.Lock()
		keepTail.Next = slot.Head // O(1): we already hold the tail
		slot.Head = keepHead
		slot.Unlock()
	}
}
