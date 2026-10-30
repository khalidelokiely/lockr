package engine

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const tickD = 10 * time.Millisecond

type recorder struct {
	mu    sync.Mutex
	w     *ExpiryWheel
	fired map[string]uint64 // key -> tick it fired on
	count map[string]int
}

func newRecorder() *recorder {
	return &recorder{fired: map[string]uint64{}, count: map[string]int{}}
}

func (r *recorder) ExpireGrant(key, _ string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fired[key] = r.w.currentTick.Load()
	r.count[key]++
}

// newManualWheel builds a wheel driven by hand: a fake clock and explicit tick() calls.
func newManualWheel(slots uint64) (*ExpiryWheel, *recorder, *atomic.Int64) {
	rec := newRecorder()
	w := NewExpiryWheel(tickD, slots, rec)
	rec.w = w
	var clock atomic.Int64
	w.elapsed = func() time.Duration { return time.Duration(clock.Load()) }
	return w, rec, &clock
}

func TestWheelFiresAtExpectedTick(t *testing.T) {
	cases := []struct {
		name    string
		ttl     time.Duration
		startAt time.Duration // fake time (and hand position) when scheduled
		want    uint64
	}{
		{"30ms at 0 -> tick 3 (last slot of the rotation)", 30 * time.Millisecond, 0, 3},
		{"30ms at 10ms -> tick 4 (wraps back to slot 1)", 30 * time.Millisecond, 10 * time.Millisecond, 4},
		{"25ms rounds UP, never early", 25 * time.Millisecond, 0, 3},
		{"100ms spans several rotations", 100 * time.Millisecond, 0, 10},
		{"1ns still waits for the next tick", 1, 0, 1},
		{"zero ttl fires on next tick", 0, 0, 1},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w, rec, clock := newManualWheel(3)
			clock.Store(int64(c.startAt))
			w.currentTick.Store(uint64(c.startAt / tickD))

			w.Schedule("k", "s", c.ttl)

			var firedAt uint64
			for i := w.currentTick.Load() + 1; i <= 30; i++ {
				clock.Store(int64(time.Duration(i) * tickD))
				w.tick()
				if rec.count["k"] > 0 {
					firedAt = i
					break
				}
			}

			if firedAt != c.want {
				t.Fatalf("fired at tick %d, want %d", firedAt, c.want)
			}
			if time.Duration(firedAt)*tickD < c.startAt+c.ttl {
				t.Fatalf("fired EARLY: %v < %v", time.Duration(firedAt)*tickD, c.startAt+c.ttl)
			}
			if w.Pending() != 0 {
				t.Fatalf("pending=%d, want 0", w.Pending())
			}
		})
	}
}

// The hand got ahead of the clock reading Schedule used (goroutine preempted).
// The old code dropped the task in an already-drained slot => a full extra rotation.
func TestWheelOverdueTargetFiresNextTick(t *testing.T) {
	w, rec, clock := newManualWheel(3)
	clock.Store(0)
	w.currentTick.Store(10) // hand is far ahead of what the clock says

	w.Schedule("k", "s", 10*time.Millisecond)
	w.tick()

	if rec.count["k"] != 1 {
		t.Fatalf("expected the task to fire on the next tick, count=%d", rec.count["k"])
	}
}

func TestWheelEachTaskFiresOnce(t *testing.T) {
	w, rec, clock := newManualWheel(4)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		w.Schedule(k, k, 40*time.Millisecond) // all collide in one slot
	}
	for i := uint64(1); i <= 40; i++ {
		clock.Store(int64(time.Duration(i) * tickD))
		w.tick()
	}
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if rec.count[k] != 1 {
			t.Fatalf("%s fired %d times", k, rec.count[k])
		}
	}
	if w.Pending() != 0 {
		t.Fatalf("pending=%d", w.Pending())
	}
}

// Real clock, real goroutine: never early, and not absurdly late.
func TestWheelRealTimeNeverEarly(t *testing.T) {
	rec := newRecorder()
	w := NewExpiryWheel(5*time.Millisecond, 8, rec)
	rec.w = w
	done := make(chan time.Time, 1)
	w.router = routerFunc(func(string, string) { done <- time.Now() })
	w.Start()
	defer w.Stop()

	ttl := 40 * time.Millisecond
	start := time.Now()
	w.Schedule("k", "s", ttl)

	select {
	case at := <-done:
		if d := at.Sub(start); d < ttl {
			t.Fatalf("fired early: %v < %v", d, ttl)
		} else if d > ttl+100*time.Millisecond {
			t.Fatalf("fired very late: %v", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("never fired")
	}
}

type routerFunc func(string, string)

func (f routerFunc) ExpireGrant(k, s string) { f(k, s) }

func TestWheelStopStartIdempotentAndCatchesUp(t *testing.T) {
	var n atomic.Int64
	w := NewExpiryWheel(2*time.Millisecond, 8, routerFunc(func(string, string) { n.Add(1) }))

	w.Stop() // stop before start: no-op
	w.Start()
	w.Start() // double start: no-op
	w.Stop()
	w.Stop()

	w.Schedule("k", "s", 10*time.Millisecond) // scheduled while stopped
	time.Sleep(30 * time.Millisecond)         // time passes, hand is parked
	if n.Load() != 0 {
		t.Fatal("fired while stopped")
	}

	w.Start()
	defer w.Stop()
	deadline := time.Now().Add(time.Second)
	for n.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n.Load() != 1 {
		t.Fatalf("overdue task should fire right after restart, n=%d", n.Load())
	}
}

// Hammer Schedule against the hand under -race: every task fires exactly once.
func TestWheelConcurrentScheduleExactlyOnce(t *testing.T) {
	const goroutines, perG = 8, 500

	var mu sync.Mutex
	seen := map[string]int{}
	var total atomic.Int64

	w := NewExpiryWheel(time.Millisecond, 16, routerFunc(func(k, _ string) {
		mu.Lock()
		seen[k]++
		mu.Unlock()
		total.Add(1)
	}))
	w.Start()
	defer w.Stop()

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				ttl := time.Duration(1+(i*7+g)%40) * time.Millisecond // crosses many rotations
				w.Schedule(string(rune('A'+g))+"-"+itoa(i), "s", ttl)
			}
		}(g)
	}
	wg.Wait()

	deadline := time.Now().Add(3 * time.Second)
	for total.Load() < goroutines*perG && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != goroutines*perG {
		t.Fatalf("fired %d distinct tasks, want %d", len(seen), goroutines*perG)
	}
	for k, c := range seen {
		if c != 1 {
			t.Fatalf("%s fired %d times", k, c)
		}
	}
	if w.Pending() != 0 {
		t.Fatalf("pending=%d", w.Pending())
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [8]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
