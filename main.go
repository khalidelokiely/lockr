package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"lockr/internal/engine"
	"lockr/internal/primitive/lock"
)

func main() {
	// 10ms tick x 512 slots: one rotation = 5.12s. Longer TTLs simply take extra rotations.
	wheel := engine.NewExpiryWheel(10*time.Millisecond, 512, nil)
	shard := engine.NewShard(2000, wheel) // wires itself in as the wheel's router
	wheel.Start()
	defer wheel.Stop()

	ctx := context.Background()
	acq := func(t lock.GrantType, ttl time.Duration, must bool) (lock.Handle, error) {
		return shard.Acquire(ctx, &engine.AcquireRequest{Type: t, TTL: ttl, Key: "abc-123", MustLock: must})
	}

	var readers []lock.Handle
	for i := 1; i <= 3; i++ {
		h, err := acq(lock.TypeRead, 5*time.Second, true)
		if err != nil {
			log.Fatal(err)
		}
		readers = append(readers, h)
		fmt.Printf("reader %d  token=%d\n", i, h.FencingToken)
	}

	// A writer queues; from now on new readers are refused (no writer starvation).
	writerDone := make(chan lock.Handle)
	go func() {
		h, err := acq(lock.TypeWrite, 300*time.Millisecond, true)
		if err != nil {
			log.Fatal(err)
		}
		writerDone <- h
	}()
	time.Sleep(50 * time.Millisecond)

	_, err := acq(lock.TypeRead, time.Second, false)
	fmt.Println("new reader while writer waits:", err)

	for _, r := range readers {
		_ = shard.Release(r.Key, r.FencingToken, r.ReleaseSecret)
	}
	w := <-writerDone
	fmt.Printf("writer     token=%d (granted after readers drained)\n", w.FencingToken)

	// Don't release: the wheel reclaims the lease after its TTL.
	start := time.Now()
	h, _ := acq(lock.TypeRead, time.Second, true)
	fmt.Printf("reader     token=%d after waiting %v for the writer's lease to expire\n",
		h.FencingToken, time.Since(start).Round(10*time.Millisecond))
}
