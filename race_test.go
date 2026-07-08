// File: race_test.go

package grevents_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gourdian25/grevents"
)

// TestRaceSyncConcurrentPublishSubscribeUnsubscribe hammers a sync bus
// from many goroutines simultaneously. Meaningless without -race; the
// race detector is what actually verifies anything here.
func TestRaceSyncConcurrentPublishSubscribeUnsubscribe(t *testing.T) {
	ctx := context.Background()
	bus, err := grevents.NewBus(grevents.WithSync())
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	const workers = 20
	const opsPerWorker = 50

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				unsubscribe, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
					return nil
				})
				if err != nil {
					return // bus closed concurrently by another goroutine's test cleanup path
				}
				_ = bus.Publish(ctx, grevents.Event{Topic: "topic"})
				bus.Use(func(next grevents.HandlerFunc) grevents.HandlerFunc { return next })
				_, _ = bus.Stats(ctx)
				unsubscribe()
				unsubscribe() // idempotent double-call
			}
		}()
	}
	wg.Wait()
}

// TestRaceAsyncConcurrentPublishCloseDrain hammers an async bus with
// concurrent publishers while Close runs, exercising the
// overflow-strategy selects, the per-subscriber fan-out goroutines, and
// the drain-timeout race-window sweep all at once under -race.
func TestRaceAsyncConcurrentPublishCloseDrain(t *testing.T) {
	ctx := context.Background()
	bus, err := grevents.NewBus(
		grevents.WithAsync(8, grevents.OverflowDrop),
		grevents.WithWorkerCount(4),
		grevents.WithRetry(2, time.Millisecond),
		grevents.WithDrainTimeout(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}

	for i := 0; i < 5; i++ {
		if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
			return nil
		}); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	}

	const workers = 10
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_ = bus.Publish(ctx, grevents.Event{Topic: "topic"})
			}
		}()
	}

	// Close concurrently with in-flight publishers. A returned error here
	// (e.g. wrapped ErrDrainTimeout, since publishers are still hammering
	// the bus) is an acceptable, non-crashing outcome for this stress
	// test — the race detector, not the return value, is what this test
	// actually checks.
	time.Sleep(2 * time.Millisecond)
	_ = bus.Close()

	wg.Wait()
}
