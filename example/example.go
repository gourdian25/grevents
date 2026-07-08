// File: example/example.go

// is package main, in the same module as grevents itself, and is not
// part of the library's test surface — mirrors grcache's and
// gourdiantoken's own example/example.go convention.
//
// Run with: go run ./example
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gourdian25/grlog"

	"github.com/gourdian25/grevents"
)

func main() {
	ctx := context.Background()

	fmt.Println("=== grevents example ===")

	demoSync(ctx)
	demoSyncErrorAggregation(ctx)
	demoAsyncWithRetryAndDeadLetter(ctx)
	demoOverflowReject(ctx)
	demoMiddlewareWithGrlog(ctx)
	demoCloseDrain(ctx)

	fmt.Println("\n=== done ===")
}

// demoSync shows the default, simplest usage: synchronous delivery to
// multiple subscribers on one topic.
func demoSync(ctx context.Context) {
	fmt.Println("\n--- sync delivery (default) ---")

	bus, err := grevents.NewBus() // WithSync() is the default
	if err != nil {
		fmt.Printf("NewBus failed: %v\n", err)
		return
	}
	defer bus.Close()

	unsubscribe, err := bus.Subscribe("role.assigned", func(ctx context.Context, event grevents.Event) error {
		fmt.Printf("[audit] role assigned: %v\n", event.Payload)
		return nil
	})
	if err != nil {
		fmt.Printf("Subscribe failed: %v\n", err)
		return
	}
	defer unsubscribe()

	if _, err := bus.Subscribe("role.assigned", func(ctx context.Context, event grevents.Event) error {
		fmt.Printf("[cache] invalidating role cache for: %v\n", event.Payload)
		return nil
	}); err != nil {
		fmt.Printf("Subscribe failed: %v\n", err)
		return
	}

	err = bus.Publish(ctx, grevents.Event{
		Topic:   "role.assigned",
		Payload: "user:42 -> admin",
	})
	fmt.Printf("Publish returned: %v\n", err)
}

// demoSyncErrorAggregation shows sync mode's errors.Join-based
// aggregation when more than one subscriber fails.
func demoSyncErrorAggregation(ctx context.Context) {
	fmt.Println("\n--- sync delivery: handler error aggregation ---")

	bus, err := grevents.NewBus(grevents.WithSync())
	if err != nil {
		fmt.Printf("NewBus failed: %v\n", err)
		return
	}
	defer bus.Close()

	errQuotaExceeded := errors.New("quota exceeded")

	if _, err := bus.Subscribe("order.placed", func(ctx context.Context, event grevents.Event) error {
		return errQuotaExceeded
	}); err != nil {
		fmt.Printf("Subscribe failed: %v\n", err)
		return
	}
	if _, err := bus.Subscribe("order.placed", func(ctx context.Context, event grevents.Event) error {
		return nil // succeeds independently of the sibling above
	}); err != nil {
		fmt.Printf("Subscribe failed: %v\n", err)
		return
	}

	err = bus.Publish(ctx, grevents.Event{Topic: "order.placed"})
	fmt.Printf("Publish returned: %v\n", err)
	fmt.Printf("errors.Is(err, errQuotaExceeded) = %v\n", errors.Is(err, errQuotaExceeded))
}

// demoAsyncWithRetryAndDeadLetter shows async delivery with retry and
// inspecting the default in-memory DeadLetterSink after retries exhaust.
func demoAsyncWithRetryAndDeadLetter(ctx context.Context) {
	fmt.Println("\n--- async delivery: retry + dead letters ---")

	dlq := grevents.NewMemoryDeadLetterSink(100)
	bus, err := grevents.NewBus(
		grevents.WithAsync(16, grevents.OverflowBlock),
		grevents.WithRetry(3, 10*time.Millisecond),
		grevents.WithDeadLetterSink(dlq),
	)
	if err != nil {
		fmt.Printf("NewBus failed: %v\n", err)
		return
	}
	defer bus.Close()

	if _, err := bus.Subscribe("payment.failed", func(ctx context.Context, event grevents.Event) error {
		return errors.New("downstream ledger service unavailable")
	}); err != nil {
		fmt.Printf("Subscribe failed: %v\n", err)
		return
	}

	if err := bus.Publish(ctx, grevents.Event{Topic: "payment.failed", Payload: "payment:123"}); err != nil {
		fmt.Printf("Publish failed: %v\n", err)
		return
	}

	// Async delivery + retry happen in the background; poll briefly for
	// the dead letter to land rather than assuming a fixed sleep.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := dlq.List(ctx, 0)
		if err != nil {
			fmt.Printf("dlq.List failed: %v\n", err)
			return
		}
		if len(entries) > 0 {
			fmt.Printf("dead-lettered after %d attempts: %s\n", entries[0].Attempts, entries[0].LastError)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	stats, _ := bus.Stats(ctx)
	fmt.Printf("Stats: published=%d failed=%d deadLettered=%d\n", stats.Published, stats.Failed, stats.DeadLettered)
}

// demoOverflowReject shows the fail-fast overflow strategy: once the
// queue is full, Publish returns ErrQueueFull immediately instead of
// blocking or silently dropping.
func demoOverflowReject(ctx context.Context) {
	fmt.Println("\n--- async delivery: OverflowReject ---")

	bus, err := grevents.NewBus(
		grevents.WithAsync(1, grevents.OverflowReject),
		grevents.WithWorkerCount(1),
	)
	if err != nil {
		fmt.Printf("NewBus failed: %v\n", err)
		return
	}
	defer bus.Close()

	rejected := 0
	for i := 0; i < 20; i++ {
		err := bus.Publish(ctx, grevents.Event{Topic: "unrelated-topic-no-subscribers"})
		if errors.Is(err, grevents.ErrQueueFull) {
			rejected++
		}
	}
	fmt.Printf("rejected %d/20 rapid publishes once the size-1 queue filled\n", rejected)
}

// demoMiddlewareWithGrlog shows grlog interoperability: *grlog.Logger
// satisfies grevents.Logger with no adapter needed, and LoggingMiddleware
// logs every handler invocation's outcome through it.
func demoMiddlewareWithGrlog(ctx context.Context) {
	fmt.Println("\n--- middleware + grlog logging ---")

	logger := grlog.NewDefaultLogger()
	defer logger.Close()

	bus, err := grevents.NewBus(grevents.WithSync(), grevents.WithLogger(logger))
	if err != nil {
		fmt.Printf("NewBus failed: %v\n", err)
		return
	}
	defer bus.Close()

	bus.Use(grevents.LoggingMiddleware(logger))

	if _, err := bus.Subscribe("user.signup", func(ctx context.Context, event grevents.Event) error {
		return nil
	}); err != nil {
		fmt.Printf("Subscribe failed: %v\n", err)
		return
	}

	_ = bus.Publish(ctx, grevents.Event{Topic: "user.signup", Payload: "user:99"})
}

// demoCloseDrain shows Close draining an in-flight async delivery within
// its configured timeout.
func demoCloseDrain(ctx context.Context) {
	fmt.Println("\n--- Close draining an in-flight delivery ---")

	bus, err := grevents.NewBus(
		grevents.WithAsync(4, grevents.OverflowBlock),
		grevents.WithDrainTimeout(2*time.Second),
	)
	if err != nil {
		fmt.Printf("NewBus failed: %v\n", err)
		return
	}

	if _, err := bus.Subscribe("shutdown.demo", func(ctx context.Context, event grevents.Event) error {
		time.Sleep(100 * time.Millisecond)
		fmt.Println("in-flight handler finished")
		return nil
	}); err != nil {
		fmt.Printf("Subscribe failed: %v\n", err)
		return
	}

	if err := bus.Publish(ctx, grevents.Event{Topic: "shutdown.demo"}); err != nil {
		fmt.Printf("Publish failed: %v\n", err)
		return
	}

	if err := bus.Close(); err != nil {
		fmt.Printf("Close returned an error: %v\n", err)
		return
	}
	fmt.Println("Close returned nil: the handler finished within the drain timeout")
}
