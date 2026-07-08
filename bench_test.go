// File: bench_test.go

package grevents_test

import (
	"context"
	"runtime"
	"testing"

	"github.com/gourdian25/grevents"
)

func benchSyncBus(b *testing.B, subscriberCount int) {
	b.Helper()
	bus, err := grevents.NewBus(grevents.WithSync())
	if err != nil {
		b.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	for i := 0; i < subscriberCount; i++ {
		if _, err := bus.Subscribe("bench", func(ctx context.Context, event grevents.Event) error {
			return nil
		}); err != nil {
			b.Fatalf("Subscribe: %v", err)
		}
	}

	ctx := context.Background()
	event := grevents.Event{Topic: "bench"}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := bus.Publish(ctx, event); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
}

func BenchmarkPublish_Sync_1Subscriber(b *testing.B)   { benchSyncBus(b, 1) }
func BenchmarkPublish_Sync_10Subscribers(b *testing.B) { benchSyncBus(b, 10) }
func BenchmarkPublish_Sync_100Subscribers(b *testing.B) {
	benchSyncBus(b, 100)
}

// benchAsyncBus measures Publish (enqueue) throughput, not end-to-end
// delivery latency — async mode's whole point is that Publish returns
// once the event is enqueued, decoupled from delivery. The queue is sized
// generously and handlers are no-ops so the worker pool can comfortably
// keep pace with b.N enqueues without OverflowBlock's backpressure
// distorting the measurement.
func benchAsyncBus(b *testing.B, subscriberCount int) {
	b.Helper()
	bus, err := grevents.NewBus(
		grevents.WithAsync(1_000_000, grevents.OverflowBlock),
		grevents.WithWorkerCount(runtime.NumCPU()),
	)
	if err != nil {
		b.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	for i := 0; i < subscriberCount; i++ {
		if _, err := bus.Subscribe("bench", func(ctx context.Context, event grevents.Event) error {
			return nil
		}); err != nil {
			b.Fatalf("Subscribe: %v", err)
		}
	}

	ctx := context.Background()
	event := grevents.Event{Topic: "bench"}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := bus.Publish(ctx, event); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
}

func BenchmarkPublish_Async_1Subscriber(b *testing.B)   { benchAsyncBus(b, 1) }
func BenchmarkPublish_Async_10Subscribers(b *testing.B) { benchAsyncBus(b, 10) }
func BenchmarkPublish_Async_100Subscribers(b *testing.B) {
	benchAsyncBus(b, 100)
}

func benchMiddlewareChain(b *testing.B, chainLength int) {
	b.Helper()
	bus, err := grevents.NewBus(grevents.WithSync())
	if err != nil {
		b.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	for i := 0; i < chainLength; i++ {
		bus.Use(func(next grevents.HandlerFunc) grevents.HandlerFunc {
			return func(ctx context.Context, event grevents.Event) error {
				return next(ctx, event)
			}
		})
	}

	if _, err := bus.Subscribe("bench", func(ctx context.Context, event grevents.Event) error {
		return nil
	}); err != nil {
		b.Fatalf("Subscribe: %v", err)
	}

	ctx := context.Background()
	event := grevents.Event{Topic: "bench"}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := bus.Publish(ctx, event); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
}

func BenchmarkMiddlewareChain_1(b *testing.B)  { benchMiddlewareChain(b, 1) }
func BenchmarkMiddlewareChain_5(b *testing.B)  { benchMiddlewareChain(b, 5) }
func BenchmarkMiddlewareChain_10(b *testing.B) { benchMiddlewareChain(b, 10) }
