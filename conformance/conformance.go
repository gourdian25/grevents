// File: conformance/conformance.go

// implementations. It is the primary test artifact for the bus's own
// package (see grevents' bus_test.go), which supplies grevents.NewBus
// itself to Run — matching the exact BusOption-based constructor
// signature so a hypothetical future Bus implementation (e.g. a
// distributed adapter) could plug into this same suite without any
// adapter shim, as long as it accepts the same BusOption surface. It is
// not an exhaustive proof of correctness; new scenarios get added here as
// gaps are found.
//
// This package imports only the root grevents package, which is what
// avoids an import cycle with grevents' own tests importing conformance.
package conformance

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gourdian25/grevents"
)

// RunOption configures Run's timing tolerances. The zero value (no
// options) uses conservative defaults suitable for CI.
type RunOption func(*runConfig)

type runConfig struct {
	eventualConsistencyTimeout time.Duration
}

// WithEventualConsistencyTimeout overrides how long async scenarios poll
// for eventual delivery before failing. Default: 3s.
func WithEventualConsistencyTimeout(d time.Duration) RunOption {
	return func(cfg *runConfig) { cfg.eventualConsistencyTimeout = d }
}

// newBusFunc is the constructor shape every scenario drives the Bus
// under test through — identical to grevents.NewBus's own signature.
type newBusFunc func(opts ...grevents.BusOption) (grevents.Bus, error)

// Run executes the full conformance suite against Bus instances obtained
// from newBus, freshly constructed (with scenario-appropriate options) for
// each scenario.
//
// Example:
//
//	func TestConformance(t *testing.T) {
//		conformance.Run(t, grevents.NewBus)
//	}
func Run(t *testing.T, newBus newBusFunc, opts ...RunOption) {
	t.Helper()

	cfg := &runConfig{eventualConsistencyTimeout: 3 * time.Second}
	for _, opt := range opts {
		opt(cfg)
	}

	t.Run("SyncDeliveryMultipleSubscribers", func(t *testing.T) { testSyncDeliveryMultipleSubscribers(t, newBus) })
	t.Run("SyncNoSubscribersIsNoop", func(t *testing.T) { testSyncNoSubscribersIsNoop(t, newBus) })
	t.Run("SyncHandlerErrorAggregation", func(t *testing.T) { testSyncHandlerErrorAggregation(t, newBus) })
	t.Run("AsyncDeliveryEventualConsistency", func(t *testing.T) { testAsyncDeliveryEventualConsistency(t, newBus, cfg) })
	t.Run("AsyncRetryThenSucceed", func(t *testing.T) { testAsyncRetryThenSucceed(t, newBus, cfg) })
	t.Run("AsyncRetryExhaustionDeadLetters", func(t *testing.T) { testAsyncRetryExhaustionDeadLetters(t, newBus, cfg) })
	t.Run("OverflowBlock", func(t *testing.T) { testOverflowBlock(t, newBus) })
	t.Run("OverflowDrop", func(t *testing.T) { testOverflowDrop(t, newBus, cfg) })
	t.Run("OverflowReject", func(t *testing.T) { testOverflowReject(t, newBus) })
	t.Run("PanicInHandlerDoesNotCrashBus", func(t *testing.T) { testPanicInHandlerDoesNotCrashBus(t, newBus) })
	t.Run("CloseDrainsWithinTimeout", func(t *testing.T) { testCloseDrainsWithinTimeout(t, newBus) })
	t.Run("CloseForceStopsAfterTimeout", func(t *testing.T) { testCloseForceStopsAfterTimeout(t, newBus) })
	t.Run("CloseIdempotent", func(t *testing.T) { testCloseIdempotent(t, newBus) })
	t.Run("PublishAfterCloseReturnsErrClosed", func(t *testing.T) { testPublishAfterCloseReturnsErrClosed(t, newBus) })
	t.Run("SubscribeAfterCloseReturnsErrClosed", func(t *testing.T) { testSubscribeAfterCloseReturnsErrClosed(t, newBus) })
	t.Run("UnsubscribeStopsDelivery", func(t *testing.T) { testUnsubscribeStopsDelivery(t, newBus) })
	t.Run("MiddlewareChainOrdering", func(t *testing.T) { testMiddlewareChainOrdering(t, newBus) })
	t.Run("StatsSanity", func(t *testing.T) { testStatsSanity(t, newBus) })
}

// --- Sync scenarios ---------------------------------------------------

func testSyncDeliveryMultipleSubscribers(t *testing.T, newBus newBusFunc) {
	t.Helper()
	ctx := context.Background()
	bus, err := newBus(grevents.WithSync())
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	const n = 10
	var mu sync.Mutex
	var invoked []int
	for i := 0; i < n; i++ {
		i := i
		if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
			mu.Lock()
			invoked = append(invoked, i)
			mu.Unlock()
			return nil
		}); err != nil {
			t.Fatalf("Subscribe(%d): %v", i, err)
		}
	}

	if err := bus.Publish(ctx, grevents.Event{Topic: "topic"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	mu.Lock()
	got := len(invoked)
	mu.Unlock()
	if got != n {
		t.Fatalf("invoked %d subscribers, want %d", got, n)
	}
}

func testSyncNoSubscribersIsNoop(t *testing.T, newBus newBusFunc) {
	t.Helper()
	ctx := context.Background()
	bus, err := newBus(grevents.WithSync())
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	if err := bus.Publish(ctx, grevents.Event{Topic: "nobody-listening"}); err != nil {
		t.Fatalf("Publish (no subscribers) = %v, want nil", err)
	}
}

func testSyncHandlerErrorAggregation(t *testing.T, newBus newBusFunc) {
	t.Helper()
	ctx := context.Background()
	bus, err := newBus(grevents.WithSync())
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	errA := errors.New("handler A failed")
	errB := errors.New("handler B failed")

	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		return errA
	}); err != nil {
		t.Fatalf("Subscribe A: %v", err)
	}
	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		return nil
	}); err != nil {
		t.Fatalf("Subscribe (ok): %v", err)
	}
	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		return errB
	}); err != nil {
		t.Fatalf("Subscribe B: %v", err)
	}

	err = bus.Publish(ctx, grevents.Event{Topic: "topic"})
	if err == nil {
		t.Fatalf("Publish = nil, want an aggregated error")
	}
	if !errors.Is(err, errA) {
		t.Fatalf("Publish error does not wrap errA: %v", err)
	}
	if !errors.Is(err, errB) {
		t.Fatalf("Publish error does not wrap errB: %v", err)
	}
}

// --- Async scenarios ---------------------------------------------------

func testAsyncDeliveryEventualConsistency(t *testing.T, newBus newBusFunc, cfg *runConfig) {
	t.Helper()
	ctx := context.Background()
	bus, err := newBus(grevents.WithAsync(16, grevents.OverflowBlock))
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	delivered := make(chan struct{})
	var once sync.Once
	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		once.Do(func() { close(delivered) })
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bus.Publish(ctx, grevents.Event{Topic: "topic"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-delivered:
	case <-time.After(cfg.eventualConsistencyTimeout):
		t.Fatalf("event not delivered within %s", cfg.eventualConsistencyTimeout)
	}
}

func testAsyncRetryThenSucceed(t *testing.T, newBus newBusFunc, cfg *runConfig) {
	t.Helper()
	ctx := context.Background()
	dlq := grevents.NewMemoryDeadLetterSink(10)
	bus, err := newBus(
		grevents.WithAsync(16, grevents.OverflowBlock),
		grevents.WithRetry(5, 5*time.Millisecond),
		grevents.WithDeadLetterSink(dlq),
	)
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	var attempts int32
	succeeded := make(chan struct{})
	var once sync.Once
	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		n := atomic.AddInt32(&attempts, 1)
		if n < 3 {
			return fmt.Errorf("attempt %d failed", n)
		}
		once.Do(func() { close(succeeded) })
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bus.Publish(ctx, grevents.Event{Topic: "topic"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-succeeded:
	case <-time.After(cfg.eventualConsistencyTimeout):
		t.Fatalf("handler never succeeded within %s (attempts=%d)", cfg.eventualConsistencyTimeout, attempts)
	}

	// Give any stray dead-letter recording a moment to land, then assert
	// the eventually-successful pair was never dead-lettered.
	time.Sleep(20 * time.Millisecond)
	entries, err := dlq.List(ctx, 0)
	if err != nil {
		t.Fatalf("dlq.List: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("dlq has %d entries, want 0 (handler eventually succeeded)", len(entries))
	}
}

func testAsyncRetryExhaustionDeadLetters(t *testing.T, newBus newBusFunc, cfg *runConfig) {
	t.Helper()
	ctx := context.Background()
	dlq := grevents.NewMemoryDeadLetterSink(10)
	handlerErr := errors.New("always fails")
	bus, err := newBus(
		grevents.WithAsync(16, grevents.OverflowBlock),
		grevents.WithRetry(3, 2*time.Millisecond),
		grevents.WithDeadLetterSink(dlq),
	)
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		return handlerErr
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bus.Publish(ctx, grevents.Event{Topic: "topic"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deadline := time.Now().Add(cfg.eventualConsistencyTimeout)
	for {
		entries, err := dlq.List(ctx, 0)
		if err != nil {
			t.Fatalf("dlq.List: %v", err)
		}
		if len(entries) == 1 {
			if entries[0].Attempts != 3 {
				t.Fatalf("DeadLetterEntry.Attempts = %d, want 3", entries[0].Attempts)
			}
			if entries[0].LastError == "" {
				t.Fatalf("DeadLetterEntry.LastError is empty, want non-empty")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dlq has %d entries after %s, want exactly 1", len(entries), cfg.eventualConsistencyTimeout)
		}
		time.Sleep(5 * time.Millisecond)
	}

	stats, err := bus.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.DeadLettered == 0 {
		t.Fatalf("Stats.DeadLettered = 0, want > 0")
	}
}

// --- Overflow scenarios -------------------------------------------------
//
// Handler execution runs on its own goroutine, decoupled from the
// worker's dequeue loop (see async.go's dispatchToSubscribers) — a slow
// handler therefore cannot, by design, create queue backpressure. These
// scenarios instead register a large number of subscribers on the flood
// topic: the worker's synchronous per-event fan-out loop (spawning one
// goroutine per subscriber) then takes long enough that a queue of
// capacity 1 can reliably be observed full by a second Publish call
// issued immediately afterward, with no sleep required.
const floodSubscriberCount = 20000

func registerFloodSubscribers(t *testing.T, bus grevents.Bus, topic string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := bus.Subscribe(topic, func(ctx context.Context, event grevents.Event) error {
			return nil
		}); err != nil {
			t.Fatalf("Subscribe flood subscriber %d: %v", i, err)
		}
	}
}

func testOverflowBlock(t *testing.T, newBus newBusFunc) {
	t.Helper()
	bus, err := newBus(grevents.WithAsync(1, grevents.OverflowBlock), grevents.WithWorkerCount(1))
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	registerFloodSubscribers(t, bus, "flood", floodSubscriberCount)

	ctx := context.Background()
	// A: dequeued near-instantly, kicks off the slow fan-out.
	if err := bus.Publish(ctx, grevents.Event{Topic: "flood"}); err != nil {
		t.Fatalf("Publish A: %v", err)
	}
	// B: fills the size-1 buffer while the worker is still busy
	// fanning out A.
	if err := bus.Publish(ctx, grevents.Event{Topic: "flood"}); err != nil {
		t.Fatalf("Publish B: %v", err)
	}
	// C: queue is full and the worker hasn't come back for it yet, so
	// this call must block. Prove it by giving it a deadline shorter
	// than the fan-out is expected to take.
	shortCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	err = bus.Publish(shortCtx, grevents.Event{Topic: "flood"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish C (queue full, OverflowBlock) error = %v, want context.DeadlineExceeded", err)
	}
}

func testOverflowReject(t *testing.T, newBus newBusFunc) {
	t.Helper()
	bus, err := newBus(grevents.WithAsync(1, grevents.OverflowReject), grevents.WithWorkerCount(1))
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	registerFloodSubscribers(t, bus, "flood", floodSubscriberCount)

	ctx := context.Background()
	if err := bus.Publish(ctx, grevents.Event{Topic: "flood"}); err != nil {
		t.Fatalf("Publish A: %v", err)
	}
	if err := bus.Publish(ctx, grevents.Event{Topic: "flood"}); err != nil {
		t.Fatalf("Publish B: %v", err)
	}
	if err := bus.Publish(ctx, grevents.Event{Topic: "flood"}); !errors.Is(err, grevents.ErrQueueFull) {
		t.Fatalf("Publish C (queue full, OverflowReject) error = %v, want ErrQueueFull", err)
	}
}

func testOverflowDrop(t *testing.T, newBus newBusFunc, cfg *runConfig) {
	t.Helper()
	bus, err := newBus(grevents.WithAsync(1, grevents.OverflowDrop), grevents.WithWorkerCount(1))
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	registerFloodSubscribers(t, bus, "flood", floodSubscriberCount)

	var mu sync.Mutex
	received := map[string]bool{}
	if _, err := bus.Subscribe("flood", func(ctx context.Context, event grevents.Event) error {
		mu.Lock()
		received[event.Metadata["id"]] = true
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("Subscribe tracker: %v", err)
	}

	ctx := context.Background()
	if err := bus.Publish(ctx, grevents.Event{Topic: "flood", Metadata: map[string]string{"id": "A"}}); err != nil {
		t.Fatalf("Publish A: %v", err)
	}
	if err := bus.Publish(ctx, grevents.Event{Topic: "flood", Metadata: map[string]string{"id": "B"}}); err != nil {
		t.Fatalf("Publish B: %v", err)
	}
	// C races into the full queue and, per OverflowDrop, is silently
	// discarded: Publish still returns nil.
	if err := bus.Publish(ctx, grevents.Event{Topic: "flood", Metadata: map[string]string{"id": "C"}}); err != nil {
		t.Fatalf("Publish C = %v, want nil (OverflowDrop never errors)", err)
	}

	deadline := time.Now().Add(cfg.eventualConsistencyTimeout)
	for {
		mu.Lock()
		gotA, gotB := received["A"], received["B"]
		mu.Unlock()
		if gotA && gotB {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("A and B not both delivered within %s (A=%v B=%v)", cfg.eventualConsistencyTimeout, gotA, gotB)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Grace period past A/B's confirmed delivery so a wrongly-enqueued C
	// would have had time to show up too.
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	gotC := received["C"]
	mu.Unlock()
	if gotC {
		t.Fatalf("event C was delivered, want dropped (OverflowDrop with a full queue)")
	}
}

// --- Panic / middleware / shutdown scenarios ----------------------------

func testPanicInHandlerDoesNotCrashBus(t *testing.T, newBus newBusFunc) {
	t.Helper()
	ctx := context.Background()
	bus, err := newBus(grevents.WithSync())
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	var otherInvoked bool
	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		panic("boom")
	}); err != nil {
		t.Fatalf("Subscribe (panicking): %v", err)
	}
	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		otherInvoked = true
		return nil
	}); err != nil {
		t.Fatalf("Subscribe (other): %v", err)
	}

	err = bus.Publish(ctx, grevents.Event{Topic: "topic"})
	if err == nil {
		t.Fatalf("Publish (one subscriber panicked) = nil, want an error")
	}
	if !otherInvoked {
		t.Fatalf("other subscriber was not invoked after a sibling panicked")
	}

	// Bus must remain usable for subsequent Publish calls.
	if _, err := bus.Subscribe("topic2", func(ctx context.Context, event grevents.Event) error {
		return nil
	}); err != nil {
		t.Fatalf("Subscribe after panic recovery: %v", err)
	}
	if err := bus.Publish(ctx, grevents.Event{Topic: "topic2"}); err != nil {
		t.Fatalf("Publish after panic recovery: %v", err)
	}
}

func testCloseDrainsWithinTimeout(t *testing.T, newBus newBusFunc) {
	t.Helper()
	ctx := context.Background()
	bus, err := newBus(grevents.WithAsync(4, grevents.OverflowBlock), grevents.WithDrainTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}

	finished := make(chan struct{})
	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		time.Sleep(200 * time.Millisecond)
		close(finished)
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bus.Publish(ctx, grevents.Event{Topic: "topic"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if err := bus.Close(); err != nil {
		t.Fatalf("Close = %v, want nil (handler finishes well within the 2s drain timeout)", err)
	}

	select {
	case <-finished:
	default:
		t.Fatalf("Close returned before the in-flight handler finished")
	}
}

func testCloseForceStopsAfterTimeout(t *testing.T, newBus newBusFunc) {
	t.Helper()
	ctx := context.Background()
	const drainTimeout = 100 * time.Millisecond
	bus, err := newBus(grevents.WithAsync(4, grevents.OverflowBlock), grevents.WithDrainTimeout(drainTimeout))
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}

	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		time.Sleep(2 * time.Second) // deliberately outlives drainTimeout
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bus.Publish(ctx, grevents.Event{Topic: "topic"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	start := time.Now()
	err = bus.Close()
	elapsed := time.Since(start)

	if !errors.Is(err, grevents.ErrDrainTimeout) {
		t.Fatalf("Close error = %v, want ErrDrainTimeout", err)
	}
	// Bounded, not instant and not indefinite: allow generous slack for
	// slow CI while still proving Close did not simply wait forever.
	if elapsed < drainTimeout {
		t.Fatalf("Close returned in %s, faster than its own drain timeout %s", elapsed, drainTimeout)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Close took %s, want roughly bounded by drainTimeout=%s", elapsed, drainTimeout)
	}

	stats, err := bus.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats after Close: %v", err)
	}
	if stats.DroppedOnClose == 0 {
		t.Fatalf("Stats.DroppedOnClose = 0, want > 0 after a forced-timeout Close")
	}
}

func testCloseIdempotent(t *testing.T, newBus newBusFunc) {
	t.Helper()
	bus, err := newBus(grevents.WithSync())
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}

	if err := bus.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := bus.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil (idempotent)", err)
	}
}

func testPublishAfterCloseReturnsErrClosed(t *testing.T, newBus newBusFunc) {
	t.Helper()
	ctx := context.Background()
	bus, err := newBus(grevents.WithSync())
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	if err := bus.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := bus.Publish(ctx, grevents.Event{Topic: "topic"}); !errors.Is(err, grevents.ErrClosed) {
		t.Fatalf("Publish after Close error = %v, want ErrClosed", err)
	}
}

func testSubscribeAfterCloseReturnsErrClosed(t *testing.T, newBus newBusFunc) {
	t.Helper()
	bus, err := newBus(grevents.WithSync())
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	if err := bus.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error { return nil }); !errors.Is(err, grevents.ErrClosed) {
		t.Fatalf("Subscribe after Close error = %v, want ErrClosed", err)
	}
}

func testUnsubscribeStopsDelivery(t *testing.T, newBus newBusFunc) {
	t.Helper()
	ctx := context.Background()
	bus, err := newBus(grevents.WithSync())
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	var count int32
	unsubscribe, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		atomic.AddInt32(&count, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bus.Publish(ctx, grevents.Event{Topic: "topic"}); err != nil {
		t.Fatalf("Publish (before unsubscribe): %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d after first Publish, want 1", count)
	}

	unsubscribe()

	if err := bus.Publish(ctx, grevents.Event{Topic: "topic"}); err != nil {
		t.Fatalf("Publish (after unsubscribe): %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d after second Publish, want still 1 (unsubscribed)", count)
	}
}

func testMiddlewareChainOrdering(t *testing.T, newBus newBusFunc) {
	t.Helper()
	ctx := context.Background()
	bus, err := newBus(grevents.WithSync())
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	var mu sync.Mutex
	var order []string
	record := func(name string) {
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
	}

	bus.Use(func(next grevents.HandlerFunc) grevents.HandlerFunc {
		return func(ctx context.Context, event grevents.Event) error {
			record("mw1")
			return next(ctx, event)
		}
	})
	bus.Use(func(next grevents.HandlerFunc) grevents.HandlerFunc {
		return func(ctx context.Context, event grevents.Event) error {
			record("mw2")
			return next(ctx, event)
		}
	})
	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		record("handler")
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bus.Publish(ctx, grevents.Event{Topic: "topic"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	want := []string{"mw1", "mw2", "handler"}
	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

func testStatsSanity(t *testing.T, newBus newBusFunc) {
	t.Helper()
	ctx := context.Background()
	bus, err := newBus(grevents.WithSync())
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	before, err := bus.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats (initial): %v", err)
	}
	if before.QueueDepth != -1 {
		t.Fatalf("Stats.QueueDepth = %d for a sync-only bus, want -1", before.QueueDepth)
	}

	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Publish(ctx, grevents.Event{Topic: "topic"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	after, err := bus.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats (after): %v", err)
	}
	if after.Published <= before.Published {
		t.Fatalf("Stats.Published did not increase: before=%d after=%d", before.Published, after.Published)
	}
	if after.Delivered <= before.Delivered {
		t.Fatalf("Stats.Delivered did not increase: before=%d after=%d", before.Delivered, after.Delivered)
	}
}
