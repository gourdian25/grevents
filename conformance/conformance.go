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
// handler therefore cannot, by design, create queue backpressure; nor can
// a slow dispatch loop be relied on to reliably do so (goroutine-spawn
// cost is too small and too variable a margin to race against
// deterministically). These scenarios instead lean on genuine
// concurrency: a burst of many producer goroutines released
// simultaneously against a single-slot queue serviced by exactly one
// worker will, with overwhelming likelihood, produce real contention —
// the same style of real-concurrency-at-scale technique grcache's own
// conformance suite uses for its concurrent-tag scenarios.
const burstSize = 300

// burstPublish releases n goroutines to call Publish(topic) on bus at
// (as close to) the same instant as possible, and returns their results
// in call order.
func burstPublish(bus grevents.Bus, topic string, n int) []error {
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			results[i] = bus.Publish(context.Background(), grevents.Event{Topic: topic})
		}()
	}
	close(start)
	wg.Wait()
	return results
}

func testOverflowReject(t *testing.T, newBus newBusFunc) {
	t.Helper()
	bus, err := newBus(grevents.WithAsync(1, grevents.OverflowReject), grevents.WithWorkerCount(1))
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	results := burstPublish(bus, "flood", burstSize)

	var rejected, accepted int
	for _, err := range results {
		switch {
		case errors.Is(err, grevents.ErrQueueFull):
			rejected++
		case err == nil:
			accepted++
		default:
			t.Fatalf("Publish returned unexpected error: %v", err)
		}
	}
	if rejected == 0 {
		t.Fatalf("0/%d concurrent publishes were rejected against a size-1 queue; want at least 1 ErrQueueFull", burstSize)
	}
	if accepted == 0 {
		t.Fatalf("0/%d concurrent publishes were accepted; want at least 1 to succeed", burstSize)
	}
}

func testOverflowDrop(t *testing.T, newBus newBusFunc, cfg *runConfig) {
	t.Helper()
	var delivered atomic.Int64
	bus, err := newBus(grevents.WithAsync(1, grevents.OverflowDrop), grevents.WithWorkerCount(1))
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	if _, err := bus.Subscribe("flood", func(ctx context.Context, event grevents.Event) error {
		delivered.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	results := burstPublish(bus, "flood", burstSize)
	for i, err := range results {
		if err != nil {
			t.Fatalf("Publish[%d] = %v, want nil (OverflowDrop never errors)", i, err)
		}
	}

	// Poll until delivery count stops climbing, then confirm strictly
	// fewer than burstSize were ever delivered — proving at least one was
	// dropped rather than merely "not yet delivered."
	var last int64 = -1
	deadline := time.Now().Add(cfg.eventualConsistencyTimeout)
	for {
		cur := delivered.Load()
		if cur == last {
			break
		}
		last = cur
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if delivered.Load() >= int64(burstSize) {
		t.Fatalf("delivered %d/%d events against a size-1 queue with OverflowDrop; want strictly fewer than %d (at least one dropped)",
			delivered.Load(), burstSize, burstSize)
	}
}

// floodSubscriberCount is registered on the topic used by
// testOverflowBlock only, so that dispatching a single dequeued event
// (dispatchToSubscribers' snapshot-then-spawn-one-goroutine-per-subscriber
// loop) takes tens of milliseconds. The worker's loop is dequeue-then-
// synchronously-dispatch-then-loop: it can only ever free one queue slot
// per dispatch cycle, no matter how much backlog is queued behind it, so
// making the backlog deeper does not extend how long the very next slot
// takes to free up — only a genuinely slow single dispatch does. This
// value is calibrated (empirically, not guessed) so that one dispatch
// cycle reliably takes several times longer than blockDeadline below.
const floodSubscriberCount = 100000

// blockQueueSize is deliberately larger than 1: filling a size-1 queue
// requires winning a genuine cross-goroutine race against the worker's
// very first dequeue, which proved unreliable even with the flood
// subscribers above (a worker that has to wake up on a different
// goroutine still sometimes drains the single slot before this
// goroutine's very next statement runs). A same-goroutine sequential fill
// of many slots sidesteps that race entirely: each fill is just a channel
// send on a goroutine that never blocks or yields, completing in
// microseconds — far faster than the worker can even get scheduled.
//
// The fill loop runs blockQueueSize+1 times, not blockQueueSize: the
// worker's very first dequeue (near-instant, since dequeuing happens
// before the slow part — dispatch) frees exactly one slot partway through
// the sequential fill, so filling only blockQueueSize times leaves one
// slot short of actually full. The +1 send lands in that freed slot,
// leaving the buffer genuinely at capacity with the worker already
// mid-dispatch of the first event.
const blockQueueSize = 32

// blockDeadline must be comfortably shorter than one flood dispatch cycle
// (see floodSubscriberCount) — the victim publish below only needs to
// wait for a single slot to free, which happens as soon as the
// in-progress dispatch finishes, regardless of how much backlog sits
// behind it.
const blockDeadline = 15 * time.Millisecond

func testOverflowBlock(t *testing.T, newBus newBusFunc) {
	t.Helper()
	bus, err := newBus(grevents.WithAsync(blockQueueSize, grevents.OverflowBlock), grevents.WithWorkerCount(1))
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	for i := 0; i < floodSubscriberCount; i++ {
		if _, err := bus.Subscribe("flood", func(ctx context.Context, event grevents.Event) error {
			return nil
		}); err != nil {
			t.Fatalf("Subscribe flood subscriber %d: %v", i, err)
		}
	}

	ctx := context.Background()
	for i := 0; i < blockQueueSize+1; i++ {
		if err := bus.Publish(ctx, grevents.Event{Topic: "flood"}); err != nil {
			t.Fatalf("Publish (filling slot %d/%d): %v", i, blockQueueSize+1, err)
		}
	}

	// The queue is now genuinely full, with the worker already
	// mid-dispatch of the first event: this same goroutine put every one
	// of those events there itself, sequentially, without ever
	// yielding — no cross-goroutine race to reason about.
	shortCtx, cancel := context.WithTimeout(ctx, blockDeadline)
	defer cancel()
	err = bus.Publish(shortCtx, grevents.Event{Topic: "flood"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish (queue filled to capacity, OverflowBlock) error = %v, want context.DeadlineExceeded", err)
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
