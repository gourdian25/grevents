// File: contract_bus_test.go

// contract_bus_test.go is the shared behavioral test suite for grevents'
// Bus implementation, run via TestBus_Contract below against
// grevents.NewBus — matching the exact BusOption-based constructor
// signature so a hypothetical future Bus implementation (e.g. a
// distributed adapter) could reuse runBusContract by supplying its own
// constructor with the same signature. It is not an exhaustive proof of
// correctness; new scenarios get added here as gaps are found.
package grevents_test

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

// runConfig carries the timing tolerances scenarios below poll against.
type runConfig struct {
	eventualConsistencyTimeout time.Duration
}

// newBusFunc is the constructor shape every scenario drives the Bus
// under test through — identical to grevents.NewBus's own signature.
type newBusFunc func(opts ...grevents.BusOption) (grevents.Bus, error)

// runBusContract executes the full contract suite against Bus instances
// obtained from newBus, freshly constructed for each scenario. See
// TestBus_Contract below for the entry point that drives it against
// grevents.NewBus.
func runBusContract(t *testing.T, newBus newBusFunc) {
	t.Helper()

	cfg := &runConfig{eventualConsistencyTimeout: 3 * time.Second}

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
	t.Run("DeadLetterSinkPanicDoesNotCrashBus", func(t *testing.T) { testDeadLetterSinkPanicDoesNotCrashBus(t, newBus) })
	t.Run("LoggerPanicDuringRecoveryDoesNotCrashBus", func(t *testing.T) { testLoggerPanicDuringRecoveryDoesNotCrashBus(t, newBus) })
	t.Run("CloseDrainsWithinTimeout", func(t *testing.T) { testCloseDrainsWithinTimeout(t, newBus) })
	t.Run("CloseForceStopsAfterTimeout", func(t *testing.T) { testCloseForceStopsAfterTimeout(t, newBus) })
	t.Run("CloseIdempotent", func(t *testing.T) { testCloseIdempotent(t, newBus) })
	t.Run("CloseClosesDeadLetterSinkEvenInSyncMode", func(t *testing.T) { testCloseClosesDeadLetterSinkEvenInSyncMode(t, newBus) })
	t.Run("PublishAfterCloseReturnsErrClosed", func(t *testing.T) { testPublishAfterCloseReturnsErrClosed(t, newBus) })
	t.Run("SubscribeAfterCloseReturnsErrClosed", func(t *testing.T) { testSubscribeAfterCloseReturnsErrClosed(t, newBus) })
	t.Run("UnsubscribeStopsDelivery", func(t *testing.T) { testUnsubscribeStopsDelivery(t, newBus) })
	t.Run("MiddlewareChainOrdering", func(t *testing.T) { testMiddlewareChainOrdering(t, newBus) })
	t.Run("MiddlewareAddedMidStreamAppliesToNextPublish", func(t *testing.T) { testMiddlewareAddedMidStreamAppliesToNextPublish(t, newBus) })
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

// panickingDeadLetterSink is a DeadLetterSink whose Record always panics,
// used to prove a misbehaving sink cannot crash the bus (see
// grevents' middleware_recovery.go: recordDeadLetter).
type panickingDeadLetterSink struct{}

func (panickingDeadLetterSink) Record(context.Context, grevents.Event, error, int) error {
	panic("conformance: DeadLetterSink.Record panics")
}
func (panickingDeadLetterSink) List(context.Context, int) ([]grevents.DeadLetterEntry, error) {
	return nil, nil
}
func (panickingDeadLetterSink) Close() error { return nil }

// closeTrackingDeadLetterSink records whether Close was ever called on it,
// used by testCloseClosesDeadLetterSinkEvenInSyncMode to prove a caller-
// supplied sink's resources are released even when the bus itself is
// sync-only and never records anything to it.
type closeTrackingDeadLetterSink struct {
	closed *atomic.Bool
}

func (closeTrackingDeadLetterSink) Record(context.Context, grevents.Event, error, int) error {
	return nil
}
func (closeTrackingDeadLetterSink) List(context.Context, int) ([]grevents.DeadLetterEntry, error) {
	return nil, nil
}
func (s closeTrackingDeadLetterSink) Close() error {
	s.closed.Store(true)
	return nil
}

func testDeadLetterSinkPanicDoesNotCrashBus(t *testing.T, newBus newBusFunc) {
	t.Helper()
	ctx := context.Background()
	bus, err := newBus(
		grevents.WithAsync(4, grevents.OverflowBlock),
		grevents.WithRetry(1, time.Millisecond), // 1 attempt = immediate dead-letter on first failure
		grevents.WithDeadLetterSink(panickingDeadLetterSink{}),
		grevents.WithWorkerCount(1),
	)
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		return errors.New("always fails, forcing a dead-letter handoff")
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bus.Publish(ctx, grevents.Event{Topic: "topic"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// If the DeadLetterSink's panic were unrecovered, the test binary
	// itself would have already crashed by the time this line runs —
	// there is nothing further to assert about the panic directly, only
	// that delivery keeps working afterward.
	var otherInvoked atomic.Bool
	if _, err := bus.Subscribe("topic2", func(ctx context.Context, event grevents.Event) error {
		otherInvoked.Store(true)
		return nil
	}); err != nil {
		t.Fatalf("Subscribe after DeadLetterSink panic: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for !otherInvoked.Load() && time.Now().Before(deadline) {
		if err := bus.Publish(ctx, grevents.Event{Topic: "topic2"}); err != nil {
			t.Fatalf("Publish after DeadLetterSink panic: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !otherInvoked.Load() {
		t.Fatal("bus stopped delivering events after a DeadLetterSink panic")
	}
}

// panickingLogger is a Logger whose Errorf always panics, used to prove a
// misbehaving Logger cannot crash the bus even when it is called from
// within a panic-recovery block that is already unwinding a different
// panic (see grevents' middleware_recovery.go: safeLogErrorf).
type panickingLogger struct{}

func (panickingLogger) Infof(string, ...interface{}) {}
func (panickingLogger) Warnf(string, ...interface{}) {}
func (panickingLogger) Errorf(string, ...interface{}) {
	panic("conformance: Logger.Errorf panics")
}

func testLoggerPanicDuringRecoveryDoesNotCrashBus(t *testing.T, newBus newBusFunc) {
	t.Helper()
	ctx := context.Background()
	bus, err := newBus(grevents.WithSync(), grevents.WithLogger(panickingLogger{}))
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		panic("boom: handler panics too, forcing invokeHandler's recover block to log through the panicking Logger")
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	err = bus.Publish(ctx, grevents.Event{Topic: "topic"})
	if err == nil {
		t.Fatalf("Publish (handler panicked) = nil, want an error")
	}

	// Bus must remain usable for subsequent Publish calls.
	if _, err := bus.Subscribe("topic2", func(ctx context.Context, event grevents.Event) error {
		return nil
	}); err != nil {
		t.Fatalf("Subscribe after Logger panic during recovery: %v", err)
	}
	if err := bus.Publish(ctx, grevents.Event{Topic: "topic2"}); err != nil {
		t.Fatalf("Publish after Logger panic during recovery: %v", err)
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

// testCloseClosesDeadLetterSinkEvenInSyncMode proves Bus.Close releases a
// caller-supplied DeadLetterSink's resources even for a sync-only bus,
// which never delivers anything through it (sync mode never retries, so
// nothing is ever dead-lettered) — a caller who explicitly wired one up
// via WithDeadLetterSink still owns it via the bus and expects Close to
// release it, exactly as it would for an async bus.
func testCloseClosesDeadLetterSinkEvenInSyncMode(t *testing.T, newBus newBusFunc) {
	t.Helper()
	var closed atomic.Bool
	bus, err := newBus(grevents.WithSync(), grevents.WithDeadLetterSink(closeTrackingDeadLetterSink{closed: &closed}))
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}

	if err := bus.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !closed.Load() {
		t.Fatalf("DeadLetterSink.Close was not called by a sync-only bus's Close")
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

// testMiddlewareAddedMidStreamAppliesToNextPublish exercises Bus.Use's
// doc comment claim directly — that middleware "is guaranteed to apply
// to every delivery that reads the chain after it was added" — rather
// than only ever testing statically pre-registered middleware ordering
// (see testMiddlewareChainOrdering above), which would pass even if the
// chain were snapshotted once at bus-construction time instead of fresh
// on every delivery.
func testMiddlewareAddedMidStreamAppliesToNextPublish(t *testing.T, newBus newBusFunc) {
	t.Helper()
	ctx := context.Background()
	bus, err := newBus(grevents.WithSync())
	if err != nil {
		t.Fatalf("newBus: %v", err)
	}
	defer bus.Close()

	var applied atomic.Int32
	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// First Publish, before any middleware is registered.
	if err := bus.Publish(ctx, grevents.Event{Topic: "topic"}); err != nil {
		t.Fatalf("Publish (before Use): %v", err)
	}
	if applied.Load() != 0 {
		t.Fatalf("applied = %d before Use was ever called, want 0", applied.Load())
	}

	bus.Use(func(next grevents.HandlerFunc) grevents.HandlerFunc {
		return func(ctx context.Context, event grevents.Event) error {
			applied.Add(1)
			return next(ctx, event)
		}
	})

	// Second Publish, after Use — the middleware must apply to this one.
	if err := bus.Publish(ctx, grevents.Event{Topic: "topic"}); err != nil {
		t.Fatalf("Publish (after Use): %v", err)
	}
	if applied.Load() != 1 {
		t.Fatalf("applied = %d after a Publish following Use, want 1", applied.Load())
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

// TestBus_Contract runs the full behavioral contract suite above against
// grevents.NewBus.
func TestBus_Contract(t *testing.T) {
	runBusContract(t, grevents.NewBus)
}
