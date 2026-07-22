// File: internal_coverage_test.go

package grevents

import (
	"context"
	"errors"
	"testing"
	"time"
)

// These tests construct eventBus (and its collaborators) directly rather
// than through NewBus, to reach a handful of defensive/internal branches
// that the public API and contract_bus_test.go's black-box suite cannot
// reach on their own — either because an earlier validation already
// forecloses the bad input (mirrors gourdiantoken's own coverage
// technique of the same name), or because the branch reacts to internal
// state (a closed queue, a negative counter) that no production code path
// ever produces.

func TestPublishAsync_OverflowBlockReturnsErrClosedWhenAlreadyClosing(t *testing.T) {
	// queue is unbuffered and nothing ever reads it, so the only select
	// case that can ever become ready is closeChan, which is already
	// closed — deterministic, no race required.
	b := &eventBus{
		overflow:  OverflowBlock,
		queue:     make(chan Event),
		closeChan: make(chan struct{}),
	}
	close(b.closeChan)

	err := b.publishAsync(context.Background(), Event{Topic: "topic"})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("publishAsync (closeChan already closed) error = %v, want ErrClosed", err)
	}
}

func TestPublishAsync_UnrecognizedOverflowStrategy(t *testing.T) {
	// Unreachable via NewBus: busConfig.validate rejects an invalid
	// OverflowStrategy before an eventBus is ever constructed.
	b := &eventBus{
		overflow:  OverflowStrategy(99),
		queue:     make(chan Event, 1),
		closeChan: make(chan struct{}),
	}

	err := b.publishAsync(context.Background(), Event{Topic: "topic"})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("publishAsync (unrecognized overflow) error = %v, want ErrInvalidConfig", err)
	}
}

func TestAsyncWorker_ReturnsWhenQueueClosed(t *testing.T) {
	// b.queue is never closed by any production code path (only
	// closeChan is) — this exercises asyncWorker's defensive handling of
	// that case directly.
	b := &eventBus{queue: make(chan Event), closeChan: make(chan struct{})}
	close(b.queue)

	b.wg.Add(1)
	done := make(chan struct{})
	go func() {
		b.asyncWorker()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("asyncWorker did not return after its queue was closed")
	}
}

func TestDispatchDrainedQueue_ReturnsWhenQueueClosed(t *testing.T) {
	b := &eventBus{queue: make(chan Event), closeChan: make(chan struct{})}
	close(b.queue)

	b.dispatchDrainedQueue() // must return promptly rather than hang or panic
}

func TestDeliverWithRetry_ZeroBackoffContinuesImmediately(t *testing.T) {
	b := &eventBus{
		logger:    NopLogger(),
		closeChan: make(chan struct{}),
		dlqSink:   NewMemoryDeadLetterSink(10),
		retry:     retryConfig{maxAttempts: 3, baseBackoff: 0},
	}
	var attempts int
	sub := &subscription{handler: func(ctx context.Context, e Event) error {
		attempts++
		return errors.New("always fails")
	}}

	b.deliverWithRetry(Event{Topic: "topic"}, sub)

	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3 (maxAttempts)", attempts)
	}
	if b.st.deadLettered.Load() != 1 {
		t.Fatalf("deadLettered = %d, want 1", b.st.deadLettered.Load())
	}
}

func TestDeliverWithRetry_CloseChanInterruptsBackoffSleep(t *testing.T) {
	b := &eventBus{
		logger:    NopLogger(),
		closeChan: make(chan struct{}),
		dlqSink:   NewMemoryDeadLetterSink(10),
		retry:     retryConfig{maxAttempts: 5, baseBackoff: 200 * time.Millisecond, maxBackoff: time.Second},
	}
	sub := &subscription{handler: func(ctx context.Context, e Event) error {
		return errors.New("always fails")
	}}

	done := make(chan struct{})
	go func() {
		b.deliverWithRetry(Event{Topic: "topic"}, sub)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond) // let the first attempt fail and enter its backoff sleep
	close(b.closeChan)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("deliverWithRetry did not return after closeChan closed mid-backoff")
	}

	if b.st.deadLettered.Load() != 1 {
		t.Fatalf("deadLettered = %d, want 1", b.st.deadLettered.Load())
	}
}

func TestDrainQueueCount(t *testing.T) {
	q := make(chan Event, 3)
	q <- Event{Topic: "a"}
	q <- Event{Topic: "b"}

	if got := drainQueueCount(q); got != 2 {
		t.Fatalf("drainQueueCount = %d, want 2", got)
	}

	close(q)
	if got := drainQueueCount(q); got != 0 {
		t.Fatalf("drainQueueCount (closed, empty) = %d, want 0", got)
	}
}

func TestClose_DefensiveNilDLQSinkAndNegativeInFlight(t *testing.T) {
	// Both branches exercised here are documented in bus.go as
	// unreachable via the public NewBus constructor (validate always
	// assigns a non-nil dlqSink; inFlight's Add(1)/Add(-1) calls are
	// always balanced) — covered by constructing the struct directly.
	b := &eventBus{
		async:        true,
		queue:        make(chan Event, 1),
		closeChan:    make(chan struct{}),
		drainTimeout: time.Second,
	}
	b.inFlight.Store(-5)

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	stats, _ := b.Stats(context.Background())
	if stats.DroppedOnClose != 0 {
		t.Fatalf("DroppedOnClose = %d, want 0 (negative inFlight clamped, nil dlqSink no-op)", stats.DroppedOnClose)
	}
}

func TestRegistrySubscribe_AppliesSubscribeOptions(t *testing.T) {
	r := newRegistry()
	var applied bool
	opt := SubscribeOption(func(s *subscription) { applied = true })

	r.subscribe("topic", func(ctx context.Context, e Event) error { return nil }, opt)

	if !applied {
		t.Fatal("SubscribeOption passed to subscribe was never applied")
	}
}

// capturingLogger records every Error call's message, used by
// TestRecordDeadLetter_SinkRecordErrorIsLogged below.
type capturingLogger struct{ errors []string }

func (l *capturingLogger) Debug(string, ...any) {}
func (l *capturingLogger) Info(string, ...any)  {}
func (l *capturingLogger) Warn(string, ...any)  {}
func (l *capturingLogger) Error(msg string, args ...any) {
	l.errors = append(l.errors, msg)
}

// erroringDeadLetterSink returns a (non-panicking) error from Record,
// distinct from contract_bus_test.go's panickingDeadLetterSink.
type erroringDeadLetterSink struct{}

func (erroringDeadLetterSink) Record(context.Context, Event, error, int) error {
	return errors.New("record failed")
}
func (erroringDeadLetterSink) List(context.Context, int) ([]DeadLetterEntry, error) {
	return nil, nil
}
func (erroringDeadLetterSink) Close() error { return nil }

func TestRecordDeadLetter_SinkRecordErrorIsLogged(t *testing.T) {
	logger := &capturingLogger{}
	recordDeadLetter(erroringDeadLetterSink{}, logger, Event{Topic: "topic"}, errors.New("boom"), 3)

	if len(logger.errors) == 0 {
		t.Fatal("recordDeadLetter did not log the sink's Record error via logger.Errorf")
	}
}
