// File: deadletter_test.go

package grevents_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gourdian25/grevents"
)

func TestMemoryDeadLetterSinkRecordAndList(t *testing.T) {
	ctx := context.Background()
	sink := grevents.NewMemoryDeadLetterSink(10)

	err := errors.New("boom")
	event := grevents.Event{Topic: "topic"}
	if err := sink.Record(ctx, event, err, 3); err != nil {
		t.Fatalf("Record: %v", err)
	}

	entries, listErr := sink.List(ctx, 0)
	if listErr != nil {
		t.Fatalf("List: %v", listErr)
	}
	if len(entries) != 1 {
		t.Fatalf("List returned %d entries, want 1", len(entries))
	}
	if entries[0].Attempts != 3 {
		t.Fatalf("Attempts = %d, want 3", entries[0].Attempts)
	}
	if entries[0].LastError != "boom" {
		t.Fatalf("LastError = %q, want %q", entries[0].LastError, "boom")
	}
	if entries[0].Event.Topic != "topic" {
		t.Fatalf("Event.Topic = %q, want %q", entries[0].Event.Topic, "topic")
	}
	if entries[0].DeadAt.IsZero() {
		t.Fatalf("DeadAt is zero, want set")
	}
}

func TestMemoryDeadLetterSinkRingEviction(t *testing.T) {
	ctx := context.Background()
	sink := grevents.NewMemoryDeadLetterSink(3)

	for i := 0; i < 5; i++ {
		event := grevents.Event{Topic: string(rune('A' + i))}
		if err := sink.Record(ctx, event, nil, 1); err != nil {
			t.Fatalf("Record %d: %v", i, err)
		}
	}

	entries, err := sink.List(ctx, 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("List returned %d entries, want 3 (capacity)", len(entries))
	}
	// Oldest two (A, B) should have been overwritten; only C, D, E remain,
	// in chronological (oldest-first) order.
	want := []string{"C", "D", "E"}
	for i, w := range want {
		if entries[i].Event.Topic != w {
			t.Fatalf("entries[%d].Event.Topic = %q, want %q", i, entries[i].Event.Topic, w)
		}
	}
}

func TestMemoryDeadLetterSinkListLimit(t *testing.T) {
	ctx := context.Background()
	sink := grevents.NewMemoryDeadLetterSink(10)

	for i := 0; i < 5; i++ {
		event := grevents.Event{Topic: string(rune('A' + i))}
		if err := sink.Record(ctx, event, nil, 1); err != nil {
			t.Fatalf("Record %d: %v", i, err)
		}
	}

	entries, err := sink.List(ctx, 2)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("List(limit=2) returned %d entries, want 2", len(entries))
	}
	// Most recent 2, oldest-first: D, E.
	if entries[0].Event.Topic != "D" || entries[1].Event.Topic != "E" {
		t.Fatalf("List(limit=2) = %q, %q, want D, E", entries[0].Event.Topic, entries[1].Event.Topic)
	}
}

func TestMemoryDeadLetterSinkDefaultCapacity(t *testing.T) {
	ctx := context.Background()
	sink := grevents.NewMemoryDeadLetterSink(0) // <=0 defaults to 1000

	if err := sink.Record(ctx, grevents.Event{Topic: "x"}, nil, 1); err != nil {
		t.Fatalf("Record: %v", err)
	}
	entries, err := sink.List(ctx, 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("List returned %d entries, want 1", len(entries))
	}
}

func TestMemoryDeadLetterSinkCloseIsNoop(t *testing.T) {
	sink := grevents.NewMemoryDeadLetterSink(1)
	if err := sink.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil", err)
	}
}

// keepOpenSink lets a test List the entries after Bus.Close has closed the
// sink it wraps.
type keepOpenSink struct{ grevents.DeadLetterSink }

func (keepOpenSink) Close() error { return nil }

// TestDeadLetterAttemptsWhenCloseInterruptsBackoff: Close cutting a
// retry's backoff short dead-letters the pair with the invocations
// actually made, not WithRetry's maxAttempts (v0.2.1 recorded 5 here).
func TestDeadLetterAttemptsWhenCloseInterruptsBackoff(t *testing.T) {
	sink := grevents.NewMemoryDeadLetterSink(10)
	bus, err := grevents.NewBus(
		grevents.WithAsync(4, grevents.OverflowBlock),
		grevents.WithRetry(5, time.Hour), // the first backoff draws from [0, 1h]: Close interrupts it
		grevents.WithDeadLetterSink(keepOpenSink{sink}),
	)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	var calls atomic.Int32
	first := make(chan struct{})
	var once sync.Once
	if _, err := bus.Subscribe("topic", func(context.Context, grevents.Event) error {
		calls.Add(1)
		once.Do(func() { close(first) })
		return errors.New("boom")
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Publish(context.Background(), grevents.Event{Topic: "topic"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	<-first
	time.Sleep(50 * time.Millisecond) // into the backoff sleep
	if err := bus.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries, _ := sink.List(context.Background(), 0)
	if len(entries) != 1 {
		t.Fatalf("dead-letter entries = %d, want 1", len(entries))
	}
	if got, want := entries[0].Attempts, int(calls.Load()); got != want {
		t.Fatalf("Attempts = %d, but the handler ran %d time(s)", got, want)
	}
}

// closedSink records whether Record was called after Close, and closes
// recorded on the first Record.
type closedSink struct {
	mu               sync.Mutex
	closed           bool
	recordAfterClose bool
	recorded         chan struct{}
	once             sync.Once
}

func (s *closedSink) Record(context.Context, grevents.Event, error, int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		s.recordAfterClose = true
	}
	s.once.Do(func() { close(s.recorded) })
	return nil
}
func (s *closedSink) List(context.Context, int) ([]grevents.DeadLetterEntry, error) { return nil, nil }
func (s *closedSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// warnSignal closes ch on the first Warn whose message contains want.
type warnSignal struct {
	want string
	ch   chan struct{}
	once sync.Once
}

func (l *warnSignal) Debug(string, ...any) {}
func (l *warnSignal) Info(string, ...any)  {}
func (l *warnSignal) Error(string, ...any) {}
func (l *warnSignal) Warn(msg string, _ ...any) {
	if strings.Contains(msg, l.want) {
		l.once.Do(func() { close(l.ch) })
	}
}

// TestDeadLetterNotRecordedOnClosedSink: a handler still running past the
// drain timeout fails after Close has closed the sink; its pair must not
// reach the closed sink, and is counted once (DroppedOnClose), not twice.
func TestDeadLetterNotRecordedOnClosedSink(t *testing.T) {
	sink := &closedSink{recorded: make(chan struct{})}
	logger := &warnSignal{want: "dead letter after close", ch: make(chan struct{})}
	bus, err := grevents.NewBus(
		grevents.WithAsync(4, grevents.OverflowBlock),
		grevents.WithDrainTimeout(50*time.Millisecond),
		grevents.WithDeadLetterSink(sink),
		grevents.WithLogger(logger),
	)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	started := make(chan struct{})
	if _, err := bus.Subscribe("topic", func(context.Context, grevents.Event) error {
		close(started)
		time.Sleep(200 * time.Millisecond) // past the drain timeout
		return errors.New("boom")
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Publish(context.Background(), grevents.Event{Topic: "topic"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	<-started
	if err := bus.Close(); !errors.Is(err, grevents.ErrDrainTimeout) {
		t.Fatalf("Close = %v, want ErrDrainTimeout", err)
	}
	select {
	case <-logger.ch: // logged, not recorded
	case <-sink.recorded: // recorded: the check below says whether after Close
	case <-time.After(2 * time.Second):
		t.Fatal("the late failure never reached the dead-letter path")
	}

	sink.mu.Lock()
	after := sink.recordAfterClose
	sink.mu.Unlock()
	if after {
		t.Fatal("Record was called on a DeadLetterSink after Close closed it")
	}
	st, _ := bus.Stats(context.Background())
	if st.DroppedOnClose != 1 || st.DeadLettered != 0 {
		t.Fatalf("DroppedOnClose=%d DeadLettered=%d, want 1 and 0 (counted once)", st.DroppedOnClose, st.DeadLettered)
	}
}
