// File: options_test.go

package grevents_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gourdian25/grevents"
)

func TestNewBusInvalidConfig(t *testing.T) {
	cases := []struct {
		name string
		opts []grevents.BusOption
	}{
		{"zero queue size", []grevents.BusOption{grevents.WithAsync(0, grevents.OverflowBlock)}},
		{"negative queue size", []grevents.BusOption{grevents.WithAsync(-1, grevents.OverflowBlock)}},
		{"unrecognized overflow strategy", []grevents.BusOption{grevents.WithAsync(1, grevents.OverflowStrategy(99))}},
		{"zero worker count", []grevents.BusOption{grevents.WithAsync(1, grevents.OverflowBlock), grevents.WithWorkerCount(0)}},
		{"zero drain timeout", []grevents.BusOption{grevents.WithAsync(1, grevents.OverflowBlock), grevents.WithDrainTimeout(0)}},
		{"zero max attempts", []grevents.BusOption{grevents.WithRetry(0, time.Millisecond)}},
		{"negative max attempts", []grevents.BusOption{grevents.WithRetry(-1, time.Millisecond)}},
		{"nil dead letter sink", []grevents.BusOption{grevents.WithDeadLetterSink(nil)}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := grevents.NewBus(tc.opts...)
			if !errors.Is(err, grevents.ErrInvalidConfig) {
				t.Fatalf("NewBus(%s) error = %v, want ErrInvalidConfig", tc.name, err)
			}
		})
	}
}

func TestNewBusDefaultsToSync(t *testing.T) {
	bus, err := grevents.NewBus()
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	stats, err := bus.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.QueueDepth != -1 {
		t.Fatalf("QueueDepth = %d, want -1 for the sync default", stats.QueueDepth)
	}
}

// TestWithLoggerOption confirms a WithLogger-supplied Logger is actually
// wired in and invoked, using the always-on panic recovery path (which
// unconditionally calls logger.Errorf) as the observable signal.
func TestWithLoggerOption(t *testing.T) {
	var recorded []string
	logger := recordingLogger{record: func(s string) { recorded = append(recorded, s) }}

	bus, err := grevents.NewBus(grevents.WithLogger(logger), grevents.WithSync())
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		panic("boom")
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bus.Publish(context.Background(), grevents.Event{Topic: "topic"}); err == nil {
		t.Fatalf("Publish (handler panicked) = nil, want an error")
	}

	if len(recorded) == 0 || recorded[0] != "error" {
		t.Fatalf("recorded = %v, want the supplied Logger's Errorf to have been called", recorded)
	}
}

type recordingLogger struct {
	record func(string)
}

func (r recordingLogger) Debug(msg string, args ...any) { r.record("debug") }
func (r recordingLogger) Info(msg string, args ...any)  { r.record("info") }
func (r recordingLogger) Warn(msg string, args ...any)  { r.record("warn") }
func (r recordingLogger) Error(msg string, args ...any) { r.record("error") }
