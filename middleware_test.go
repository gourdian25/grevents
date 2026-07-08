package grevents_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gourdian25/grevents"
)

func TestLoggingMiddleware(t *testing.T) {
	var recorded []string
	logger := recordingLogger{record: func(s string) { recorded = append(recorded, s) }}

	bus, err := grevents.NewBus(grevents.WithSync())
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	bus.Use(grevents.LoggingMiddleware(logger))

	if _, err := bus.Subscribe("ok-topic", func(ctx context.Context, event grevents.Event) error {
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Publish(context.Background(), grevents.Event{Topic: "ok-topic"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(recorded) == 0 || recorded[0] != "info" {
		t.Fatalf("recorded = %v, want LoggingMiddleware to log a success via Infof", recorded)
	}

	recorded = nil
	handlerErr := errors.New("boom")
	if _, err := bus.Subscribe("fail-topic", func(ctx context.Context, event grevents.Event) error {
		return handlerErr
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Publish(context.Background(), grevents.Event{Topic: "fail-topic"}); err == nil {
		t.Fatalf("Publish (failing handler) = nil, want an error")
	}
	if len(recorded) == 0 || recorded[0] != "warn" {
		t.Fatalf("recorded = %v, want LoggingMiddleware to log a failure via Warnf", recorded)
	}
}

func TestLoggingMiddlewareNilLoggerIsSafe(t *testing.T) {
	bus, err := grevents.NewBus(grevents.WithSync())
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	bus.Use(grevents.LoggingMiddleware(nil)) // OrNop must make this safe

	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Publish(context.Background(), grevents.Event{Topic: "topic"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestTracingMiddlewarePassesThrough(t *testing.T) {
	bus, err := grevents.NewBus(grevents.WithSync())
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	bus.Use(grevents.TracingMiddleware())

	var invoked bool
	if _, err := bus.Subscribe("topic", func(ctx context.Context, event grevents.Event) error {
		invoked = true
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Publish(context.Background(), grevents.Event{Topic: "topic"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !invoked {
		t.Fatalf("handler was not invoked through TracingMiddleware")
	}
}
