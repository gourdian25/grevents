// File: middleware_recovery.go

package grevents

import (
	"context"
	"fmt"
	"runtime/debug"
)

// invokeHandler is the single funnel every delivery path (sync and async)
// calls through. It wraps the entire composed middleware chain — not just
// the terminal handler — in a panic recovery, since a panic inside
// user-supplied middleware must be caught exactly like one inside the
// terminal handler. This is always-on and not configurable via Use: one
// misbehaving subscriber must never crash the bus or the host process.
// The same guarantee is extended to grevents' other user-pluggable
// extension points — see safeLogError and recordDeadLetter below, which
// apply it to Logger and DeadLetterSink respectively.
//
// A recovered panic is converted into an error and treated exactly like
// any other handler error for retry-counting purposes in async mode, and
// is always logged via logger.Error regardless of whether a later retry
// succeeds.
func invokeHandler(ctx context.Context, chain HandlerFunc, event Event, logger Logger) (err error) {
	defer func() {
		if r := recover(); r != nil {
			safeLogError(logger, "grevents: recovered panic in handler chain",
				"topic", event.Topic, "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("grevents: handler panic: %v", r)
		}
	}()
	return chain(ctx, event)
}

// safeLogError calls logger.Error, recovering any panic raised by a
// misbehaving Logger implementation itself so a bad logger can never
// crash the host process either — the same guarantee already applied to
// HandlerFunc and Middleware. Needed specifically because invokeHandler's
// own recover block logs through logger.Error: without this guard, a
// panicking Logger would panic again while panic-handling is already in
// progress, and that second panic has no enclosing recover left to catch
// it.
func safeLogError(logger Logger, msg string, args ...any) {
	defer func() { _ = recover() }()
	logger.Error(msg, args...)
}

// recordDeadLetter hands event off to sink, recovering any panic from a
// misbehaving DeadLetterSink implementation. Mirrors invokeHandler's
// shape: it is the funnel every dead-letter handoff goes through, so a
// bad sink can never crash the delivery goroutine (or the host process)
// that calls it.
func recordDeadLetter(sink DeadLetterSink, logger Logger, event Event, lastErr error, attempts int) {
	defer func() {
		if r := recover(); r != nil {
			safeLogError(logger, "grevents: recovered panic in DeadLetterSink", "topic", event.Topic, "panic", r)
		}
	}()
	if err := sink.Record(context.Background(), event, lastErr, attempts); err != nil {
		safeLogError(logger, "grevents: dead-letter record failed", "topic", event.Topic, "error", err)
	}
}
