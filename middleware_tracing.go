// File: middleware_tracing.go

package grevents

import "context"

// TracingMiddleware is a stub extension point for a future distributed
// tracing integration (e.g. span injection).
//
// Returns:
//   - Middleware: currently a pure passthrough — it does not modify ctx,
//     the event, or the handler's return value
//
// Notes:
//   - v1 ships no real tracing dependency; this exists only to mark where
//     such an integration would attach later without requiring a
//     breaking API change (e.g. adding an OpenTelemetry tracer parameter
//     to this same function)
//
// Use case: register it now (Use(TracingMiddleware())) as a placeholder
// so call sites don't need to change when real tracing support lands.
func TracingMiddleware() Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, event Event) error {
			return next(ctx, event)
		}
	}
}
