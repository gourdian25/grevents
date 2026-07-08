// File: middleware_tracing.go

package grevents

import "context"

// TracingMiddleware is a stub extension point for a future distributed
// tracing integration (e.g. span injection). v1 ships no real tracing
// dependency: this middleware currently passes the handler through
// unchanged and exists only to mark where such an integration would
// attach without requiring a breaking API change later.
func TracingMiddleware() Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, event Event) error {
			return next(ctx, event)
		}
	}
}
