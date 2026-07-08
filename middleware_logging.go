// File: middleware_logging.go

package grevents

import "context"

// LoggingMiddleware returns a Middleware that logs each handler
// invocation's outcome via logger.
//
// Parameters:
//   - logger: Logger — nil is treated as NopLogger()
//
// Returns:
//   - Middleware: logs at Infof on success, Warnf on failure (the
//     underlying error is not swallowed — it is still returned unchanged
//     for the rest of the chain and for Bus.Publish's caller)
//
// Notes:
//   - Unlike panic recovery (always-on, not a Middleware at all), this is
//     opt-in: register it explicitly with Use(LoggingMiddleware(logger))
//
// Use case: observing every delivery attempt (topic, success/failure)
// without instrumenting each individual subscriber handler.
func LoggingMiddleware(logger Logger) Middleware {
	logger = OrNop(logger)
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, event Event) error {
			if err := next(ctx, event); err != nil {
				logger.Warnf("grevents: handler failed for topic %q: %v", event.Topic, err)
				return err
			}
			logger.Infof("grevents: delivered event for topic %q", event.Topic)
			return nil
		}
	}
}
