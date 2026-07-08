// File: middleware_logging.go

package grevents

import "context"

// LoggingMiddleware returns a Middleware that logs each handler
// invocation's outcome via logger. Unlike panic recovery (always-on, not
// a Middleware at all), this is opt-in: register it explicitly with
// Use(LoggingMiddleware(logger)).
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
