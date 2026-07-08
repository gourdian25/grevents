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
//
// A recovered panic is converted into an error and treated exactly like
// any other handler error for retry-counting purposes in async mode, and
// is always logged via logger.Errorf regardless of whether a later retry
// succeeds.
func invokeHandler(ctx context.Context, chain HandlerFunc, event Event, logger Logger) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("grevents: recovered panic in handler chain for topic %q: %v\n%s",
				event.Topic, r, debug.Stack())
			err = fmt.Errorf("grevents: handler panic: %v", r)
		}
	}()
	return chain(ctx, event)
}
