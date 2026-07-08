// File: sync.go

package grevents

import (
	"context"
	"errors"
	"fmt"
)

// publishSync invokes every subscriber's handler for event.Topic through
// the middleware chain and blocks until all have returned. There is no
// retry: a failing handler counts as failed for this Publish call and its
// error is joined into the returned error. At-most-once per subscriber
// per Publish call.
func (b *eventBus) publishSync(ctx context.Context, event Event) error {
	subs := b.registry.snapshot(event.Topic)
	if len(subs) == 0 {
		return nil // silent no-op: no subscribers is not an error
	}

	var errs []error
	for _, sub := range subs {
		chain := b.buildChain(sub.handler)
		if err := invokeHandler(ctx, chain, event, b.logger); err != nil {
			errs = append(errs, err)
			b.st.failed.Add(1)
			continue
		}
		b.st.delivered.Add(1)
	}

	if len(errs) > 0 {
		return fmt.Errorf("grevents: publish %q: %d/%d subscriber(s) failed: %w",
			event.Topic, len(errs), len(subs), errors.Join(errs...))
	}
	return nil
}
