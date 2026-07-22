// File: async.go

package grevents

import (
	"context"
	"fmt"
	"time"
)

// publishAsync enqueues event according to the configured
// OverflowStrategy. It never invokes a handler itself — delivery happens
// on the worker goroutines started by startWorkers.
func (b *eventBus) publishAsync(ctx context.Context, event Event) error {
	switch b.overflow {
	case OverflowBlock:
		select {
		case b.queue <- event:
			return nil
		case <-b.closeChan:
			return ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	case OverflowDrop:
		select {
		case b.queue <- event:
			return nil
		default:
			b.logger.Warn("grevents: event dropped, queue full", "topic", event.Topic)
			return nil // silently dropped; not enqueued, outside the delivery guarantee
		}
	case OverflowReject:
		select {
		case b.queue <- event:
			return nil
		default:
			b.logger.Warn("grevents: event rejected, queue full", "topic", event.Topic)
			return ErrQueueFull
		}
	default:
		return fmt.Errorf("grevents: unrecognized overflow strategy: %w", ErrInvalidConfig)
	}
}

// startWorkers launches the configured number of dequeue workers. Worker
// count controls how fast events leave the queue (dequeue parallelism),
// not how many subscriber deliveries run concurrently — see
// dispatchToSubscribers.
func (b *eventBus) startWorkers() {
	for i := 0; i < b.workerCount; i++ {
		b.wg.Add(1)
		go b.asyncWorker()
	}
}

func (b *eventBus) asyncWorker() {
	defer b.wg.Done()
	for {
		select {
		case event, ok := <-b.queue:
			if !ok {
				return
			}
			b.dispatchToSubscribers(event)
		case <-b.closeChan:
			b.dispatchDrainedQueue()
			return
		}
	}
}

// dispatchDrainedQueue non-blockingly dispatches whatever is already
// buffered in the queue at the moment a worker observes shutdown.
func (b *eventBus) dispatchDrainedQueue() {
	for {
		select {
		case event, ok := <-b.queue:
			if !ok {
				return
			}
			b.dispatchToSubscribers(event)
		default:
			return
		}
	}
}

// dispatchToSubscribers snapshots event.Topic's current subscribers and
// spawns one independent goroutine per subscriber to own that
// (event, subscriber) pair's full middleware + retry lifecycle, then
// returns immediately without waiting for them. This fan-out — not
// worker count — is what guarantees one slow or failing subscriber's
// retries never block delivery to another subscriber of the same event,
// nor delivery of the next queued event.
//
// Every spawned goroutine registers on the same WaitGroup the dequeue
// workers use, so Close's wg.Wait() waits for both to finish. This is
// safe under the WaitGroup's happens-before rules because at least one
// dequeue worker (whose own Add(1) from startWorkers is still
// outstanding) is always alive while dispatchToSubscribers runs, so the
// counter never observably reaches zero mid-dispatch.
func (b *eventBus) dispatchToSubscribers(event Event) {
	subs := b.registry.snapshot(event.Topic)
	for _, sub := range subs {
		b.wg.Add(1)
		b.inFlight.Add(1)
		go func(sub *subscription) {
			defer b.wg.Done()
			defer b.inFlight.Add(-1)
			b.deliverWithRetry(event, sub)
		}(sub)
	}
}

// deliverWithRetry owns one (event, subscriber) pair's entire delivery
// lifecycle: invoke, and on failure, sleep for a Full Jitter backoff
// (interruptible by Close) before retrying, up to retry.maxAttempts total
// invocations. On exhaustion, hands off to the DeadLetterSink. Runs
// entirely on its own goroutine — never blocks a dequeue worker or any
// other subscriber's delivery.
func (b *eventBus) deliverWithRetry(event Event, sub *subscription) {
	chain := b.buildChain(sub.handler)
	ctx := context.Background() // async delivery outlives the originating Publish call's context
	var lastErr error

retryLoop:
	for attempt := 0; attempt < b.retry.maxAttempts; attempt++ {
		err := invokeHandler(ctx, chain, event, b.logger)
		if err == nil {
			b.st.delivered.Add(1)
			return
		}
		lastErr = err
		b.st.failed.Add(1)

		if attempt == b.retry.maxAttempts-1 {
			break
		}

		wait := computeBackoff(b.retry, attempt)
		if wait <= 0 {
			continue
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-b.closeChan:
			timer.Stop()
			break retryLoop
		}
	}

	b.st.deadLettered.Add(1)
	recordDeadLetter(b.dlqSink, b.logger, event, lastErr, b.retry.maxAttempts)
}

// drainQueueCount non-blockingly counts and discards whatever is left in
// queue. Used by Close's final sweep — see bus.go's Close comment for why
// stragglers are counted as dropped rather than dispatched.
func drainQueueCount(queue chan Event) uint64 {
	var n uint64
	for {
		select {
		case _, ok := <-queue:
			if !ok {
				return n
			}
			n++
		default:
			return n
		}
	}
}
