// File: options.go

package grevents

import (
	"fmt"
	"time"
)

// OverflowStrategy controls what happens when an async bus's queue is
// full at Publish time. There is no drop-oldest strategy: no sibling
// repo in the gourdian ecosystem has ever needed to evict a live buffered
// channel's existing head, and Go's channel primitive doesn't support it
// directly (it would require a different, more complex queue structure —
// not justified for v1). There is also no inline-synchronous-fallback
// strategy: unlike grlog's OverflowSyncFallback (safe because writing a
// log entry is cheap and bounded), grevents' equivalent would mean
// running arbitrary subscriber handlers — potentially mid retry-backoff —
// on the publisher's own goroutine, reintroducing exactly the "caller
// blocks for the full backoff duration" footgun sync mode is designed to
// avoid.
type OverflowStrategy int

const (
	// OverflowBlock blocks Publish's calling goroutine on enqueue until
	// queue space frees up, the bus is closed (returns ErrClosed), or
	// ctx is done (returns ctx.Err()). Preserves strict enqueue ordering
	// at the cost of publisher latency under load. Ported verbatim from
	// grlog's OverflowBlock semantics.
	OverflowBlock OverflowStrategy = iota

	// OverflowDrop never blocks; if the queue is full, the incoming
	// event is discarded and Publish returns nil. Dropped events never
	// entered the delivery path and are outside the async at-least-once
	// guarantee entirely. Ported verbatim from grlog's OverflowDrop
	// semantics (drops the incoming/newest event, never an existing
	// queued one).
	OverflowDrop

	// OverflowReject never blocks; if the queue is full, Publish returns
	// ErrQueueFull immediately and nothing is enqueued. New to grevents:
	// grlog never needed this because it always had a safe inline
	// fallback (OverflowSyncFallback) to fall back to instead.
	OverflowReject
)

func (o OverflowStrategy) valid() bool {
	switch o {
	case OverflowBlock, OverflowDrop, OverflowReject:
		return true
	default:
		return false
	}
}

// BusOption configures a Bus at construction time via NewBus.
type BusOption func(*busConfig)

type busConfig struct {
	async        bool
	queueSize    int
	overflow     OverflowStrategy
	workerCount  int
	retry        retryConfig
	dlqSink      DeadLetterSink
	dlqExplicit  bool
	logger       Logger
	drainTimeout time.Duration
}

const (
	defaultWorkerCount  = 4
	defaultDrainTimeout = 5 * time.Second
	defaultMaxAttempts  = 1 // no retry unless WithRetry configures more
)

func defaultBusConfig() *busConfig {
	return &busConfig{
		async:        false,
		workerCount:  defaultWorkerCount,
		overflow:     OverflowBlock,
		drainTimeout: defaultDrainTimeout,
		retry: retryConfig{
			maxAttempts: defaultMaxAttempts,
			maxBackoff:  defaultMaxBackoff,
		},
	}
}

// WithSync selects synchronous delivery. This is the default if no
// delivery-mode option is given.
func WithSync() BusOption {
	return func(c *busConfig) { c.async = false }
}

// WithAsync selects asynchronous delivery with a queue buffered to
// queueSize, using overflow when the queue is full at Publish time.
func WithAsync(queueSize int, overflow OverflowStrategy) BusOption {
	return func(c *busConfig) {
		c.async = true
		c.queueSize = queueSize
		c.overflow = overflow
	}
}

// WithRetry configures async-mode retry: up to maxAttempts total
// invocations per (event, subscriber) pair (including the first), with
// Full Jitter exponential backoff starting at baseBackoff and capped at a
// fixed internal ceiling (see defaultMaxBackoff). Has no effect in sync
// mode, which never retries.
func WithRetry(maxAttempts int, baseBackoff time.Duration) BusOption {
	return func(c *busConfig) {
		c.retry.maxAttempts = maxAttempts
		c.retry.baseBackoff = baseBackoff
	}
}

// WithDeadLetterSink overrides the default in-memory DeadLetterSink.
// Passing nil is a configuration error caught at NewBus time.
func WithDeadLetterSink(sink DeadLetterSink) BusOption {
	return func(c *busConfig) {
		c.dlqSink = sink
		c.dlqExplicit = true
	}
}

// WithLogger sets the Logger used for diagnostic logging (recovered
// panics, dead-letter recording failures). Nil is treated as NopLogger().
func WithLogger(logger Logger) BusOption {
	return func(c *busConfig) { c.logger = logger }
}

// WithWorkerCount sets the number of dequeue worker goroutines for an
// async bus. Worker count controls dequeue parallelism only — it does
// not bound per-subscriber delivery concurrency, since each dequeued
// event fans out into one independent goroutine per subscriber (see
// async.go). Has no effect in sync mode.
func WithWorkerCount(n int) BusOption {
	return func(c *busConfig) { c.workerCount = n }
}

// WithDrainTimeout bounds how long Close waits for an async bus's queue
// and in-flight deliveries to finish before giving up and reporting the
// shortfall. Has no effect in sync mode, which has nothing to drain.
func WithDrainTimeout(d time.Duration) BusOption {
	return func(c *busConfig) { c.drainTimeout = d }
}

func (c *busConfig) validate() error {
	if c.async {
		if c.queueSize <= 0 {
			return fmt.Errorf("grevents: async queue size must be > 0: %w", ErrInvalidConfig)
		}
		if !c.overflow.valid() {
			return fmt.Errorf("grevents: unrecognized overflow strategy: %w", ErrInvalidConfig)
		}
		if c.workerCount <= 0 {
			return fmt.Errorf("grevents: worker count must be > 0: %w", ErrInvalidConfig)
		}
		if c.drainTimeout <= 0 {
			return fmt.Errorf("grevents: drain timeout must be > 0: %w", ErrInvalidConfig)
		}
	}
	if c.retry.maxAttempts < 1 {
		return fmt.Errorf("grevents: max attempts must be >= 1: %w", ErrInvalidConfig)
	}
	if c.dlqExplicit && c.dlqSink == nil {
		return fmt.Errorf("grevents: dead-letter sink must not be nil: %w", ErrInvalidConfig)
	}
	if c.dlqSink == nil {
		c.dlqSink = NewMemoryDeadLetterSink(defaultDLQCapacity)
	}
	return nil
}
