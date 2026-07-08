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
// OverflowStrategy is documented as a block; see each constant below for
// per-value semantics.
//
// Use case: passed to WithAsync to choose how Publish behaves when the
// async queue is full.
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
//
// Use case: the functional-option pattern used throughout grevents'
// public API — each With* function below returns a BusOption.
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

// WithSync selects synchronous delivery.
//
// Returns:
//   - BusOption
//
// Notes:
//   - This is the default if no delivery-mode option is given to NewBus;
//     WithSync only exists so callers can say so explicitly
//
// Use case: request-path publishers that need to know every subscriber
// ran (and whether any failed) before Publish returns.
func WithSync() BusOption {
	return func(c *busConfig) { c.async = false }
}

// WithAsync selects asynchronous delivery.
//
// Parameters:
//   - queueSize: int — buffered channel capacity; must be > 0
//   - overflow: OverflowStrategy — behavior when the queue is full at
//     Publish time; see OverflowBlock, OverflowDrop, OverflowReject
//
// Returns:
//   - BusOption
//
// Use case: publishers that must not block on subscriber work — e.g. an
// HTTP handler emitting an audit event that shouldn't add latency to the
// response.
func WithAsync(queueSize int, overflow OverflowStrategy) BusOption {
	return func(c *busConfig) {
		c.async = true
		c.queueSize = queueSize
		c.overflow = overflow
	}
}

// WithRetry configures async-mode retry.
//
// Parameters:
//   - maxAttempts: int — total invocations per (event, subscriber) pair,
//     including the first; must be >= 1 (1 means no retry)
//   - baseBackoff: time.Duration — the starting point for Full Jitter
//     exponential backoff (sleep = random(0, min(cap, base*2^attempt)));
//     capped at a fixed internal ceiling, see defaultMaxBackoff
//
// Returns:
//   - BusOption
//
// Notes:
//   - Has no effect in sync mode, which never retries (see docs.go for
//     why)
//
// Use case: subscribers calling a flaky downstream (a network call, a
// database write) that's worth a few automatic retries before falling
// back to the DeadLetterSink.
func WithRetry(maxAttempts int, baseBackoff time.Duration) BusOption {
	return func(c *busConfig) {
		c.retry.maxAttempts = maxAttempts
		c.retry.baseBackoff = baseBackoff
	}
}

// WithDeadLetterSink overrides the default in-memory DeadLetterSink.
//
// Parameters:
//   - sink: DeadLetterSink — must not be nil (see NewMemoryDeadLetterSink
//     for the default)
//
// Returns:
//   - BusOption
//
// Notes:
//   - Passing nil is a configuration error caught at NewBus time
//     (wraps ErrInvalidConfig), not silently ignored
//
// Use case: swapping in a durable sink (e.g. a future graudit-backed
// implementation) once one exists, without changing any other bus
// configuration.
func WithDeadLetterSink(sink DeadLetterSink) BusOption {
	return func(c *busConfig) {
		c.dlqSink = sink
		c.dlqExplicit = true
	}
}

// WithLogger sets the Logger used for diagnostic logging (recovered
// panics, dead-letter recording failures, drain-timeout warnings).
//
// Parameters:
//   - logger: Logger — nil is treated as NopLogger()
//
// Returns:
//   - BusOption
//
// Use case: passing a *grlog.Logger (or any type structurally satisfying
// Logger) to surface grevents' internal diagnostics through the same
// logging pipeline as the rest of an application.
func WithLogger(logger Logger) BusOption {
	return func(c *busConfig) { c.logger = logger }
}

// WithWorkerCount sets the number of dequeue worker goroutines for an
// async bus.
//
// Parameters:
//   - n: int — must be > 0; defaults to 4 if this option is never
//     supplied
//
// Returns:
//   - BusOption
//
// Notes:
//   - Worker count controls dequeue parallelism only — it does not bound
//     per-subscriber delivery concurrency, since each dequeued event fans
//     out into one independent goroutine per subscriber (see async.go);
//     has no effect in sync mode
//
// Use case: tuning how quickly events leave the queue under sustained
// publish load, independent of how many subscribers each event fans out
// to.
func WithWorkerCount(n int) BusOption {
	return func(c *busConfig) { c.workerCount = n }
}

// WithDrainTimeout bounds how long Close waits for an async bus's queue
// and in-flight deliveries to finish before giving up and reporting the
// shortfall.
//
// Parameters:
//   - d: time.Duration — must be > 0; defaults to 5s if this option is
//     never supplied
//
// Returns:
//   - BusOption
//
// Notes:
//   - Has no effect in sync mode, which has nothing to drain
//
// Use case: bounding shutdown latency in a service with a strict
// termination deadline (e.g. a container orchestrator's SIGTERM grace
// period), at the cost of accepting some events may go unfinished.
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
