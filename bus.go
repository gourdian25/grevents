// File: bus.go

package grevents

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Bus is the primary interface for publishing and subscribing to events.
// The concrete implementation returned by NewBus runs entirely
// in-process — see docs.go for the full set of delivery guarantees
// before depending on ordering, retry, or dead-letter behavior.
type Bus interface {
	// Publish delivers event to every subscriber of event.Topic. Delivery
	// mode (sync vs async) is fixed at construction time by BusOption,
	// not chosen per call.
	//
	// Parameters:
	//   - ctx: context.Context — in sync mode, cancellation has no effect
	//     on handlers already invoked (they run to completion); in async
	//     mode with OverflowBlock, a canceled/expired ctx unblocks a
	//     Publish call that is waiting for queue space
	//   - event: Event — Timestamp is set to time.Now() by Publish if the
	//     caller left it zero
	//
	// Returns:
	//   - error: nil if publishing to a topic with no subscribers (a
	//     silent no-op by design); in sync mode, an error wrapping
	//     errors.Join of every failing subscriber's error (inspect with
	//     errors.Is/errors.As); in async mode, ErrQueueFull
	//     (OverflowReject with a full queue), ctx.Err() (OverflowBlock
	//     with a full queue and a canceled/expired ctx), or nil once the
	//     event is enqueued or intentionally dropped (OverflowDrop);
	//     ErrClosed if the bus has been closed
	//
	// Use case: the single entry point a producer calls to announce a
	// state change without knowing or caring what (if anything) consumes
	// it.
	Publish(ctx context.Context, event Event) error

	// Subscribe registers handler for topic.
	//
	// Parameters:
	//   - topic: string — matched by exact string equality only; there is
	//     no wildcard/glob matching in v1
	//   - handler: HandlerFunc — invoked through the middleware chain and
	//     always wrapped in panic recovery, whether or not any Middleware
	//     is registered
	//   - opts: ...SubscribeOption — extension point; no concrete option
	//     is defined in v1
	//
	// Returns:
	//   - Unsubscribe: stops further delivery to handler when called;
	//     safe to call more than once
	//   - error: ErrClosed if the bus has been closed
	//
	// Notes:
	//   - Multiple subscribers on the same topic are all invoked; order
	//     across subscribers on the same topic is not guaranteed
	//
	// Use case: registering a consumer that reacts to a producer's
	// events without the producer needing a reference to it.
	Subscribe(topic string, handler HandlerFunc, opts ...SubscribeOption) (Unsubscribe, error)

	// Use appends mw to the middleware chain applied to every handler
	// invocation, in the order added (first-added wraps outermost).
	//
	// Parameters:
	//   - mw: Middleware
	//
	// Notes:
	//   - The chain is snapshotted at delivery time, not at Subscribe
	//     time: a Use call taking effect concurrently with an in-flight
	//     Publish has undefined ordering relative to that specific
	//     in-flight call, but is guaranteed to apply to every delivery
	//     that reads the chain after it was added
	//
	// Use case: cross-cutting concerns — logging every delivery attempt
	// (see LoggingMiddleware), tracing/span injection (see
	// TracingMiddleware) — applied uniformly without touching every
	// handler.
	Use(mw Middleware)

	// Stats returns a snapshot of delivery counters and current queue
	// depth.
	//
	// Parameters:
	//   - ctx: context.Context — unused by the in-process implementation,
	//     accepted for interface symmetry and future backends that may
	//     need it
	//
	// Returns:
	//   - Stats: see the Stats type doc comment for field semantics
	//   - error: always nil for the in-process implementation
	//
	// Notes:
	//   - Unlike Publish/Subscribe, Stats remains callable after Close
	//     returns — it is the mechanism for inspecting drain shortfall
	//     via Stats.DroppedOnClose
	Stats(ctx context.Context) (Stats, error)

	// Close stops accepting new Publish/Subscribe calls immediately
	// (both return ErrClosed thereafter) and, for an async bus, drains
	// the queue up to the configured WithDrainTimeout.
	//
	// Returns:
	//   - error: nil on a clean drain (or immediately for a sync-only
	//     bus, which has nothing to drain); an error wrapping
	//     ErrDrainTimeout if draining did not finish within the
	//     configured timeout
	//
	// Notes:
	//   - Idempotent — safe to call more than once; every call after the
	//     first returns nil
	//   - Cannot forcibly terminate goroutines still in flight past a
	//     drain timeout — Go has no such primitive; it only bounds how
	//     long Close itself blocks the caller
	Close() error
}

// Event is the unit published and delivered by a Bus.
//
// Fields:
//   - Topic: string — matched by exact string equality against Subscribe
//     calls
//   - Payload: any — opaque as far as grevents is concerned; no schema
//     validation, no versioning (see docs.go's scope notes)
//   - Timestamp: time.Time — set to time.Now() by Publish if left zero by
//     the caller
//   - Metadata: map[string]string — optional cross-cutting data (trace
//     ID, actor ID, tenant ID) that middleware can read/write without
//     needing to know the Payload's concrete type
type Event struct {
	Topic     string
	Payload   any
	Timestamp time.Time
	Metadata  map[string]string
}

// HandlerFunc handles one delivery of an Event to a subscriber.
//
// Parameters:
//   - ctx: context.Context — see Bus.Publish for what cancellation means
//     in each delivery mode
//   - event: Event
//
// Returns:
//   - error: a non-nil return counts as a failed delivery attempt; in
//     async mode with retry configured, it triggers a retry (see
//     WithRetry) rather than immediate dead-lettering
//
// Notes:
//   - A panic inside a HandlerFunc (or inside Middleware wrapping it) is
//     always recovered and converted into an error — see docs.go
//
// Use case: the callback a consumer supplies to Bus.Subscribe to react to
// events on one topic.
type HandlerFunc func(ctx context.Context, event Event) error

// Unsubscribe removes the subscription it was returned for.
//
// Notes:
//   - Safe to call more than once; calls after the first are no-ops
//
// Use case: typically deferred right after a successful Subscribe call —
// defer unsubscribe().
type Unsubscribe func()

// Middleware wraps a HandlerFunc to add cross-cutting behavior around
// every handler invocation it is applied to.
//
// Parameters:
//   - next: HandlerFunc — the next link in the chain (either another
//     Middleware's wrapped function, or the terminal subscriber handler)
//
// Returns:
//   - HandlerFunc: the wrapped function Bus invokes in next's place
//
// Notes:
//   - Registered via Bus.Use, applied in the order added (first-added
//     wraps outermost); panic recovery already wraps the entire composed
//     chain unconditionally, so Middleware does not need to recover its
//     own panics
//
// Use case: see LoggingMiddleware and TracingMiddleware for ready-made
// examples; write a custom one for anything else cross-cutting (metrics,
// auth checks, payload validation).
type Middleware func(next HandlerFunc) HandlerFunc

// Stats is a point-in-time snapshot of a Bus's delivery counters,
// returned by Bus.Stats. All counters are monotonically non-decreasing
// for the lifetime of a Bus.
//
// Fields:
//   - Published: uint64 — incremented once per Publish call that reaches
//     dispatch (the bus was not already closed), regardless of delivery
//     outcome — including events later discarded by OverflowDrop or
//     rejected by OverflowReject
//   - Delivered: uint64 — incremented once per subscriber whose handler
//     eventually returned nil (on any attempt, including a retried one)
//   - Failed: uint64 — incremented once per failed handler invocation,
//     including every individual retry attempt in async mode, not just
//     the first failure
//   - DeadLettered: uint64 — incremented once per (event, subscriber)
//     pair that exhausted its retry attempts and was handed to the
//     DeadLetterSink
//   - DroppedOnClose: uint64 — events still queued or in flight when
//     Close returned, whether because its drain timeout was reached or
//     because of the rare race-window sweep described on Bus.Close
//   - QueueDepth: int64 — current async queue occupancy; -1 for
//     sync-only buses, which have no queue
type Stats struct {
	Published      uint64
	Delivered      uint64
	Failed         uint64
	DeadLettered   uint64
	DroppedOnClose uint64
	QueueDepth     int64
}

// eventBus is the concrete Bus implementation. Async-only fields
// (queue, closeChan, wg, workerCount, overflow, drainTimeout, retry,
// dlqSink, inFlight) are zero-valued and unused when async is false.
type eventBus struct {
	async    bool
	registry *registry
	logger   Logger

	middlewaresMu sync.RWMutex
	middlewares   []Middleware

	st busStats

	closed atomic.Bool

	queue        chan Event
	closeChan    chan struct{}
	wg           sync.WaitGroup
	workerCount  int
	overflow     OverflowStrategy
	drainTimeout time.Duration
	retry        retryConfig
	dlqSink      DeadLetterSink
	inFlight     atomic.Int64
}

// NewBus constructs a Bus according to the supplied options.
//
// Parameters:
//   - opts: ...BusOption — see options.go for the full list (WithSync,
//     WithAsync, WithRetry, WithDeadLetterSink, WithLogger,
//     WithWorkerCount, WithDrainTimeout)
//
// Returns:
//   - Bus: ready to use immediately; for an async bus, its worker
//     goroutines are already running
//   - error: wraps ErrInvalidConfig if opts describe an invalid
//     combination (e.g. a non-positive async queue size, an unrecognized
//     OverflowStrategy, a nil WithDeadLetterSink)
//
// Notes:
//   - Sync delivery is the default if no delivery-mode option is given
//
// Example:
//
//	bus, err := grevents.NewBus(
//		grevents.WithAsync(256, grevents.OverflowBlock),
//		grevents.WithRetry(5, 100*time.Millisecond),
//	)
func NewBus(opts ...BusOption) (Bus, error) {
	cfg := defaultBusConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	b := &eventBus{
		async:        cfg.async,
		registry:     newRegistry(),
		logger:       OrNop(cfg.logger),
		workerCount:  cfg.workerCount,
		overflow:     cfg.overflow,
		drainTimeout: cfg.drainTimeout,
		retry:        cfg.retry,
		dlqSink:      cfg.dlqSink,
	}

	if b.async {
		b.queue = make(chan Event, cfg.queueSize)
		b.closeChan = make(chan struct{})
		b.startWorkers()
	}

	return b, nil
}

func (b *eventBus) Use(mw Middleware) {
	b.middlewaresMu.Lock()
	defer b.middlewaresMu.Unlock()
	b.middlewares = append(b.middlewares, mw)
}

func (b *eventBus) middlewareSnapshot() []Middleware {
	b.middlewaresMu.RLock()
	defer b.middlewaresMu.RUnlock()
	if len(b.middlewares) == 0 {
		return nil
	}
	snap := make([]Middleware, len(b.middlewares))
	copy(snap, b.middlewares)
	return snap
}

// buildChain composes a fresh middleware snapshot around handler, in the
// order middleware was added (first-added wraps outermost).
func (b *eventBus) buildChain(handler HandlerFunc) HandlerFunc {
	chain := handler
	mws := b.middlewareSnapshot()
	for i := len(mws) - 1; i >= 0; i-- {
		chain = mws[i](chain)
	}
	return chain
}

func (b *eventBus) Publish(ctx context.Context, event Event) error {
	if b.closed.Load() {
		return ErrClosed
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	b.st.published.Add(1)

	if b.async {
		return b.publishAsync(ctx, event)
	}
	return b.publishSync(ctx, event)
}

func (b *eventBus) Subscribe(topic string, handler HandlerFunc, opts ...SubscribeOption) (Unsubscribe, error) {
	if b.closed.Load() {
		return nil, ErrClosed
	}
	return b.registry.subscribe(topic, handler, opts...), nil
}

func (b *eventBus) Stats(ctx context.Context) (Stats, error) {
	queueDepth := int64(-1)
	if b.async {
		queueDepth = int64(len(b.queue))
	}
	return Stats{
		Published:      b.st.published.Load(),
		Delivered:      b.st.delivered.Load(),
		Failed:         b.st.failed.Load(),
		DeadLettered:   b.st.deadLettered.Load(),
		DroppedOnClose: b.st.droppedOnClose.Load(),
		QueueDepth:     queueDepth,
	}, nil
}

func (b *eventBus) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return nil // idempotent
	}
	if !b.async {
		return nil // sync-only bus: no queue, no workers, nothing to drain
	}

	close(b.closeChan)

	drained := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(drained)
	}()

	var timedOut bool
	select {
	case <-drained:
	case <-time.After(b.drainTimeout):
		timedOut = true
	}

	// Sweep whatever is still sitting in the queue: on the timeout path
	// this is genuinely undelivered work; on the happy path it catches
	// the rare race where an event was enqueued by a concurrent Publish
	// call between that call's closed.Load() check and this CAS above.
	// Either way, Close must not spawn new delivery goroutines once it
	// has stopped waiting for the ones it already knows about — these
	// are counted as dropped, not delivered.
	straggled := drainQueueCount(b.queue)
	b.st.droppedOnClose.Store(straggled + uint64(b.inFlight.Load()))

	if b.dlqSink != nil {
		_ = b.dlqSink.Close()
	}

	if timedOut {
		return fmt.Errorf("grevents: drain timeout after %s, %d event(s) undelivered: %w",
			b.drainTimeout, b.st.droppedOnClose.Load(), ErrDrainTimeout)
	}
	return nil
}
