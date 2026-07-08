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
type Bus interface {
	// Publish delivers event to every subscriber of event.Topic. Delivery
	// mode (sync vs async) is determined by how the Bus was constructed
	// (see BusOption), not per call. See docs.go for the precise
	// delivery-guarantee language for each mode.
	Publish(ctx context.Context, event Event) error

	// Subscribe registers handler for topic. Returns an Unsubscribe func.
	// Multiple subscribers on the same topic are all invoked; order
	// across subscribers on the same topic is not guaranteed.
	Subscribe(topic string, handler HandlerFunc, opts ...SubscribeOption) (Unsubscribe, error)

	// Use appends middleware to the chain applied to every handler
	// invocation, in the order added. The chain is snapshotted at
	// delivery time, not at Subscribe time: a Use call taking effect
	// concurrently with an in-flight Publish has undefined ordering
	// relative to that specific in-flight call.
	Use(mw Middleware)

	// Stats returns a snapshot of delivery counters and current queue
	// depth. Unlike Publish/Subscribe, Stats remains callable after
	// Close returns — it is the mechanism for inspecting drain
	// shortfall.
	Stats(ctx context.Context) (Stats, error)

	// Close stops accepting new Publish/Subscribe calls immediately,
	// drains the async queue up to the configured drain timeout, then
	// releases resources. Idempotent — safe to call more than once.
	Close() error
}

// Event is the unit published and delivered by a Bus.
type Event struct {
	Topic     string
	Payload   any
	Timestamp time.Time
	// Metadata carries optional cross-cutting data (trace ID, actor ID,
	// tenant ID) that middleware can read/write without needing to know
	// about the Payload's concrete type.
	Metadata map[string]string
}

// HandlerFunc handles one delivery of an Event to a subscriber.
type HandlerFunc func(ctx context.Context, event Event) error

// Unsubscribe removes the subscription it was returned for. Safe to call
// more than once; calls after the first are no-ops.
type Unsubscribe func()

// Middleware wraps a HandlerFunc to add cross-cutting behavior (logging,
// tracing) around every handler invocation it is applied to.
type Middleware func(next HandlerFunc) HandlerFunc

// Stats is a point-in-time snapshot of a Bus's delivery counters.
type Stats struct {
	Published      uint64
	Delivered      uint64
	Failed         uint64
	DeadLettered   uint64
	DroppedOnClose uint64
	QueueDepth     int64 // -1 for sync-only buses (no queue exists)
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

// NewBus constructs a Bus according to the supplied options. Sync
// delivery is the default if no delivery-mode option is given.
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
