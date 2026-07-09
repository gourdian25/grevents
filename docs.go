// File: docs.go

// Package grevents provides a lightweight, pluggable, in-process event bus
// for the gourdian ecosystem.
//
// Overview:
//
// grevents decouples producers of state changes (e.g. grauth assigning a
// role) from consumers that react to them (graudit recording it, grcache
// invalidating a related tag, a future notification system emailing
// someone) without the producer needing to know any consumer exists.
//
// grevents is explicitly not an attempt to replace Kafka, NATS, or
// RabbitMQ. It runs entirely within a single process's memory: two
// replicas of the same service using grevents do not see each other's
// events, and a process restart loses any queued or dead-lettered events.
// These are the defined boundaries of what this package is, not bugs to
// fix later. A consumer that needs durable, cross-process, at-least-once
// delivery at scale needs a separate adapter package built on top of an
// actual message broker.
//
// Getting Started:
//
//	bus, err := grevents.NewBus() // sync delivery by default
//	if err != nil {
//	    // handle err
//	}
//	defer bus.Close()
//
//	unsubscribe, err := bus.Subscribe("role.assigned", func(ctx context.Context, event grevents.Event) error {
//	    // react to the event
//	    return nil
//	})
//	if err != nil {
//	    // handle err
//	}
//	defer unsubscribe()
//
//	err = bus.Publish(context.Background(), grevents.Event{
//	    Topic:   "role.assigned",
//	    Payload: someStruct{},
//	})
//
// Delivery Modes:
//
// A Bus is constructed for either synchronous or asynchronous delivery;
// the mode is fixed at construction time via BusOption, not chosen per
// Publish call.
//
// Sync mode (the default, WithSync): Publish invokes every subscriber's
// handler for the event's topic through the middleware chain and blocks
// until all of them have returned. If one or more handlers fail, Publish
// returns a single error aggregating every failure via errors.Join —
// inspect individual failures with errors.Is/errors.As against the
// returned error. There is no retry in sync mode: retrying synchronously
// would mean Publish blocks the caller for the full backoff duration,
// which is a footgun for anything calling Publish from a request path.
//
// Async mode (WithAsync): Publish enqueues the event and returns
// immediately, unless the queue is full, in which case behavior is
// governed by the configured OverflowStrategy. A pool of worker
// goroutines (WithWorkerCount) dequeues events and, for every subscriber
// of that event's topic, delivers independently with retry-with-backoff —
// one slow or failing subscriber's retries never block delivery to other
// subscribers of the same event, nor delivery of other queued events.
//
// Delivery Guarantees (read this before assuming more than it says):
//
//   - Async mode is at-least-once for any event that was successfully
//     enqueued: a retried handler may run more than once if a retry races
//     a late-failing success. An event discarded by OverflowDrop, or
//     rejected by OverflowReject, never entered the delivery path at all
//     and is outside this guarantee — it was never delivered, period.
//   - Sync mode is at-most-once per subscriber per Publish call: each
//     subscriber's handler runs exactly once per Publish, with no retry.
//   - There is no cross-process deduplication, ever, in either mode.
//     grevents has no concept of another process.
//   - A panic in a subscriber's handler, user-supplied middleware, a
//     Logger, or a DeadLetterSink is always recovered — converted into an
//     error where one is expected (handler/middleware), or swallowed
//     after a best-effort log attempt otherwise (Logger/DeadLetterSink,
//     since there is no error return to convert it into). None of
//     grevents' user-pluggable extension points can crash the bus or the
//     host process. This is not optional and cannot be disabled.
//
// Topics:
//
// Subscribe matches topics by exact string equality only. There is no
// wildcard or glob matching in v1 (e.g. "role.*" does not match
// "role.assigned").
//
// Middleware:
//
// Use appends a Middleware to the chain applied to every handler
// invocation, in the order added. Middleware is snapshotted at delivery
// time, not at Subscribe time: a Use call taking effect concurrently with
// an in-flight Publish has undefined ordering relative to that specific
// in-flight call, but is guaranteed to apply to every delivery that reads
// the chain after it was added.
//
// Dead Letters:
//
// In async mode, an event that exhausts its configured retry attempts for
// a given subscriber is handed to a DeadLetterSink instead of being
// silently dropped. The default sink (NewMemoryDeadLetterSink) is a
// capacity-bounded, in-memory ring buffer: a best-effort recent-history
// buffer, not a durable audit log. Entries are lost on process restart,
// and once the buffer is at capacity, each new dead-lettered event
// silently overwrites the oldest entry.
//
// Shutdown:
//
// Close stops accepting new Publish and Subscribe calls immediately
// (both return ErrClosed thereafter) and, for an async bus, drains the
// queue up to the configured WithDrainTimeout. If draining does not
// finish within that timeout, Close stops waiting and returns an error
// wrapping ErrDrainTimeout — it does not and cannot forcibly terminate
// goroutines still in flight; Go has no such primitive. Stats().DroppedOnClose
// reports how many events were not accounted for when Close returned:
// either genuinely undelivered work left behind by a timeout, or (rarely,
// even on a clean drain) an event that raced into the queue between a
// concurrent Publish call's closed-check and Close's own CompareAndSwap.
// DroppedOnClose remains readable after Close returns (Stats is the one
// method that does not return ErrClosed post-close, since it is the
// mechanism for inspecting drain shortfall). Close is idempotent: calling
// it more than once is safe and a no-op after the first call.
package grevents
