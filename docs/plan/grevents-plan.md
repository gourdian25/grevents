# grevents — Detailed Scope & Implementation Planning Document

**Repo path (to be created):** `~/Dev/gourdian25/grevents`
**Reference repos already in workspace:** `~/Dev/gourdian25/gourdiantoken`,
`~/Dev/gourdian25/grlog`, `~/Dev/gourdian25/grcache`

---

## Instructions for the IDE agent

Before writing any code:

1. **Read `~/Dev/gourdian25/grlog` in full**, with specific focus on:
   - Its async logging path — the queue, worker pool, and **overflow
     strategies** (what happens when the queue is full: block, drop-oldest,
     drop-newest, etc.). This is the closest existing prior art in the
     ecosystem for "async processing with backpressure," and grevents' async
     delivery path should reuse the same shape of solution, not invent a new
     one.
   - Its race-safe shutdown pattern — how does grlog guarantee all queued log
     entries are flushed (or intentionally dropped) before `Close()` returns?
     grevents needs the same guarantee for in-flight async events.
   - How `Logger` is exposed as a small structural interface for optional
     injection (confirm this is the same pattern grcache adopted for its own
     optional `Logger` field/option).

2. **Read `~/Dev/gourdian25/grcache` in full**, with specific focus on:
   - The `Close()` idempotency pattern (`sync.Once` — confirmed present on
     every grcache backend per the recent audit). grevents' `Bus.Close()`
     must follow the identical pattern.
   - The sentinel-error style (`errors.Is`-compatible, translated from any
     lower-level errors, no `IsX(err) bool` helper functions).
   - The optional-logger injection pattern (structural interface, nil-safe,
     no hard dependency on grlog from the root package).
   - The conformance-test-suite pattern (one shared behavioral test suite
     run against every implementation) — grevents should follow this same
     testing philosophy even though, unlike grcache, it will likely start
     with a single in-process implementation rather than multiple backends.
   - The recent audit findings on grcache (the `Pipeline()` vs `TxPipeline()`
     atomicity bug, and the documentation-overclaim pattern). grevents should
     be audited the same way once built: check that any doc comment claiming
     "guaranteed," "atomic," "at-least-once," or "at-most-once" delivery is
     actually backed by code, not aspirational.

3. **Read `~/Dev/gourdian25/gourdiantoken`** for:
   - Sentinel error naming/wrapping conventions (for consistency across all
     three now-existing repos plus this new one).
   - Background-cleanup-goroutine pattern (relevant to DLQ retention/sweeping,
     if the in-memory DLQ implementation needs to expire old entries).

4. **Only after all three reads**, produce an implementation plan that states:
   - Exactly which piece of grlog's async/overflow code will be reused or
     closely mirrored, and where grevents' requirements diverge (event
     delivery has retry/backoff semantics that log writes don't — call out
     specifically what's new).
   - The folder structure, matching conventions across all three sibling
     repos.
   - Any place where grevents' delivery-guarantee documentation needs to be
     conservative rather than aspirational, learning directly from the
     grcache Pipeline/TxPipeline lesson — i.e., don't write "guaranteed
     delivery" anywhere unless the code actually enforces it end to end.

Do not begin implementation until this plan has been reviewed.

---

## 1. Vision

grevents is a lightweight, pluggable, **in-process** event bus for the
gourdian ecosystem. It exists to decouple producers of state changes (e.g.
`grauth` assigning a role) from consumers that react to them (e.g. `graudit`
recording it, `grcache` invalidating a related tag, a future notification
system emailing someone) — without the producer needing to know any
consumer exists.

It is explicitly **not** an attempt to replace Kafka, NATS, or RabbitMQ. If
a consumer eventually needs durable, cross-process, at-least-once delivery
at scale, that is a separate adapter package (e.g. a hypothetical
`grevents-nats` bridge) built later — not a reason to grow this package's
core scope now.

---

## 2. Scope

### 2.1 In scope (v1)

- Core `Bus` interface: `Publish`, `Subscribe`, `Close`.
- **Synchronous delivery**: `Publish` blocks until every subscriber's handler
  for that topic has run (or failed/timed out), and returns an aggregated
  error if any handler failed.
- **Asynchronous delivery**: `Publish` enqueues the event and returns
  immediately; a worker pool (modeled directly on grlog's async logging
  worker pool) delivers to subscribers in the background.
- **Retry with backoff**: a failed async handler invocation is retried a
  configurable number of times with exponential backoff (with jitter) before
  being considered permanently failed.
- **Dead-letter handling**: an event that exhausts its retries is handed to a
  `DeadLetterSink` (default: in-memory, bounded, inspectable) instead of
  being silently dropped.
- **Middleware chain**: a chain of `Middleware` functions wraps every handler
  invocation — for cross-cutting concerns like logging each delivery
  attempt, tracing/span injection, and panic recovery (a subscriber handler
  panicking must not crash the publisher or the whole bus).
- **Topic-exact-match subscription** — `Subscribe("role.assigned", handler)`
  matches only that exact topic string. No wildcard/glob matching in v1 (see
  Roadmap).
- **Backpressure/overflow strategy** for the async queue, directly reusing
  grlog's existing strategy set (block / drop-oldest / drop-newest — confirm
  exact names and semantics from grlog's actual code, don't guess).
- Basic stats: published count, delivered count, failed count, dead-lettered
  count, current queue depth — exposed via a `Stats()` snapshot method,
  matching grcache's `Stats()` precedent.

### 2.2 Explicitly out of scope (v1)

- **Cross-process / distributed delivery.** grevents runs entirely within a
  single process's memory. If two replicas of `gourdianerp` both use
  grevents, they do not see each other's events. This is not a bug to fix in
  v1 — it's the defined boundary of what this package is.
- **Durability across restarts.** Events (and the DLQ) live in memory only.
  A process restart loses any queued or dead-lettered events. If a consumer
  needs durability, that's what Kafka/NATS/RabbitMQ (or a future adapter) is
  for.
- **Schema registry / event versioning / schema evolution.** An `Event`'s
  payload is an opaque `any`/`[]byte` as far as grevents is concerned — no
  schema validation, no versioned-payload migration tooling. Explicitly
  punted to v2 if it comes up.
- **Wildcard/pattern topic subscriptions** (e.g. `role.*`). Exact-match only
  in v1 — see Roadmap.
- **Guaranteed exactly-once delivery.** v1 targets **at-least-once for async
  delivery** (a handler may be invoked more than once if a retry occurs after
  a handler partially succeeded but returned an error late) and
  **at-most-once for sync delivery inside a single `Publish` call** (each
  subscriber's handler runs exactly once per `Publish`, but there is no
  cross-process dedup). This must be stated plainly in the docs, not
  glossed over — this is exactly the kind of claim the grcache audit taught
  us to be precise about.

---

## 3. Public API

### 3.1 Core interface

```go
package grevents

import (
	"context"
	"time"
)

// Bus is the primary interface for publishing and subscribing to events.
type Bus interface {
	// Publish delivers event to every subscriber of event.Topic.
	// Delivery mode (sync vs async) is determined by how the Bus was
	// constructed (see BusOption), not per-call.
	//
	// Sync mode: blocks until all subscriber handlers have returned (or
	// been retried to exhaustion), returns an aggregated error if any
	// handler ultimately failed.
	//
	// Async mode: enqueues the event and returns nil immediately unless the
	// queue itself is full and the configured overflow strategy is
	// "block" (in which case Publish blocks on enqueue, not on delivery)
	// or "reject" (in which case Publish returns ErrQueueFull immediately).
	Publish(ctx context.Context, event Event) error

	// Subscribe registers handler for topic. Returns an Unsubscribe func.
	// Multiple subscribers on the same topic are all invoked; order across
	// subscribers on the same topic is not guaranteed.
	Subscribe(topic string, handler HandlerFunc, opts ...SubscribeOption) (Unsubscribe, error)

	// Use appends middleware to the chain applied to every handler
	// invocation, in the order added. Must be called before any Publish
	// that should be affected by it — adding middleware concurrently with
	// in-flight publishes has undefined ordering (documented, not silently
	// assumed safe).
	Use(mw Middleware)

	// Stats returns a snapshot of delivery counters and current queue depth.
	Stats(ctx context.Context) (Stats, error)

	// Close stops accepting new Publish calls, drains the async queue up to
	// the configured drain timeout, then releases resources. Idempotent —
	// safe to call more than once (sync.Once-guarded, matching grcache's
	// Close() convention).
	Close() error
}

type Event struct {
	Topic     string
	Payload   any
	Timestamp time.Time
	// Metadata carries optional cross-cutting data (trace ID, actor ID,
	// tenant ID) that middleware can read/write without needing to know
	// about the Payload's concrete type.
	Metadata  map[string]string
}

type HandlerFunc func(ctx context.Context, event Event) error

type Unsubscribe func()

type Middleware func(next HandlerFunc) HandlerFunc

type Stats struct {
	Published    uint64
	Delivered    uint64
	Failed       uint64
	DeadLettered uint64
	QueueDepth   int64 // -1 for sync-only buses (no queue exists)
}
```

### 3.2 Sentinel errors

```go
package grevents

import "errors"

var (
	ErrQueueFull   = errors.New("grevents: async queue is full")
	ErrClosed      = errors.New("grevents: bus is closed")
	ErrNoSubscribers = errors.New("grevents: no subscribers for topic") // see §3.4 — may be downgraded to non-error
)
```
*(Agent: confirm against grcache's/gourdiantoken's actual sentinel style —
should these be `fmt.Errorf`-wrapped anywhere, and does the ecosystem use a
common `errors.Join` pattern for aggregating multiple handler failures in
sync mode? Check before implementing the aggregation logic in §3.1's
`Publish` doc comment.)*

### 3.3 Constructor and options

```go
func NewBus(opts ...BusOption) (Bus, error)

func WithAsync(queueSize int, overflow OverflowStrategy) BusOption
func WithSync() BusOption // default if no delivery-mode option given
func WithRetry(maxAttempts int, baseBackoff time.Duration) BusOption
func WithDeadLetterSink(sink DeadLetterSink) BusOption // default: in-memory bounded sink
func WithLogger(logger Logger) BusOption // structural interface, same shape as grcache's
func WithWorkerCount(n int) BusOption // async worker pool size

type OverflowStrategy int

const (
	OverflowBlock OverflowStrategy = iota
	OverflowDropOldest
	OverflowDropNewest
	OverflowReject
)
```

**Agent decision required:** confirm the exact overflow-strategy names and
semantics grlog already uses internally, and reuse those names verbatim
here (renaming during the port would create needless cross-repo
inconsistency for something conceptually identical).

### 3.4 Open design question: no-subscriber behavior

Is publishing to a topic with zero subscribers an error (`ErrNoSubscribers`),
or a silent no-op? Recommendation: **silent no-op, no error** — a producer
publishing "role.assigned" shouldn't have to know or care whether anything
is listening yet (this is the whole point of decoupling). Keep
`ErrNoSubscribers` defined but only used internally for `Stats`/debug
logging purposes, not returned from `Publish`. Agent should confirm this
reasoning holds once real usage patterns from `grauth`/`graudit` are known,
but default to no-op for v1.

### 3.5 Dead-letter sink interface

```go
type DeadLetterSink interface {
	Record(ctx context.Context, event Event, lastErr error, attempts int) error
	List(ctx context.Context, limit int) ([]DeadLetterEntry, error)
	Close() error
}

type DeadLetterEntry struct {
	Event     Event
	LastError string
	Attempts  int
	DeadAt    time.Time
}
```

Default implementation: bounded in-memory ring buffer (oldest entries
dropped once capacity is reached — document this capacity limit clearly,
this is exactly the kind of "guaranteed" language trap to avoid: it's a
best-effort recent-history buffer, not a durable audit log). A future
`graudit`-backed `DeadLetterSink` implementation is a natural integration
point once `graudit` exists — do not build that now, just make sure the
interface doesn't preclude it.

---

## 4. Architecture

```
grevents (root)
├── bus.go              // Bus interface, Event, HandlerFunc, Middleware, sentinel errors
├── options.go           // BusOption, all With* constructors
├── async.go              // async delivery path: queue + worker pool (ported from grlog's async logging pattern)
├── sync.go                // sync delivery path
├── retry.go                 // backoff + retry logic
├── deadletter/
│   └── memory.go              // default in-memory DeadLetterSink
├── middleware/
│   ├── logging.go                // optional logging middleware, satisfies grcache-style Logger interface
│   ├── recovery.go                 // panic-recovery middleware — should be applied by default, not opt-in
│   └── tracing.go                    // stub/extension point, no real tracing dependency in v1
└── conformance/
    └── conformance.go                  // shared behavioral test suite, same philosophy as grcache's
```

**Note on `middleware/recovery.go`:** unlike the other middleware, panic
recovery should likely be baked in as a non-optional default (every handler
invocation wrapped in a `recover()`), since a single misbehaving subscriber
panicking and taking down the entire bus (and potentially the host process,
if unrecovered) is a correctness issue, not a nice-to-have. Agent to confirm
this reasoning and implement recovery as always-on rather than
middleware-optional.

---

## 5. Delivery semantics — worked through in detail

### 5.1 Sync mode

`Publish` iterates subscribers for the topic, invokes each handler through
the middleware chain, collects any errors (`errors.Join`), and returns the
joined error (or nil) once all have run. No retry in sync mode by default —
retrying synchronously would mean the caller's `Publish` call blocks for
the full backoff duration, which is a footgun. If sync + retry is genuinely
wanted, it should be an explicit, separately-named opt-in
(`WithSyncRetry(...)`) so no one accidentally makes their request handler
hang for 10 seconds because a downstream subscriber is retrying with
backoff. Flag this as a design decision for the agent to validate, not
assume.

### 5.2 Async mode

`Publish` pushes the event onto an internal channel/queue (sized per
`WithAsync(queueSize, ...)`). A pool of `n` worker goroutines
(`WithWorkerCount`) pulls events and delivers to all subscribers of that
topic, applying middleware and retry-with-backoff per subscriber
independently (one slow/failing subscriber's retries should not block
delivery to other subscribers of the same event).

### 5.3 Retry + backoff

Exponential backoff with jitter, capped at a max backoff, matching whatever
pattern (if any) already exists in gourdiantoken for retry logic (e.g. does
its Redis backend retry connection attempts with backoff already? Reuse
that helper if so, rather than writing a second implementation of the same
algorithm in this ecosystem).

### 5.4 Shutdown / `Close()`

Mirrors grlog's race-safe shutdown: stop accepting new `Publish` calls
immediately, then drain the async queue up to a configurable timeout
(`WithDrainTimeout`, default e.g. 5s), delivering whatever it can within
that window, then force-stop and report (via `Stats` or a returned error
from `Close`) how many events were dropped undelivered. This must be
tested explicitly — a repeat of the exact kind of thing the grcache audit
caught (docs claiming a guarantee the code doesn't enforce) is very easy to
introduce here if `Close()`'s draining behavior isn't verified under load.

---

## 6. Testing & benchmarking strategy

- Conformance suite covering: sync delivery to N subscribers, async
  delivery + eventual consistency (poll until delivered, with timeout),
  retry-then-succeed, retry-exhaustion → dead-letter, overflow strategy
  behavior under a full queue (one test per strategy:
  block/drop-oldest/drop-newest/reject), panic-in-handler does not crash the
  bus, `Close()` drains within timeout and reports correct counts,
  `Close()` idempotency.
- Race detector mandatory (`go test -race`) — this package is nothing but
  concurrency, more so than grcache's memory backend.
- Benchmarks: publish throughput (sync vs async) at varying subscriber
  counts (1, 10, 100 subscribers per topic), and specifically the cost of
  the middleware chain at varying chain lengths (1, 5, 10 middlewares) since
  that's the most likely hidden overhead source, analogous to grcache's
  `InvalidateTag` cardinality benchmarks.

---

## 7. Dependencies

- `grlog` — **optional**, structural `Logger` interface only (same pattern
  as grcache), not a hard import. grevents' root package must not require
  grlog to build.
- `grconfig` — not yet built; grevents' constructor accepts
  `BusOption`s directly for now (same reasoning as grcache: don't block on
  grconfig's existence).
- No dependency on grcache or gourdiantoken. (grcache may later *consume*
  grevents for distributed-invalidation pub/sub per its own roadmap, but
  that's a one-directional future dependency, not something grevents needs
  to know about now.)

---

## 8. Roadmap / explicitly deferred items

- Wildcard/pattern topic subscriptions (`role.*`).
- Schema registry / event payload versioning.
- Cross-process adapters (NATS/Kafka/RabbitMQ bridges) as separate modules.
- `graudit`-backed `DeadLetterSink` implementation, once `graudit` exists.
- Real OpenTelemetry tracing middleware (v1 ships only a stub extension
  point in `middleware/tracing.go`).
- Fault-injection testing for mid-delivery crash scenarios (the same kind of
  gap the grcache audit flagged for Redis pipelining — worth tracking from
  day one here rather than discovering it later).

---

## 9. Evaluation questions for the agent (answer before implementing)

1. What exact overflow-strategy names and semantics does grlog's async
   logging path use? Confirm grevents reuses them verbatim.
2. Does grlog's worker-pool implementation use a buffered channel, a
   custom ring buffer, or something else? Should grevents' async queue
   directly reuse that same underlying data structure/code, or does the
   retry-per-subscriber requirement mean it needs a meaningfully different
   shape?
3. Does gourdiantoken already implement exponential-backoff-with-jitter
   anywhere (e.g. for Redis reconnection)? If so, extract/mirror it instead
   of writing a second implementation.
4. Does grcache's sentinel-error style ever use `errors.Join` for
   aggregating multiple failures from one operation? If not, is there
   ecosystem precedent to follow, or is this the first repo to need it —
   and if so, what convention should be established now for future repos to
   follow?
5. Given grlog's and grcache's `Close()` patterns (both `sync.Once`-guarded),
   confirm grevents' `Close()` follows the identical guard, and specifically
   verify the drain-timeout behavior is covered by a real test that
   artificially delays a handler to force the timeout path, rather than only
   testing the happy path where draining finishes early.
6. Is panic recovery in v1 implemented as always-on (non-optional) as
   recommended in §4, or does the agent find a reason from the existing
   codebases' conventions to make it opt-in instead?