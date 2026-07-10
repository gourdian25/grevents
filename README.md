# 🎉 grevents - Lightweight In-Process Event Bus for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/gourdian25/grevents.svg)](https://pkg.go.dev/github.com/gourdian25/grevents)
[![Go Version](https://img.shields.io/badge/go-1.26.4+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A lightweight, pluggable, **in-process** event bus for the gourdian ecosystem. It decouples producers of state changes (e.g. `grauth` assigning a role) from consumers that react to them (`graudit` recording it, `grcache` invalidating a related tag, a future notification system emailing someone) — without the producer needing to know any consumer exists.

grevents is explicitly **not** an attempt to replace Kafka, NATS, or RabbitMQ. It runs entirely within a single process's memory — see [Delivery Guarantees](#-delivery-guarantees) before assuming more than that.

## 🌐 Part of the gourdian25 ecosystem

grevents is one of several small, independent Go libraries meant to be used
together:

- [gourdiantoken](https://github.com/gourdian25/gourdiantoken) — JWT
  access/refresh token issuance, verification, revocation, and rotation.
- [grlog](https://github.com/gourdian25/grlog) — zero-dependency structured
  logging; grevents' optional `Logger` interface is satisfied by it directly.
- [grcache](https://github.com/gourdian25/grcache) — backend-agnostic
  caching abstraction; a consumer of grevents for tag invalidation.
- [graudit](https://github.com/gourdian25/graudit) — an append-only,
  tamper-evident audit log; publishes an `"audit.recorded"` event through
  grevents on every successful write.
- [grpolicy](https://github.com/gourdian25/grpolicy) — attribute-based
  policy evaluation (RBAC/ABAC), independent of any notion of "user" or
  "role".

## 🌟 Why grevents?

- 🔌 **Pluggable delivery** — synchronous (blocks until every subscriber has run) or asynchronous (a worker pool delivers in the background), chosen once at construction
- 🔁 **Retry with backoff** — Full Jitter exponential backoff for async subscribers, independently per subscriber, so one flaky consumer never blocks another
- 💀 **Dead-letter handling** — an event that exhausts its retries lands in an inspectable `DeadLetterSink` instead of vanishing
- 🛡️ **Panic-safe by default** — a panic in a handler, middleware, `Logger`, or `DeadLetterSink` is always recovered; none of grevents' user-pluggable extension points can crash the bus or the host process, and this is not optional
- 🧵 **Honest shutdown** — `Close` drains within a configurable timeout and tells you exactly how many events it couldn't finish, rather than pretending everything always completes
- 📊 **Built-in stats** — published/delivered/failed/dead-lettered counters and queue depth via `Stats()`
- 🪵 **grlog interoperability** — `*grlog.Logger` satisfies grevents' `Logger` interface with zero adapter code

## 📚 Table of Contents

- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Core Concepts](#-core-concepts)
- [Delivery Modes](#-delivery-modes)
- [Overflow Strategies](#-overflow-strategies)
- [Retry and Dead Letters](#-retry-and-dead-letters)
- [Middleware](#-middleware)
- [Delivery Guarantees](#-delivery-guarantees)
- [Testing](#-testing)
- [Contributing](#-contributing)

## 📦 Installation

```bash
go get github.com/gourdian25/grevents
```

Requires Go 1.26.4+ (ecosystem-aligned minimum; only 1.24+ is functionally needed).

## 🚀 Quick Start

```go
package main

import (
	"context"
	"log"

	"github.com/gourdian25/grevents"
)

func main() {
	bus, err := grevents.NewBus() // synchronous delivery by default
	if err != nil {
		log.Fatal(err)
	}
	defer bus.Close()

	unsubscribe, err := bus.Subscribe("role.assigned", func(ctx context.Context, event grevents.Event) error {
		log.Printf("role assigned: %v", event.Payload)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	defer unsubscribe()

	err = bus.Publish(context.Background(), grevents.Event{
		Topic:   "role.assigned",
		Payload: "user:42 -> admin",
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

See [`example/example.go`](example/example.go) for a complete runnable walkthrough of every feature, including `grlog` integration — run it with `go run ./example`.

## 🧩 Core Concepts

- **`Bus`** — the entry point: `Publish`, `Subscribe`, `Use`, `Stats`, `Close`.
- **`Event`** — `Topic` (matched by exact string equality — no wildcards in v1), `Payload` (opaque `any`), `Timestamp` (defaulted to now), `Metadata` (a `map[string]string` middleware can read/write).
- **`HandlerFunc`** — `func(ctx context.Context, event Event) error`, what a subscriber supplies to `Subscribe`.
- **`Middleware`** — `func(next HandlerFunc) HandlerFunc`, applied via `Use` in the order added; panic recovery wraps the *entire* chain unconditionally, so middleware never needs to recover its own panics.

## 🔀 Delivery Modes

Delivery mode is fixed at construction time, not chosen per `Publish` call.

**Sync (`WithSync`, the default):**

```go
bus, _ := grevents.NewBus(grevents.WithSync())
```

`Publish` invokes every subscriber's handler and blocks until all have returned. Failures from multiple subscribers are aggregated with `errors.Join` — inspect with `errors.Is`/`errors.As`. There is no retry in sync mode: retrying would mean `Publish` blocks the caller for the full backoff duration, a footgun for anything called from a request path.

**Async (`WithAsync`):**

```go
bus, _ := grevents.NewBus(
	grevents.WithAsync(256, grevents.OverflowBlock),
	grevents.WithWorkerCount(4),
)
```

`Publish` enqueues and returns immediately (unless the queue is full — see below). A worker pool dequeues events and fans each one out to an independent goroutine per subscriber, so one slow or failing subscriber's retries never block delivery to another subscriber of the same event, nor delivery of the next queued event. `WithWorkerCount` controls dequeue parallelism only, not per-subscriber delivery concurrency.

## 🚦 Overflow Strategies

Chosen via the second argument to `WithAsync`:

| Strategy | Behavior when the queue is full |
|---|---|
| `OverflowBlock` | Blocks `Publish`'s caller until space frees, the bus closes (`ErrClosed`), or `ctx` is done (`ctx.Err()`) |
| `OverflowDrop` | Never blocks; silently discards the incoming event, `Publish` returns `nil` |
| `OverflowReject` | Never blocks; `Publish` returns `ErrQueueFull` immediately |

There is no drop-oldest strategy — evicting a live buffered channel's existing head isn't something Go's channel primitive supports without a different (more complex) queue structure, and no sibling library in this ecosystem has needed it.

## 🔁 Retry and Dead Letters

```go
dlq := grevents.NewMemoryDeadLetterSink(1000)
bus, _ := grevents.NewBus(
	grevents.WithAsync(256, grevents.OverflowBlock),
	grevents.WithRetry(5, 100*time.Millisecond),
	grevents.WithDeadLetterSink(dlq),
)
```

`WithRetry(maxAttempts, baseBackoff)` retries a failed async handler with Full Jitter exponential backoff (`sleep = random(0, min(cap, base*2^attempt))`), capped at a fixed internal ceiling. Once `maxAttempts` is exhausted, the `(event, subscriber)` pair is handed to the configured `DeadLetterSink` — the default in-memory implementation is a capacity-bounded ring buffer: a best-effort recent-history buffer, explicitly **not** a durable audit log.

```go
entries, _ := dlq.List(context.Background(), 0) // 0 = all entries, oldest first
for _, e := range entries {
	fmt.Printf("topic=%s attempts=%d lastErr=%s\n", e.Event.Topic, e.Attempts, e.LastError)
}
```

## 🧵 Middleware

```go
bus.Use(grevents.LoggingMiddleware(logger)) // logs each delivery attempt's outcome
bus.Use(grevents.TracingMiddleware())       // stub extension point for future tracing
bus.Use(func(next grevents.HandlerFunc) grevents.HandlerFunc {
	return func(ctx context.Context, event grevents.Event) error {
		// your own cross-cutting logic
		return next(ctx, event)
	}
})
```

Middleware is snapshotted at *delivery* time, not `Subscribe` time — a `Use` call taking effect concurrently with an in-flight `Publish` has undefined ordering relative to that specific call, but is guaranteed to apply to every delivery that reads the chain afterward.

### grlog interoperability

```go
import "github.com/gourdian25/grlog"

logger := grlog.NewDefaultLogger()
bus, _ := grevents.NewBus(grevents.WithLogger(logger))
```

`*grlog.Logger`'s `Infof`/`Warnf`/`Errorf` methods satisfy grevents' `Logger` interface structurally — grevents itself never imports grlog (it's a test-only dependency of this module; see `logger_test.go`).

## ⚖️ Delivery Guarantees

Stated precisely rather than aspirationally — see `docs.go` for the full text:

- **Async mode is at-least-once** for any event that was successfully enqueued. A retried handler may run more than once if a retry races a late-failing success. An event discarded by `OverflowDrop` or rejected by `OverflowReject` never entered the delivery path and is outside this guarantee.
- **Sync mode is at-most-once** per subscriber per `Publish` call — no retry, ever.
- **No cross-process deduplication, ever, in either mode.** grevents has no concept of another process; two replicas using grevents do not see each other's events, and a restart loses any queued or dead-lettered events.

## 🧪 Testing

```bash
make test           # go test -cover ./...
make race            # go test -race ./...  (mandatory before any commit touching delivery code)
make bench           # sync vs async throughput, middleware chain cost
make coverage-check  # root package must meet an 80% coverage threshold
```

The [`conformance`](conformance/) package is the primary test artifact — a shared behavioral test suite covering sync delivery, async delivery with retry and dead-lettering, all three overflow strategies under genuine concurrent load, panic recovery, middleware ordering, and `Close` draining both within and past its timeout.

## 🤝 Contributing

Issues and PRs are welcome at [github.com/gourdian25/grevents](https://github.com/gourdian25/grevents). Please run `make ci` (lint + test) before submitting.

## 📄 License

MIT — see [LICENSE](LICENSE).
