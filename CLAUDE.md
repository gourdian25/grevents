# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

grevents is a lightweight, pluggable, **in-process** event bus for the gourdian ecosystem — decouples producers of state changes (e.g. `grauth` assigning a role) from consumers that react to them (e.g. `graudit`, `grcache` tag invalidation), without the producer knowing consumers exist. It is explicitly **not** a Kafka/NATS/RabbitMQ replacement: it runs entirely within a single process's memory, two replicas never see each other's events, and a process restart loses any queued or dead-lettered events — see `docs.go` for the full statement of scope and delivery guarantees before assuming more than it promises.

It is a library — there is nothing to build or run except lint and test. The whole implementation is a flat `package grevents` at the repo root; there are no subpackages. `example/example.go` (`package main`) is a standalone runnable demo, not part of the test surface. `docs/plan/grevents-plan.md` is the original design/research document that predates the code; the implementation and this file are now authoritative — treat the plan doc as historical context, not a spec to re-derive behavior from.

## Commands

```sh
make test               # go test -cover ./...
make race                # go test -race ./...  (mandatory before any commit touching delivery code)
make bench                # go test -bench=. -benchmem -run=^$ ./...
make lint / lint-fix       # golangci-lint run [--fix] ./...
make coverage-summary       # per-function coverage
make coverage-check           # root package must meet a 95% threshold (COVERAGE_MIN in the Makefile)
make ci                          # golangci-lint + test
```

Run a single test with `go test -run TestName ./...` (add `-race` for anything touching delivery/shutdown — see `race_test.go`, which is meaningless without `-race`).

Releases: `make release VERSION=vX.Y.Z` (tags, pushes, runs GoReleaser). VERSION is required.

## Architecture

Everything is in package `grevents`, structured around one interface and its supporting concerns, each in its own file:

- **`Bus`** (`bus.go`) — the entry point: `Publish`, `Subscribe`, `Use`, `Stats`, `Close`. `NewBus(opts ...BusOption)` constructs the only implementation, `eventBus`. Delivery mode (sync vs async) is fixed at construction time, not chosen per `Publish` call.
- **`options.go`** — `BusOption`/`busConfig` (functional options: `WithSync`, `WithAsync`, `WithRetry`, `WithDeadLetterSink`, `WithLogger`, `WithWorkerCount`, `WithDrainTimeout`) and `OverflowStrategy` (`OverflowBlock`, `OverflowDrop`, `OverflowReject` — no drop-oldest, no inline-sync-fallback; see the type's doc comment for why not).
- **`sync.go`** / **`async.go`** — the two delivery paths. Sync (`publishSync`) invokes every subscriber through the middleware chain and blocks until all return, aggregating failures with `errors.Join`; no retry, ever. Async (`publishAsync`) enqueues onto a buffered channel per the configured `OverflowStrategy`; a pool of worker goroutines (`asyncWorker`) dequeues and fans each event out to **one independent goroutine per subscriber** (`dispatchToSubscribers`) — this fan-out, not worker count, is what guarantees one slow/failing subscriber's retries never block delivery to another subscriber or to the next queued event. `deliverWithRetry` owns one `(event, subscriber)` pair's full retry-with-backoff lifecycle before handing off to the `DeadLetterSink` on exhaustion.
- **`retry.go`** — `computeBackoff`: Full Jitter exponential backoff (`random(0, min(cap, base*2^attempt))`), capped by `defaultMaxBackoff` (30s) unless the cap would overflow `time.Duration`, guarded explicitly.
- **`deadletter.go`** — `DeadLetterSink` interface (`Record`, `List`, `Close`) and the default `memoryDeadLetterSink`: a capacity-bounded ring buffer, best-effort recent history, not a durable log.
- **`subscriber.go`** — `registry`: the topic → subscriptions index. `snapshot` returns a copy so delivery never holds the registry lock while invoking handlers.
- **`middleware_recovery.go`** — `invokeHandler` is the single funnel every delivery path calls through; panic recovery is always-on (not a `Middleware`, not configurable) and extends to `Logger`/`DeadLetterSink` panics too (`safeLogErrorf`, `recordDeadLetter`) — none of grevents' user-pluggable extension points can crash the bus or host process.
- **`middleware_logging.go`** / **`middleware_tracing.go`** — optional, opt-in middleware (`Use(...)`); `TracingMiddleware` is a pure-passthrough stub extension point for a future real tracing integration.
- **`logger.go`** — `Logger` is a minimal structural interface (`Infof`/`Warnf`/`Errorf`), satisfied by `*grlog.Logger` without grevents importing grlog (grlog is a test-only dependency of this module — see `logger_test.go`).
- **`errors.go`** — sentinel errors for `errors.Is` (`ErrClosed`, `ErrQueueFull`, `ErrInvalidConfig`, `ErrDrainTimeout`, `ErrNoSubscribers`). No `IsX(err) bool` helpers, by convention.
- **`stats.go`** — lock-free `atomic`-backed counters backing `Bus.Stats`.

### Delivery guarantees (precise, not aspirational)

Async is at-least-once for anything successfully enqueued (a retry may race a late-failing success, so a handler can run more than once); an event discarded by `OverflowDrop` or rejected by `OverflowReject` never entered the delivery path and is outside this guarantee. Sync is at-most-once per subscriber per `Publish` call, no retry. There is no cross-process dedup, ever, in either mode. See `docs.go` for the full text — match this precision in any new doc comments rather than reaching for words like "guaranteed" or "atomic" that the code doesn't actually enforce.

### Shutdown

`Close()` is idempotent (`atomic.Bool` CompareAndSwap), stops accepting new `Publish`/`Subscribe` calls immediately, and for an async bus drains the queue up to `WithDrainTimeout` before force-stopping. `Stats().DroppedOnClose` remains readable after `Close` returns (the one method that doesn't itself return `ErrClosed` post-close) — it reports both genuine drain-timeout shortfall and the rare race-window sweep of an event that was enqueued between a concurrent `Publish` call's closed-check and `Close`'s own CAS. `Close` always releases a configured `DeadLetterSink` (via its `Close` method, panic-safe) regardless of delivery mode, even though a sync-only bus never delivers anything through it.

## Testing conventions

- `contract_bus_test.go` (`package grevents_test`) is the primary test artifact — a shared behavioral suite (`runBusContract`, run via `TestBus_Contract` against `grevents.NewBus`) covering sync delivery, async delivery with retry/dead-lettering, all three overflow strategies under genuine concurrent load, panic recovery, middleware ordering, and close/drain timing. It was originally a separate, publicly-importable `conformance` package (so a hypothetical future `Bus` implementation could reuse it) but has been folded into the root package's own tests for consistency with the rest of the gourdian ecosystem — `runBusContract` still takes a `newBus` constructor function, so a future backend could still drive it by copying the pattern, just not by importing it directly.
- `internal_coverage_test.go` (`package grevents`, white-box) constructs `eventBus`/`subscription` directly — bypassing `NewBus` — to reach a handful of branches unreachable from the public API alone: either an earlier validation already forecloses the bad input (e.g. `busConfig.validate` rejects an invalid `OverflowStrategy` before an `eventBus` is ever built), or the branch reacts to internal state no production code path produces (e.g. a closed `queue` channel — only `closeChan` is ever closed by real code; a negative `inFlight` counter).
- `retry_test.go` (`package grevents`) unit-tests `computeBackoff` directly.
- `race_test.go` hammers sync and async buses concurrently; meaningless without `-race` — the race detector, not the assertions, is what it actually checks.
- Other `*_test.go` files (`bench_test.go`, `deadletter_test.go`, `logger_test.go`, `middleware_test.go`, `options_test.go`) are `package grevents_test`, organized by concern rather than mirroring source files 1:1.
- Coverage is measured on `.` (root package) via `make coverage-check`, not `./...` — `example/` is a runnable demo with no tests of its own. `noopLogger`'s three single-line no-op methods (`logger.go`) permanently report 0.0% individually in `go tool cover -func` output regardless of being exercised — a Go tooling artifact for completely empty-bodied methods (they contribute 0 total statements, so the aggregate percentage `coverage-check` actually gates on is unaffected).
