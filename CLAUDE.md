# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Current state: pre-implementation

This repository contains **no Go code yet** — no `go.mod`, no source files, no Makefile. The only content is [docs/plan/grevents-plan.md](docs/plan/grevents-plan.md), a detailed scope/spec document (untracked in git as of this writing). Before writing any code here, read that plan document in full — it is long and specific, and this file only summarizes it. Do not treat the summary below as a substitute for reading the plan doc's actual "Instructions for the IDE agent" section (§0) and "Evaluation questions for the agent" section (§9), which require reading three sibling repos before implementation starts.

## Mandatory pre-implementation research

The plan explicitly requires reading these sibling repos **in full** before writing code, because grevents is meant to reuse their patterns rather than reinvent them:

- **`~/Dev/gourdian25/grlog`** — study its async logging worker pool (queue + overflow strategy: block/drop-oldest/drop-newest — confirm exact names in `grlog.go`, don't guess) and its race-safe `Close()`/drain pattern. grevents' async delivery path and shutdown should mirror this shape, not invent a new one. Also confirm the structural `Logger` interface pattern.
- **`~/Dev/gourdian25/grcache`** — study its `sync.Once`-guarded `Close()` idempotency, its sentinel-error style (`errors.Is`-compatible, no `IsX(err) bool` helpers), its optional-logger injection (structural interface, nil-safe, no hard grlog dependency), and its `conformance/` shared-test-suite pattern (`conformance.Run(t, newCache, opts...)`). Also review the `Pipeline()` vs `TxPipeline()` atomicity bug noted in grcache's own CLAUDE.md/CHANGELOG — grevents must not repeat the "docs claim a guarantee the code doesn't enforce" mistake (e.g. "at-least-once", "atomic", "guaranteed delivery" must be backed by actual code, not aspirational wording).
- **`~/Dev/gourdian25/gourdiantoken`** — study its sentinel error naming/wrapping conventions and its background-cleanup-goroutine pattern (relevant to dead-letter-sink retention/sweeping).

Only after these reads should an implementation plan be produced and reviewed — see plan doc §9 for the specific questions to answer first (exact overflow-strategy names, whether grlog's queue is a buffered channel or something else, whether gourdiantoken already has exponential-backoff-with-jitter to reuse, whether `errors.Join` has ecosystem precedent, and whether panic recovery should be always-on).

## What grevents is

A lightweight, pluggable, **in-process** event bus for the gourdian ecosystem — decouples producers of state changes (e.g. `grauth` assigning a role) from consumers that react to them (e.g. `graudit`, `grcache` tag invalidation), without the producer knowing consumers exist. It is explicitly **not** a Kafka/NATS/RabbitMQ replacement; cross-process durable delivery is out of scope for v1 and would be a separate adapter package later.

Planned public API surface (`package grevents`, root package): `Bus` interface (`Publish`, `Subscribe`, `Use`, `Stats`, `Close`), `Event`/`HandlerFunc`/`Middleware`/`Unsubscribe` types, sentinel errors (`ErrQueueFull`, `ErrClosed`, `ErrNoSubscribers`), and a `NewBus(opts ...BusOption)` constructor with functional options (`WithAsync`, `WithSync`, `WithRetry`, `WithDeadLetterSink`, `WithLogger`, `WithWorkerCount`). Full signatures are in plan doc §3.

## Planned architecture (from the plan doc — subject to revision during implementation)

```
grevents (root)
├── bus.go              // Bus interface, Event, HandlerFunc, Middleware, sentinel errors
├── options.go          // BusOption, all With* constructors
├── async.go            // async delivery: queue + worker pool (ported from grlog's async pattern)
├── sync.go              // sync delivery path
├── retry.go               // backoff + retry logic
├── deadletter/
│   └── memory.go           // default in-memory DeadLetterSink (bounded ring buffer)
├── middleware/
│   ├── logging.go            // optional logging middleware
│   ├── recovery.go             // panic-recovery — always-on by default, not opt-in
│   └── tracing.go                // stub extension point only, no real tracing dep in v1
└── conformance/
    └── conformance.go            // shared behavioral test suite (mirrors grcache's)
```

Key design decisions already made in the plan (validate, don't relitigate, unless implementation reveals a real problem):

- **Delivery mode is fixed at bus construction** (`WithSync()` vs `WithAsync(...)`), not chosen per-`Publish` call.
- **Sync mode has no retry by default** — retrying synchronously would block the caller for the full backoff duration. A separate opt-in (`WithSyncRetry`) would be needed if that's ever wanted.
- **Async mode retries per-subscriber independently** — one slow/failing subscriber must not block delivery to other subscribers of the same event.
- **No wildcard/glob topic matching in v1** — `Subscribe` is exact-string match only.
- **Delivery guarantees are precise, not aspirational**: at-least-once for async (a handler may run more than once if a retry races a late-failing success), at-most-once for sync within one `Publish` call, no cross-process dedup ever (single-process only). State this plainly in doc comments.
- **Panic recovery is always-on**, wrapping every handler invocation, and also covers `Logger`/`DeadLetterSink` panics (see `safeLogErrorf`/`recordDeadLetter` in `middleware_recovery.go`) — none of grevents' user-pluggable extension points can crash the bus or host process.
- **`Close()` is idempotent** (`sync.Once`, matching grlog/grcache/gourdiantoken), stops accepting new `Publish` calls immediately, drains the async queue up to a configurable timeout, then force-stops and reports how many events were dropped.
- **No hard dependency on grlog** — its `Logger` is consumed only as a structural interface, same pattern as grcache.

## Sibling repo conventions to match once code exists

Based on `grlog`, `grcache`, and `gourdiantoken` (all already-established gourdian-ecosystem repos):

- Module path will be `github.com/gourdian25/grevents`; single flat root package unless a genuine multi-backend need arises (grcache's subpackage-per-backend split exists because backends pull in different heavy dependencies — grevents has no such need in v1, since it ships one in-memory implementation).
- Source files use a `// File: <relative-path>` header maintained by the `bark` tool (see sibling repos' `.bark.toml`/`bark.txt`) — check whether this repo adopts the same tool before assuming it, but match the convention if so.
- Expect a `Makefile` with targets equivalent to siblings' `test`, `race` (mandatory — this package is concurrency-heavy), `bench`, `lint`, `coverage-check`, `release VERSION=vX.Y.Z`. Race detector coverage is more important here than in any sibling repo, per the plan's testing section (§6).
- `docs.go` for package-level godoc only, no logic — matches all three siblings.
- Sentinel errors: `errors.Is`-compatible, defined once, no `IsX(err) bool` helper functions (grcache's `errors.go` is the closest precedent since grevents has no storage backends to model after gourdiantoken's per-backend errors).
- Once `docs/plan/grevents-plan.md` has served its purpose, treat it as historical context (matching how grcache's CLAUDE.md treats its own `docs/plan/grcache-plan.md`) — the actual code and this file become authoritative, not the plan.
