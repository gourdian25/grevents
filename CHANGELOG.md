# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2026-07-10

Ecosystem-alignment pass ahead of `grauth`: no functional/API changes.

### Changed

- `go.mod`'s `go` directive raised from `1.24` to `1.26.4`, aligning with
  the rest of the gourdian25 ecosystem.
- README: added `grpolicy` to the ecosystem section and corrected the Go
  version badge/requirement from `1.24+` to `1.26.4+`.
- Bumped `github.com/gourdian25/grlog` to `v0.1.1`.

## [0.1.0] - 2026-07-09

Initial release: an in-process, pluggable event bus for the gourdian
ecosystem, built directly on top of proven patterns from `grlog` and
`grcache` — see `docs/plan/grevents-plan.md` for the full research and
design rationale behind each of these decisions.

### Added

- Core `Bus` interface: `Publish`, `Subscribe`, `Use`, `Stats`, `Close`
- Synchronous delivery (`WithSync`, the default): `Publish` blocks until
  every subscriber's handler has run, aggregating any failures with
  `errors.Join` — a direct port of grlog's own `Close`/`MultiSink`
  aggregation idiom, not a new convention
- Asynchronous delivery (`WithAsync`): a configurable worker pool
  (`WithWorkerCount`) dequeues events and fans each one out to an
  independent goroutine per subscriber, so one slow or failing
  subscriber's retries never block delivery to another subscriber of the
  same event or to the next queued event
- `OverflowStrategy` for a full async queue: `OverflowBlock` and
  `OverflowDrop` port grlog's `OverflowPolicy` semantics verbatim;
  `OverflowReject` is new (grlog always had a safe inline synchronous
  fallback to fall back to instead, which grevents deliberately does not
  replicate — see docs.go)
- Retry with Full Jitter exponential backoff (`WithRetry`) — the first
  backoff-with-jitter implementation in the gourdian ecosystem, since
  neither `grlog` nor `gourdiantoken` had prior art to port
- Dead-letter handling (`WithDeadLetterSink`): an event that exhausts its
  retries is handed to a `DeadLetterSink` instead of being silently
  dropped; the default (`NewMemoryDeadLetterSink`) is a capacity-bounded,
  in-memory ring buffer, explicitly documented as best-effort recent
  history rather than a durable audit log
- Always-on panic recovery: a panic anywhere in the middleware chain or
  terminal handler is recovered and converted into an error — not
  optional, not a `Middleware`, cannot be disabled
- `Middleware` chain via `Use`, plus two ready-made middlewares:
  `LoggingMiddleware` and a `TracingMiddleware` stub extension point (no
  real tracing dependency in v1)
- Bounded, honest shutdown: `Close` drains the async queue up to
  `WithDrainTimeout`, then stops waiting (it cannot forcibly terminate
  in-flight goroutines — Go has no such primitive) and reports the
  shortfall via `Stats().DroppedOnClose`
- Structural `Logger` interface (`Infof`/`Warnf`/`Errorf`), satisfied by
  `*grlog.Logger` with zero import — `grlog` is a test-only dependency of
  this module (see `logger_test.go`), never a hard dependency of any
  non-test file
- `conformance` package: a shared behavioral test suite covering sync
  delivery, async delivery + retry + dead-lettering, all three overflow
  strategies under genuine concurrent load, panic recovery, middleware
  ordering, and `Close` draining both within and past its timeout
- `example/example.go`: a standalone runnable demonstration of every
  major feature, including `grlog` interoperability

### Fixed

- Panic recovery now genuinely covers every user-pluggable extension
  point, not just `HandlerFunc`/`Middleware`: a panic in a user-supplied
  `Logger` or `DeadLetterSink` implementation is also recovered and can no
  longer crash the bus or the host process (`safeLogErrorf` and
  `recordDeadLetter` in `middleware_recovery.go`). Found via an empirical
  audit that reproduced a real process crash from a panicking
  `DeadLetterSink.Record`, in the same spirit as the grcache
  Pipeline/TxPipeline "docs claimed more than the code enforced" lesson.
  Covered by two new conformance scenarios,
  `DeadLetterSinkPanicDoesNotCrashBus` and
  `LoggerPanicDuringRecoveryDoesNotCrashBus`.

### Delivery guarantees (stated precisely, per the grcache Pipeline/
TxPipeline lesson — see CLAUDE.md)

- **Async mode is at-least-once** for any event that was successfully
  enqueued. Events discarded by `OverflowDrop` or rejected by
  `OverflowReject` never entered the delivery path and are outside this
  guarantee.
- **Sync mode is at-most-once** per subscriber per `Publish` call: no
  retry, ever.
- **No cross-process deduplication, ever, in either mode.** grevents runs
  entirely in one process's memory.

[Unreleased]: https://github.com/gourdian25/grevents/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/gourdian25/grevents/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/gourdian25/grevents/releases/tag/v0.1.0
