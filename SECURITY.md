# Security Policy

## Supported Versions

Security fixes are applied to the latest released minor version.

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅        |
| < 0.1   | ❌        |

## Reporting a Vulnerability

Please report suspected vulnerabilities privately via
[GitHub Security Advisories](https://github.com/gourdian25/grevents/security/advisories/new)
rather than opening a public issue.

Include:

- A description of the issue and its impact
- Steps or a proof-of-concept to reproduce
- Affected version(s)

You can expect an acknowledgment within a week. Once a fix is available, the
advisory will be published together with a patched release.

## Scope Notes

grevents is an in-process event bus with no network listeners and no
dependencies outside the Go standard library (grlog is a test-only
dependency — see logger_test.go — and never imported by any non-test file).
The most relevant security considerations for users are:

- **No cross-process boundary**: grevents never sends data over a network or
  to another process. All `Payload`/`Metadata` on an `Event` stay in the
  publishing process's memory.
- **Sensitive data in events**: the library does not redact or encrypt
  `Event.Payload` or `Event.Metadata`; do not put secrets there unless your
  own subscribers/middleware are trusted to handle them appropriately.
- **Panic recovery is not a sandbox**: a subscriber's panic is recovered so
  it cannot crash the bus or host process, but the panicking handler still
  runs with the same privileges as any other code in your process — this is
  a resilience feature, not an isolation boundary.
- **Dead-letter sink retention**: the default in-memory `DeadLetterSink`
  retains recent failed events (including their `Payload`) for inspection;
  size it deliberately (`NewMemoryDeadLetterSink(capacity)`) and consider
  what a compromised process reading that buffer could learn.
