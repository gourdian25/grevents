// File: logger.go

package grevents

// Logger is the minimal logging interface grevents accepts for optional
// diagnostic logging (recovered panics, dead-letter recording failures,
// drain-timeout warnings).
//
// Notes:
//   - Satisfied structurally by *grlog.Logger's printf-style methods
//     (Infof/Warnf/Errorf) — grevents itself does not import grlog; see
//     logger_test.go for a compile-time proof (var _ Logger =
//     (*grlog.Logger)(nil))
//   - A nil Logger passed via WithLogger is replaced with NopLogger()
//     once, at construction time, so every internal call site can assume
//     a non-nil Logger
//
// Use case: implement this interface yourself, or pass a *grlog.Logger
// directly, to route grevents' internal diagnostics into an existing
// logging pipeline.
type Logger interface {
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type noopLogger struct{}

func (noopLogger) Infof(string, ...interface{})  {}
func (noopLogger) Warnf(string, ...interface{})  {}
func (noopLogger) Errorf(string, ...interface{}) {}

// NopLogger returns a Logger whose methods do nothing.
//
// Returns:
//   - Logger: safe to call any method on; never panics
//
// Use case: the default when WithLogger is never supplied to NewBus, and
// a convenient explicit no-op for tests that don't care about log output.
func NopLogger() Logger { return noopLogger{} }

// OrNop returns l if it is non-nil, otherwise NopLogger().
//
// Parameters:
//   - l: Logger — may be nil
//
// Returns:
//   - Logger: never nil
//
// Use case: called exactly once, at construction time, by any code
// accepting an optional Logger — grevents' own NewBus does this so every
// later call site can use the result unconditionally, with no repeated
// nil checks.
func OrNop(l Logger) Logger {
	if l == nil {
		return NopLogger()
	}
	return l
}
