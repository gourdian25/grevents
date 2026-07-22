// File: logger.go

package grevents

// Logger is the minimal logging interface grevents accepts for optional
// diagnostic logging (recovered panics, dead-letter recording failures,
// drain-timeout warnings, queue overflow). Its four methods match
// *slog.Logger's own signatures exactly, so *slog.Logger satisfies it
// structurally.
//
// Notes:
//   - grevents itself does not import grlog or log/slog; see logger_test.go
//     for a compile-time proof (var _ Logger = (*slog.Logger)(nil))
//   - A nil Logger passed via WithLogger is replaced with NopLogger()
//     once, at construction time, so every internal call site can assume
//     a non-nil Logger
//
// Use case: implement this interface yourself, or pass a *slog.Logger
// directly (e.g. slog.New(grlog.NewSlogHandler(logger))), to route
// grevents' internal diagnostics into an existing logging pipeline.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type noopLogger struct{}

func (noopLogger) Debug(string, ...any) {}
func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}

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
