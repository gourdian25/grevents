// File: logger.go

package grevents

// Logger is the minimal logging interface grevents accepts for optional
// diagnostic logging (recovered panics, dead-letter recording failures,
// drain-timeout warnings). It is satisfied structurally by *grlog.Logger's
// printf-style methods (Infof/Warnf/Errorf) — grevents itself does not
// import grlog.
//
// A nil Logger passed via WithLogger is replaced with NopLogger() once,
// at construction time, so every call site can assume a non-nil Logger.
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
func NopLogger() Logger { return noopLogger{} }

// OrNop returns l if it is non-nil, otherwise NopLogger().
func OrNop(l Logger) Logger {
	if l == nil {
		return NopLogger()
	}
	return l
}
