// File: logger_test.go

package grevents_test

// This file proves *slog.Logger satisfies grevents.Logger structurally,
// without grevents itself importing grlog or log/slog — grlog is a
// test-only dependency of this module (see also example/example.go, which
// demonstrates the same interoperability at runtime), so it never leaks
// into consumers who don't want a logging dependency at all.

import (
	"log/slog"
	"testing"

	"github.com/gourdian25/grlog"

	"github.com/gourdian25/grevents"
)

var _ grevents.Logger = (*slog.Logger)(nil)

func TestGrlogSatisfiesLoggerInterface(t *testing.T) {
	logger := grlog.NewDefaultLogger()
	defer func() { _ = logger.Close() }()

	slogger := slog.New(grlog.NewSlogHandler(logger))
	var l grevents.Logger = slogger

	l.Debug("grevents test", "level", "debug")
	l.Info("grevents test", "level", "info")
	l.Warn("grevents test", "level", "warn")
	l.Error("grevents test", "level", "error")
}

func TestNopLogger(t *testing.T) {
	l := grevents.NopLogger()
	// Must not panic with no logger installed.
	l.Debug("noop")
	l.Info("noop")
	l.Warn("noop")
	l.Error("noop")
}

func TestOrNop(t *testing.T) {
	if grevents.OrNop(nil) == nil {
		t.Fatal("OrNop(nil) = nil, want a non-nil no-op Logger")
	}

	logger := grlog.NewDefaultLogger()
	defer func() { _ = logger.Close() }()
	slogger := slog.New(grlog.NewSlogHandler(logger))
	if grevents.OrNop(slogger) != grevents.Logger(slogger) {
		t.Fatal("OrNop(non-nil) did not return the given logger unchanged")
	}
}
