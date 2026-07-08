package grevents_test

// This file proves *grlog.Logger satisfies grevents.Logger structurally,
// without grevents itself importing grlog — grlog is a test-only
// dependency of this module (see also example/example.go, which
// demonstrates the same interoperability at runtime), so it never leaks
// into consumers who don't want a logging dependency at all.

import (
	"testing"

	"github.com/gourdian25/grlog"

	"github.com/gourdian25/grevents"
)

var _ grevents.Logger = (*grlog.Logger)(nil)

func TestGrlogSatisfiesLoggerInterface(t *testing.T) {
	logger := grlog.NewDefaultLogger()
	defer logger.Close()

	var l grevents.Logger = logger
	l.Infof("grevents test: %s", "info")
	l.Warnf("grevents test: %s", "warn")
	l.Errorf("grevents test: %s", "error")
}

func TestNopLogger(t *testing.T) {
	l := grevents.NopLogger()
	// Must not panic with no logger installed.
	l.Infof("noop")
	l.Warnf("noop")
	l.Errorf("noop")
}

func TestOrNop(t *testing.T) {
	if grevents.OrNop(nil) == nil {
		t.Fatal("OrNop(nil) = nil, want a non-nil no-op Logger")
	}

	logger := grlog.NewDefaultLogger()
	defer logger.Close()
	if grevents.OrNop(logger) != grevents.Logger(logger) {
		t.Fatal("OrNop(non-nil) did not return the given logger unchanged")
	}
}
