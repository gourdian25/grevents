// File: bus_test.go

package grevents_test

import (
	"testing"

	"github.com/gourdian25/grevents"
	"github.com/gourdian25/grevents/conformance"
)

func TestConformance(t *testing.T) {
	conformance.Run(t, grevents.NewBus)
}
