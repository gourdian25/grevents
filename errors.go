// File: errors.go

package grevents

import "errors"

// Sentinel errors for use with errors.Is. There is deliberately no
// IsX(err error) bool helper: callers use errors.Is(err, grevents.ErrX)
// directly, consistent with how gourdiantoken's and grcache's sentinel
// errors are consumed.
var (
	// ErrClosed indicates the bus has been closed; Publish and Subscribe
	// return it for every call made after Close.
	ErrClosed = errors.New("grevents: bus is closed")

	// ErrQueueFull indicates an async bus configured with OverflowReject
	// had a full queue at Publish time.
	ErrQueueFull = errors.New("grevents: async queue is full")

	// ErrNoSubscribers is never returned from Publish (publishing to a
	// topic with no subscribers is a silent no-op by design); it exists
	// only for internal Stats/debug-logging use.
	ErrNoSubscribers = errors.New("grevents: no subscribers for topic")

	// ErrInvalidConfig indicates NewBus was called with an invalid
	// combination of BusOptions.
	ErrInvalidConfig = errors.New("grevents: invalid bus configuration")

	// ErrDrainTimeout indicates Close's async queue drain did not finish
	// within the configured WithDrainTimeout.
	ErrDrainTimeout = errors.New("grevents: close drain timeout exceeded")
)
