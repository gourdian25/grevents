// File: stats.go

package grevents

import "sync/atomic"

// busStats holds the lock-free counters backing Stats(). Published
// increments once per Publish call that reaches dispatch (i.e. the bus
// was not already closed), regardless of delivery outcome — including
// events later discarded by OverflowDrop or rejected by OverflowReject.
// Delivered/Failed count per-subscriber delivery attempts: in async mode
// with retry, a single (event,subscriber) pair can contribute one
// Delivered plus zero or more Failed depending on how many attempts it
// took; a permanently exhausted pair contributes only to DeadLettered
// (its final attempt is also counted in Failed).
type busStats struct {
	published      atomic.Uint64
	delivered      atomic.Uint64
	failed         atomic.Uint64
	deadLettered   atomic.Uint64
	droppedOnClose atomic.Uint64
}
