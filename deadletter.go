// File: deadletter.go

package grevents

import (
	"context"
	"sync"
	"time"
)

// defaultDLQCapacity is the entry count NewBus wires up automatically
// when WithDeadLetterSink is never supplied.
const defaultDLQCapacity = 1000

// DeadLetterSink receives events that exhausted their retry attempts in
// async mode, instead of being silently dropped. See
// NewMemoryDeadLetterSink for the default implementation, wired in
// automatically by NewBus unless WithDeadLetterSink overrides it.
type DeadLetterSink interface {
	// Record stores one permanently-failed (event, subscriber) delivery.
	//
	// Parameters:
	//   - ctx: context.Context — grevents always calls this with
	//     context.Background(), since the originating Publish call's ctx
	//     may already be done by the time retries exhaust
	//   - event: Event — the event that could not be delivered
	//   - lastErr: error — the error from the final attempt (may be nil)
	//   - attempts: int — total invocations made, including the first
	//
	// Returns:
	//   - error: implementation-defined; grevents logs (via the
	//     configured Logger) but does not otherwise act on a non-nil
	//     return
	Record(ctx context.Context, event Event, lastErr error, attempts int) error

	// List returns recorded entries in chronological (oldest-first)
	// order.
	//
	// Parameters:
	//   - ctx: context.Context
	//   - limit: int — the maximum number of entries to return, taking
	//     the most recent limit; 0 or negative returns every entry
	//     currently held
	//
	// Returns:
	//   - []DeadLetterEntry
	//   - error: implementation-defined
	List(ctx context.Context, limit int) ([]DeadLetterEntry, error)

	// Close releases any resources held by the sink. Called once by
	// Bus.Close during shutdown.
	//
	// Returns:
	//   - error: implementation-defined
	Close() error
}

// DeadLetterEntry is one permanently-failed delivery recorded by a
// DeadLetterSink.
//
// Fields:
//   - Event: Event — the event that could not be delivered
//   - LastError: string — the final attempt's error, stringified (empty
//     if that error was nil)
//   - Attempts: int — total invocations made, including the first
//   - DeadAt: time.Time — when the entry was recorded
type DeadLetterEntry struct {
	Event     Event
	LastError string
	Attempts  int
	DeadAt    time.Time
}

// memoryDeadLetterSink is the default DeadLetterSink: a capacity-bounded,
// in-memory ring buffer. It is a best-effort recent-history buffer, not a
// durable audit log — entries are lost on process restart, and once at
// capacity, each new dead-lettered event silently overwrites the oldest
// entry. Do not treat List's output as a complete or durable record of
// every failure the bus has ever produced.
type memoryDeadLetterSink struct {
	mu       sync.Mutex
	entries  []DeadLetterEntry
	capacity int
	next     int
	full     bool
}

// NewMemoryDeadLetterSink returns the default in-memory DeadLetterSink.
//
// Parameters:
//   - capacity: int — ring buffer size; capacity<=0 defaults to 1000
//
// Returns:
//   - DeadLetterSink
//
// Notes:
//   - Best-effort recent-history buffer, not a durable audit log:
//     entries are lost on process restart, and once at capacity, each new
//     dead-lettered event silently overwrites the oldest entry
//
// Use case: inspecting recent permanent delivery failures during local
// development or in a dashboard, without standing up external storage.
func NewMemoryDeadLetterSink(capacity int) DeadLetterSink {
	if capacity <= 0 {
		capacity = defaultDLQCapacity
	}
	return &memoryDeadLetterSink{
		entries:  make([]DeadLetterEntry, capacity),
		capacity: capacity,
	}
}

func (s *memoryDeadLetterSink) Record(_ context.Context, event Event, lastErr error, attempts int) error {
	entry := DeadLetterEntry{
		Event:    event,
		Attempts: attempts,
		DeadAt:   time.Now(),
	}
	if lastErr != nil {
		entry.LastError = lastErr.Error()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[s.next] = entry
	s.next = (s.next + 1) % s.capacity
	if s.next == 0 {
		s.full = true
	}
	return nil
}

func (s *memoryDeadLetterSink) List(_ context.Context, limit int) ([]DeadLetterEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var chronological []DeadLetterEntry
	if s.full {
		chronological = make([]DeadLetterEntry, 0, s.capacity)
		chronological = append(chronological, s.entries[s.next:]...)
		chronological = append(chronological, s.entries[:s.next]...)
	} else {
		chronological = make([]DeadLetterEntry, s.next)
		copy(chronological, s.entries[:s.next])
	}

	if limit <= 0 || limit >= len(chronological) {
		return chronological, nil
	}
	return chronological[len(chronological)-limit:], nil // most recent `limit`, oldest first
}

func (s *memoryDeadLetterSink) Close() error { return nil }
