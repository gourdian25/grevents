// File: deadletter_test.go

package grevents_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gourdian25/grevents"
)

func TestMemoryDeadLetterSinkRecordAndList(t *testing.T) {
	ctx := context.Background()
	sink := grevents.NewMemoryDeadLetterSink(10)

	err := errors.New("boom")
	event := grevents.Event{Topic: "topic"}
	if err := sink.Record(ctx, event, err, 3); err != nil {
		t.Fatalf("Record: %v", err)
	}

	entries, listErr := sink.List(ctx, 0)
	if listErr != nil {
		t.Fatalf("List: %v", listErr)
	}
	if len(entries) != 1 {
		t.Fatalf("List returned %d entries, want 1", len(entries))
	}
	if entries[0].Attempts != 3 {
		t.Fatalf("Attempts = %d, want 3", entries[0].Attempts)
	}
	if entries[0].LastError != "boom" {
		t.Fatalf("LastError = %q, want %q", entries[0].LastError, "boom")
	}
	if entries[0].Event.Topic != "topic" {
		t.Fatalf("Event.Topic = %q, want %q", entries[0].Event.Topic, "topic")
	}
	if entries[0].DeadAt.IsZero() {
		t.Fatalf("DeadAt is zero, want set")
	}
}

func TestMemoryDeadLetterSinkRingEviction(t *testing.T) {
	ctx := context.Background()
	sink := grevents.NewMemoryDeadLetterSink(3)

	for i := 0; i < 5; i++ {
		event := grevents.Event{Topic: string(rune('A' + i))}
		if err := sink.Record(ctx, event, nil, 1); err != nil {
			t.Fatalf("Record %d: %v", i, err)
		}
	}

	entries, err := sink.List(ctx, 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("List returned %d entries, want 3 (capacity)", len(entries))
	}
	// Oldest two (A, B) should have been overwritten; only C, D, E remain,
	// in chronological (oldest-first) order.
	want := []string{"C", "D", "E"}
	for i, w := range want {
		if entries[i].Event.Topic != w {
			t.Fatalf("entries[%d].Event.Topic = %q, want %q", i, entries[i].Event.Topic, w)
		}
	}
}

func TestMemoryDeadLetterSinkListLimit(t *testing.T) {
	ctx := context.Background()
	sink := grevents.NewMemoryDeadLetterSink(10)

	for i := 0; i < 5; i++ {
		event := grevents.Event{Topic: string(rune('A' + i))}
		if err := sink.Record(ctx, event, nil, 1); err != nil {
			t.Fatalf("Record %d: %v", i, err)
		}
	}

	entries, err := sink.List(ctx, 2)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("List(limit=2) returned %d entries, want 2", len(entries))
	}
	// Most recent 2, oldest-first: D, E.
	if entries[0].Event.Topic != "D" || entries[1].Event.Topic != "E" {
		t.Fatalf("List(limit=2) = %q, %q, want D, E", entries[0].Event.Topic, entries[1].Event.Topic)
	}
}

func TestMemoryDeadLetterSinkDefaultCapacity(t *testing.T) {
	ctx := context.Background()
	sink := grevents.NewMemoryDeadLetterSink(0) // <=0 defaults to 1000

	if err := sink.Record(ctx, grevents.Event{Topic: "x"}, nil, 1); err != nil {
		t.Fatalf("Record: %v", err)
	}
	entries, err := sink.List(ctx, 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("List returned %d entries, want 1", len(entries))
	}
}

func TestMemoryDeadLetterSinkCloseIsNoop(t *testing.T) {
	sink := grevents.NewMemoryDeadLetterSink(1)
	if err := sink.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil", err)
	}
}
