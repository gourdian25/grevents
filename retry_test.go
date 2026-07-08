// File: retry_test.go

package grevents

import (
	"testing"
	"time"
)

func TestComputeBackoffZeroBaseIsZero(t *testing.T) {
	rc := retryConfig{baseBackoff: 0, maxBackoff: time.Second}
	if got := computeBackoff(rc, 0); got != 0 {
		t.Fatalf("computeBackoff with baseBackoff=0 = %v, want 0", got)
	}
}

func TestComputeBackoffWithinBounds(t *testing.T) {
	rc := retryConfig{baseBackoff: 10 * time.Millisecond, maxBackoff: time.Second}
	for attempt := 0; attempt < 20; attempt++ {
		for i := 0; i < 50; i++ { // repeat since the output is randomized
			got := computeBackoff(rc, attempt)
			if got < 0 {
				t.Fatalf("computeBackoff(attempt=%d) = %v, want >= 0", attempt, got)
			}
			if got > rc.maxBackoff {
				t.Fatalf("computeBackoff(attempt=%d) = %v, want <= cap %v", attempt, got, rc.maxBackoff)
			}
		}
	}
}

func TestComputeBackoffRespectsCapAtLargeAttempts(t *testing.T) {
	rc := retryConfig{baseBackoff: time.Millisecond, maxBackoff: 100 * time.Millisecond}
	for _, attempt := range []int{10, 30, 61, 62, 100, 1000} {
		got := computeBackoff(rc, attempt)
		if got > rc.maxBackoff {
			t.Fatalf("computeBackoff(attempt=%d) = %v, want <= cap %v", attempt, got, rc.maxBackoff)
		}
	}
}

func TestComputeBackoffDefaultsMaxBackoffWhenUnset(t *testing.T) {
	rc := retryConfig{baseBackoff: time.Hour} // deliberately huge to force clamping
	got := computeBackoff(rc, 5)
	if got > defaultMaxBackoff {
		t.Fatalf("computeBackoff with maxBackoff unset = %v, want <= defaultMaxBackoff %v", got, defaultMaxBackoff)
	}
}

func TestComputeBackoffNoOverflowPanicAtHighAttempts(t *testing.T) {
	rc := retryConfig{baseBackoff: time.Millisecond, maxBackoff: time.Second}
	for _, attempt := range []int{60, 61, 62, 63, 100, 1000, 1_000_000} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("computeBackoff(attempt=%d) panicked: %v", attempt, r)
				}
			}()
			computeBackoff(rc, attempt)
		}()
	}
}
