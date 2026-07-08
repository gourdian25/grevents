// File: retry.go

package grevents

import (
	"math/rand"
	"time"
)

// defaultMaxBackoff is a fixed internal safety ceiling applied whenever
// WithRetry is used, since its public signature (maxAttempts, baseBackoff)
// has no cap parameter of its own. There is no ecosystem precedent for
// backoff-with-jitter in the gourdian ecosystem to port from — this is a
// new, self-contained algorithm, not a reuse of an existing one. A future
// WithRetryBackoffCap option can make this configurable without breaking
// the existing WithRetry signature.
const defaultMaxBackoff = 30 * time.Second

type retryConfig struct {
	maxAttempts int
	baseBackoff time.Duration
	maxBackoff  time.Duration
}

// computeBackoff returns a Full Jitter sleep duration (per AWS's
// well-known backoff formula) for the given 0-indexed attempt that just
// failed: sleep = random(0, min(cap, base * 2^attempt)). Full Jitter is
// used over Decorrelated Jitter because it needs no state threaded
// between attempts, keeping it a pure function of (config, attempt).
//
// Guards against 1<<attempt overflowing time.Duration's int64 by clamping
// to the cap before the shift can overflow.
func computeBackoff(rc retryConfig, attempt int) time.Duration {
	if rc.baseBackoff <= 0 {
		return 0
	}
	ceiling := rc.maxBackoff
	if ceiling <= 0 {
		ceiling = defaultMaxBackoff
	}

	exp := ceiling
	if attempt >= 0 && attempt < 62 { // 1<<62 already exceeds any sane baseBackoff*factor; avoid signed-shift overflow beyond this
		if scaled := rc.baseBackoff * (1 << uint(attempt)); scaled > 0 && scaled < ceiling { //nolint:gosec // attempt is bounded to [0,62) on this branch
			exp = scaled
		}
	}

	return time.Duration(rand.Int63n(int64(exp) + 1)) //nolint:gosec // backoff jitter has no cryptographic requirement
}
