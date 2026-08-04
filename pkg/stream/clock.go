package stream

import (
	"context"
	"math/rand/v2"
	"time"
)

// Clock abstracts time so the tailer can be tested without real delays.
//
// The production implementation sleeps; the test implementation returns
// immediately and records what it was asked to wait for. That keeps the state
// machine's tests fast and, more importantly, deterministic — a streaming
// loop tested against wall-clock timing is a flaky test waiting to happen.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// Sleep waits for d, returning early with ctx.Err() if the context is
	// cancelled first.
	Sleep(ctx context.Context, d time.Duration) error
}

// RealClock is the production Clock.
type RealClock struct{}

// Now implements Clock.
func (RealClock) Now() time.Time { return time.Now() }

// Sleep implements Clock, waking early on cancellation so that Ctrl-C is
// responsive even in the middle of a long backoff.
func (RealClock) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Backoff computes retry delays after a failed fetch.
type Backoff struct {
	// Initial is the delay after the first failure.
	Initial time.Duration
	// Max caps the delay however many failures accumulate.
	Max time.Duration
	// Factor multiplies the delay after each successive failure.
	Factor float64
	// Jitter randomises the delay across [d/2, d] when true. Jitter matters
	// when several prism instances watch the same contract: without it they
	// reconnect in lockstep and hammer the RPC in synchronised waves.
	Jitter bool

	// rand is injectable so tests can assert exact delays.
	rand func() float64
}

// DefaultBackoff is tuned for Soroban RPC: quick enough that a brief blip is
// invisible, capped low enough that a recovered server is picked up within
// one ledger close.
func DefaultBackoff() Backoff {
	return Backoff{
		Initial: 500 * time.Millisecond,
		Max:     30 * time.Second,
		Factor:  2,
		Jitter:  true,
	}
}

// Delay returns the wait before retry number attempt, counting from 1.
func (b Backoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	initial := b.Initial
	if initial <= 0 {
		initial = 100 * time.Millisecond
	}
	factor := b.Factor
	if factor < 1 {
		factor = 2
	}
	maxDelay := b.Max
	if maxDelay <= 0 {
		maxDelay = 30 * time.Second
	}

	d := float64(initial)
	for range attempt - 1 {
		d *= factor
		if d >= float64(maxDelay) {
			d = float64(maxDelay)
			break
		}
	}
	if d > float64(maxDelay) {
		d = float64(maxDelay)
	}

	if b.Jitter {
		r := b.rand
		if r == nil {
			r = rand.Float64
		}
		// Full jitter over the lower half keeps a floor on the delay while
		// still spreading reconnects out.
		d = d/2 + d/2*r()
	}
	return time.Duration(d)
}
