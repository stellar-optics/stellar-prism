package stream

import (
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// RoughLedgerCloseInterval is the ordinary Stellar ledger cadence on mainnet
// and testnet — roughly one close every five seconds. prism deliberately
// keeps the --since conversion offline and deterministic: no extra RPC call
// to measure the true rate, because the point of a relative window is to be
// approximate ("start about ten minutes back"), and determinism keeps the
// behaviour testable and documented. Streams that need an exact ledger use
// --from instead.
const RoughLedgerCloseInterval = 5 * time.Second

// StartLedgerForWindow converts a relative time window into an approximate
// start ledger, counting back from latest at RoughLedgerCloseInterval.
//
// The conversion rounds the window up to whole ledger intervals, so
// `--since 10m` never starts *after* ten minutes ago: a slightly early start
// is harmless for tailing, while a late one silently omits records inside
// the requested window. Windows longer than the chain clamp to ledger 1 and
// the server then reports its retention floor from there, which is exactly
// the behaviour streams that reach too far back want. A non-positive window
// degenerates to starting at latest, which is how the --since flag's caller
// treats an unconfigured reset.
func StartLedgerForWindow(latest uint32, window time.Duration) uint32 {
	if window <= 0 {
		return latest
	}

	roundUp := func(d time.Duration) time.Duration {
		if rem := d % RoughLedgerCloseInterval; rem != 0 {
			return d + RoughLedgerCloseInterval - rem
		}
		return d
	}

	ledgersBack := roundUp(window) / RoughLedgerCloseInterval
	// The window is positive here, so ledgersBack is a non-negative count of
	// intervals rather than a span of nanoseconds; only as a count can it be
	// compared against a ledger sequence.
	if int64(ledgersBack) >= int64(latest) {
		// The window reaches before the first ledger ever closed.
		return 1
	}
	return latest - uint32(ledgersBack) //nolint:gosec // bounded by the guard above
}

// WindowSummary describes the events inside one sliding window.
type WindowSummary struct {
	// WindowStart and WindowEnd bound the window, (start, end].
	WindowStart time.Time
	WindowEnd   time.Time
	// Window is the configured window length.
	Window time.Duration
	// Count is the number of events inside the window.
	Count int
	// EventsPerSec is Count over the window length, or over the time since
	// the first event while the stream has been running for less than a
	// full window, so the rate is not understated during warm-up.
	EventsPerSec float64
	// UniqueSenders is the number of distinct non-empty senders inside the
	// window.
	UniqueSenders int
}

type windowEntry struct {
	at     time.Time
	sender string
}

// SlidingWindowBuffer keeps the events of the last Window and answers rate
// questions about them: how many, how fast, from how many distinct senders.
//
// Memory is bounded by the events inside one window. Every Add and Snapshot
// first evicts entries that have rolled off, the backing slice is compacted
// as the live region drifts forward, and sender counts are reference counted
// so a sender disappears from the unique set as soon as its last event
// leaves the window.
//
// Timestamps are expected in roughly ascending order, as ledger close times
// are. An event older than the window at the time it is added is dropped. The
// buffer is not safe for concurrent use; callers serialise access.
type SlidingWindowBuffer struct {
	window  time.Duration
	entries []windowEntry
	head    int // entries[head:] are live
	senders map[string]int
	first   time.Time
	newest  time.Time
}

// NewSlidingWindowBuffer returns a buffer covering the trailing window.
func NewSlidingWindowBuffer(window time.Duration) (*SlidingWindowBuffer, error) {
	if window <= 0 {
		return nil, fmt.Errorf("sliding window must be positive, got %s", window)
	}
	return &SlidingWindowBuffer{window: window, senders: map[string]int{}}, nil
}

// Window returns the configured window length.
func (b *SlidingWindowBuffer) Window() time.Duration { return b.window }

// Len returns the number of events currently held.
func (b *SlidingWindowBuffer) Len() int { return len(b.entries) - b.head }

// Add records one event at time at, attributed to sender (which may be empty
// when the event has no identifiable sender).
func (b *SlidingWindowBuffer) Add(at time.Time, sender string) {
	if at.After(b.newest) {
		b.newest = at
	}
	b.evict(b.newest)
	if !at.After(b.newest.Add(-b.window)) {
		return // already outside the window
	}
	if b.first.IsZero() || at.Before(b.first) {
		b.first = at
	}
	b.entries = append(b.entries, windowEntry{at: at, sender: sender})
	if sender != "" {
		b.senders[sender]++
	}
}

// Snapshot evicts everything at or before now-Window and summarises what is
// left. now is normally the wall clock for a live stream, or the stream's
// own event clock while replaying history.
func (b *SlidingWindowBuffer) Snapshot(now time.Time) WindowSummary {
	b.evict(now)
	s := WindowSummary{
		WindowStart:   now.Add(-b.window),
		WindowEnd:     now,
		Window:        b.window,
		Count:         b.Len(),
		UniqueSenders: len(b.senders),
	}
	span := b.window
	if !b.first.IsZero() {
		if elapsed := now.Sub(b.first); elapsed < span {
			span = elapsed
		}
	}
	if span < time.Second {
		span = time.Second
	}
	s.EventsPerSec = float64(s.Count) / span.Seconds()
	return s
}

func (b *SlidingWindowBuffer) evict(now time.Time) {
	cutoff := now.Add(-b.window)
	for b.head < len(b.entries) && !b.entries[b.head].at.After(cutoff) {
		e := b.entries[b.head]
		if e.sender != "" {
			if n := b.senders[e.sender] - 1; n > 0 {
				b.senders[e.sender] = n
			} else {
				delete(b.senders, e.sender)
			}
		}
		b.entries[b.head] = windowEntry{} // release the sender string
		b.head++
	}
	switch {
	case b.head == len(b.entries):
		// Empty: reuse the storage from the start.
		b.entries = b.entries[:0]
		b.head = 0
	case b.head >= 1024 && b.head*2 >= len(b.entries):
		// More dead than live: slide the live region down so the backing
		// array stops growing with the stream's lifetime.
		n := copy(b.entries, b.entries[b.head:])
		clear(b.entries[n:])
		b.entries = b.entries[:n]
		b.head = 0
	}
	// A burst can leave a large backing array behind; once traffic falls
	// well below it, reallocate so memory follows the current window.
	if live := b.Len(); cap(b.entries) > 4096 && live*4 < cap(b.entries) {
		shrunk := make([]windowEntry, live, live*2)
		copy(shrunk, b.entries[b.head:])
		b.entries = shrunk
		b.head = 0
	}
}

// EventSender returns the address that initiated an event, for unique-sender
// metrics. It follows the SEP-41 token convention, where topic 0 names the
// event and the first address-typed topic after it is the account that moved
// funds ("from" for transfer and burn, the recipient for mint), which also
// fits most custom contracts that index an actor in their topics. Events with
// no address topic have no sender and return "".
func EventSender(r Record) string {
	if r.Event == nil || len(r.Event.TopicXDR) < 2 {
		return ""
	}
	for _, raw := range r.Event.TopicXDR[1:] {
		var v xdr.ScVal
		if err := xdr.SafeUnmarshalBase64(raw, &v); err != nil {
			continue
		}
		if v.Type != xdr.ScValTypeScvAddress || v.Address == nil {
			continue
		}
		if s, err := v.Address.String(); err == nil {
			return s
		}
	}
	return ""
}
