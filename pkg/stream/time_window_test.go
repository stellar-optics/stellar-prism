package stream_test

import (
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// TestStartLedgerForWindow pins the arithmetic contract: windows round up to
// whole interval units, so a --since window never starts late, long windows
// clamp to ledger 1, and degenerate windows start at latest.
func TestStartLedgerForWindow(t *testing.T) {
	t.Parallel()

	const latest = uint32(1_000_000)

	tests := []struct {
		name   string
		window time.Duration
		// latest overrides the head the window counts back from; zero means
		// "use the table's default".
		latest uint32
		want   uint32
	}{
		{name: "one interval", window: 5 * time.Second, want: latest - 1},
		{name: "sub-interval rounds up", window: 1 * time.Second, want: latest - 1},
		{name: "exactly two intervals", window: 10 * time.Second, want: latest - 2},
		{name: "ten minutes", window: 10 * time.Minute, want: latest - 120},
		{name: "two hours", window: 2 * time.Hour, want: latest - 1440},
		{name: "just under an interval rounds up", window: 4*time.Second + 999*time.Millisecond, want: latest - 1},
		{name: "zero window starts at latest", window: 0, want: latest},
		{name: "negative window starts at latest", window: -time.Minute, want: latest},
		{name: "longer than the chain clamps", window: 300 * 24 * time.Hour, want: 1},
		{name: "latest of 1 clamps", window: time.Minute, latest: 1, want: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			at := latest
			if tc.latest != 0 {
				at = tc.latest
			}
			if got := stream.StartLedgerForWindow(at, tc.window); got != tc.want {
				t.Errorf("StartLedgerForWindow(%d, %s) = %d, want %d", at, tc.window, got, tc.want)
			}
		})
	}
}

// TestStartLedgerForWindowAlwaysStartsNotLate checks the conversion rounds
// the window up for every duration in the first two ledger intervals rather
// than only the table's few hand-picked points.
func TestStartLedgerForWindowAlwaysStartsNotLate(t *testing.T) {
	t.Parallel()

	const latest = uint32(1000)
	for ms := time.Duration(1); ms <= 10*1000; ms += 100 {
		got := stream.StartLedgerForWindow(latest, ms*time.Millisecond)
		want := latest - uint32((ms+stream.RoughLedgerCloseInterval-1)/stream.RoughLedgerCloseInterval) //nolint:gosec // bounded by latest
		if want < 1 {
			want = 1
		}
		if got > want {
			t.Fatalf("window %s resolved to ledger %d, later than %d", ms*time.Millisecond, got, want)
		}
	}
}

var windowEpoch = time.Unix(1_700_000_000, 0).UTC()

func newWindow(t *testing.T, d time.Duration) *stream.SlidingWindowBuffer {
	t.Helper()
	b, err := stream.NewSlidingWindowBuffer(d)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestNewSlidingWindowBufferRejectsNonPositiveWindow(t *testing.T) {
	t.Parallel()
	for _, d := range []time.Duration{0, -time.Second} {
		if _, err := stream.NewSlidingWindowBuffer(d); err == nil {
			t.Errorf("NewSlidingWindowBuffer(%v) accepted a non-positive window", d)
		}
	}
}

// TestSlidingWindowBufferRollsOff checks counts, the rate and the window
// boundary as events age out: the window is (now-window, now].
func TestSlidingWindowBufferRollsOff(t *testing.T) {
	t.Parallel()

	b := newWindow(t, time.Minute)
	// Ten events, one every 10s, across 90s.
	for i := 0; i < 10; i++ {
		b.Add(windowEpoch.Add(time.Duration(i)*10*time.Second), "")
	}

	at := windowEpoch.Add(90 * time.Second)
	got := b.Snapshot(at)
	// (30s, 90s] holds the events at 40s..90s.
	if got.Count != 6 {
		t.Fatalf("Count at 90s = %d, want 6", got.Count)
	}
	if want := 6.0 / 60; got.EventsPerSec != want {
		t.Fatalf("EventsPerSec = %v, want %v", got.EventsPerSec, want)
	}
	if !got.WindowStart.Equal(at.Add(-time.Minute)) || !got.WindowEnd.Equal(at) || got.Window != time.Minute {
		t.Fatalf("window bounds = (%v, %v] over %v", got.WindowStart, got.WindowEnd, got.Window)
	}

	// An event exactly at the lower bound has rolled off.
	if got := b.Snapshot(windowEpoch.Add(100 * time.Second)); got.Count != 5 {
		t.Fatalf("Count at 100s = %d, want 5 (the 40s event sits on the boundary)", got.Count)
	}
	if got := b.Snapshot(windowEpoch.Add(10 * time.Minute)); got.Count != 0 || got.EventsPerSec != 0 {
		t.Fatalf("after idle: %+v, want empty", got)
	}
	if b.Len() != 0 {
		t.Fatalf("Len() after full roll-off = %d, want 0", b.Len())
	}
}

// TestSlidingWindowBufferUniqueSenders checks a sender stays counted until
// its last event leaves the window, and empty senders are not counted.
func TestSlidingWindowBufferUniqueSenders(t *testing.T) {
	t.Parallel()

	b := newWindow(t, 30*time.Second)
	b.Add(windowEpoch, "alice")
	b.Add(windowEpoch.Add(10*time.Second), "bob")
	b.Add(windowEpoch.Add(20*time.Second), "alice")
	b.Add(windowEpoch.Add(20*time.Second), "")

	if got := b.Snapshot(windowEpoch.Add(25 * time.Second)); got.UniqueSenders != 2 || got.Count != 4 {
		t.Fatalf("at 25s: %+v, want 4 events from 2 senders", got)
	}
	// alice's first event rolls off but her second keeps her in the set.
	if got := b.Snapshot(windowEpoch.Add(35 * time.Second)); got.UniqueSenders != 2 || got.Count != 3 {
		t.Fatalf("at 35s: %+v, want 3 events from 2 senders", got)
	}
	// bob's only event rolls off.
	if got := b.Snapshot(windowEpoch.Add(45 * time.Second)); got.UniqueSenders != 1 || got.Count != 2 {
		t.Fatalf("at 45s: %+v, want 2 events from 1 sender", got)
	}
}

// TestSlidingWindowBufferWarmUpRate checks the rate is taken over the time
// the stream has actually been observed until a full window has passed.
func TestSlidingWindowBufferWarmUpRate(t *testing.T) {
	t.Parallel()

	b := newWindow(t, time.Minute)
	for i := 0; i < 20; i++ {
		b.Add(windowEpoch.Add(time.Duration(i)*500*time.Millisecond), "")
	}
	got := b.Snapshot(windowEpoch.Add(10 * time.Second))
	if want := 2.0; got.EventsPerSec != want {
		t.Fatalf("warm-up EventsPerSec = %v, want %v (20 events over 10s, not 60s)", got.EventsPerSec, want)
	}
}

// TestSlidingWindowBufferDropsStaleEvents checks an event already older than
// the window, relative to the newest event seen, is not retained.
func TestSlidingWindowBufferDropsStaleEvents(t *testing.T) {
	t.Parallel()

	b := newWindow(t, time.Minute)
	b.Add(windowEpoch.Add(5*time.Minute), "")
	b.Add(windowEpoch, "late")
	if b.Len() != 1 {
		t.Fatalf("Len() = %d, want 1; a stale event was retained", b.Len())
	}
	if got := b.Snapshot(windowEpoch.Add(5 * time.Minute)); got.UniqueSenders != 0 {
		t.Fatalf("UniqueSenders = %d, want 0", got.UniqueSenders)
	}
}

// TestSlidingWindowBufferMemoryBounded streams a day of events at 50/s
// through a one-minute window and checks the buffer never holds more than a
// window's worth, however long the stream runs.
func TestSlidingWindowBufferMemoryBounded(t *testing.T) {
	t.Parallel()

	const perSec = 50
	b := newWindow(t, time.Minute)
	limit := 60*perSec + 1
	step := time.Second / perSec
	at := windowEpoch
	for i := 0; i < 24*60*60*perSec/20; i++ { // ~1.2h of stream time, 216k events
		at = at.Add(step)
		b.Add(at, "")
		if b.Len() > limit {
			t.Fatalf("after %d events Len() = %d, exceeds one window (%d)", i+1, b.Len(), limit)
		}
	}
	if got := b.Snapshot(at); got.Count < limit-2 || got.Count > limit {
		t.Fatalf("steady-state Count = %d, want ~%d", got.Count, limit-1)
	}
}

func addressTopic(t *testing.T, suffix byte) (string, string) {
	t.Helper()
	var id xdr.ContractId
	id[31] = suffix
	v := xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &id,
	}}
	raw, err := xdr.MarshalBase64(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw, strkey.MustEncode(strkey.VersionByteContract, id[:])
}

func TestEventSenderUsesFirstAddressTopic(t *testing.T) {
	t.Parallel()

	sym, err := xdr.MarshalBase64(xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: ptrSymbol("transfer")})
	if err != nil {
		t.Fatal(err)
	}
	from, fromStr := addressTopic(t, 1)
	to, _ := addressTopic(t, 2)

	ev := stream.Record{Kind: stream.KindEvent, Event: &stream.Event{TopicXDR: []string{sym, from, to}}}
	if got := stream.EventSender(ev); got != fromStr {
		t.Fatalf("EventSender() = %q, want %q", got, fromStr)
	}
	noAddr := stream.Record{Kind: stream.KindEvent, Event: &stream.Event{TopicXDR: []string{sym, sym}}}
	if got := stream.EventSender(noAddr); got != "" {
		t.Fatalf("EventSender() without an address topic = %q, want empty", got)
	}
	if got := stream.EventSender(stream.Record{Kind: stream.KindLedger}); got != "" {
		t.Fatalf("EventSender() for a non-event = %q, want empty", got)
	}
}

func ptrSymbol(s string) *xdr.ScSymbol {
	v := xdr.ScSymbol(s)
	return &v
}
