package stream_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// TestParseRewind covers every way a rewind distance can be written, and
// every way it can be wrong. A typo in --rewind must fail loudly rather than
// silently rewinding by nothing.
func TestParseRewind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		value       string
		wantLedgers uint32
		wantErr     bool
	}{
		{name: "a bare number counts ledgers", value: "500", wantLedgers: 500},
		{name: "an l suffix counts ledgers", value: "500l", wantLedgers: 500},
		{name: "an upper-case l suffix counts ledgers", value: "500L", wantLedgers: 500},
		{name: "seconds become ledgers", value: "45s", wantLedgers: 9},
		{name: "minutes become ledgers", value: "30m", wantLedgers: 360},
		{name: "hours become ledgers", value: "2h", wantLedgers: 1440},
		{name: "a compound duration becomes ledgers", value: "1h30m", wantLedgers: 1080},
		{name: "days become ledgers", value: "1d", wantLedgers: 17280},
		{name: "a sub-ledger duration still steps back one ledger", value: "1s", wantLedgers: 1},
		{name: "an empty value is rejected", value: "", wantErr: true},
		{name: "zero is rejected", value: "0", wantErr: true},
		{name: "zero ledgers are rejected", value: "0l", wantErr: true},
		{name: "a negative count is rejected", value: "-5", wantErr: true},
		{name: "a negative duration is rejected", value: "-5m", wantErr: true},
		{name: "a zero duration is rejected", value: "0s", wantErr: true},
		{name: "nonsense is rejected", value: "soon", wantErr: true},
		{name: "a bare unit is rejected", value: "m", wantErr: true},
		{name: "a non-numeric day count is rejected", value: "day", wantErr: true},
		{name: "a trailing unit without a number is rejected", value: "l", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			distance, err := stream.ParseRewind(tc.value)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseRewind(%q) error = nil, want a failure", tc.value)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseRewind(%q) error = %v", tc.value, err)
			}

			ledgers, err := distance.Ledgers()
			if err != nil {
				t.Fatalf("Ledgers() error = %v", err)
			}
			if ledgers != tc.wantLedgers {
				t.Errorf("Ledgers() = %d, want %d", ledgers, tc.wantLedgers)
			}
		})
	}
}

// TestRewindDistanceString checks that a distance renders the way it would be
// written on a command line, which is what error messages quote back.
func TestRewindDistanceString(t *testing.T) {
	t.Parallel()

	ledgers, err := stream.ParseRewind("500")
	if err != nil {
		t.Fatalf("ParseRewind() error = %v", err)
	}
	if got, want := ledgers.String(), "500l"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}

	duration, err := stream.ParseRewind("30m")
	if err != nil {
		t.Fatalf("ParseRewind() error = %v", err)
	}
	if got := duration.String(); !strings.Contains(got, "30m") {
		t.Errorf("String() = %q, want it to mention 30m", got)
	}
}

// TestRewindDistanceRejectsUnknownUnit covers the zero value, which is not a
// distance at all.
func TestRewindDistanceRejectsUnknownUnit(t *testing.T) {
	t.Parallel()

	if _, err := (stream.RewindDistance{}).Ledgers(); err == nil {
		t.Fatal("Ledgers() error = nil, want a failure for the zero value")
	}
	if _, err := (stream.RewindDistance{Unit: "fortnights"}).Ledgers(); err == nil {
		t.Fatal("Ledgers() error = nil, want a failure for an unknown unit")
	}
	if _, err := (stream.RewindDistance{LedgerCount: 0, Unit: stream.RewindLedgers}).Ledgers(); err == nil {
		t.Fatal("Ledgers() error = nil, want a failure for zero ledgers")
	}
}

// TestRewindDistanceRejectsMalformedDays covers a day count that is not a
// number, and the rendering of a distance that has no unit at all.
func TestRewindDistanceRejectsMalformedDays(t *testing.T) {
	t.Parallel()

	if _, err := stream.ParseRewind("xd"); err == nil {
		t.Fatal("ParseRewind() error = nil, want a failure")
	}
	if got, want := (stream.RewindDistance{}).String(), "0l"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

// TestRewindTarget covers where a rewind lands, including the clamp at the
// first ledger: "as far back as the chain goes" is a request, not a failure.
func TestRewindTarget(t *testing.T) {
	t.Parallel()

	distance := stream.RewindDistance{LedgerCount: 10, Unit: stream.RewindLedgers}

	t.Run("an ordinary step lands before the anchor", func(t *testing.T) {
		t.Parallel()

		target, err := distance.Target(1000)
		if err != nil {
			t.Fatalf("Target() error = %v", err)
		}
		if target.Ledger != 990 {
			t.Errorf("Ledger = %d, want 990", target.Ledger)
		}
		if target.Anchor != 1000 {
			t.Errorf("Anchor = %d, want 1000", target.Anchor)
		}
		if target.Back != 10 {
			t.Errorf("Back = %d, want 10", target.Back)
		}
		if target.Clamped {
			t.Error("Clamped = true, want false for a step inside the chain")
		}
	})

	t.Run("a step past the first ledger is clamped", func(t *testing.T) {
		t.Parallel()

		target, err := distance.Target(5)
		if err != nil {
			t.Fatalf("Target() error = %v", err)
		}
		if target.Ledger != stream.FirstLedger {
			t.Errorf("Ledger = %d, want %d", target.Ledger, stream.FirstLedger)
		}
		if target.Back != 4 {
			t.Errorf("Back = %d, want 4", target.Back)
		}
		if !target.Clamped {
			t.Error("Clamped = false, want true")
		}
	})

	t.Run("an anchor at the first ledger stays there", func(t *testing.T) {
		t.Parallel()

		target, err := distance.Target(stream.FirstLedger)
		if err != nil {
			t.Fatalf("Target() error = %v", err)
		}
		if target.Ledger != stream.FirstLedger {
			t.Errorf("Ledger = %d, want %d", target.Ledger, stream.FirstLedger)
		}
		if target.Back != 0 {
			t.Errorf("Back = %d, want 0", target.Back)
		}
		if !target.Clamped {
			t.Error("Clamped = false, want true")
		}
	})

	t.Run("an invalid distance is rejected", func(t *testing.T) {
		t.Parallel()

		if _, err := (stream.RewindDistance{}).Target(100); err == nil {
			t.Fatal("Target() error = nil, want a failure")
		}
	})
}

// TestRewindController covers the seam between whoever asks for a rewind and
// the tailer, which is the only goroutine allowed to move a cursor.
func TestRewindController(t *testing.T) {
	t.Parallel()

	controller := stream.NewRewindController()
	if controller.Pending() {
		t.Error("Pending() = true on a fresh controller")
	}

	distance := stream.RewindDistance{LedgerCount: 5, Unit: stream.RewindLedgers}
	if err := controller.Request(stream.RewindRequest{Distance: distance, Anchor: 42}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if !controller.Pending() {
		t.Error("Pending() = false after a request")
	}

	req, ok := controller.Take()
	if !ok {
		t.Fatal("Take() returned nothing after a request")
	}
	if req.Anchor != 42 {
		t.Errorf("Anchor = %d, want 42", req.Anchor)
	}
	if req.Distance.LedgerCount != 5 {
		t.Errorf("LedgerCount = %d, want 5", req.Distance.LedgerCount)
	}
	if controller.Pending() {
		t.Error("Pending() = true after Take()")
	}
	if controller.Taken() != 1 {
		t.Errorf("Taken() = %d, want 1", controller.Taken())
	}

	if _, ok := controller.Take(); ok {
		t.Error("Take() returned a request twice")
	}
	if controller.Taken() != 1 {
		t.Errorf("Taken() = %d, want 1", controller.Taken())
	}
}

// TestRewindControllerRejectsInvalidDistance checks that a nonsense request
// is refused rather than queued.
func TestRewindControllerRejectsInvalidDistance(t *testing.T) {
	t.Parallel()

	controller := stream.NewRewindController()
	if err := controller.Request(stream.RewindRequest{}); err == nil {
		t.Fatal("Request() error = nil, want a failure")
	}
	if controller.Pending() {
		t.Error("an invalid request must not be queued")
	}
}

// TestRewindControllerReplacesPendingRequest checks that the latest intent
// wins: asking twice must not queue two passes over the same records.
func TestRewindControllerReplacesPendingRequest(t *testing.T) {
	t.Parallel()

	controller := stream.NewRewindController()
	first := stream.RewindDistance{LedgerCount: 5, Unit: stream.RewindLedgers}
	second := stream.RewindDistance{LedgerCount: 9, Unit: stream.RewindLedgers}

	if err := controller.Request(stream.RewindRequest{Distance: first}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if err := controller.Request(stream.RewindRequest{Distance: second}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}

	req, ok := controller.Take()
	if !ok {
		t.Fatal("Take() returned nothing")
	}
	if req.Distance.LedgerCount != 9 {
		t.Errorf("LedgerCount = %d, want 9", req.Distance.LedgerCount)
	}
}

// rewindSink records what it is given and asks for a rewind the first time
// it is used, which is how a rewind is requested while a stream is running.
type rewindSink struct {
	ids      []string
	requests int
	request  func()
}

func (s *rewindSink) Emit(r stream.Record) error {
	s.ids = append(s.ids, r.ID)
	if s.requests == 0 && s.request != nil {
		s.requests++
		s.request()
	}
	return nil
}

func (s *rewindSink) Flush() error { return nil }

// TestTailAppliesRewind is the integration test for the feature: a rewind
// requested while a stream is running must reposition the source, clear the
// cursor and re-emit the records in between.
func TestTailAppliesRewind(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 20)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(5))

	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100, PageLimit: 5})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	controller := stream.NewRewindController()
	sink := &rewindSink{}
	sink.request = func() {
		_ = controller.Request(stream.RewindRequest{
			Distance: stream.RewindDistance{LedgerCount: 3, Unit: stream.RewindLedgers},
		})
	}

	var rewinds []stream.RewindTarget
	tl := stream.NewTailer(src, sink, stream.Options{
		Clock:  newFakeClock(),
		Rewind: controller,
		OnRewind: func(target stream.RewindTarget) {
			rewinds = append(rewinds, target)
		},
	})
	if err := tl.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	ids := idsOf(events)
	// The first page is emitted as usual. The rewind then lands three
	// ledgers before the cursor the stream had reached (ledger 104), so the
	// records from ledger 101 onwards are emitted a second time.
	want := append([]string{}, ids[0:5]...)
	want = append(want, ids[1:20]...)

	if len(sink.ids) != len(want) {
		t.Fatalf("emitted %d records, want %d\n got: %v\nwant: %v",
			len(sink.ids), len(want), sink.ids, want)
	}
	for i := range want {
		if sink.ids[i] != want[i] {
			t.Errorf("record %d = %q, want %q", i, sink.ids[i], want[i])
		}
	}

	if tl.Stats().Rewinds != 1 {
		t.Errorf("Stats().Rewinds = %d, want 1", tl.Stats().Rewinds)
	}
	if len(rewinds) != 1 {
		t.Fatalf("OnRewind was called %d time(s), want 1", len(rewinds))
	}
	if rewinds[0].Ledger != 101 {
		t.Errorf("rewind landed on ledger %d, want 101", rewinds[0].Ledger)
	}
	if rewinds[0].Back != 3 {
		t.Errorf("rewind stepped back %d ledger(s), want 3", rewinds[0].Back)
	}
}

// TestTailRewindsWhileFollowing checks that a rewind takes effect in a
// follow loop, not only in a finite drain: the stream is caught up, asleep
// between polls, and the rewind brings it back to earlier records.
func TestTailRewindsWhileFollowing(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 20)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(5))

	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100, PageLimit: 5})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	controller := stream.NewRewindController()
	sink := &collector{}

	clock := newFakeClock()
	// Bound the follow loop: a regression that stops the tailer
	// terminating must fail rather than hang the suite.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock.maxSleeps = 3
	clock.onBudget = cancel

	requested := false
	clock.onWake = func() {
		if requested {
			return
		}
		requested = true
		_ = controller.Request(stream.RewindRequest{
			Distance: stream.RewindDistance{LedgerCount: 3, Unit: stream.RewindLedgers},
		})
	}

	tl := stream.NewTailer(src, sink, stream.Options{
		Follow: true,
		Clock:  clock,
		Rewind: controller,
	})
	if err := tl.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	ids := idsOf(events)
	want := append([]string{}, ids...)
	want = append(want, ids[16:]...)

	if len(sink.ids) != len(want) {
		t.Fatalf("emitted %d records, want %d\n got: %v\nwant: %v",
			len(sink.ids), len(want), sink.ids, want)
	}
	for i := range want {
		if sink.ids[i] != want[i] {
			t.Errorf("record %d = %q, want %q", i, sink.ids[i], want[i])
		}
	}
	if tl.Stats().Rewinds != 1 {
		t.Errorf("Stats().Rewinds = %d, want 1", tl.Stats().Rewinds)
	}
}

// TestTailRewindChecksRetentionOnlyOnce covers a moving retention floor.
//
// The server's oldest retained ledger advances with time, so a rewind target
// that was inside the window when the rewind happened can be outside it a
// few pages later. That is not a gap: the stream has moved past the target
// by then, and the records between are long gone either way.
func TestTailRewindChecksRetentionOnlyOnce(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 20)
	srv := fakerpc.NewServer(events,
		fakerpc.WithPageLimit(5),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			// The page right after the rewind is still inside the window.
			1: {OldestLedger: 100},
			// A later page reports a floor that has moved past the rewind
			// target but not past the cursor, which must not be mistaken
			// for a gap.
			2: {OldestLedger: 105},
		}),
	)

	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100, PageLimit: 5})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	controller := stream.NewRewindController()
	sink := &rewindSink{}
	sink.request = func() {
		_ = controller.Request(stream.RewindRequest{
			Distance: stream.RewindDistance{LedgerCount: 3, Unit: stream.RewindLedgers},
		})
	}

	tl := stream.NewTailer(src, sink, stream.Options{
		Clock:  newFakeClock(),
		Rewind: controller,
	})
	if err := tl.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v, want no gap", err)
	}

	if tl.Stats().Gaps != 0 {
		t.Errorf("Stats().Gaps = %d, want 0", tl.Stats().Gaps)
	}
}

// TestTailRewindUsesAnExplicitAnchor checks that a request carrying an
// anchor is measured from that anchor rather than from the stream's own
// position.
func TestTailRewindUsesAnExplicitAnchor(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 20)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(5))

	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100, PageLimit: 5})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	controller := stream.NewRewindController()
	sink := &rewindSink{}
	sink.request = func() {
		_ = controller.Request(stream.RewindRequest{
			Distance: stream.RewindDistance{LedgerCount: 2, Unit: stream.RewindLedgers},
			Anchor:   110,
		})
	}

	tl := stream.NewTailer(src, sink, stream.Options{
		Clock:  newFakeClock(),
		Rewind: controller,
	})
	if err := tl.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	ids := idsOf(events)
	want := append([]string{}, ids[0:5]...)
	want = append(want, ids[8:20]...)

	if len(sink.ids) != len(want) {
		t.Fatalf("emitted %d records, want %d\n got: %v\nwant: %v",
			len(sink.ids), len(want), sink.ids, want)
	}
	for i := range want {
		if sink.ids[i] != want[i] {
			t.Errorf("record %d = %q, want %q", i, sink.ids[i], want[i])
		}
	}
}

// TestTailRewindReportsRetentionGap covers the one way a rewind can lose
// records: landing before the server's retention window. It must be reported
// like any other gap, never skipped over.
func TestTailRewindReportsRetentionGap(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 20)
	// From the second call onward the server claims it has pruned
	// everything before ledger 1_000_000, far past where the rewind lands.
	srv := fakerpc.NewServer(events,
		fakerpc.WithPageLimit(5),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {OldestLedger: 1000000},
		}),
	)

	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100, PageLimit: 5})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	controller := stream.NewRewindController()
	sink := &rewindSink{}
	sink.request = func() {
		_ = controller.Request(stream.RewindRequest{
			Distance: stream.RewindDistance{LedgerCount: 1000000, Unit: stream.RewindLedgers},
		})
	}

	tl := stream.NewTailer(src, sink, stream.Options{
		Clock:  newFakeClock(),
		Rewind: controller,
	})
	runErr := tl.Run(context.Background())

	var gap *stream.GapError
	if !errors.As(runErr, &gap) {
		t.Fatalf("Run() error = %v, want a *stream.GapError", runErr)
	}
	if gap.From != stream.FirstLedger {
		t.Errorf("GapError.From = %d, want %d", gap.From, stream.FirstLedger)
	}
	if gap.Oldest != 1000000 {
		t.Errorf("GapError.Oldest = %d, want 1000000", gap.Oldest)
	}
}

// TestTailRewindNeedsARewinder checks that a source that cannot be
// repositioned fails with a clear message instead of quietly continuing.
// plainSource is the source without a Rewinder, from the gap-recovery tests.
func TestTailRewindNeedsARewinder(t *testing.T) {
	t.Parallel()

	controller := stream.NewRewindController()
	if err := controller.Request(stream.RewindRequest{
		Distance: stream.RewindDistance{LedgerCount: 5, Unit: stream.RewindLedgers},
		Anchor:   100,
	}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}

	tl := stream.NewTailer(plainSource{name: "plain"}, &collector{}, stream.Options{
		Clock:  newFakeClock(),
		Rewind: controller,
	})
	err := tl.Run(context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want a failure")
	}
	if !contains(err.Error(), "Rewinder") {
		t.Errorf("error = %q, want it to mention Rewinder", err.Error())
	}
}

// TestTailRewindNeedsAPosition checks that a rewind requested before the
// stream has reached anything fails clearly rather than guessing a position.
func TestTailRewindNeedsAPosition(t *testing.T) {
	t.Parallel()

	srv := fakerpc.NewServer(nil)
	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	controller := stream.NewRewindController()
	if err := controller.Request(stream.RewindRequest{
		Distance: stream.RewindDistance{LedgerCount: 5, Unit: stream.RewindLedgers},
	}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}

	tl := stream.NewTailer(src, &collector{}, stream.Options{
		Clock:  newFakeClock(),
		Rewind: controller,
	})
	err = tl.Run(context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want a failure")
	}
	if !contains(err.Error(), "position") {
		t.Errorf("error = %q, want it to mention the missing position", err.Error())
	}
}

// TestTailWithoutARewindControllerIsUnchanged checks that a stream with no
// rewind support behaves exactly as before: no rewinds, no repositioning.
func TestTailWithoutARewindControllerIsUnchanged(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 12)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(5))

	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100, PageLimit: 5})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	sink := &collector{}
	tl := stream.NewTailer(src, sink, stream.Options{Clock: newFakeClock()})
	if err := tl.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	assertStreamInvariants(t, sink.ids, idsOf(events))
	if tl.Stats().Rewinds != 0 {
		t.Errorf("Stats().Rewinds = %d, want 0", tl.Stats().Rewinds)
	}
}
