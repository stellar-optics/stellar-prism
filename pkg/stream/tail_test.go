package stream_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// fakeClock makes the tailer's waits instantaneous and observable, so the
// streaming tests are fast and free of wall-clock flakiness.
type fakeClock struct {
	now    time.Time
	slept  []time.Duration
	onWake func()

	// maxSleeps bounds how long a follow loop may run. Without it, a
	// regression that stops the tailer terminating turns a failing test into
	// a hanging one, which is far harder to diagnose in CI.
	maxSleeps int
	onBudget  func()
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1700000000, 0).UTC()}
}

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.slept = append(c.slept, d)
	c.now = c.now.Add(d)
	if c.onWake != nil {
		c.onWake()
	}
	if c.maxSleeps > 0 && len(c.slept) >= c.maxSleeps && c.onBudget != nil {
		c.onBudget()
	}
	return ctx.Err()
}

// collector records everything emitted, which is what the invariants are
// checked against.
type collector struct {
	ids     []string
	records []stream.Record
	failAt  int
	err     error
}

func (c *collector) Emit(r stream.Record) error {
	if c.failAt > 0 && len(c.ids) == c.failAt {
		return c.err
	}
	c.ids = append(c.ids, r.ID)
	c.records = append(c.records, r)
	return nil
}

func (c *collector) Flush() error { return nil }

// assertStreamInvariants checks the three properties the delivery guarantee
// rests on. Any streaming bug worth catching shows up as a violation of one
// of them, which is why every scenario funnels through this helper rather
// than asserting on ad-hoc output.
func assertStreamInvariants(t *testing.T, got []string, want []string) {
	t.Helper()

	// 1. Strictly increasing: proves ordering was preserved and that a
	//    reordered page could not corrupt the stream.
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Errorf("IDs are not strictly increasing at %d: %q then %q", i, got[i-1], got[i])
		}
	}

	// 2. No duplicates.
	seen := make(map[string]bool, len(got))
	for _, id := range got {
		if seen[id] {
			t.Errorf("duplicate ID emitted: %q", id)
		}
		seen[id] = true
	}

	// 3. Set equality with what was expected: this is the assertion that
	//    actually proves "no gap". Counting records would not.
	if len(got) != len(want) {
		t.Errorf("emitted %d records, want %d\n got: %v\nwant: %v", len(got), len(want), got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("record %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func idsOf(events []protocol.EventInfo) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, e.ID)
	}
	return out
}

// runTail wires a fake server to a tailer and runs it to completion.
func runTail(t *testing.T, srv *fakerpc.Server, opts stream.Options, q stream.EventQuery) (*collector, *fakeClock, error) {
	t.Helper()

	src, err := stream.NewEventSource(srv, q)
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}
	sink := &collector{}
	clock := newFakeClock()
	opts.Clock = clock

	tl := stream.NewTailer(src, sink, opts)
	runErr := tl.Run(context.Background())
	return sink, clock, runErr
}

// TestTailDeliversEveryRecordOnce is the happy path: a clean server must
// yield exactly the universe of events, in order.
func TestTailDeliversEveryRecordOnce(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 25)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(10))

	sink, _, err := runTail(t, srv, stream.Options{}, stream.EventQuery{StartLedger: 100, PageLimit: 10})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertStreamInvariants(t, sink.ids, idsOf(events))
}

func TestTailerFiltersRecordsAndCountsThem(t *testing.T) {
	t.Parallel()
	events := fakerpc.MakeEvents(100, 6)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(10))
	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100, PageLimit: 10})
	if err != nil {
		t.Fatal(err)
	}
	filter, err := stream.CompileExpression(`ledger % 2 == 0`)
	if err != nil {
		t.Fatal(err)
	}
	out := &collector{}
	tailer := stream.NewTailer(src, out, stream.Options{Filter: filter, Clock: newFakeClock()})
	if err := tailer.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, want := tailer.Stats().Filtered, 3; got != want {
		t.Errorf("Filtered = %d, want %d", got, want)
	}
	if got, want := len(out.records), 3; got != want {
		t.Errorf("emitted %d records, want %d", got, want)
	}
}

// TestTailSurvivesDropsWithoutGapOrDuplicate is the central test. Connections
// fail at several points; the stream must still deliver every record exactly
// once, because a failed fetch retries the same cursor.
func TestTailSurvivesDropsWithoutGapOrDuplicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script map[int]fakerpc.Behaviour
	}{
		{
			name:   "drop on first call",
			script: map[int]fakerpc.Behaviour{0: {Err: fakerpc.ErrDropped}},
		},
		{
			name:   "drop mid-stream",
			script: map[int]fakerpc.Behaviour{2: {Err: fakerpc.ErrDropped}},
		},
		{
			name: "consecutive drops",
			script: map[int]fakerpc.Behaviour{
				1: {Err: fakerpc.ErrDropped},
				2: {Err: fakerpc.ErrDropped},
				3: {Err: fakerpc.ErrDropped},
			},
		},
		{
			name: "drops scattered across the stream",
			script: map[int]fakerpc.Behaviour{
				0: {Err: fakerpc.ErrDropped},
				3: {Err: fakerpc.ErrDropped},
				6: {Err: fakerpc.ErrDropped},
				9: {Err: fakerpc.ErrDropped},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			events := fakerpc.MakeEvents(500, 40)
			srv := fakerpc.NewServer(events,
				fakerpc.WithPageLimit(8),
				fakerpc.WithScript(tc.script),
			)

			sink, clock, err := runTail(t, srv,
				stream.Options{}, stream.EventQuery{StartLedger: 500, PageLimit: 8})
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			assertStreamInvariants(t, sink.ids, idsOf(events))

			if len(clock.slept) == 0 {
				t.Error("no backoff was applied despite a dropped connection")
			}
		})
	}
}

// TestTailSuppressesReplayedPages covers a server that re-sends the last page
// after a reconnect, which the RPC docs warn clients to expect.
func TestTailSuppressesReplayedPages(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(200, 30)
	srv := fakerpc.NewServer(events,
		fakerpc.WithPageLimit(10),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {ReplayLast: true},
			3: {ReplayLast: true},
		}),
	)

	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 200, PageLimit: 10})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}
	sink := &collector{}
	tl := stream.NewTailer(src, sink, stream.Options{Clock: newFakeClock()})
	if err := tl.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	assertStreamInvariants(t, sink.ids, idsOf(events))

	if tl.Stats().Duplicates == 0 {
		t.Error("Stats().Duplicates = 0, want the replayed records to be counted as suppressed")
	}
}

// TestTailRejectsOutOfOrderPages covers a server that violates its ordering
// contract. The tailer must never emit a record older than one it has already
// emitted, even at the cost of dropping the offending records.
func TestTailRejectsOutOfOrderPages(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(300, 20)
	srv := fakerpc.NewServer(events,
		fakerpc.WithPageLimit(5),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{2: {Reorder: true}}),
	)

	sink, _, err := runTail(t, srv,
		stream.Options{}, stream.EventQuery{StartLedger: 300, PageLimit: 5})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	// Ordering and uniqueness must hold even though the server misbehaved.
	for i := 1; i < len(sink.ids); i++ {
		if sink.ids[i] <= sink.ids[i-1] {
			t.Fatalf("out-of-order page corrupted the stream at %d: %q then %q",
				i, sink.ids[i-1], sink.ids[i])
		}
	}
	if len(sink.ids) == 0 {
		t.Fatal("no records emitted")
	}
}

// TestTailDetectsRetentionGap covers the one way records can genuinely be
// lost. It must be reported, never passed over.
func TestTailDetectsRetentionGap(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(1000, 30)
	// From the second call onward the server claims it has pruned everything
	// before ledger 1_000_000, far past where the stream is.
	srv := fakerpc.NewServer(events,
		fakerpc.WithPageLimit(5),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {OldestLedger: 1000000},
		}),
	)

	src, srcErr := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 1000, PageLimit: 5})
	if srcErr != nil {
		t.Fatalf("NewEventSource() error = %v", srcErr)
	}
	sink := &collector{}

	// Bound the run: if gap detection ever regresses, this follow loop would
	// otherwise spin forever and hang the suite instead of failing it.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := newFakeClock()
	clock.maxSleeps = 50
	clock.onBudget = cancel

	err := stream.NewTailer(src, sink, stream.Options{Follow: true, Clock: clock}).Run(ctx)

	var gap *stream.GapError
	if !errors.As(err, &gap) {
		t.Fatalf("Run() error = %v, want a *stream.GapError", err)
	}
	if gap.Oldest != 1000000 {
		t.Errorf("GapError.Oldest = %d, want 1000000", gap.Oldest)
	}
	if gap.Source != "events" {
		t.Errorf("GapError.Source = %q, want %q", gap.Source, "events")
	}
	// The error must name the missed range, or it is not actionable.
	for _, want := range []string{"gap", "missed"} {
		if !contains(gap.Error(), want) {
			t.Errorf("GapError text %q does not mention %q", gap.Error(), want)
		}
	}
	// Whatever was delivered before the gap must still be clean.
	assertStreamInvariants(t, sink.ids, idsOf(events)[:len(sink.ids)])
}

// TestTailToleratesLaggingBackend covers a load-balanced RPC pool where one
// backend is behind. Stellar has no reorgs — ledgers are final on close — so
// the realistic hazard is a server reporting an older head, not a rewritten
// history.
func TestTailToleratesLaggingBackend(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(400, 20)
	srv := fakerpc.NewServer(events,
		fakerpc.WithPageLimit(5),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {Empty: true, LatestLedger: 401}, // backend rewinds its view
			2: {Empty: true, LatestLedger: 402},
		}),
	)

	sink, _, err := runTail(t, srv,
		stream.Options{}, stream.EventQuery{StartLedger: 400, PageLimit: 5})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	// An empty page from a lagging backend must not be mistaken for the end
	// of the stream in a way that loses records, nor produce duplicates.
	for i := 1; i < len(sink.ids); i++ {
		if sink.ids[i] <= sink.ids[i-1] {
			t.Fatalf("lagging backend corrupted ordering at %d", i)
		}
	}
}

// TestTailStopsOnPermanentError checks that an unfixable failure aborts
// instead of retrying forever.
func TestTailStopsOnPermanentError(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 5)
	srv := fakerpc.NewServer(events,
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			0: {Err: errors.New("filter type invalid: bad request")},
		}),
	)

	_, clock, err := runTail(t, srv,
		stream.Options{}, stream.EventQuery{StartLedger: 100})
	if err == nil {
		t.Fatal("Run() error = nil, want a permanent failure")
	}
	if len(clock.slept) != 0 {
		t.Errorf("backed off %d time(s) on a permanent error, want 0", len(clock.slept))
	}
}

// TestTailGivesUpAfterMaxRetries checks the bound on consecutive failures.
func TestTailGivesUpAfterMaxRetries(t *testing.T) {
	t.Parallel()

	script := map[int]fakerpc.Behaviour{}
	for i := range 20 {
		script[i] = fakerpc.Behaviour{Err: fakerpc.ErrDropped}
	}
	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 5), fakerpc.WithScript(script))

	_, _, err := runTail(t, srv,
		stream.Options{MaxRetries: 3}, stream.EventQuery{StartLedger: 100})
	if err == nil {
		t.Fatal("Run() error = nil, want failure after exhausting retries")
	}
	if !contains(err.Error(), "giving up") {
		t.Errorf("error = %q, want it to say it gave up", err.Error())
	}
}

// TestTailRespectsLimit checks that --limit stops the stream promptly.
func TestTailRespectsLimit(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 50)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(10))

	sink, _, err := runTail(t, srv,
		stream.Options{Limit: 7}, stream.EventQuery{StartLedger: 100, PageLimit: 10})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertStreamInvariants(t, sink.ids, idsOf(events)[:7])
}

// TestTailFollowStopsOnCancel checks that Ctrl-C during a follow is a clean
// stop rather than an error.
func TestTailFollowStopsOnCancel(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 5)
	srv := fakerpc.NewServer(events,
		fakerpc.WithPageLimit(10),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {Empty: true}, 2: {Empty: true}, 3: {Empty: true},
		}),
	)

	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100, PageLimit: 10})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	clock := newFakeClock()
	// Cancel from inside the first poll wait, which is where a follow spends
	// almost all of its time.
	clock.onWake = func() { cancel() }

	sink := &collector{}
	tl := stream.NewTailer(src, sink, stream.Options{Follow: true, Clock: clock})

	if err := tl.Run(ctx); err != nil {
		t.Fatalf("Run() after cancellation = %v, want nil (a clean stop)", err)
	}
	assertStreamInvariants(t, sink.ids, idsOf(events))
}

// TestTailPropagatesSinkErrors checks that a failing consumer stops the
// stream rather than being silently ignored.
func TestTailPropagatesSinkErrors(t *testing.T) {
	t.Parallel()

	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 20), fakerpc.WithPageLimit(10))
	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100, PageLimit: 10})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	wantErr := errors.New("broken pipe downstream")
	sink := &collector{failAt: 3, err: wantErr}
	tl := stream.NewTailer(src, sink, stream.Options{Clock: newFakeClock()})

	runErr := tl.Run(context.Background())
	if !errors.Is(runErr, wantErr) {
		t.Fatalf("Run() error = %v, want it to wrap %v", runErr, wantErr)
	}
	if len(sink.ids) != 3 {
		t.Errorf("emitted %d records before the sink failed, want 3", len(sink.ids))
	}
}

// TestTailDrainsFullPagesWithoutWaiting checks the backfill path: when a page
// comes back full, the tailer should fetch again immediately rather than
// sleeping a poll interval per page.
func TestTailDrainsFullPagesWithoutWaiting(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 100)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(10))

	sink, clock, err := runTail(t, srv,
		stream.Options{}, stream.EventQuery{StartLedger: 100, PageLimit: 10})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertStreamInvariants(t, sink.ids, idsOf(events))

	if len(clock.slept) != 0 {
		t.Errorf("slept %d time(s) while draining full pages, want 0", len(clock.slept))
	}
}

// TestTailCursorAdvances checks that the resumable position tracks the stream.
func TestTailCursorAdvances(t *testing.T) {
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

	if got, want := tl.Cursor(), events[len(events)-1].ID; got != want {
		t.Errorf("Cursor() = %q, want the last emitted ID %q", got, want)
	}
	if tl.Stats().Emitted != len(events) {
		t.Errorf("Stats().Emitted = %d, want %d", tl.Stats().Emitted, len(events))
	}
}

// TestTailLedgerStream exercises the second Source implementation through the
// same state machine, which is the check that the Source seam is real.
func TestTailLedgerStream(t *testing.T) {
	t.Parallel()

	ledgers := fakerpc.MakeLedgers(700, 25)
	srv := fakerpc.NewLedgerServer(ledgers,
		fakerpc.WithPageLimit(6),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			2: {Err: fakerpc.ErrDropped},
		}),
	)

	src := stream.NewLedgerSource(srv, stream.LedgerQuery{StartLedger: 700, PageLimit: 6})
	sink := &collector{}
	tl := stream.NewTailer(src, sink, stream.Options{Clock: newFakeClock()})
	if err := tl.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	want := make([]string, 0, len(ledgers))
	for _, l := range ledgers {
		want = append(want, fmt.Sprintf("%010d", l.Sequence))
	}
	assertStreamInvariants(t, sink.ids, want)
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && stringsContains(haystack, needle)
}

func stringsContains(h, n string) bool {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return true
		}
	}
	return false
}

func TestTailStopsAtUntilLedger(t *testing.T) {
	t.Parallel()

	srv := fakerpc.NewServer(
		fakerpc.MakeEvents(100, 25),
		fakerpc.WithPageLimit(10),
	)
	sink, _, err := runTail(t, srv, stream.Options{
		UntilLedger: 105,
	}, stream.EventQuery{StartLedger: 100, PageLimit: 10})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, record := range sink.records {
		if record.Sequence > 105 {
			t.Fatalf("emitted ledger %d after until-ledger 105", record.Sequence)
		}
	}
// --- Adaptive poller: ledger close time estimation ---

// observeCloses feeds the estimator a run of consecutive ledgers closing at
// the given intervals, each observed promptly, and returns the last close.
func observeCloses(est *stream.LedgerCloseEstimator, first uint32, at time.Time, intervals ...time.Duration) (uint32, time.Time) {
	ledger := first
	est.Observe(ledger, at, at)
	for _, d := range intervals {
		ledger++
		at = at.Add(d)
		est.Observe(ledger, at, at)
	}
	return ledger, at
}

func repeatDuration(d time.Duration, n int) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = d
	}
	return out
}

func approxDuration(got, want, tolerance time.Duration) bool {
	diff := got - want
	if diff < 0 {
		diff = -diff
	}
	return diff <= tolerance
}

// TestLedgerCloseEstimatorTracksFluctuatingIntervals pins the EMA: it starts
// from the nominal cadence, converges on a steady interval, and follows a
// stretch from 5s to 8s within about one ten-ledger window, then recovers.
func TestLedgerCloseEstimatorTracksFluctuatingIntervals(t *testing.T) {
	t.Parallel()

	est := stream.NewLedgerCloseEstimator(0, 0, 0)
	if got := est.Interval(); got != stream.RoughLedgerCloseInterval {
		t.Fatalf("Interval() before any close = %v, want %v", got, stream.RoughLedgerCloseInterval)
	}

	start := time.Unix(1_700_000_000, 0).UTC()
	ledger, at := observeCloses(est, 100, start, repeatDuration(5*time.Second, 20)...)
	if got := est.Interval(); !approxDuration(got, 5*time.Second, time.Millisecond) {
		t.Fatalf("Interval() after steady 5s closes = %v, want 5s", got)
	}

	// Validator delays stretch closes to 8s. After one EMA window the
	// estimate must have moved most of the way there (1-(9/11)^10 ≈ 87%).
	ledger, at = observeCloses(est, ledger, at, repeatDuration(8*time.Second, 10)...)
	if got := est.Interval(); got < 7*time.Second || got > 8*time.Second {
		t.Fatalf("Interval() after ten 8s closes = %v, want in [7s, 8s]", got)
	}

	// The network recovers to 4s closes; the estimate follows back down.
	_, _ = observeCloses(est, ledger, at, repeatDuration(4*time.Second, 20)...)
	if got := est.Interval(); got < 4*time.Second || got > 4200*time.Millisecond {
		t.Fatalf("Interval() after twenty 4s closes = %v, want ~4s", got)
	}
}

// TestLedgerCloseEstimatorSkippedLedgersAverageOverDelta checks a poll that
// sees several new ledgers at once divides the elapsed time between them
// rather than treating it as one long interval.
func TestLedgerCloseEstimatorSkippedLedgersAverageOverDelta(t *testing.T) {
	t.Parallel()

	est := stream.NewLedgerCloseEstimator(0, 0, 0)
	start := time.Unix(1_700_000_000, 0).UTC()
	est.Observe(10, start, start)
	est.Observe(13, start.Add(18*time.Second), start.Add(18*time.Second))
	if got := est.Interval(); got != 6*time.Second {
		t.Fatalf("Interval() = %v, want 6s (18s over 3 ledgers)", got)
	}
}

// TestLedgerCloseEstimatorSleepSubtractsLatency checks the scheduling
// formula: sleepTime = nextClose - now - estimatedRpcLatency.
func TestLedgerCloseEstimatorSleepSubtractsLatency(t *testing.T) {
	t.Parallel()

	est := stream.NewLedgerCloseEstimator(0, 0, 0)
	est.ObserveLatency(200 * time.Millisecond)
	start := time.Unix(1_700_000_000, 0).UTC()
	_, last := observeCloses(est, 1, start, repeatDuration(5*time.Second, 10)...)

	if got, want := est.NextClose(), last.Add(5*time.Second); !approxDuration(got.Sub(want), 0, time.Millisecond) {
		t.Fatalf("NextClose() = %v, want %v", got, want)
	}
	now := last.Add(time.Second)
	// 5s after the last close, minus the 1s already elapsed and the 200ms
	// round trip. The self-tuning margin may have crept a little earlier
	// after ten on-time ledgers, never by more than marginStep (250ms) per
	// four hits.
	want := 3800 * time.Millisecond
	if got := est.Sleep(now); got > want || got < want-time.Second {
		t.Fatalf("Sleep() = %v, want just under %v", got, want)
	}
}

// TestLedgerCloseEstimatorSleepClamps checks both bounds: a stalled network
// cannot park the stream for longer than 10s, and an overdue ledger is
// retried no sooner than 500ms, backing off on consecutive empty polls.
func TestLedgerCloseEstimatorSleepClamps(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0).UTC()

	t.Run("long intervals clamp to max", func(t *testing.T) {
		est := stream.NewLedgerCloseEstimator(0, 0, 0)
		_, last := observeCloses(est, 1, start, repeatDuration(30*time.Second, 5)...)
		if got := est.Sleep(last); got != stream.DefaultCloseEstimateMaxSleep {
			t.Fatalf("Sleep() = %v, want %v", got, stream.DefaultCloseEstimateMaxSleep)
		}
	})

	t.Run("overdue ledger clamps to min then backs off", func(t *testing.T) {
		est := stream.NewLedgerCloseEstimator(0, 0, 0)
		ledger, last := observeCloses(est, 1, start, repeatDuration(5*time.Second, 10)...)
		now := last.Add(6 * time.Second) // the next ledger is a second late
		if got := est.Sleep(now); got != stream.DefaultCloseEstimateMinSleep {
			t.Fatalf("Sleep() overdue = %v, want %v", got, stream.DefaultCloseEstimateMinSleep)
		}
		var prev time.Duration
		for i := 0; i < 5; i++ {
			est.Observe(ledger, last, now) // still no new ledger
			got := est.Sleep(now)
			if got < stream.DefaultCloseEstimateMinSleep || got > est.Interval() {
				t.Fatalf("miss %d: Sleep() = %v, want within [%v, %v]", i+1, got, stream.DefaultCloseEstimateMinSleep, est.Interval())
			}
			if got < prev {
				t.Fatalf("miss %d: Sleep() = %v shrank from %v; consecutive misses must back off", i+1, got, prev)
			}
			prev = got
			now = now.Add(got)
		}
		if prev <= stream.DefaultCloseEstimateMinSleep {
			t.Fatalf("Sleep() after repeated misses = %v, want growth beyond %v", prev, stream.DefaultCloseEstimateMinSleep)
		}
	})

	t.Run("custom bounds are honoured", func(t *testing.T) {
		est := stream.NewLedgerCloseEstimator(0, 2*time.Second, 3*time.Second)
		_, last := observeCloses(est, 1, start, repeatDuration(30*time.Second, 5)...)
		if got := est.Sleep(last); got != 3*time.Second {
			t.Fatalf("Sleep() = %v, want 3s", got)
		}
		if got := est.Sleep(last.Add(time.Hour)); got < 2*time.Second {
			t.Fatalf("Sleep() overdue = %v, want at least 2s", got)
		}
	})
}

type pollSimStats struct {
	polls, empty int
	// staleness is the mean delay between a ledger becoming available on
	// the RPC and the poller first seeing it.
	staleness time.Duration
}

// simulateLedgerPolling replays two hours of a network whose close interval
// fluctuates between calm (4-6s) and congested (7-9s) phases against a
// polling strategy. The RPC serves each ledger 300ms after it closes, reports
// close times truncated to whole seconds as the real API does, and answers
// each request in 120ms.
func simulateLedgerPolling(seed uint64, next func(head uint32, closedAt, observedAt time.Time) time.Duration) pollSimStats {
	const (
		ingest  = 300 * time.Millisecond
		latency = 120 * time.Millisecond
	)
	rng := rand.New(rand.NewPCG(seed, seed*7+1)) //nolint:gosec // deterministic test data
	start := time.Unix(1_700_000_000, 0).UTC()
	var closes []time.Time
	for at := start; at.Sub(start) < 2*time.Hour; {
		lo, hi := 4.0, 6.0
		if int(at.Sub(start)/(15*time.Minute))%3 == 1 {
			lo, hi = 7.0, 9.0
		}
		at = at.Add(time.Duration((lo + rng.Float64()*(hi-lo)) * float64(time.Second)))
		closes = append(closes, at)
	}
	end := closes[len(closes)-1]

	var (
		stats      pollSimStats
		prev       uint32
		staleTotal time.Duration
		seen       int
		available  int // closes[:available] are served at the current time
	)
	for now := start; now.Before(end); {
		served := now.Add(latency / 2)
		for available < len(closes) && !closes[available].Add(ingest).After(served) {
			available++
		}
		head := uint32(available) //nolint:gosec // bounded by the simulation length
		var closedAt time.Time
		if available > 0 {
			closedAt = closes[available-1].Truncate(time.Second)
		}
		observed := now.Add(latency)
		stats.polls++
		if head == prev {
			stats.empty++
		} else if prev != 0 {
			for l := prev + 1; l <= head; l++ {
				staleTotal += observed.Sub(closes[l-1].Add(ingest))
				seen++
			}
		}
		prev = head
		now = observed.Add(next(head, closedAt, observed))
	}
	if seen > 0 {
		stats.staleness = staleTotal / time.Duration(seen)
	}
	return stats
}

// TestLedgerCloseEstimatorReducesEmptyPolls is the acceptance check for the
// EMA scheduler: against the ledger-count poller it replaces, it must cut
// polls that find no new ledger by more than 40% across fluctuating close
// intervals, while still seeing each ledger well within one close interval.
func TestLedgerCloseEstimatorReducesEmptyPolls(t *testing.T) {
	t.Parallel()

	for _, seed := range []uint64{1, 2, 3} {
		oldPoller := stream.NewAdaptivePoller(0, 0, 2)
		var oldPrev uint32
		baseline := simulateLedgerPolling(seed, func(head uint32, _, _ time.Time) time.Duration {
			n := 0
			if oldPrev != 0 && head > oldPrev {
				n = int(head - oldPrev)
			}
			oldPrev = head
			return oldPoller.Observe(n)
		})

		est := stream.NewLedgerCloseEstimator(0, 0, 0)
		ema := simulateLedgerPolling(seed, func(head uint32, closedAt, observedAt time.Time) time.Duration {
			est.ObserveLatency(120 * time.Millisecond)
			est.Observe(head, closedAt, observedAt)
			return est.Sleep(observedAt)
		})

		reduction := 1 - float64(ema.empty)/float64(baseline.empty)
		t.Logf("seed %d: baseline %d polls (%d empty, staleness %v); EMA %d polls (%d empty, staleness %v); empty-poll reduction %.1f%%",
			seed, baseline.polls, baseline.empty, baseline.staleness.Round(time.Millisecond),
			ema.polls, ema.empty, ema.staleness.Round(time.Millisecond), 100*reduction)
		if reduction <= 0.40 {
			t.Errorf("seed %d: empty-poll reduction %.1f%%, want > 40%%", seed, 100*reduction)
		}
		if ema.staleness > 2*time.Second {
			t.Errorf("seed %d: mean staleness %v, want under 2s", seed, ema.staleness)
		}
	}
}

// headSource is a Source whose head advances with the fake clock at a fixed
// cadence and reports the close time of each ledger, standing in for an
// idle contract on a live network.
type headSource struct {
	clock    *fakeClock
	start    time.Time
	interval time.Duration
	lastHead uint32
	fetches  int
	empty    int
}

func (s *headSource) Name() string { return "head" }

func (s *headSource) CursorLedger(string) (uint32, bool) { return 0, false }

func (s *headSource) Fetch(context.Context, string) (stream.Batch, error) {
	elapsed := s.clock.Now().Sub(s.start)
	head := uint32(elapsed/s.interval) + 1 //nolint:gosec // bounded test duration
	s.fetches++
	if head == s.lastHead {
		s.empty++
	}
	s.lastHead = head
	return stream.Batch{
		Records:         []stream.Record{},
		NextCursor:      "cursor",
		Latest:          head,
		LatestCloseTime: s.start.Add(time.Duration(head-1) * s.interval),
	}, nil
}

// TestTailAdaptiveSchedulesPollsAtLedgerCloses runs the real tailer with
// --adaptive against a head advancing every 6s and checks it settles into
// roughly one poll per ledger, with every wait inside the 500ms-10s clamp.
func TestTailAdaptiveSchedulesPollsAtLedgerCloses(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	src := &headSource{clock: clock, start: clock.Now(), interval: 6 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock.maxSleeps = 200
	clock.onBudget = cancel

	err := stream.NewTailer(src, &collector{}, stream.Options{Follow: true, Adaptive: true, Clock: clock}).Run(ctx)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for i, d := range clock.slept {
		if d < stream.DefaultCloseEstimateMinSleep || d > stream.DefaultCloseEstimateMaxSleep {
			t.Fatalf("wait %d = %v, outside [%v, %v]", i, d, stream.DefaultCloseEstimateMinSleep, stream.DefaultCloseEstimateMaxSleep)
		}
	}
	if frac := float64(src.empty) / float64(src.fetches); frac > 0.25 {
		t.Fatalf("%d of %d polls found no new ledger (%.0f%%), want at most 25%%", src.empty, src.fetches, 100*frac)
	}
}
