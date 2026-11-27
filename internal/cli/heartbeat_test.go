package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
)

// recordingClock stands in for the tailer's Clock: it returns immediately,
// recording what it was asked to wait for, so the slicing is asserted
// without real delays.
type recordingClock struct {
	waits   []time.Duration
	onSleep func()
}

func (c *recordingClock) Now() time.Time { return time.Time{} }

func (c *recordingClock) Sleep(ctx context.Context, d time.Duration) error {
	c.waits = append(c.waits, d)
	if c.onSleep != nil {
		c.onSleep()
	}
	return ctx.Err()
}

// TestNewHeartbeatDisabled covers the flag being off: no reporter, and
// every method a nil-safe no-op, so the wiring in runStream does not branch.
func TestNewHeartbeatDisabled(t *testing.T) {
	var buf bytes.Buffer
	beat := newHeartbeat(0, &buf)
	if beat != nil {
		t.Fatalf("newHeartbeat(0) = %v, want nil", beat)
	}
	if beat := newHeartbeat(-time.Second, &buf); beat != nil {
		t.Fatalf("newHeartbeat(-1s) = %v, want nil", beat)
	}

	beat.mark()
	beat.slept(30 * time.Second)
	if buf.Len() != 0 {
		t.Errorf("a disabled heartbeat wrote %q, want nothing", buf.String())
	}
}

// TestHeartbeatBeatsOncePerInterval checks the line itself: one per
// interval crossed, carrying the remainder, and printing the idle total it
// crossed rather than a bare dot.
func TestHeartbeatBeatsOncePerInterval(t *testing.T) {
	var buf bytes.Buffer
	beat := newHeartbeat(30*time.Second, &buf)

	// A wait below the interval beats nothing.
	beat.slept(10 * time.Second)
	if buf.Len() != 0 {
		t.Fatalf("slept(10s) wrote %q before the interval, want nothing", buf.String())
	}

	// Consecutive short waits beat at their cumulative total.
	beat.slept(10 * time.Second)
	beat.slept(10 * time.Second)
	const want = "prism: heartbeat: idle for 30s with no records\n"
	if got := buf.String(); got != want {
		t.Errorf("after three short waits got %q, want %q", got, want)
	}

	// A long wait beats once per interval crossed, not once at the end.
	buf.Reset()
	beat.slept(45 * time.Second)
	if got := buf.String(); got != "prism: heartbeat: idle for 45s with no records\n" {
		t.Errorf("slept(45s) got %q, want the idle total", got)
	}
}

// TestHeartbeatMarkRestartsTheWindow covers the idle side of the contract:
// a delivered page means the stream is not idle however long it then waits.
func TestHeartbeatMarkRestartsTheWindow(t *testing.T) {
	var buf bytes.Buffer
	beat := newHeartbeat(30*time.Second, &buf)

	beat.slept(20 * time.Second)
	beat.mark()
	beat.slept(20 * time.Second)
	if buf.Len() != 0 {
		t.Fatalf("a mark did not restart the idle window: %q", buf.String())
	}

	beat.slept(10 * time.Second)
	const want = "prism: heartbeat: idle for 30s with no records\n"
	if got := buf.String(); got != want {
		t.Errorf("after the mark got %q, want %q", got, want)
	}
}

// TestHeartbeatClockSlicesWaits checks the mechanism: each wait is cut at
// the interval boundary so the beat lands while the stream is waiting,
// which is what separates a quiet contract from a hung process.
func TestHeartbeatClockSlicesWaits(t *testing.T) {
	var buf bytes.Buffer
	beat := newHeartbeat(30*time.Second, &buf)
	clk := &recordingClock{}
	clock := &heartbeatClock{clock: clk, beat: beat}

	if err := clock.Sleep(context.Background(), 65*time.Second); err != nil {
		t.Fatalf("Sleep() error = %v", err)
	}
	if got, want := clk.waits, []time.Duration{30 * time.Second, 30 * time.Second, 5 * time.Second}; !slices.Equal(got, want) {
		t.Errorf("Sleep(65s) waited %v, want slices %v", got, want)
	}
	if n := strings.Count(buf.String(), "prism: heartbeat:"); n != 2 {
		t.Errorf("Sleep(65s) printed %d beats, want 2\n%s", n, buf.String())
	}

	// A wait shorter than the interval is one piece: nothing to beat in.
	buf.Reset()
	clk.waits = nil
	if err := clock.Sleep(context.Background(), 10*time.Second); err != nil {
		t.Fatalf("Sleep() error = %v", err)
	}
	if got, want := clk.waits, []time.Duration{10 * time.Second}; !slices.Equal(got, want) {
		t.Errorf("Sleep(10s) waited %v, want %v", got, want)
	}
	if buf.Len() != 0 {
		t.Errorf("Sleep(10s) wrote %q, want nothing", buf.String())
	}
}

// TestHeartbeatClockStopsOnCancellation checks that slicing does not delay
// Ctrl-C: a cancelled context interrupts the wait at the next slice, the
// error is passed through untouched, and no beat is printed for a wait that
// never finished.
func TestHeartbeatClockStopsOnCancellation(t *testing.T) {
	var buf bytes.Buffer
	beat := newHeartbeat(30*time.Second, &buf)
	clk := &recordingClock{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	clk.onSleep = func() {
		calls++
		if calls == 2 {
			cancel()
		}
	}
	clock := &heartbeatClock{clock: clk, beat: beat}

	err := clock.Sleep(ctx, 65*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep() error = %v, want context.Canceled", err)
	}
	if got, want := clk.waits, []time.Duration{30 * time.Second, 30 * time.Second}; !slices.Equal(got, want) {
		t.Errorf("Sleep() waited %v, want %v before the cancellation", got, want)
	}
	if n := strings.Count(buf.String(), "prism: heartbeat:"); n != 1 {
		t.Errorf("a cancelled wait printed %d beats, want 1\n%s", n, buf.String())
	}
}

// TestHeartbeatClockPassesShortWaitsThrough checks that a non-positive wait
// keeps the wrapped clock's exact semantics — including how a cancelled
// context is reported — and that a clock with no beat slices nothing.
func TestHeartbeatClockPassesShortWaitsThrough(t *testing.T) {
	clk := &recordingClock{}
	clock := &heartbeatClock{clock: clk, beat: newHeartbeat(30*time.Second, &bytes.Buffer{})}

	for _, d := range []time.Duration{0, -time.Second} {
		if err := clock.Sleep(context.Background(), d); err != nil {
			t.Errorf("Sleep(%v) error = %v", d, err)
		}
	}
	if got, want := clk.waits, []time.Duration{0, -time.Second}; !slices.Equal(got, want) {
		t.Errorf("waits = %v, want the short waits passed through unchanged: %v", got, want)
	}

	// Without a beat the clock is a pure pass-through.
	clk.waits = nil
	plain := &heartbeatClock{clock: clk, beat: nil}
	if got := plain.Now(); !got.Equal(clk.Now()) {
		t.Errorf("Now() = %v, want the wrapped clock's %v", got, clk.Now())
	}
	if err := plain.Sleep(context.Background(), 45*time.Second); err != nil {
		t.Fatalf("Sleep() error = %v", err)
	}
	if got, want := clk.waits, []time.Duration{45 * time.Second}; !slices.Equal(got, want) {
		t.Errorf("waits = %v, want %v", got, want)
	}
}

// TestEventsCommandHeartbeatBeatsWhileIdle is the feature's contract: once
// the server has nothing more to give, a --follow run with --heartbeat
// keeps saying so on stderr, and the record stream on stdout is untouched.
func TestEventsCommandHeartbeatBeatsWhileIdle(t *testing.T) {
	// The signal is delivered to this process, so its disposition is
	// claimed before anything else happens: without a handler the runtime
	// would terminate the test binary.
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, syscall.SIGTERM)
	defer signal.Stop(interrupts)

	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout := &safeBuffer{}
	stderr := &safeBuffer{}
	done := make(chan error, 1)
	go func() {
		done <- Execute([]string{
			"events",
			"--from", "1000",
			"--json",
			"--follow",
			"--poll", "5ms",
			"--heartbeat", "10ms",
		}, stdout, stderr)
	}()

	// The first records prove the run is streaming; the heartbeat must
	// then beat, because the server has nothing more to give.
	waitForOutput(t, stdout, `"ledger":1001`)
	waitForOutput(t, stderr, "prism: heartbeat: idle for 10ms with no records")

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("Kill() error = %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute() error = %v, want a clean shutdown", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the command did not stop after the signal")
	}

	if text := stdout.String(); strings.Contains(text, "prism: heartbeat:") {
		t.Errorf("the heartbeat leaked into stdout\n%s", text)
	}
}

// TestEventsCommandHeartbeatOffByDefault guards backward compatibility: a
// follow that did not ask for heartbeats prints nothing new.
func TestEventsCommandHeartbeatOffByDefault(t *testing.T) {
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, syscall.SIGTERM)
	defer signal.Stop(interrupts)

	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout := &safeBuffer{}
	stderr := &safeBuffer{}
	done := make(chan error, 1)
	go func() {
		done <- Execute([]string{
			"events", "--from", "1000", "--json", "--follow", "--poll", "5ms",
		}, stdout, stderr)
	}()

	waitForOutput(t, stdout, `"ledger":1001`)

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("Kill() error = %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute() error = %v, want a clean shutdown", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the command did not stop after the signal")
	}

	if text := stderr.String(); strings.Contains(text, "prism: heartbeat:") {
		t.Errorf("stderr holds a heartbeat without --heartbeat\n%s", text)
	}
}

// TestEventsCommandHeartbeatStillRecordsTheCursor covers the seam the beat
// is wired through: OnPage already records the cursor for --state-file, and
// marking idle must not break it.
func TestEventsCommandHeartbeatStillRecordsTheCursor(t *testing.T) {
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, syscall.SIGTERM)
	defer signal.Stop(interrupts)

	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	stdout := &safeBuffer{}
	stderr := &safeBuffer{}
	done := make(chan error, 1)
	go func() {
		done <- Execute([]string{
			"events",
			"--from", "1000",
			"--json",
			"--follow",
			"--poll", "5ms",
			"--heartbeat", "10ms",
			"--state-file", stateFile,
		}, stdout, stderr)
	}()

	waitForOutput(t, stderr, "prism: heartbeat: idle for 10ms with no records")

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("Kill() error = %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute() error = %v, want a clean shutdown", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the command did not stop after the signal")
	}

	data, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("reading the state file: %v", err)
	}
	var found bool
	for _, e := range committedCursors(t, data) {
		found = true
		if e.Ledger != 1002 {
			t.Errorf("the committed cursor is at ledger %d, want 1002", e.Ledger)
		}
	}
	if !found {
		t.Errorf("the state file holds no cursor\n%s", data)
	}
}

// TestEventsCommandRejectsBadHeartbeat covers the flag surface: a negative
// interval is a mistake, and an interval without --follow can never beat.
func TestEventsCommandRejectsBadHeartbeat(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 2))
	withFake(t, srv)

	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "negative interval",
			args: []string{"events", "--from", "100", "--follow", "--heartbeat=-1s"},
			want: "--heartbeat must not be negative",
		},
		{
			name: "interval without --follow",
			args: []string{"events", "--from", "100", "--heartbeat", "5s"},
			want: "--heartbeat requires --follow",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := run(t, tc.args...)
			if err == nil {
				t.Fatal("Execute() error = nil, want a validation failure")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Execute() error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// TestEventsHelpAdvertisesTheHeartbeatFlag checks the flag is discoverable.
func TestEventsHelpAdvertisesTheHeartbeatFlag(t *testing.T) {
	stdout, _, err := run(t, "events", "--help")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, want := range []string{"--heartbeat", "waiting in silence"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("help does not mention %q", want)
		}
	}
}
