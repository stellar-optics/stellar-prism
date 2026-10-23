package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// TestEventsCommandRewind checks the flag end to end: the stream starts
// before the requested ledger and reprocesses the records in between.
func TestEventsCommandRewind(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 10), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, stderr, err := run(t, "events", "--from", "1005", "--rewind", "3", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	lines := nonEmptyLines(stdout)
	if len(lines) != 8 {
		t.Fatalf("emitted %d records, want 8\n%s", len(lines), stdout)
	}
	if got := cliFirstLedger(t, lines[0]); got != 1002 {
		t.Errorf("first record is at ledger %d, want 1002", got)
	}
	if !strings.Contains(stderr, "rewinding") {
		t.Errorf("stderr does not report the rewind\n%s", stderr)
	}
}

// TestEventsCommandRewindDuration checks that a duration is accepted as well
// as a ledger count, and that it lands on the same place.
func TestEventsCommandRewindDuration(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 10), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, _, err := run(t, "events", "--from", "1005", "--rewind", "15s", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	lines := nonEmptyLines(stdout)
	if len(lines) != 8 {
		t.Fatalf("emitted %d records, want 8\n%s", len(lines), stdout)
	}
	if got := cliFirstLedger(t, lines[0]); got != 1002 {
		t.Errorf("first record is at ledger %d, want 1002", got)
	}
}

// TestEventsCommandRewindClampsAtFirstLedger checks that a rewind reaching
// past the start of the chain lands on the first ledger rather than failing.
func TestEventsCommandRewindClampsAtFirstLedger(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 10), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, stderr, err := run(t, "events", "--from", "1005", "--rewind", "100000", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	lines := nonEmptyLines(stdout)
	if len(lines) != 10 {
		t.Fatalf("emitted %d records, want 10\n%s", len(lines), stdout)
	}
	if got := cliFirstLedger(t, lines[0]); got != 1000 {
		t.Errorf("first record is at ledger %d, want 1000", got)
	}
	if !strings.Contains(stderr, "ledger 1") {
		t.Errorf("stderr does not report the clamped target\n%s", stderr)
	}
}

// TestEventsCommandRejectsInvalidRewind checks that a typo fails loudly
// rather than rewinding by nothing.
func TestEventsCommandRejectsInvalidRewind(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 2))
	withFake(t, srv)

	for _, value := range []string{"soon", "0", "-5"} {
		if _, _, err := run(t, "events", "--from", "1000", "--rewind", value); err == nil {
			t.Errorf("Execute() error = nil for --rewind %s, want a validation failure", value)
		}
	}
}

// TestEventsHelpAdvertisesRewind checks that the flag is discoverable.
func TestEventsHelpAdvertisesRewind(t *testing.T) {
	stdout, _, err := run(t, "events", "--help")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(stdout, "--rewind") {
		t.Error("help does not mention --rewind")
	}
}

// TestRewindWatcherRequestsRewindOnSignal checks that a signal becomes a
// rewind request.
func TestRewindWatcherRequestsRewindOnSignal(t *testing.T) {
	controller := stream.NewRewindController()
	signals := make(chan os.Signal, 1)
	var stderr bytes.Buffer
	watcher := newRewindWatcher(context.Background(), signals, controller,
		stream.RewindDistance{LedgerCount: 5, Unit: stream.RewindLedgers}, &stderr)
	defer watcher.Close()

	signals <- syscall.SIGUSR2

	select {
	case <-watcher.Handled():
	case <-time.After(5 * time.Second):
		t.Fatal("the watcher did not handle the signal")
	}

	if !controller.Pending() {
		t.Error("the watcher did not request a rewind")
	}
}

// TestRewindWatcherStopsWithTheContext checks that the watcher's goroutine
// ends with the command, so a finished run leaves nothing behind.
func TestRewindWatcherStopsWithTheContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	controller := stream.NewRewindController()
	signals := make(chan os.Signal, 1)
	var stderr bytes.Buffer
	watcher := newRewindWatcher(ctx, signals, controller,
		stream.RewindDistance{LedgerCount: 5, Unit: stream.RewindLedgers}, &stderr)

	cancel()

	done := make(chan struct{})
	go func() {
		watcher.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not return; the watcher kept its goroutine")
	}
}

// TestRewindWatcherLeavesNoGoroutineBehind checks that closing the watcher
// leaves nothing running, which is what keeps a finished command from
// leaking a goroutine per run.
func TestRewindWatcherLeavesNoGoroutineBehind(t *testing.T) {
	before := runtime.NumGoroutine()

	controller := stream.NewRewindController()
	signals := make(chan os.Signal, 1)
	var stderr bytes.Buffer
	watcher := newRewindWatcher(context.Background(), signals, controller,
		stream.RewindDistance{LedgerCount: 5, Unit: stream.RewindLedgers}, &stderr)
	watcher.Close()

	// The goroutine returns just after it reports, so give the runtime a
	// moment to reap it.
	for range 100 {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Errorf("goroutine count = %d, want at most %d", after, before)
	}
}

// cliFirstLedger reads the ledger of the first NDJSON record.
func cliFirstLedger(t *testing.T, line string) uint32 {
	t.Helper()

	var rec struct {
		Ledger uint32 `json:"ledger"`
	}
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("not valid JSON: %v\n%s", err, line)
	}
	return rec.Ledger
}
