package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func cliContractID(suffix byte) string {
	data := make([]byte, 32)
	data[31] = suffix
	return strkey.MustEncode(strkey.VersionByteContract, data)
}

func writeWatchlist(t *testing.T, ids ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "watchlist.txt")
	if err := os.WriteFile(path, []byte("# watched contracts\n"+strings.Join(ids, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestEventsWatchlistFlagStreamsWatchedContracts checks --watchlist loads the
// file, announces it, and lets the watched contract's events through.
func TestEventsWatchlistFlagStreamsWatchedContracts(t *testing.T) {
	watched := cliContractID(5)
	events := fakerpc.MakeEvents(8100, 4)
	for i := range events {
		events[i].ContractID = watched
	}
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(10))
	withFake(t, srv)
	path := writeWatchlist(t, watched, cliContractID(9))

	stdout, stderr, err := run(t, "events", "--from", "8100", "--json", "--watchlist", path)
	if err != nil {
		t.Fatalf("Execute() error = %v\n%s", err, stderr)
	}
	if got := len(nonEmptyLines(stdout)); got != 4 {
		t.Errorf("emitted %d records, want 4", got)
	}
	if !strings.Contains(stderr, "watching 2 contracts from "+path) {
		t.Errorf("stderr does not announce the watchlist:\n%s", stderr)
	}
}

// TestEventsWatchlistFilterDropsUnwatchedContracts checks the client-side
// half of the filter: events from a contract outside the list never reach
// the output, whatever the server sends.
func TestEventsWatchlistFilterDropsUnwatchedContracts(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(8200, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)
	path := writeWatchlist(t, cliContractID(7))

	stdout, stderr, err := run(t, "events", "--from", "8200", "--json", "--watchlist", path)
	if err != nil {
		t.Fatalf("Execute() error = %v\n%s", err, stderr)
	}
	if got := len(nonEmptyLines(stdout)); got != 0 {
		t.Errorf("emitted %d records from an unwatched contract, want 0:\n%s", got, stdout)
	}
}

func TestEventsWatchlistFlagConflicts(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(8300, 1))
	withFake(t, srv)
	a := writeWatchlist(t, cliContractID(1))
	b := writeWatchlist(t, cliContractID(2))

	if _, _, err := run(t, "events", "--watchlist", a, "--contracts-file", b); err == nil ||
		!strings.Contains(err.Error(), "different files") {
		t.Errorf("--watchlist with a different --contracts-file: err = %v, want a conflict", err)
	}
	if _, _, err := run(t, "events", "--watchlist", a, "--contract", cliContractID(3)); err == nil ||
		!strings.Contains(err.Error(), "--contract cannot be combined with --watchlist") {
		t.Errorf("--watchlist with --contract: err = %v, want a conflict", err)
	}
}

func TestResolveWatchlistFlag(t *testing.T) {
	cases := []struct {
		watchlist, contractsFile, want string
		wantErr                        bool
	}{
		{"", "", "", false},
		{"a.txt", "", "a.txt", false},
		{"", "b.txt", "b.txt", false},
		{"a.txt", "a.txt", "a.txt", false},
		{"a.txt", "b.txt", "", true},
	}
	for _, tc := range cases {
		got, err := resolveWatchlistFlag(tc.watchlist, tc.contractsFile)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("resolveWatchlistFlag(%q, %q) = %q, %v; want %q, err=%v",
				tc.watchlist, tc.contractsFile, got, err, tc.want, tc.wantErr)
		}
	}
}

func TestLogWatchlistChangeTruncatesLongLists(t *testing.T) {
	added := make([]string, watchlistLogLimit+3)
	for i := range added {
		added[i] = cliContractID(byte(i + 1))
	}
	var buf bytes.Buffer
	logWatchlistChange(&buf, "wl.txt", stream.WatchlistChange{Added: added, Removed: []string{cliContractID(99)}, Total: len(added)})
	out := buf.String()
	for _, want := range []string{
		"watchlist wl.txt reloaded: +13 added, -1 removed, now watching 13 contracts",
		"+ " + added[0],
		"+ ... and 3 more",
		"- " + cliContractID(99),
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log is missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, added[watchlistLogLimit]) {
		t.Errorf("log lists more than %d additions:\n%s", watchlistLogLimit, out)
	}

	buf.Reset()
	logWatchlistChange(&buf, "wl.txt", stream.WatchlistChange{Removed: []string{cliContractID(1)}, Total: 0})
	if !strings.Contains(buf.String(), "watchlist is empty") {
		t.Errorf("emptying the watchlist is not called out:\n%s", buf.String())
	}
}

// TestEventsAggregatePrintsWindowSummary is the end-to-end check for
// `prism events --aggregate --window 1m`: events are summarised rather than
// printed, and a bounded run ends with the window it saw.
func TestEventsAggregatePrintsWindowSummary(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(8400, 6), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, stderr, err := run(t, "events", "--from", "8400", "--json", "--aggregate", "--window", "1m")
	if err != nil {
		t.Fatalf("Execute() error = %v\n%s", err, stderr)
	}
	lines := nonEmptyLines(stdout)
	if len(lines) != 1 {
		t.Fatalf("got %d output lines, want exactly one final summary:\n%s", len(lines), stdout)
	}
	var got windowSummaryJSON
	if err := json.Unmarshal([]byte(lines[0]), &got); err != nil {
		t.Fatalf("summary is not JSON: %v\n%s", err, lines[0])
	}
	// MakeEvents closes one ledger per second, so all six fall in a minute.
	if got.Type != "window" || got.Window != "1m0s" || got.Count != 6 {
		t.Fatalf("summary = %+v, want 6 events in a 1m window", got)
	}
	if got.EventsPerSec <= 0 {
		t.Fatalf("events_per_sec = %v, want a positive rate", got.EventsPerSec)
	}
}

func TestEventsAggregateFlagValidation(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(8500, 1))
	withFake(t, srv)

	cases := map[string][]string{
		"--window requires --aggregate": {"events", "--window", "1m"},
		"--window must be positive":     {"events", "--aggregate", "--window", "0s"},
		"--aggregate-interval":          {"events", "--aggregate", "--aggregate-interval", "-1s"},
		"cannot be combined with CSV":   {"events", "--aggregate", "--format", "csv"},
	}
	for want, args := range cases {
		if _, _, err := run(t, args...); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%v: err = %v, want it to mention %q", args, err, want)
		}
	}
}

// lockedBuffer lets the sink's ticker goroutine and the test share output.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestWindowAggregateSinkRollsWithEventClock drives the sink with a fake
// wall clock: throughput reflects recent events, decays as they age past
// the window, and the human line carries every metric.
func TestWindowAggregateSinkRollsWithEventClock(t *testing.T) {
	var (
		clockMu sync.Mutex
		wall    = time.Unix(1_800_000_000, 0).UTC()
	)
	now := func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return wall }
	advance := func(d time.Duration) { clockMu.Lock(); wall = wall.Add(d); clockMu.Unlock() }

	out := &lockedBuffer{}
	// A long interval keeps the ticker out of the way; the test prints by hand.
	s := newWindowAggregateSink(out, 30*time.Second, time.Hour, false, now)

	eventAt := func(at time.Time) stream.Record {
		return stream.Record{Kind: stream.KindEvent, Time: at, Event: &stream.Event{ContractID: "C"}}
	}
	for i := 0; i < 10; i++ {
		if err := s.Emit(eventAt(now())); err != nil {
			t.Fatal(err)
		}
		advance(time.Second)
	}
	if err := s.Emit(stream.Record{Kind: stream.KindLedger}); err != nil {
		t.Fatal(err)
	}
	s.print()
	if line := out.String(); !strings.Contains(line, "window=30s  events=10  events/sec=1.00  unique_senders=0") {
		t.Fatalf("first summary = %q, want 10 events at 1/s", line)
	}

	advance(time.Minute)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if last := lines[len(lines)-1]; !strings.Contains(last, "events=0") {
		t.Fatalf("summary after a quiet minute = %q, want the window to have drained", last)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close() = %v, want nil", err)
	}
}
