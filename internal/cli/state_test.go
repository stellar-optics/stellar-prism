package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// stateEntries reads the cursor metadata a run left behind.
func stateEntries(t *testing.T, path, key string) []map[string]any {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the state file: %v", err)
	}
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("the state file holds a line that is not JSON: %v\n%s", err, line)
		}
		if entry["key"] != key {
			continue
		}
		out = append(out, entry)
	}
	return out
}

// TestEventsCommandRecordsCursorMetadata checks that a run leaves a
// checkpoint behind for every page it delivered.
func TestEventsCommandRecordsCursorMetadata(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 6), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	if _, _, err := run(t, "events", "--from", "1000", "--json",
		"--page-size", "2", "--state-file", path); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	entries := stateEntries(t, path, "events")
	if len(entries) != 3 {
		t.Fatalf("recorded %d checkpoints, want 3", len(entries))
	}
	if entries[0]["cursor"] == "" {
		t.Errorf("the first checkpoint has no cursor: %+v", entries[0])
	}
	if entries[0]["ledger"] != float64(1001) {
		t.Errorf("the first checkpoint is at ledger %v, want 1001", entries[0]["ledger"])
	}
}

// TestEventsCommandPrunesTheStateFile is the point of the whole feature: the
// metadata file must not grow without bound while the stream runs.
func TestEventsCommandPrunesTheStateFile(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(2000, 20), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	if _, _, err := run(t, "events", "--from", "2000", "--json",
		"--page-size", "2", "--state-file", path, "--prune-keep", "2"); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	entries := stateEntries(t, path, "events")
	if len(entries) != 2 {
		t.Fatalf("the state file holds %d checkpoints, want 2", len(entries))
	}
	// The newest checkpoint must survive: it is the one a later run
	// resumes from.
	if entries[1]["ledger"] != float64(2019) {
		t.Errorf("the newest checkpoint is at ledger %v, want 2019", entries[1]["ledger"])
	}
}

// TestEventsCommandResumesFromTheStateFile runs two streams against the same
// state file and checks that the second picks up where the first stopped.
func TestEventsCommandResumesFromTheStateFile(t *testing.T) {
	first := fakerpc.NewServer(fakerpc.MakeEvents(3000, 5), fakerpc.WithPageLimit(10))
	withFake(t, first)

	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	if _, _, err := run(t, "events", "--from", "3000", "--json", "--state-file", path); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// The chain moved on while prism was not watching.
	second := fakerpc.NewServer(fakerpc.MakeEvents(3000, 9), fakerpc.WithPageLimit(10))
	withFake(t, second)

	stdout, stderr, err := run(t, "events", "--json", "--state-file", path, "--resume")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(stderr, "resuming") {
		t.Errorf("stderr does not mention the resume:\n%s", stderr)
	}

	lines := nonEmptyLines(stdout)
	if len(lines) != 4 {
		t.Fatalf("the resumed run emitted %d records, want the 4 it had not seen:\n%s", len(lines), stdout)
	}
	var first2 struct {
		Ledger uint32 `json:"ledger"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &first2); err != nil {
		t.Fatalf("line 0 is not valid JSON: %v", err)
	}
	if first2.Ledger != 3005 {
		t.Errorf("the resumed run started at ledger %d, want 3005", first2.Ledger)
	}
}

// TestEventsCommandExplicitFromBeatsARecordedCursor documents the
// precedence: an operator who asks for a position gets that position.
func TestEventsCommandExplicitFromBeatsARecordedCursor(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(4000, 4), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	if _, _, err := run(t, "events", "--from", "4000", "--json", "--state-file", path); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	stdout, _, err := run(t, "events", "--from", "4002", "--json", "--state-file", path, "--resume")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(nonEmptyLines(stdout)); got != 2 {
		t.Errorf("the run emitted %d records, want the 2 from ledger 4002", got)
	}
}

// TestEventsCommandDropsExpiredCheckpoints checks the age based retention
// policy end to end: a checkpoint that is too old is removed even though it
// is the only one for its stream.
func TestEventsCommandDropsExpiredCheckpoints(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(5000, 2), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	stale := `{"key":"events","cursor":"0000000000000000001-0000000000","ledger":1,"recorded_at":"2000-01-01T00:00:00Z"}`
	if err := os.WriteFile(path, []byte(stale+"\n"), 0o600); err != nil {
		t.Fatalf("writing the stale checkpoint: %v", err)
	}

	if _, _, err := run(t, "events", "--from", "5000", "--json",
		"--state-file", path, "--prune-max-age", "1h"); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	entries := stateEntries(t, path, "events")
	if len(entries) != 1 {
		t.Fatalf("the state file holds %d checkpoints, want only the fresh one", len(entries))
	}
	if entries[0]["ledger"] != float64(5001) {
		t.Errorf("the surviving checkpoint is at ledger %v, want 5001", entries[0]["ledger"])
	}
}

// TestEventsCommandPruneIntervalZeroCompactsOnce checks that the background
// loop can be turned off without losing the compaction.
func TestEventsCommandPruneIntervalZeroCompactsOnce(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(6000, 20), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	if _, _, err := run(t, "events", "--from", "6000", "--json",
		"--page-size", "2", "--state-file", path,
		"--prune-interval", "0", "--prune-keep", "1"); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	entries := stateEntries(t, path, "events")
	if len(entries) != 1 {
		t.Fatalf("the state file holds %d checkpoints, want 1", len(entries))
	}
	if entries[0]["ledger"] != float64(6019) {
		t.Errorf("the surviving checkpoint is at ledger %v, want 6019", entries[0]["ledger"])
	}
}

// TestStateFlagsRequireAStateFile checks that the pruning flags are rejected
// on their own rather than being silently ignored.
func TestStateFlagsRequireAStateFile(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 1), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	tests := []struct {
		name string
		args []string
	}{
		{name: "resume", args: []string{"events", "--from", "100", "--resume"}},
		{name: "prune keep", args: []string{"events", "--from", "100", "--prune-keep", "5"}},
		{name: "prune interval", args: []string{"events", "--from", "100", "--prune-interval", "1s"}},
		{name: "prune max age", args: []string{"events", "--from", "100", "--prune-max-age", "1h"}},
		{name: "negative keep", args: []string{"events", "--from", "100", "--state-file", "cursor.jsonl", "--prune-keep", "-1"}},
		{name: "negative interval", args: []string{"events", "--from", "100", "--state-file", "cursor.jsonl", "--prune-interval", "-1s"}},
		{name: "negative max age", args: []string{"events", "--from", "100", "--state-file", "cursor.jsonl", "--prune-max-age", "-1h"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := run(t, tc.args...); err == nil {
				t.Fatal("Execute() error = nil, want a validation failure")
			}
		})
	}
}

// TestStateFlagsRejectNegativeValues checks that a negative limit is refused
// with a message that names the flag.
//
// A negative interval or age is not a limit the pruner can honour: it would
// compact on every pass, or drop every checkpoint. Refusing it at the command
// line is clearer than quietly reading it as "disabled".
func TestStateFlagsRejectNegativeValues(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 1), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "interval",
			args: []string{"events", "--from", "100", "--state-file", "cursor.jsonl", "--prune-interval", "-1s"},
			want: "--prune-interval",
		},
		{
			name: "max age",
			args: []string{"events", "--from", "100", "--state-file", "cursor.jsonl", "--prune-max-age", "-1h"},
			want: "--prune-max-age",
		},
		{
			name: "keep",
			args: []string{"events", "--from", "100", "--state-file", "cursor.jsonl", "--prune-keep", "-1"},
			want: "--prune-keep",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := run(t, tc.args...)
			if err == nil {
				t.Fatal("Execute() error = nil, want a validation failure")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Execute() error = %q, want it to name %s", err, tc.want)
			}
		})
	}
}

// TestStateFileMustBeWritable checks that a state file in a directory that
// does not exist fails immediately rather than hours into a run.
func TestStateFileMustBeWritable(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 1), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	path := filepath.Join(t.TempDir(), "missing", "cursor.jsonl")
	if _, _, err := run(t, "events", "--from", "100", "--json", "--state-file", path); err == nil {
		t.Fatal("Execute() error = nil, want a missing-directory failure")
	}
}

// TestEventsHelpAdvertisesTheStateFlags checks that the new flags are
// discoverable.
func TestEventsHelpAdvertisesTheStateFlags(t *testing.T) {
	stdout, _, err := run(t, "events", "--help")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, flag := range []string{
		"--state-file", "--resume", "--prune-interval", "--prune-keep", "--prune-max-age",
	} {
		if !strings.Contains(stdout, flag) {
			t.Errorf("help does not mention %s", flag)
		}
	}
}

// TestCursorStateSkipsACursorlessPage checks that a page the source could not
// put a position to is not recorded.
//
// Recording an empty cursor would push a usable checkpoint out of reach: the
// newest entry would no longer be a position a later run could resume from.
func TestCursorStateSkipsACursorlessPage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	store, err := stream.NewFileMetadataStore(path)
	if err != nil {
		t.Fatalf("NewFileMetadataStore() error = %v", err)
	}

	state := &cursorState{store: store, key: "events"}
	if err := state.record(context.Background(), stream.Page{}); err != nil {
		t.Fatalf("record() error = %v", err)
	}

	entries, err := store.Entries(context.Background(), "events")
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("recorded %d checkpoints, want none", len(entries))
	}
}

// TestCursorStateRecordsAPage checks the ordinary path: a page with a
// position is written to the journal.
func TestCursorStateRecordsAPage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	store, err := stream.NewFileMetadataStore(path)
	if err != nil {
		t.Fatalf("NewFileMetadataStore() error = %v", err)
	}

	state := &cursorState{store: store, key: "events"}
	err = state.record(context.Background(), stream.Page{
		Source:  "events",
		Cursor:  "0000000012884139520-0000000000",
		Ledger:  42,
		Records: 3,
	})
	if err != nil {
		t.Fatalf("record() error = %v", err)
	}

	entries, err := store.Entries(context.Background(), "events")
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("recorded %d checkpoints, want 1", len(entries))
	}
	if entries[0].Ledger != 42 {
		t.Errorf("the checkpoint is at ledger %d, want 42", entries[0].Ledger)
	}
	if entries[0].Cursor != "0000000012884139520-0000000000" {
		t.Errorf("the checkpoint holds cursor %q", entries[0].Cursor)
	}
}

// TestCursorStateCloseReportsAFailedFinalCompaction checks that a state
// directory that cannot be compacted is reported rather than swallowed.
//
// A journal that cannot be compacted is the one failure this feature exists
// to prevent, so it must not pass unnoticed at shutdown.
func TestCursorStateCloseReportsAFailedFinalCompaction(t *testing.T) {
	// A directory in place of the journal cannot be written to or read as
	// a file, which is exactly the failure being exercised.
	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	if err := os.Mkdir(path, 0o750); err != nil {
		t.Fatalf("creating the directory: %v", err)
	}

	store, err := stream.NewFileMetadataStore(path)
	if err != nil {
		t.Fatalf("NewFileMetadataStore() error = %v", err)
	}

	pruner, err := stream.NewPruner(store, stream.PruneOptions{})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}

	state := &cursorState{store: store, pruner: pruner, key: "events"}
	if err := state.close(context.Background()); err == nil {
		t.Fatal("close() error = nil, want the failed compaction to be reported")
	}
}

// TestEventsCommandReportsAStateFileItCannotWrite checks that a journal
// that cannot be written to is reported on stderr.
//
// The stream itself still delivers records: a metadata file that cannot be
// written is not a reason to stop streaming, but it is a reason to say so.
func TestEventsCommandReportsAStateFileItCannotWrite(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(7000, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	if err := os.Mkdir(path, 0o750); err != nil {
		t.Fatalf("creating the directory: %v", err)
	}

	_, stderr, err := run(t, "events", "--from", "7000", "--json", "--state-file", path)
	if err == nil {
		t.Fatal("Execute() error = nil, want the unwritable journal to be reported")
	}
	if !strings.Contains(stderr, "recording cursor metadata") {
		t.Errorf("stderr does not report the failed write:\n%s", stderr)
	}
}

// TestEventsCommandWithConcurrentBackfillKeepsTheStateFileBounded checks
// that the concurrent backfill path is recorded and pruned too.
//
// The backfill drains a historical range with several workers before the
// follow phase begins, so it is the phase that produces the most pages — and
// therefore the most checkpoints.
func TestEventsCommandWithConcurrentBackfillKeepsTheStateFileBounded(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(8000, 40), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	if _, _, err := run(t, "events", "--from", "8000", "--json",
		"--backfill-workers", "4", "--state-file", path, "--prune-keep", "2"); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	entries := stateEntries(t, path, "events")
	if len(entries) == 0 {
		t.Fatal("the backfill recorded no checkpoints at all")
	}
	if len(entries) > 2 {
		t.Fatalf("the state file holds %d checkpoints, want at most 2", len(entries))
	}

	// The newest checkpoint must be the furthest the drain reached, so a
	// later run picks the stream up from there.
	last := entries[len(entries)-1]
	if last["ledger"] != float64(8039) {
		t.Errorf("the newest checkpoint is at ledger %v, want 8039", last["ledger"])
	}
}

// TestStateKeySeparatesDifferentStreams checks that two streams watching
// different things never share a recorded cursor.
func TestStateKeySeparatesDifferentStreams(t *testing.T) {
	a := eventsStateKey([]string{"CDLZF"}, nil, nil)
	b := eventsStateKey([]string{"CDLZF"}, []string{"AAAADw==,*"}, nil)
	c := eventsStateKey(nil, nil, nil)

	if a == b {
		t.Errorf("two differently filtered streams share the key %q", a)
	}
	if a == c {
		t.Errorf("a filtered and an unfiltered stream share the key %q", a)
	}
	if a != eventsStateKey([]string{"CDLZF"}, nil, nil) {
		t.Errorf("the key for the same filters is not stable: %q", a)
	}
	excluded := eventsStateKey([]string{"CDLZF"}, nil, nil, filterPart("exclude-contract", []string{"CAAA"}))
	if excluded == a {
		t.Errorf("a blacklisted stream shares the unblacklisted cursor key %q", a)
	}
}

// TestStateKeyIgnoresEmptyFilters checks that a filter left out does not
// change the key, so a stream keeps its checkpoints when a filter is added
// and removed again.
func TestStateKeyIgnoresEmptyFilters(t *testing.T) {
	if got, want := eventsStateKey(nil, nil, nil), "events"; got != want {
		t.Errorf("stateKey = %q, want %q", got, want)
	}
	if got, want := eventsStateKey([]string{"CDLZF"}, nil, nil), "events|contract=CDLZF"; got != want {
		t.Errorf("stateKey = %q, want %q", got, want)
	}
}

// TestEventsCommandWithAStateFileStillExitsTwoOnAGap checks that the
// cursor journal does not disturb the exit-code contract.
//
// The journal is closed after the stream fails, and that close is joined with
// the failure rather than replacing it, so the gap must still be what the
// process reports.
func TestEventsCommandWithAStateFileStillExitsTwoOnAGap(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(9000, 40),
		fakerpc.WithPageLimit(5),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {OldestLedger: 9000000},
		}),
	)
	withFake(t, srv)

	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	_, _, err := run(t, "events", "--from", "9000", "--json",
		"--page-size", "5", "--state-file", path)
	if err == nil {
		t.Fatal("Execute() error = nil, want a gap failure")
	}
	code, printable := ExitCode(err)
	if code != 2 {
		t.Errorf("exit code = %d, want 2", code)
	}
	if printable {
		t.Error("a gap should print its own guidance rather than the generic error")
	}
}
