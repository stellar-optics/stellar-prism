package stream_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// --- StateFile unit tests -------------------------------------------------

// TestStateFileSaveLoadRoundTrip checks the core contract: what is saved is
// exactly what comes back, and the on-disk document keeps the schema the CLI
// and humans are promised.
func TestStateFileSaveLoadRoundTrip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "cursor.json")
	sf, err := stream.OpenStateFile(path)
	if err != nil {
		t.Fatalf("OpenStateFile() error = %v", err)
	}

	want := stream.State{
		Source:      "events",
		Ledger:      3966820,
		PagingToken: "0017037362169118720-0000000000",
		LastID:      "0017037362169118720-0000000000",
		UpdatedAt:   time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC),
	}
	if err := sf.Save(want); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	got, err := sf.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.Version != stream.StateVersion {
		t.Errorf("Version = %d, want %d", got.Version, stream.StateVersion)
	}
	if got.Source != want.Source || got.Ledger != want.Ledger ||
		got.PagingToken != want.PagingToken || got.LastID != want.LastID {
		t.Errorf("Load() = %+v, want %+v", got, want)
	}
	if !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Errorf("UpdatedAt = %v, want %v", got.UpdatedAt, want.UpdatedAt)
	}

	// Pin the on-disk schema: a silent field rename would strand every
	// existing state file without a version bump.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading state file: %v", err)
	}
	for _, field := range []string{`"version"`, `"source"`, `"ledger"`, `"paging_token"`, `"last_id"`, `"updated_at"`} {
		if !strings.Contains(string(raw), field) {
			t.Errorf("state file does not contain field %s\n%s", field, raw)
		}
	}
}

func TestStateFileMissingFileLoadsZeroState(t *testing.T) {
	t.Parallel()

	sf, err := stream.OpenStateFile(filepath.Join(t.TempDir(), "absent", "cursor.json"))
	if err != nil {
		t.Fatalf("OpenStateFile() error = %v", err)
	}
	got, err := sf.Load()
	if err != nil {
		t.Fatalf("Load() on a missing file error = %v, want the zero state and no error", err)
	}
	if !reflect.DeepEqual(got, stream.State{}) {
		t.Errorf("Load() = %+v, want the zero state", got)
	}
}

func TestStateFileEmptyFileLoadsZeroState(t *testing.T) {
	t.Parallel()

	for _, content := range []string{"", "\n\n"} {
		path := filepath.Join(t.TempDir(), "cursor.json")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("writing fixture: %v", err)
		}
		sf, err := stream.OpenStateFile(path)
		if err != nil {
			t.Fatalf("OpenStateFile() error = %v", err)
		}
		got, err := sf.Load()
		if err != nil {
			t.Fatalf("Load() on %q error = %v, want the zero state and no error", content, err)
		}
		if !reflect.DeepEqual(got, stream.State{}) {
			t.Errorf("Load() = %+v, want the zero state", got)
		}
	}
}

// TestStateFileCorruptFileFailsToLoad checks that a damaged file is reported
// with its cause, never silently treated as "nothing persisted" — that would
// restart the stream from the top and re-emit everything.
func TestStateFileCorruptFileFailsToLoad(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "cursor.json")
	if err := os.WriteFile(path, []byte("not json {"), 0o600); err != nil {
		t.Fatalf("writing fixture: %v", err)
	}

	_, err := stream.OpenStateFile(path)
	if err == nil {
		t.Fatal("OpenStateFile() error = nil, want a parse failure")
	}
	var syntaxErr *json.SyntaxError
	if !errors.As(err, &syntaxErr) {
		t.Errorf("error = %v, want it to wrap *json.SyntaxError", err)
	}
	if !strings.Contains(err.Error(), path) {
		t.Errorf("error = %q, want it to name the file", err)
	}
}

func TestStateFileUnsupportedVersionFailsToLoad(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "cursor.json")
	raw, err := json.Marshal(map[string]any{
		"version": stream.StateVersion + 1, "source": "events", "ledger": 1,
	})
	if err != nil {
		t.Fatalf("marshalling fixture: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("writing fixture: %v", err)
	}

	_, err = stream.OpenStateFile(path)
	if err == nil {
		t.Fatal("OpenStateFile() error = nil, want a version failure")
	}
	if !strings.Contains(err.Error(), "schema version") {
		t.Errorf("error = %q, want it to mention the schema version", err)
	}
}

func TestStateFileSaveRejectsStateWithoutSource(t *testing.T) {
	t.Parallel()

	sf, err := stream.OpenStateFile(filepath.Join(t.TempDir(), "cursor.json"))
	if err != nil {
		t.Fatalf("OpenStateFile() error = %v", err)
	}
	if err := sf.Save(stream.State{PagingToken: "orphan"}); err == nil {
		t.Fatal("Save() error = nil, want a rejection of state with no stream name")
	}
}

// TestStateFileSaveLeavesNoTempFiles checks the atomic-write machinery does
// not litter the directory: after a save, only the state file itself exists.
func TestStateFileSaveLeavesNoTempFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sf, err := stream.OpenStateFile(filepath.Join(dir, "cursor.json"))
	if err != nil {
		t.Fatalf("OpenStateFile() error = %v", err)
	}
	for range 3 {
		if err := sf.Save(stream.State{Source: "events", Ledger: 1}); err != nil {
			t.Fatalf("Save() error = %v", err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading state directory: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "cursor.json" {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("state directory contains %v, want only [cursor.json]", names)
	}
}

func TestStateFileSaveOverwritesPreviousState(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "cursor.json")
	sf, err := stream.OpenStateFile(path)
	if err != nil {
		t.Fatalf("OpenStateFile() error = %v", err)
	}
	if err := sf.Save(stream.State{Source: "events", Ledger: 10, PagingToken: "a", LastID: "a"}); err != nil {
		t.Fatalf("first Save() error = %v", err)
	}
	if err := sf.Save(stream.State{Source: "events", Ledger: 20, PagingToken: "b", LastID: "b"}); err != nil {
		t.Fatalf("second Save() error = %v", err)
	}
	got, err := sf.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.Ledger != 20 || got.PagingToken != "b" || got.LastID != "b" {
		t.Errorf("Load() = %+v, want the second saved position", got)
	}
}

func TestOpenStateFileCreatesParentDirs(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "a", "b", "cursor.json")
	sf, err := stream.OpenStateFile(path)
	if err != nil {
		t.Fatalf("OpenStateFile() error = %v", err)
	}
	if got := sf.Path(); got != path {
		t.Errorf("Path() = %q, want %q", got, path)
	}
	if err := sf.Save(stream.State{Source: "ledgers", Ledger: 7}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
}

func TestOpenStateFileRejectsDirectoryPath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if _, err := stream.OpenStateFile(dir); err == nil {
		t.Fatal("OpenStateFile() error = nil, want a failure when the path is a directory")
	}
}

func TestOpenStateFileRejectsEmptyPath(t *testing.T) {
	t.Parallel()

	if _, err := stream.OpenStateFile(""); err == nil {
		t.Fatal("OpenStateFile() error = nil, want a rejection of the empty path")
	}
}

// TestStateFileSaveFailsWhenDestinationIsDirectory exercises the replace
// step failing: the error must name the destination, and the temporary file
// must be cleaned up.
func TestStateFileSaveFailsWhenDestinationIsDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "cursor.json")
	sf, err := stream.OpenStateFile(path)
	if err != nil {
		t.Fatalf("OpenStateFile() error = %v", err)
	}
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatalf("creating conflicting directory: %v", err)
	}

	err = sf.Save(stream.State{Source: "events", Ledger: 1})
	if err == nil {
		t.Fatal("Save() error = nil, want the replace step to fail")
	}
	if !strings.Contains(err.Error(), "replacing state file") {
		t.Errorf("error = %q, want it to name the replace step", err)
	}
	entries, readErr := os.ReadDir(dir)
	if readErr != nil {
		t.Fatalf("reading state directory: %v", readErr)
	}
	for _, e := range entries {
		if e.Name() != "cursor.json" {
			t.Errorf("failed Save() left %q behind", e.Name())
		}
	}
}

func TestStateFileSaveFailsWhenDirectoryDisappears(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "cursor.json")
	sf, err := stream.OpenStateFile(path)
	if err != nil {
		t.Fatalf("OpenStateFile() error = %v", err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("removing state directory: %v", err)
	}

	err = sf.Save(stream.State{Source: "events", Ledger: 1})
	if err == nil {
		t.Fatal("Save() error = nil, want a failure with no directory to write into")
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("error = %v, want it to wrap fs.ErrNotExist", err)
	}
}

// --- Tailer integration with a durable position ---------------------------

// failingStateStore substitutes for *StateFile so error paths can be driven
// without making the filesystem fail. A nil error field means "work".
type failingStateStore struct {
	loadErr error
	saveErr error
	saved   []stream.State
}

func (s *failingStateStore) Load() (stream.State, error) {
	if s.loadErr != nil {
		return stream.State{}, s.loadErr
	}
	return stream.State{}, nil
}

func (s *failingStateStore) Save(st stream.State) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	s.saved = append(s.saved, st)
	return nil
}

func mustStateFile(t *testing.T) *stream.StateFile {
	t.Helper()
	sf, err := stream.OpenStateFile(filepath.Join(t.TempDir(), "cursor.json"))
	if err != nil {
		t.Fatalf("OpenStateFile() error = %v", err)
	}
	return sf
}

// TestTailStateResumesAcrossRestart is the central durable-cursor test: two
// independent runs sharing only a state file must together deliver the whole
// universe exactly once, in order. The second run re-uses the fake's real
// cursor semantics, so this is the same code path a restarted CLI takes.
func TestTailStateResumesAcrossRestart(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(1000, 30)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(4))
	state := mustStateFile(t)

	src1, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 1000, PageLimit: 4})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}
	sink1 := &collector{}
	tl1 := stream.NewTailer(src1, sink1, stream.Options{Limit: 7, Clock: newFakeClock(), State: state})
	if err := tl1.Run(context.Background()); err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	if len(sink1.ids) != 7 {
		t.Fatalf("first run emitted %d records, want 7", len(sink1.ids))
	}

	// A brand-new tailer and source, sharing nothing but the file — this is
	// what a process restart looks like.
	src2, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 1000, PageLimit: 4})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}
	sink2 := &collector{}
	tl2 := stream.NewTailer(src2, sink2, stream.Options{Clock: newFakeClock(), State: state})
	if err := tl2.Run(context.Background()); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}

	combined := append(append([]string{}, sink1.ids...), sink2.ids...)
	assertStreamInvariants(t, combined, idsOf(events))

	// The persisted position must track the stream to the end.
	st, err := state.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	last := events[len(events)-1]
	if st.Source != "events" || st.LastID != last.ID || st.PagingToken != last.ID {
		t.Errorf("final state = %+v, want the last event %q as both token and high-water mark", st, last.ID)
	}
	if st.Ledger != uint32(last.Ledger) { //nolint:gosec // test data
		t.Errorf("final state Ledger = %d, want %d", st.Ledger, last.Ledger)
	}
	if st.UpdatedAt.IsZero() {
		t.Error("final state UpdatedAt is zero, want a timestamp")
	}
}

// TestTailStateSuppressesReplayAfterRestart proves the high-water mark is
// restored, not just the cursor: a server that replays the last page to a
// freshly restarted client must not cause re-emission.
func TestTailStateSuppressesReplayAfterRestart(t *testing.T) {
	t.Parallel()

	// 22 events over pages of 5,5,5,5,2: run 1 therefore finishes on a
	// non-full, non-empty page, so the fake's "last page" — the one a
	// reconnecting server replays — is genuinely the tail of the stream
	// rather than an empty page.
	events := fakerpc.MakeEvents(200, 22)
	script := map[int]fakerpc.Behaviour{}
	srv := fakerpc.NewServer(events,
		fakerpc.WithPageLimit(5),
		fakerpc.WithScript(script),
	)
	state := mustStateFile(t)

	runOnce := func() (*collector, error) {
		src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 200, PageLimit: 5})
		if err != nil {
			t.Fatalf("NewEventSource() error = %v", err)
		}
		sink := &collector{}
		tl := stream.NewTailer(src, sink, stream.Options{Clock: newFakeClock(), State: state})
		return sink, tl.Run(context.Background())
	}

	sink1, err := runOnce()
	if err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	assertStreamInvariants(t, sink1.ids, idsOf(events))

	// The next call — the restarted run's first — replays the previous page.
	script[srv.Calls()] = fakerpc.Behaviour{ReplayLast: true}

	sink2, err := runOnce()
	if err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	if len(sink2.ids) != 0 {
		t.Errorf("second run re-emitted %v, want nothing: the replay must be suppressed", sink2.ids)
	}
}

// TestTailStateRefusesPositionFromAnotherStream checks a state file is not
// silently misapplied to a different stream, where its token means something
// else entirely.
func TestTailStateRefusesPositionFromAnotherStream(t *testing.T) {
	t.Parallel()

	state := mustStateFile(t)
	if err := state.Save(stream.State{Source: "events", Ledger: 700, PagingToken: "0000000700-0001-0000-0000"}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	srv := fakerpc.NewLedgerServer(fakerpc.MakeLedgers(700, 5), fakerpc.WithPageLimit(5))
	src := stream.NewLedgerSource(srv, stream.LedgerQuery{StartLedger: 700, PageLimit: 5})

	err := stream.NewTailer(src, &collector{}, stream.Options{Clock: newFakeClock(), State: state}).Run(context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want a refusal of the foreign state file")
	}
	for _, want := range []string{`"events"`, `"ledgers"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to name both streams (%s)", err, want)
		}
	}
}

// TestTailStateRejectsUninterpretableToken checks a token the source cannot
// parse fails loudly at startup rather than as a confusing server rejection
// after the stream has begun.
func TestTailStateRejectsUninterpretableToken(t *testing.T) {
	t.Parallel()

	state := mustStateFile(t)
	if err := state.Save(stream.State{Source: "events", Ledger: 1, PagingToken: "not-a-cursor"}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 2))
	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	runErr := stream.NewTailer(src, &collector{}, stream.Options{Clock: newFakeClock(), State: state}).Run(context.Background())
	if runErr == nil {
		t.Fatal("Run() error = nil, want a cursor validation failure")
	}
	if !strings.Contains(runErr.Error(), "not a valid events cursor") {
		t.Errorf("error = %q, want it to reject the cursor", runErr)
	}
	if srv.Calls() != 0 {
		t.Errorf("made %d RPC call(s) before rejecting the state file, want 0", srv.Calls())
	}
}

// TestTailStateRecordsPositionAfterEachEmit checks the crash window: when a
// sink fails partway through a page, the file must hold the position of the
// last successfully emitted record — never one further along.
func TestTailStateRecordsPositionAfterEachEmit(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(500, 10)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(5))
	state := mustStateFile(t)

	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 500, PageLimit: 5})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}
	wantErr := errors.New("downstream consumer died")
	sink := &collector{failAt: 3, err: wantErr}

	runErr := stream.NewTailer(src, sink, stream.Options{Clock: newFakeClock(), State: state}).Run(context.Background())
	if !errors.Is(runErr, wantErr) {
		t.Fatalf("Run() error = %v, want it to wrap %v", runErr, wantErr)
	}

	st, err := state.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if want := events[2].ID; st.LastID != want || st.PagingToken != want {
		t.Errorf("state = %+v, want the position of the last successful emit %q", st, want)
	}
	if want := uint32(events[2].Ledger); st.Ledger != want { //nolint:gosec // test data
		t.Errorf("state Ledger = %d, want %d", st.Ledger, want)
	}
}

// TestTailStateSaveFailureStopsRun checks a failed persist is fatal: a run
// that silently stopped updating its position would make --state-file a lie.
func TestTailStateSaveFailureStopsRun(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 5)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(5))
	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100, PageLimit: 5})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	wantErr := errors.New("disk full")
	store := &failingStateStore{saveErr: wantErr}
	tl := stream.NewTailer(src, &collector{}, stream.Options{Clock: newFakeClock(), State: store})

	runErr := tl.Run(context.Background())
	if !errors.Is(runErr, wantErr) {
		t.Fatalf("Run() error = %v, want it to wrap %v", runErr, wantErr)
	}
	if tl.Stats().Emitted != 1 {
		t.Errorf("Stats().Emitted = %d, want the first record emitted before the persist failed", tl.Stats().Emitted)
	}
}

// TestTailStateLoadFailureStopsRun checks a store that cannot produce the
// position aborts before any record is delivered.
func TestTailStateLoadFailureStopsRun(t *testing.T) {
	t.Parallel()

	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 5), fakerpc.WithPageLimit(5))
	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100, PageLimit: 5})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	wantErr := errors.New("unreadable state")
	sink := &collector{}
	tl := stream.NewTailer(src, sink, stream.Options{
		Clock: newFakeClock(), State: &failingStateStore{loadErr: wantErr},
	})

	runErr := tl.Run(context.Background())
	if !errors.Is(runErr, wantErr) {
		t.Fatalf("Run() error = %v, want it to wrap %v", runErr, wantErr)
	}
	if len(sink.ids) != 0 {
		t.Errorf("emitted %d records before the state load failed, want 0", len(sink.ids))
	}
}

// TestTailStateGapDetectedOnResume checks the restored cursor is subject to
// the same retention check as an in-run cursor: resuming past the server's
// retention window is a gap, reported and stopped, not silently skipped.
func TestTailStateGapDetectedOnResume(t *testing.T) {
	t.Parallel()

	// The saved position sits at ledger 1000; the server retains nothing
	// before 5000.
	oldEvents := fakerpc.MakeEvents(1000, 1)
	state := mustStateFile(t)
	if err := state.Save(stream.State{
		Source: "events", Ledger: 1000,
		PagingToken: oldEvents[0].ID, LastID: oldEvents[0].ID,
	}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	srv := fakerpc.NewServer(fakerpc.MakeEvents(5000, 10), fakerpc.WithPageLimit(5))
	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 5000, PageLimit: 5})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	runErr := stream.NewTailer(src, &collector{}, stream.Options{Clock: newFakeClock(), State: state}).Run(context.Background())
	var gap *stream.GapError
	if !errors.As(runErr, &gap) {
		t.Fatalf("Run() error = %v, want a *stream.GapError", runErr)
	}
	if gap.Oldest != 5000 {
		t.Errorf("GapError.Oldest = %d, want 5000", gap.Oldest)
	}
}

// TestTailStateLeavesNoGoroutinesAfterCancel is the termination check for
// the "zero memory leaks and proper goroutine cleanup" rule: persistence is
// synchronous and file-backed, so a cancelled follow must return the
// goroutine count to its baseline.
func TestTailStateLeavesNoGoroutinesAfterCancel(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 5)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(5))
	state := mustStateFile(t)

	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100, PageLimit: 5})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := newFakeClock()
	// Cancel from inside the first poll wait, which is where a follow
	// spends its life.
	clock.onWake = func() { cancel() }

	baseline := runtime.NumGoroutine()
	tl := stream.NewTailer(src, &collector{}, stream.Options{Follow: true, Clock: clock, State: state})
	if err := tl.Run(ctx); err != nil {
		t.Fatalf("Run() after cancellation = %v, want nil (a clean stop)", err)
	}

	// No goroutine should outlive the run; poll briefly to avoid tripping
	// over unrelated runtime housekeeping.
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > baseline && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > baseline {
		t.Errorf("goroutines after cancellation = %d, want at most the baseline %d", got, baseline)
	}
}

// openShared opens path as the partition streamID of a shared state file.
func openShared(t *testing.T, path, streamID string) *stream.StateFile {
	t.Helper()
	sf, err := stream.OpenStateFileWithOptions(path, stream.StateFileOptions{StreamID: streamID})
	if err != nil {
		t.Fatalf("OpenStateFileWithOptions(%q) error = %v", streamID, err)
	}
	return sf
}

// readShared decodes a shared state file.
func readShared(t *testing.T, path string) map[string]stream.State {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading state file: %v", err)
	}
	var doc struct {
		Version int                     `json:"version"`
		Streams map[string]stream.State `json:"streams"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("state file is not valid JSON: %v\n%s", err, data)
	}
	if doc.Version != stream.PartitionedStateVersion {
		t.Fatalf("state file version = %d, want %d\n%s", doc.Version, stream.PartitionedStateVersion, data)
	}
	return doc.Streams
}

// TestStateFileStreamsShareOneFile is the core of stream partitioning: two
// streams with different IDs write one file and each reads back only its own
// position.
func TestStateFileStreamsShareOneFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "shared.json")
	amm := openShared(t, path, "events_amm")
	txs := openShared(t, path, "transactions_main")

	if err := amm.Save(stream.State{Source: "events", Ledger: 10, PagingToken: "e10"}); err != nil {
		t.Fatalf("events Save() error = %v", err)
	}
	if err := txs.Save(stream.State{Source: "transactions", Ledger: 20, PagingToken: "t20"}); err != nil {
		t.Fatalf("transactions Save() error = %v", err)
	}
	if err := amm.Save(stream.State{Source: "events", Ledger: 11, PagingToken: "e11"}); err != nil {
		t.Fatalf("second events Save() error = %v", err)
	}

	gotAMM, err := amm.Load()
	if err != nil {
		t.Fatalf("events Load() error = %v", err)
	}
	if gotAMM.Source != "events" || gotAMM.Ledger != 11 || gotAMM.PagingToken != "e11" {
		t.Errorf("events state = %+v, want ledger 11 / e11", gotAMM)
	}
	gotTxs, err := txs.Load()
	if err != nil {
		t.Fatalf("transactions Load() error = %v", err)
	}
	if gotTxs.Source != "transactions" || gotTxs.Ledger != 20 || gotTxs.PagingToken != "t20" {
		t.Errorf("transactions state = %+v, want ledger 20 / t20; another stream's save overwrote it", gotTxs)
	}

	streams := readShared(t, path)
	if len(streams) != 2 {
		t.Errorf("file holds %d streams, want 2: %v", len(streams), streams)
	}
	ids, err := amm.StreamIDs()
	if err != nil {
		t.Fatalf("StreamIDs() error = %v", err)
	}
	if !reflect.DeepEqual(ids, []string{"events_amm", "transactions_main"}) {
		t.Errorf("StreamIDs() = %v", ids)
	}

	// A third stream that has never saved starts from nothing.
	fresh := openShared(t, path, "ledgers")
	got, err := fresh.Load()
	if err != nil {
		t.Fatalf("fresh Load() error = %v", err)
	}
	if got.Source != "" || got.PagingToken != "" {
		t.Errorf("unsaved stream loaded %+v, want the zero State", got)
	}
}

// stateHelperEnv turns the test binary into a writer for
// TestStateFileSharedAcrossProcesses: "<path>|<stream id>|<saves>".
const stateHelperEnv = "PRISM_STATE_HELPER"

// TestStateHelperWriter is not a test of its own. Re-executed with
// stateHelperEnv set, the binary saves its stream's position as fast as it
// can, one ledger at a time, then exits.
func TestStateHelperWriter(t *testing.T) {
	spec := os.Getenv(stateHelperEnv)
	if spec == "" {
		t.Skip("helper process only")
	}
	parts := strings.Split(spec, "|")
	if len(parts) != 3 {
		t.Fatalf("bad helper spec %q", spec)
	}
	var saves int
	if _, err := fmt.Sscan(parts[2], &saves); err != nil {
		t.Fatal(err)
	}
	sf, err := stream.OpenStateFileWithOptions(parts[0], stream.StateFileOptions{StreamID: parts[1]})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= saves; i++ {
		if err := sf.Save(stream.State{Source: "events", Ledger: uint32(i), PagingToken: fmt.Sprintf("%s-%d", parts[1], i)}); err != nil { //nolint:gosec // test data
			t.Fatal(err)
		}
	}
}

// TestStateFileSharedAcrossProcesses runs several prism-like processes that
// hammer one shared state file at the same time, each with its own stream ID.
// Every read-modify-write is serialised by the cross-process lock, so when
// they finish every stream holds its own final position: no process's rename
// discarded another's update.
func TestStateFileSharedAcrossProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns subprocesses")
	}
	path := filepath.Join(t.TempDir(), "shared.json")
	const writers, saves = 4, 150

	cmds := make([]*exec.Cmd, writers)
	for i := range cmds {
		id := fmt.Sprintf("stream-%d", i)
		cmd := exec.Command(os.Args[0], "-test.run=^TestStateHelperWriter$", "-test.count=1")
		cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%s|%s|%d", stateHelperEnv, path, id, saves))
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		cmds[i] = cmd
	}
	for i, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			t.Fatalf("writer %d failed: %v", i, err)
		}
	}

	streams := readShared(t, path)
	if len(streams) != writers {
		t.Fatalf("file holds %d streams, want %d: a concurrent save dropped a stream", len(streams), writers)
	}
	for i := range writers {
		id := fmt.Sprintf("stream-%d", i)
		st := streams[id]
		if st.Ledger != saves || st.PagingToken != fmt.Sprintf("%s-%d", id, saves) {
			t.Errorf("%s = ledger %d / %q, want its final position %d", id, st.Ledger, st.PagingToken, saves)
		}
	}
	if _, err := os.Stat(path + ".lock"); err != nil {
		t.Errorf("shared state file has no lock file beside it: %v", err)
	}
}

// TestStateFileSharedConcurrentGoroutines is the in-process counterpart: many
// StateFile values over one path, saving concurrently.
func TestStateFileSharedConcurrentGoroutines(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "shared.json")
	const writers, saves = 8, 40
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := range writers {
		sf := openShared(t, path, fmt.Sprintf("s%d", i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 1; n <= saves; n++ {
				if err := sf.Save(stream.State{Source: "ledgers", Ledger: uint32(n)}); err != nil { //nolint:gosec // test data
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("Save() error = %v", err)
	}
	streams := readShared(t, path)
	for i := range writers {
		if got := streams[fmt.Sprintf("s%d", i)].Ledger; got != saves {
			t.Errorf("s%d ledger = %d, want %d", i, got, saves)
		}
	}
}

// TestStateFileSharedUpgradesSingleStreamFile checks the migration path: a
// stream that starts using a stream ID over an existing single-stream file
// keeps its position, and the file becomes a shared one.
func TestStateFileSharedUpgradesSingleStreamFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "cursor.json")
	legacy, err := stream.OpenStateFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.Save(stream.State{Source: "events", Ledger: 77, PagingToken: "p77", LastID: "p77"}); err != nil {
		t.Fatal(err)
	}

	shared := openShared(t, path, "events/abc")
	got, err := shared.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.Ledger != 77 || got.PagingToken != "p77" {
		t.Fatalf("Load() = %+v, want the single-stream position adopted", got)
	}
	if err := shared.Save(stream.State{Source: "events", Ledger: 78, PagingToken: "p78"}); err != nil {
		t.Fatal(err)
	}
	streams := readShared(t, path)
	if len(streams) != 1 || streams["events/abc"].Ledger != 78 {
		t.Errorf("upgraded file = %+v, want only events/abc at ledger 78", streams)
	}
}

// TestStateFileSharedUpgradeKeepsOtherStreamsPosition checks that upgrading
// a single-stream file on behalf of a *different* stream keeps the original
// position under LegacyStreamID instead of dropping it.
func TestStateFileSharedUpgradeKeepsOtherStreamsPosition(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "cursor.json")
	legacy, err := stream.OpenStateFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.Save(stream.State{Source: "ledgers", Ledger: 5, PagingToken: "5"}); err != nil {
		t.Fatal(err)
	}
	if err := openShared(t, path, "events_amm").Save(stream.State{Source: "events", Ledger: 9, PagingToken: "e9"}); err != nil {
		t.Fatal(err)
	}
	streams := readShared(t, path)
	if streams[stream.LegacyStreamID].Source != "ledgers" || streams[stream.LegacyStreamID].Ledger != 5 {
		t.Errorf("legacy position = %+v, want the ledgers stream at 5 under %q", streams[stream.LegacyStreamID], stream.LegacyStreamID)
	}
	if streams["events_amm"].Ledger != 9 {
		t.Errorf("events_amm = %+v, want ledger 9", streams["events_amm"])
	}
}

// TestStateFileSingleStreamRefusesSharedFile checks that a reader without a
// stream ID refuses a shared file instead of misreading it — and that a
// single-stream writer cannot clobber every partition with its own state.
func TestStateFileSingleStreamRefusesSharedFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "shared.json")
	if err := openShared(t, path, "a").Save(stream.State{Source: "events", Ledger: 1}); err != nil {
		t.Fatal(err)
	}
	_, err := stream.OpenStateFile(path)
	if err == nil || !strings.Contains(err.Error(), "--stream-id") {
		t.Fatalf("OpenStateFile() error = %v, want a request for --stream-id", err)
	}
}

// TestStateFileSharedRewindTouchesOnlyItsStream checks that rewinding one
// stream of a shared file leaves the others exactly as they were.
func TestStateFileSharedRewindTouchesOnlyItsStream(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "shared.json")
	a := openShared(t, path, "a")
	b := openShared(t, path, "b")
	if err := a.Save(stream.State{Source: "events", Ledger: 100, PagingToken: "a100", LastID: "a100"}); err != nil {
		t.Fatal(err)
	}
	if err := b.Save(stream.State{Source: "events", Ledger: 200, PagingToken: "b200", LastID: "b200"}); err != nil {
		t.Fatal(err)
	}
	backup, err := a.Rewind(50)
	if err != nil {
		t.Fatalf("Rewind() error = %v", err)
	}
	if _, err := os.Stat(backup); err != nil {
		t.Errorf("backup %s missing: %v", backup, err)
	}
	streams := readShared(t, path)
	if got := streams["a"]; got.Ledger != 50 || got.PagingToken != "" || got.LastID != "" {
		t.Errorf("a after rewind = %+v, want ledger 50 with cleared cursor", got)
	}
	if got := streams["b"]; got.Ledger != 200 || got.PagingToken != "b200" {
		t.Errorf("b after rewinding a = %+v, want it untouched", got)
	}
}

func TestValidateStreamID(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"events_amm", "events/3fa2c1d09b7e4a55", "tx-main.v2"} {
		if err := stream.ValidateStreamID(id); err != nil {
			t.Errorf("ValidateStreamID(%q) = %v, want ok", id, err)
		}
	}
	for _, id := range []string{"", "has space", "tab\tid", "nl\n", strings.Repeat("x", 257)} {
		if err := stream.ValidateStreamID(id); err == nil {
			t.Errorf("ValidateStreamID(%q) = nil, want an error", id)
		}
	}
	if _, err := stream.OpenStateFileWithOptions(filepath.Join(t.TempDir(), "s.json"), stream.StateFileOptions{StreamID: "bad id"}); err == nil {
		t.Error("OpenStateFileWithOptions accepted an invalid stream ID")
	}
}
