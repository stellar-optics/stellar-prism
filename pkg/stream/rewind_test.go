package stream_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

func TestRewindCursorByLedgerCount(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.json")
	state, err := stream.OpenStateFile(path)
	if err != nil {
		t.Fatalf("OpenStateFile() error = %v", err)
	}
	if err := state.Save(stream.State{
		Source: "events", Ledger: 1000, PagingToken: "cursor", Cursor: "cursor", LastID: "record",
	}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	if err := stream.RewindCursor(path, 100, 0); err != nil {
		t.Fatalf("RewindCursor() error = %v", err)
	}
	got, err := state.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.Ledger != 900 {
		t.Errorf("Ledger = %d, want 900", got.Ledger)
	}
	if got.PagingToken != "" || got.Cursor != "" || got.LastID != "" {
		t.Errorf("rewound state retained cursor data: %+v", got)
	}
}

func TestRewindCursorByDuration(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.json")
	state, err := stream.OpenStateFile(path)
	if err != nil {
		t.Fatalf("OpenStateFile() error = %v", err)
	}
	if err := state.Save(stream.State{Source: "events", Ledger: 1000}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	if err := stream.RewindCursor(path, 0, 2*time.Hour); err != nil {
		t.Fatalf("RewindCursor() error = %v", err)
	}
	got, err := state.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.Ledger != 1000-1440 {
		t.Errorf("Ledger = %d, want %d", got.Ledger, 1000-1440)
	}
}

func TestRewindCursorRejectsInvalidDistance(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		ledgers  uint32
		duration time.Duration
	}{
		{}, {ledgers: 1, duration: time.Second}, {duration: -time.Second},
	} {
		if err := stream.RewindCursor(filepath.Join(t.TempDir(), "state.json"), tc.ledgers, tc.duration); err == nil {
			t.Errorf("RewindCursor(%d, %s) error = nil, want a validation error", tc.ledgers, tc.duration)
		}
	}
}

func TestRewindCursorClampsAtFirstLedger(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "state.json")
	state, err := stream.OpenStateFile(path)
	if err != nil {
		t.Fatalf("OpenStateFile() error = %v", err)
	}
	if err := state.Save(stream.State{Source: "events", Ledger: 5}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	if err := stream.RewindCursor(path, 100, 0); err != nil {
		t.Fatalf("RewindCursor() error = %v", err)
	}
	got, err := state.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.Ledger != stream.FirstLedger {
		t.Errorf("Ledger = %d, want %d", got.Ledger, stream.FirstLedger)
	}
}

// TestRewindCursorRejectsEmptyState ensures an absent cursor is not rewritten
// as though there were a known position to rewind from.
func TestRewindCursorRejectsEmptyState(t *testing.T) {
	t.Parallel()
	if err := stream.RewindCursor(filepath.Join(t.TempDir(), "state.json"), 1, 0); err == nil {
		t.Fatal("RewindCursor() error = nil, want empty state failure")
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

func TestRewindDistanceRejectsUnknownUnit(t *testing.T) {
	t.Parallel()
	if _, err := (stream.RewindDistance{}).Ledgers(); err == nil {
		t.Fatal("Ledgers() error = nil, want invalid distance error")
	}
	if _, err := (stream.RewindDistance{Unit: "fortnights"}).Ledgers(); err == nil {
		t.Fatal("Ledgers() error = nil, want unknown unit error")
	}
	if _, err := (stream.RewindDistance{Unit: stream.RewindLedgers}).Ledgers(); err == nil {
		t.Fatal("Ledgers() error = nil, want zero distance error")
	}
}

func TestRewindTarget(t *testing.T) {
	t.Parallel()
	distance := stream.RewindDistance{LedgerCount: 10, Unit: stream.RewindLedgers}
	target, err := distance.Target(1000)
	if err != nil {
		t.Fatalf("Target() error = %v", err)
	}
	if target.Ledger != 990 {
		t.Errorf("Ledger = %d, want 990", target.Ledger)
	}
}

// TestRewindCursorWritesBackup checks an atomic rewind retains the previous
// complete state in a backup file.
func TestRewindCursorWritesBackup(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "state.json")
	state, err := stream.OpenStateFile(path)
	if err != nil {
		t.Fatalf("OpenStateFile() error = %v", err)
	}
	if err := state.Save(stream.State{Source: "events", Ledger: 100}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	if err := stream.RewindCursor(path, 10, 0); err != nil {
		t.Fatalf("RewindCursor() error = %v", err)
	}
	entries, err := filepath.Glob(path + ".backup-*")
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("backup count = %d, want 1", len(entries))
	}
}

var _ = context.Background
var _ = errors.Is
var _ = fakerpc.NewServer
