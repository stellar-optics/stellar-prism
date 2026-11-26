package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// committedCursor is one recorded position read back from a state file.
type committedCursor struct {
	Cursor string `json:"cursor"`
	Ledger uint32 `json:"ledger"`
}

// committedCursors returns every non-empty cursor recorded in a state file,
// whichever layout it uses: a cursor metadata journal (one entry per line), a
// single-stream state file (one entry), or a shared state file (one entry
// per stream).
func committedCursors(t *testing.T, data []byte) []committedCursor {
	t.Helper()
	var out []committedCursor
	for _, line := range nonEmptyLines(string(data)) {
		var meta struct {
			committedCursor
			Streams map[string]committedCursor `json:"streams"`
		}
		if err := json.Unmarshal([]byte(line), &meta); err != nil {
			t.Errorf("the state file holds an unreadable entry: %v\n%s", err, line)
			continue
		}
		entries := append([]committedCursor{meta.committedCursor}, mapValues(meta.Streams)...)
		for _, e := range entries {
			if e.Cursor != "" {
				out = append(out, e)
			}
		}
	}
	return out
}

func mapValues[K comparable, V any](m map[K]V) []V {
	out := make([]V, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

// newStreamIDCmd builds a command shaped like a streaming subcommand: one
// filter flag registered before the shared stream flags and one after, so
// derivation is tested against both registration orders.
func newStreamIDCmd(t *testing.T, args ...string) (*cobra.Command, *streamFlags) {
	t.Helper()
	var sf streamFlags
	var contracts, topics []string
	cmd := &cobra.Command{Use: "events", RunE: func(*cobra.Command, []string) error { return nil }}
	cmd.Flags().StringSliceVar(&contracts, "contract", nil, "")
	sf.register(cmd, 100)
	cmd.Flags().StringArrayVar(&topics, "topic", nil, "")
	if err := cmd.ParseFlags(args); err != nil {
		t.Fatalf("ParseFlags(%v) error = %v", args, err)
	}
	return cmd, &sf
}

func TestResolveStreamIDIsDeterministicOverFilters(t *testing.T) {
	const contract = "CBGSBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKKY3"
	id := func(args ...string) string {
		cmd, sf := newStreamIDCmd(t, args...)
		return sf.resolveStreamID(cmd, "testnet")
	}

	base := id("--contract", contract)
	if !strings.HasPrefix(base, "events/") || len(base) != len("events/")+16 {
		t.Errorf("derived ID = %q, want events/<16 hex digits>", base)
	}
	if again := id("--contract", contract); again != base {
		t.Errorf("same command derived %q then %q; the ID must be deterministic", base, again)
	}
	// Operational flags describe delivery, not the stream.
	if got := id("--contract", contract, "--from", "123", "--limit", "5", "--json", "--follow",
		"--state-file", "x.json", "--prune-after", "1d", "--output-file", "out.ndjson"); got != base {
		t.Errorf("operational flags changed the derived ID: %q vs %q", got, base)
	}
	// Filters — registered before or after the shared flags — are the stream.
	if got := id(); got == base {
		t.Error("dropping the contract filter kept the same ID")
	}
	if got := id("--contract", contract, "--topic", "AAAA"); got == base {
		t.Error("adding a topic filter kept the same ID")
	}
	// Flag order on the command line does not matter.
	if a, b := id("--contract", contract, "--topic", "AAAA"), id("--topic", "AAAA", "--contract", contract); a != b {
		t.Errorf("flag order changed the ID: %q vs %q", a, b)
	}
	// The network is part of the stream.
	cmd, sf := newStreamIDCmd(t, "--contract", contract)
	if got := sf.resolveStreamID(cmd, "mainnet"); got == base {
		t.Error("a different network derived the same ID")
	}
	// An explicit --stream-id wins.
	cmd, sf = newStreamIDCmd(t, "--contract", contract, "--stream-id", "events_amm")
	if got := sf.resolveStreamID(cmd, "testnet"); got != "events_amm" {
		t.Errorf("explicit --stream-id resolved to %q", got)
	}
}

func TestParseRetention(t *testing.T) {
	ok := map[string]time.Duration{
		"":    0,
		"0":   0,
		"7d":  7 * 24 * time.Hour,
		"1D":  24 * time.Hour,
		"36h": 36 * time.Hour,
		"90m": 90 * time.Minute,
	}
	for in, want := range ok {
		got, err := parseRetention(in)
		if err != nil || got != want {
			t.Errorf("parseRetention(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"7x", "-1h", "d", "1.5d", "seven"} {
		if _, err := parseRetention(in); err == nil {
			t.Errorf("parseRetention(%q) = nil error, want a rejection", in)
		}
	}
}

func TestStateFlagsValidation(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 1))
	withFake(t, srv)
	dir := t.TempDir()
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"events", "--stream-id", "x"}, "--stream-id requires --state-file"},
		{[]string{"events", "--prune-after", "1d"}, "--prune-after requires --state-file"},
		{[]string{"events", "--state-file", filepath.Join(dir, "s.json"), "--stream-id", "has space"}, "invalid --stream-id"},
		{[]string{"events", "--state-file", filepath.Join(dir, "s.json"), "--prune-after", "soon"}, "invalid --prune-after"},
		{[]string{"events", "--ha-standby-poll", "-1s"}, "--ha-standby-poll must not be negative"},
	}
	for _, tc := range cases {
		_, _, err := run(t, tc.args...)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v: error = %v, want %q", tc.args, err, tc.want)
		}
	}
}

// TestStreamIDsShareOneStateFileAcrossRuns runs several independent CLI
// invocations against one --state-file: each --stream-id gets its own
// partition, each resumes only its own cursor, and a run without --stream-id
// lands in its derived partition.
func TestStreamIDsShareOneStateFileAcrossRuns(t *testing.T) {
	events := fakerpc.MakeEvents(1000, 6)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(3))
	withFake(t, srv)
	path := filepath.Join(t.TempDir(), "shared.state.json")

	runIDs := func(args ...string) []string {
		t.Helper()
		stdout, stderr, err := run(t, append([]string{"events", "--json", "--state-file", path, "--page-size", "3"}, args...)...)
		if err != nil {
			t.Fatalf("events %v error = %v\n%s", args, err, stderr)
		}
		var ids []string
		for _, line := range nonEmptyLines(stdout) {
			var rec struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				t.Fatalf("not valid JSON: %v\n%s", err, line)
			}
			ids = append(ids, rec.ID)
		}
		return ids
	}

	// events_amm takes two records, events_main takes four.
	if got := runIDs("--stream-id", "events_amm", "--from", "1000", "--limit", "2"); len(got) != 2 {
		t.Fatalf("events_amm first run delivered %d records", len(got))
	}
	if got := runIDs("--stream-id", "events_main", "--from", "1000", "--limit", "4"); len(got) != 4 {
		t.Fatalf("events_main first run delivered %d records", len(got))
	}
	// Each resumes from its own cursor, not the other's.
	amm := runIDs("--stream-id", "events_amm")
	main := runIDs("--stream-id", "events_main")
	if len(amm) != 4 || amm[0] != events[2].ID {
		t.Errorf("events_amm resumed with %v, want the 4 records after its own cursor (from %s)", amm, events[2].ID)
	}
	if len(main) != 2 || main[0] != events[4].ID {
		t.Errorf("events_main resumed with %v, want the 2 records after its own cursor (from %s)", main, events[4].ID)
	}
	// A run without --stream-id gets a derived partition of its own.
	if got := runIDs("--from", "1000", "--limit", "1"); len(got) != 1 {
		t.Fatalf("derived-ID run delivered %d records", len(got))
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Version int                     `json:"version"`
		Streams map[string]stream.State `json:"streams"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("state file is not valid JSON: %v\n%s", err, data)
	}
	if len(doc.Streams) != 3 {
		t.Fatalf("state file holds %d streams, want 3 (events_amm, events_main, derived)\n%s", len(doc.Streams), data)
	}
	last := events[len(events)-1].ID
	for _, id := range []string{"events_amm", "events_main"} {
		if got := doc.Streams[id].LastID; got != last {
			t.Errorf("%s last ID = %q, want %q", id, got, last)
		}
	}

	// prism rewind needs to know which stream to rewind in a shared file.
	if _, _, err := run(t, "rewind", "--state-file", path, "--back", "1"); err == nil ||
		!strings.Contains(err.Error(), "--stream-id") {
		t.Errorf("rewind without --stream-id on a shared file: error = %v, want a request for --stream-id", err)
	}
	out, _, err := run(t, "rewind", "--state-file", path, "--stream-id", "events_amm", "--to-ledger", "1001")
	if err != nil {
		t.Fatalf("rewind --stream-id events_amm error = %v", err)
	}
	if !strings.Contains(out, "events_amm") {
		t.Errorf("rewind output %q does not name the stream", out)
	}
}
