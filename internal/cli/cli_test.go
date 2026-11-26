package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/rpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// withFake swaps the RPC constructor for one returning srv, restoring it when
// the test finishes. This is what lets the commands be exercised end to end
// with no network.
func withFake(t *testing.T, srv *fakerpc.Server) {
	t.Helper()
	original := newClient
	newClient = func([]string, time.Duration) (rpc.Client, error) { return srv, nil }
	t.Cleanup(func() { newClient = original })
}

// fixture reads real captured mainnet XDR, so the explain path is exercised
// against a genuine transaction rather than a synthetic one.
func fixture(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return strings.TrimSpace(string(b))
}

func run(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	var out, errBuf bytes.Buffer
	err = Execute(args, &out, &errBuf)
	return out.String(), errBuf.String(), err
}

func TestEventsCommandHumanOutput(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, _, err := run(t, "events", "--from", "1000", "--color", "never")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	for _, want := range []string{"ledger 1000", "contract", "topic[0]", "value", "ScVal"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("output does not contain %q\n%s", want, stdout)
		}
	}
	// Payloads must be rendered by lens, which means the ScVal decodes to its
	// enum constant rather than a bare number.
	if !strings.Contains(stdout, "ScValTypeScvU32") {
		t.Errorf("payload was not rendered through lens\n%s", stdout)
	}
}

func TestEventsCommandNDJSON(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(2000, 4), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, _, err := run(t, "events", "--from", "2000", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	lines := nonEmptyLines(stdout)
	if len(lines) != 4 {
		t.Fatalf("got %d NDJSON lines, want 4\n%s", len(lines), stdout)
	}

	for i, line := range lines {
		var rec struct {
			Kind     string          `json:"kind"`
			ID       string          `json:"id"`
			Ledger   uint32          `json:"ledger"`
			Contract string          `json:"contract"`
			Value    json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("line %d is not valid JSON: %v\n%s", i, err, line)
		}
		if rec.Kind != "event" {
			t.Errorf("line %d kind = %q, want %q", i, rec.Kind, "event")
		}
		if rec.ID == "" || rec.Ledger == 0 {
			t.Errorf("line %d is missing id or ledger: %s", i, line)
		}
		// The decoded value must carry lens's {type, value} envelope, so jq
		// expressions written against lens work here unchanged.
		if !bytes.Contains(rec.Value, []byte(`"type"`)) {
			t.Errorf("line %d value is not a lens envelope: %s", i, rec.Value)
		}
	}
}

func TestEventsCommandDryRunHasNoOutputSideEffects(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(2100, 2), fakerpc.WithPageLimit(10))
	withFake(t, srv)
	path := filepath.Join(t.TempDir(), "records.ndjson")

	stdout, stderr, err := run(t, "events", "--from", "2100", "--dry-run", "--output-file", path)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if stdout != "" {
		t.Fatalf("stdout = %q, want no delivered records", stdout)
	}
	if !strings.Contains(stderr, "dry-run: match") {
		t.Fatalf("stderr = %q, want dry-run diagnostics", stderr)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("output file exists or stat failed: %v", err)
	}
}

func TestEventsCommandRawSkipsDecoding(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(3000, 2), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, _, err := run(t, "events", "--from", "3000", "--json", "--raw")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(stdout, "valueXdr") {
		t.Errorf("--raw did not preserve base64 XDR\n%s", stdout)
	}
	if strings.Contains(stdout, `"value":{`) {
		t.Errorf("--raw still decoded the payload\n%s", stdout)
	}
}

func TestEventsCommandLimit(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(4000, 50), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, _, err := run(t, "events", "--from", "4000", "--json", "--limit", "3")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(nonEmptyLines(stdout)); got != 3 {
		t.Errorf("emitted %d records, want 3", got)
	}
}

// TestEventsCommandGapExitsTwo covers the documented exit-code contract for
// the one case where records are genuinely lost.
func TestEventsCommandGapExitsTwo(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(5000, 40),
		fakerpc.WithPageLimit(5),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {OldestLedger: 9000000},
		}),
	)
	withFake(t, srv)

	_, stderr, err := run(t, "events", "--from", "5000", "--json", "--page-size", "5")
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
	for _, want := range []string{"gap", "missed", "--from"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr does not mention %q\n%s", want, stderr)
		}
	}
}

func TestEventsCommandRejectsBadFilters(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 2))
	withFake(t, srv)

	tests := []struct {
		name string
		args []string
	}{
		{
			name: "too many contracts",
			args: []string{"events", "--from", "100",
				"--contract", "C1", "--contract", "C2", "--contract", "C3",
				"--contract", "C4", "--contract", "C5", "--contract", "C6"},
		},
		{
			name: "unknown event type",
			args: []string{"events", "--from", "100", "--type", "nonsense"},
		},
		{
			name: "topic segment is not base64 XDR",
			args: []string{"events", "--from", "100", "--topic", "not-valid-xdr"},
		},
		{
			name: "zero-or-more wildcard not last",
			args: []string{"events", "--from", "100", "--topic", "**,AAAAAwAAAAc="},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := run(t, tc.args...); err == nil {
				t.Fatal("Execute() error = nil, want a validation failure")
			}
		})
	}
}

func TestLedgersCommand(t *testing.T) {
	srv := fakerpc.NewLedgerServer(fakerpc.MakeLedgers(7000, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, _, err := run(t, "ledgers", "--from", "7000", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	lines := nonEmptyLines(stdout)
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3\n%s", len(lines), stdout)
	}
	var rec struct {
		Kind   string `json:"kind"`
		Ledger uint32 `json:"ledger"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	if rec.Kind != "ledger" || rec.Ledger != 7000 {
		t.Errorf("got kind=%q ledger=%d, want kind=ledger ledger=7000", rec.Kind, rec.Ledger)
	}
}

// TestStreamStartsAtHeadWithoutFrom checks that omitting --from resolves the
// server's latest ledger rather than failing or starting from genesis.
func TestStreamStartsAtHeadWithoutFrom(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(8000, 5), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, _, err := run(t, "events", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	// The fake's head is the last event's ledger, so only that one qualifies.
	if got := len(nonEmptyLines(stdout)); got != 1 {
		t.Errorf("emitted %d records starting at head, want 1\n%s", got, stdout)
	}
}

func TestTxCommand(t *testing.T) {
	srv := fakerpc.NewServer(nil)
	srv.SetTransaction("abc123", protocol.GetTransactionResponse{
		TransactionDetails: protocol.TransactionDetails{
			Status:          protocol.TransactionStatusSuccess,
			TransactionHash: "abc123",
			EnvelopeXDR:     fixture(t, "tx_failed.env.txt"),
			ResultXDR:       fixture(t, "tx_failed.res.txt"),
		},
	})
	withFake(t, srv)

	stdout, _, err := run(t, "tx", "abc123", "--color", "never")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	// The output must come from the lens explain path.
	for _, want := range []string{"Source", "Operations", "Outcome"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("output does not contain %q\n%s", want, stdout)
		}
	}
}

func TestTxCommandNotFound(t *testing.T) {
	srv := fakerpc.NewServer(nil)
	withFake(t, srv)

	_, _, err := run(t, "tx", "missing")
	if err == nil {
		t.Fatal("Execute() error = nil, want a not-found failure")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error = %q, want it to say the transaction was not found", err)
	}
}

func TestVersionCommand(t *testing.T) {
	stdout, _, err := run(t, "version")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if strings.TrimSpace(stdout) == "" {
		t.Error("version printed nothing")
	}
}

func TestBareInvocationPrintsHelp(t *testing.T) {
	stdout, _, err := run(t)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, want := range []string{"events", "ledgers", "tx", "Usage"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("help does not mention %q", want)
		}
	}
}

// TestColorFlagMatchesLens checks the shared convention: prism uses --color
// with the same three values as lens, not a --no-color boolean.
func TestColorFlagMatchesLens(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 1), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	always, _, err := run(t, "events", "--from", "100", "--color", "always")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	withFake(t, srv)
	never, _, err := run(t, "events", "--from", "100", "--color", "never")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if !strings.Contains(always, "\x1b[") {
		t.Error("--color always produced no escape sequences")
	}
	if strings.Contains(never, "\x1b[") {
		t.Error("--color never produced escape sequences")
	}
}

// TestEventsCommandStateFileResumesAcrossRestarts is the end-to-end proof of
// durable cursors: two independent CLI runs sharing nothing but a state file
// must together deliver exactly the event universe — in order, with no gaps
// and no duplicates — the way a restarted process would.
func TestEventsCommandStateFileResumesAcrossRestarts(t *testing.T) {
	events := fakerpc.MakeEvents(1000, 6)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(3))
	withFake(t, srv)

	statePath := filepath.Join(t.TempDir(), "prism.state.json")

	// First run: take two records and stop.
	stdout1, _, err := run(t,
		"events", "--from", "1000", "--json", "--state-file", statePath,
		"--page-size", "3", "--limit", "2")
	if err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}

	// Second run: no --from, no limit. The state file must carry the
	// position; nothing else links the two runs.
	stdout2, _, err := run(t, "events", "--json", "--state-file", statePath, "--page-size", "3")
	if err != nil {
		t.Fatalf("second Execute() error = %v", err)
	}

	var got []string
	for _, line := range append(nonEmptyLines(stdout1), nonEmptyLines(stdout2)...) {
		var rec struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("not valid JSON: %v\n%s", err, line)
		}
		got = append(got, rec.ID)
	}

	want := make([]string, 0, len(events))
	for _, e := range events {
		want = append(want, e.ID)
	}
	if len(got) != len(want) {
		t.Fatalf("delivered %d records across restarts, want %d\n got: %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("record %d = %q, want %q", i, got[i], want[i])
		}
	}

	// The file must end at the true final position, in the documented
	// schema.
	data, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("reading state file: %v", err)
	}
	// The CLI keys every stream by its stream ID, so the file is a shared
	// (partitioned) document holding this one stream.
	var doc struct {
		Version int                     `json:"version"`
		Streams map[string]stream.State `json:"streams"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("state file is not valid JSON: %v\n%s", err, data)
	}
	if doc.Version != stream.PartitionedStateVersion || len(doc.Streams) != 1 {
		t.Fatalf("state file = %s, want one stream in a version %d document", data, stream.PartitionedStateVersion)
	}
	var st stream.State
	for id, entry := range doc.Streams {
		if !strings.HasPrefix(id, "events/") {
			t.Errorf("derived stream ID = %q, want it to start with the command name", id)
		}
		st = entry
	}
	last := events[len(events)-1]
	if st.Version != stream.StateVersion || st.Source != "events" ||
		st.LastID != last.ID || st.PagingToken != last.ID ||
		st.Ledger != uint32(last.Ledger) { //nolint:gosec // test data
		t.Errorf("final state = %+v, want the position of event %q", st, last.ID)
	}
}

// TestEventsCommandStateFileSaysSoWhenIgnoringFrom checks the precedence
// rule is visible: a resumed run that also carries --from must announce that
// the flag was overridden, not silently drop it.
func TestEventsCommandStateFileSaysSoWhenIgnoringFrom(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(2000, 3), fakerpc.WithPageLimit(3))
	withFake(t, srv)

	statePath := filepath.Join(t.TempDir(), "prism.state.json")
	if _, _, err := run(t,
		"events", "--from", "2000", "--json", "--state-file", statePath, "--limit", "1"); err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}

	_, stderr, err := run(t,
		"events", "--from", "2000", "--json", "--state-file", statePath)
	if err != nil {
		t.Fatalf("second Execute() error = %v", err)
	}
	for _, want := range []string{"resuming", "ignoring --from"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr does not mention %q\n%s", want, stderr)
		}
	}
}

// TestEventsCommandStateFileFailsFastOnCorruption checks a damaged state
// file is reported before any streaming happens, rather than silently
// restarting the stream from the top.
func TestEventsCommandStateFileFailsFastOnCorruption(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(3000, 2), fakerpc.WithPageLimit(2))
	withFake(t, srv)

	statePath := filepath.Join(t.TempDir(), "prism.state.json")
	if err := os.WriteFile(statePath, []byte("not json"), 0o600); err != nil {
		t.Fatalf("writing corrupt state file: %v", err)
	}

	_, _, err := run(t, "events", "--from", "3000", "--json", "--state-file", statePath)
	if err == nil {
		t.Fatal("Execute() error = nil, want a state file failure")
	}
	if !strings.Contains(err.Error(), "not valid JSON") {
		t.Errorf("error = %q, want it to report the parse failure", err)
	}
	if srv.Calls() != 0 {
		t.Errorf("made %d RPC call(s) before rejecting the state file, want 0", srv.Calls())
	}
}

// TestLedgersCommandStateFileFlagRegistered checks the shared --state-file
// flag reaches the other streaming commands and produces a durable position
// there too (the resume path itself is covered by the events tests above).
func TestLedgersCommandStateFileFlagRegistered(t *testing.T) {
	srv := fakerpc.NewLedgerServer(fakerpc.MakeLedgers(7000, 1), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	statePath := filepath.Join(t.TempDir(), "prism.state.json")
	stdout, _, err := run(t, "ledgers", "--from", "7000", "--json", "--state-file", statePath)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(nonEmptyLines(stdout)) != 1 {
		t.Errorf("emitted %d lines, want 1\n%s", len(nonEmptyLines(stdout)), stdout)
	}
	if _, err := os.Stat(statePath); err != nil {
		t.Errorf("state file was not written: %v", err)
	}
}

func nonEmptyLines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.TrimSpace(l) != "" {
			out = append(out, l)
		}
	}
	return out
}

func TestEventsCommandGapResume(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(6000, 35),
		fakerpc.WithPageLimit(5),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {OldestLedger: 6010},
		}),
	)
	withFake(t, srv)

	stdout, stderr, err := run(t, "events", "--from", "6000", "--json",
		"--page-size", "5", "--gap-strategy", "resume")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	lines := nonEmptyLines(stdout)
	if len(lines) != 30 {
		t.Fatalf("emitted %d records after resuming, want 30\n%s", len(lines), stdout)
	}
	if !strings.Contains(stderr, "recovering by resuming at ledger 6010") {
		t.Errorf("stderr does not explain the recovery\n%s", stderr)
	}
}

func TestEventsCommandBackfillWorkers(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(9000, 30), fakerpc.WithPageLimit(5))
	withFake(t, srv)

	stdout, _, err := run(t, "events", "--from", "9000", "--json", "--page-size", "5", "--backfill-workers", "3")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(nonEmptyLines(stdout)); got != 30 {
		t.Errorf("emitted %d records with backfill workers, want 30\n%s", got, stdout)
	}
}

func TestEventsCommandBatching(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(10000, 20), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, _, err := run(t, "events", "--from", "10000", "--json",
		"--batch", "--batch-size", "3", "--batch-timeout", "20ms", "--limit", "7")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(nonEmptyLines(stdout)); got != 7 {
		t.Errorf("emitted %d batched records, want 7\n%s", got, stdout)
	}
}

func TestEventsCommandRejectsInvalidStreamOptions(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 2))
	withFake(t, srv)

	tests := []struct {
		name string
		args []string
	}{
		{
			name: "unknown gap strategy",
			args: []string{"events", "--from", "100", "--gap-strategy", "skip"},
		},
		{
			name: "backfill workers below one",
			args: []string{"events", "--from", "100", "--backfill-workers", "0"},
		},
		{
			name: "batching without a threshold",
			args: []string{"events", "--from", "100", "--batch", "--batch-size", "0", "--batch-timeout", "0"},
		},
		{
			name: "negative batch size",
			args: []string{"events", "--from", "100", "--batch", "--batch-size", "-1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := run(t, tc.args...); err == nil {
				t.Fatal("Execute() error = nil, want a validation failure")
			}
		})
	}
}

func TestEventsHelpAdvertisesStreamOptions(t *testing.T) {
	stdout, _, err := run(t, "events", "--help")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, flag := range []string{
		"--backfill-workers", "--batch", "--batch-size", "--batch-timeout",
		"--gap-strategy", "--max-gap-recoveries", "--dlq-file",
	} {
		if !strings.Contains(stdout, flag) {
			t.Errorf("help does not mention %s", flag)
		}
	}
}

// TestEventsCommandSince converts a relative window into an approximate
// start ledger against the fake's head, covering the whole flag path.
func TestEventsCommandSince(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(2026, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, stderr, err := run(t, "events", "--since", "1m", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// The fake's head is ledger 2028, so --since 1m resolves to 2028 - 12
	// = 2016, and all three events qualify.
	if got := len(nonEmptyLines(stdout)); got != 3 {
		t.Errorf("emitted %d records with --since 1m, want 3\n%s", got, stdout)
	}
	for _, want := range []string{"--since 1m", "start ledger"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr does not mention %q\n%s", want, stderr)
		}
	}
}

// TestEventsCommandSinceRejects checks the guard rails: --since and --from
// cannot mix, a negative window is refused, and both before any network call
// beyond the head lookup a lone --since needs.
func TestEventsCommandSinceRejects(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 2), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "together with --from",
			args: []string{"events", "--from", "100", "--since", "1m"},
			want: "mutually exclusive",
		},
		{
			name: "negative window",
			args: []string{"events", "--since", "-5m"},
			want: "--since must not be negative",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := run(t, tc.args...)
			if err == nil {
				t.Fatal("Execute() error = nil, want a rejection")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err.Error(), tc.want)
			}
		})
	}
}

// TestLedgersCommandSince checks the flag is shared by the streaming
// commands, not welded to events.
func TestLedgersCommandSince(t *testing.T) {
	srv := fakerpc.NewLedgerServer(fakerpc.MakeLedgers(7000, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	// Head is 7002 here, so --since 1m starts from 6990 and all three
	// ledgers qualify.
	stdout, _, err := run(t, "ledgers", "--since", "1m", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(nonEmptyLines(stdout)); got != 3 {
		t.Errorf("emitted %d ledger records with --since 1m, want 3\n%s", got, stdout)
	}
}

// TestEventsCommandTopicPattern covers the client-side filter end to end:
// matching patterns pass records, non-matching ones suppress them, and a
// malformed pattern fails the whole command at construction time.
func TestEventsCommandTopicPattern(t *testing.T) {
	contract := "CBGSBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKKY3"
	srv := fakerpc.NewServer(fakerpc.MakeEvents(4000, 4), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	// Every fixture topic is the u32 value 7, so the decoded-text pattern
	// "7" passes everything and "nope" passes nothing.
	for _, tc := range []struct {
		name string
		pat  string
		want int
	}{
		{name: "numeric leaf matches", pat: "7", want: 4},
		{name: "non-matching pattern", pat: "nope", want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withFake(t, srv)
			stdout, _, err := run(t, "events", "--from", "4000", "--topic-pattern", tc.pat, "--json")
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if got := len(nonEmptyLines(stdout)); got != tc.want {
				t.Errorf("pattern %q emitted %d lines, want %d\n%s", tc.pat, got, tc.want, stdout)
			}
		})
	}

	withFake(t, srv)
	_, _, err := run(t, "events", "--from", "4000", "--topic-pattern", "re:[")
	if err == nil {
		t.Fatal("Execute() with an invalid regex error = nil, want a compile failure")
	}
	if strings.Contains(err.Error(), contract) {
		t.Errorf("error needlessly quotes the contract id: %q", err.Error())
	}
}

// TestEventsCommandFanoutDir checks the per-contract NDJSON wiring: one file
// per contract, carrying exactly the stream's events, while stdout keeps the
// combined output.
func TestEventsCommandFanoutDir(t *testing.T) {
	dir := t.TempDir()
	contract := fakerpc.MakeEvents(6000, 1)[0].ContractID
	srv := fakerpc.NewServer(fakerpc.MakeEvents(6000, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, _, err := run(t, "events", "--from", "6000", "--json", "--fanout-dir", dir)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got, want := len(nonEmptyLines(stdout)), 3; got != want {
		t.Errorf("stdout carried %d lines, want %d\n%s", got, want, stdout)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("fanout wrote %d files, want 1", len(entries))
	}
	wantName := contract + ".ndjson"
	if entries[0].Name() != wantName {
		t.Errorf("fanout file named %q, want %q", entries[0].Name(), wantName)
	}

	b, err := os.ReadFile(filepath.Join(dir, wantName))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	lines := nonEmptyLines(string(b))
	if len(lines) != 3 {
		t.Fatalf("fanout file has %d lines, want 3: %s", len(lines), lines)
	}
	for i, line := range lines {
		var rec struct {
			Kind     string `json:"kind"`
			ID       string `json:"id"`
			Contract string `json:"contract"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("line %d is not valid JSON: %v\n%s", i, err, line)
		}
		if rec.Kind != "event" || rec.Contract != contract {
			t.Errorf("line %d = kind %q contract %q, want event/%s", i, rec.Kind, rec.Contract, contract)
		}
	}
}

// TestEventsCommandFanoutDirRejectsBadDirectory checks a misconfigured
// directory fails before streaming instead of per record.
func TestEventsCommandFanoutDirRejectsBadDirectory(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(7000, 1), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	// A file, not a directory: mkdir fails on the spot.
	file := filepath.Join(t.TempDir(), "plain.txt")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	_, _, err := run(t, "events", "--from", "7000", "--fanout-dir", file)
	if err == nil {
		t.Fatal("Execute() error = nil, want a directory failure")
	}
}

// TestEventsCommandAdaptiveFlagRuns checks the adaptive switch wires through
// the opts without disturbing the drain path a non-follow run takes.
func TestEventsCommandAdaptiveFlagRuns(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(8000, 5), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, _, err := run(t, "events", "--from", "8000", "--json", "--adaptive")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got, want := len(nonEmptyLines(stdout)), 5; got != want {
		t.Errorf("emitted %d records with --adaptive, want %d", got, want)
	}
}

// safeBuffer is a buffer that may be read while a command is still writing
// to it, which is what a test that interrupts a running command needs.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// waitForOutput waits until the buffer holds want, or the test gives up.
func waitForOutput(t *testing.T, b *safeBuffer, want string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(b.String(), want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("the output never contained %q\n%s", want, b.String())
}

// TestEventsCommandShutsDownCleanlyOnSignal covers the graceful drain: a
// SIGTERM stops the fetching, the records still in memory are flushed, the
// final cursor is committed, and the command exits cleanly.
func TestEventsCommandShutsDownCleanlyOnSignal(t *testing.T) {
	// The signal is delivered to this process, so its disposition is
	// claimed before anything else happens: without a handler the runtime
	// would terminate the test binary.
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
			"--poll", "10ms",
			"--state-file", stateFile,
		}, stdout, stderr)
	}()

	// The first record proves the command is running, and therefore that
	// its signal handler is already in place.
	waitForOutput(t, stdout, `"ledger":1000`)

	// Interrupt the run the way an orchestrator does.
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

	// Every record the server had must be on stdout, and every line must
	// be whole: a shutdown must not leave a half-written record behind.
	lines := nonEmptyLines(stdout.String())
	if len(lines) != 3 {
		t.Errorf("stdout has %d records, want 3\n%s", len(lines), stdout.String())
	}
	for i, line := range lines {
		var rec struct {
			Kind   string `json:"kind"`
			Ledger uint32 `json:"ledger"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Errorf("line %d is not valid JSON: %v\n%s", i, err, line)
			continue
		}
		if rec.Kind != "event" {
			t.Errorf("line %d kind = %q, want %q", i, rec.Kind, "event")
		}
	}

	if !strings.Contains(stderr.String(), "draining") {
		t.Errorf("stderr does not report the drain\n%s", stderr.String())
	}

	// The final cursor must be committed, or the next run starts over
	// instead of where this one stopped.
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
