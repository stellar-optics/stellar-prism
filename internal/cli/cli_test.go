package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// withFake swaps the RPC constructor for one returning srv, restoring it when
// the test finishes. This is what lets the commands be exercised end to end
// with no network.
func withFake(t *testing.T, srv *fakerpc.Server) {
	t.Helper()
	original := newClient
	newClient = func(string, time.Duration) rpc.Client { return srv }
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

func nonEmptyLines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.TrimSpace(l) != "" {
			out = append(out, l)
		}
	}
	return out
}
