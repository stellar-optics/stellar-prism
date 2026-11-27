package cli

import (
	"bytes"
	"context"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestSuperviseServeRestartsAfterPanic(t *testing.T) {
	var calls int
	var delays []time.Duration
	var stderr bytes.Buffer
	err := superviseServe(context.Background(), 1, &stderr, func() error {
		calls++
		if calls == 1 {
			panic("injected crash")
		}
		return nil
	}, func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("run called %d times, want 2", calls)
	}
	if len(delays) != 1 || delays[0] != 5*time.Second {
		t.Fatalf("restart delays = %v, want [5s]", delays)
	}
	if !strings.Contains(stderr.String(), "recovered panic") || !strings.Contains(stderr.String(), "injected crash") {
		t.Fatalf("restart was not logged: %s", stderr.String())
	}
}

func TestSuperviseServeStopsAtMaxRestarts(t *testing.T) {
	var calls int
	err := superviseServe(context.Background(), 1, io.Discard, func() error {
		calls++
		panic("injected crash")
	}, func(context.Context, time.Duration) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "restart limit 1 reached") {
		t.Fatalf("supervisor error = %v, want restart limit error", err)
	}
	if calls != 2 {
		t.Fatalf("run called %d times, want 2", calls)
	}
}

func TestSuperviseServeRejectsNegativeMaxRestarts(t *testing.T) {
	called := false
	err := superviseServe(context.Background(), -1, io.Discard, func() error {
		called = true
		return nil
	}, func(context.Context, time.Duration) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "--max-restarts must not be negative") {
		t.Fatalf("supervisor error = %v, want invalid restart limit", err)
	}
	if called {
		t.Fatal("run called with invalid restart limit")
	}
}

func TestServeRestartDelayGrowsAndCaps(t *testing.T) {
	for _, test := range []struct {
		restarts int
		want     time.Duration
	}{
		{restarts: 1, want: 5 * time.Second},
		{restarts: 2, want: 10 * time.Second},
		{restarts: 3, want: 20 * time.Second},
		{restarts: 10, want: 5 * time.Minute},
	} {
		if got := serveRestartDelay(test.restarts); got != test.want {
			t.Errorf("serveRestartDelay(%d) = %s, want %s", test.restarts, got, test.want)
		}
	}
}

func TestBroadcastCLIFlagsAndServe(t *testing.T) {
	for _, args := range [][]string{
		{"events", "--serve-sse", "127.0.0.1:0"},
		{"serve", "--sse", "127.0.0.1:0"},
		{"serve", "--ws", "127.0.0.1:0"},
		{"serve", "--sse", "127.0.0.1:0", "--ws", "127.0.0.1:0", "--batch"},
	} {
		srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 1))
		withFake(t, srv)
		args = append(args, "--from", "100", "--limit", "1", "--json", "--raw")
		stdout, stderr, err := run(t, args...)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(stdout, `"kind":"event"`) || !strings.Contains(stderr, "broadcast listening at") {
			t.Fatalf("stdout=%s stderr=%s", stdout, stderr)
		}
	}
	if _, _, err := run(t, "serve"); err == nil {
		t.Fatal("serve accepted no listeners")
	}
	for _, args := range [][]string{{"ledgers", "--consensus", "--backfill-workers", "2"}, {"ledgers", "--expected-validator", "GEXPECTED"}} {
		if _, _, err := run(t, args...); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
func TestBroadcastOccupiedPortAndCleanup(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	out := stream.SinkFunc(func(stream.Record) error { return nil })
	if _, _, err := startBroadcasts(ctx, &globalFlags{serveSSE: listener.Addr().String()}, out, io.Discard, cancel); err == nil {
		t.Fatal("accepted occupied port")
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	sink, closeServers, err := startBroadcasts(ctx, &globalFlags{serveSSE: addr}, out, io.Discard, cancel)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Emit(stream.Record{ID: "1", Kind: stream.KindLedger, Close: &stream.LedgerClose{}}); err != nil {
		t.Fatal(err)
	}
	closeServers()
	rebound, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listener leaked: %v", err)
	}
	_ = rebound.Close()
}

func TestTransactionMemoCLI(t *testing.T) {
	var transactions []protocol.TransactionInfo
	for i, text := range []string{"skip", "invoice:\x00é", "invoice:second"} {
		var e xdr.TransactionEnvelope
		if err := xdr.SafeUnmarshalBase64(cliUploadEnvelope(t, []byte{0, 97, 115, 109}), &e); err != nil {
			t.Fatal(err)
		}
		e.V1.Tx.Memo = xdr.MemoText(text)
		encoded, err := xdr.MarshalBase64(e)
		if err != nil {
			t.Fatal(err)
		}
		transactions = append(transactions, protocol.TransactionInfo{TransactionDetails: protocol.TransactionDetails{Ledger: 100, ApplicationOrder: int32(i + 1), EnvelopeXDR: encoded}})
	}
	transactions[0].Status = protocol.TransactionStatusSuccess
	transactions[1].Status = protocol.TransactionStatusSuccess
	transactions[2].Status = protocol.TransactionStatusFailed
	withFake(t, fakerpc.NewTransactionServer(transactions))
	stdout, _, err := run(t, "transactions", "--from", "100", "--memo", "invoice:", "--json", "--raw")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(stdout, `"kind":"transaction"`) != 2 || !strings.Contains(stdout, `"value":"invoice:é"`) || strings.Contains(stdout, `"value":"skip"`) {
		t.Fatal(stdout)
	}
}

func TestTransactionMemoCombinesWithStatusAndCursorState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	var transactions []protocol.TransactionInfo
	for i, text := range []string{"invoice:paid", "other", "invoice:failed"} {
		var e xdr.TransactionEnvelope
		if err := xdr.SafeUnmarshalBase64(cliUploadEnvelope(t, []byte{0, 97, 115, 109}), &e); err != nil {
			t.Fatal(err)
		}
		e.V1.Tx.Memo = xdr.MemoText(text)
		encoded, err := xdr.MarshalBase64(e)
		if err != nil {
			t.Fatal(err)
		}
		status := protocol.TransactionStatusSuccess
		if i == 2 {
			status = protocol.TransactionStatusFailed
		}
		transactions = append(transactions, protocol.TransactionInfo{TransactionDetails: protocol.TransactionDetails{Ledger: 100, ApplicationOrder: int32(i + 1), EnvelopeXDR: encoded, Status: status}})
	}
	withFake(t, fakerpc.NewTransactionServer(transactions))
	for _, prefix := range []string{"invoice:", "other"} {
		stdout, _, err := run(t, "transactions", "--from", "100", "--memo", prefix, "--status", "success", "--json", "--raw", "--state-file", path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(stdout, `"kind":"transaction"`) != 1 || strings.Contains(stdout, "invoice:failed") {
			t.Fatal(stdout)
		}
		key := stateKey("transactions", "status=success", "memo="+prefix)
		if entries := stateEntries(t, path, key); len(entries) == 0 {
			t.Fatalf("no cursor for %s", key)
		}
	}
	// Existing unfiltered checkpoints retain their original state key.
	if _, _, err := run(t, "transactions", "--from", "100", "--json", "--raw", "--state-file", path); err != nil {
		t.Fatal(err)
	}
	if entries := stateEntries(t, path, "transactions"); len(entries) == 0 {
		t.Fatal("default cursor key changed")
	}
}
