package stream_test

import (
	"context"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestDiagnosticSourceBasics(t *testing.T) {
	cur := protocol.Cursor{Ledger: 10, Tx: 1, Op: 0, Event: 0}.String()
	events := []protocol.EventInfo{
		{
			ID:              cur,
			EventType:       "diagnostic",
			ContractID:      "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM",
			TransactionHash: "a1b2c3d4",
			Ledger:          10,
			LedgerClosedAt:  "2026-01-01T00:00:00Z",
		},
	}
	srv := fakerpc.NewServer(events)
	src, err := stream.NewDiagnosticSource(srv, stream.DiagnosticQuery{
		StartLedger: 10,
		PageLimit:   10,
	})
	if err != nil {
		t.Fatal(err)
	}

	if src.Name() != "diagnostics" {
		t.Fatalf("expected name diagnostics, got %s", src.Name())
	}

	seq, ok := src.CursorLedger(cur)
	if !ok || seq != 10 {
		t.Fatalf("expected cursor ledger 10, got %d (ok=%v)", seq, ok)
	}

	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(batch.Records))
	}
	rec := batch.Records[0]
	if rec.Kind != stream.KindEvent || rec.Event.Type != "diagnostic" {
		t.Fatalf("unexpected record: %+v", rec)
	}

	src.Rewind(20)
}
