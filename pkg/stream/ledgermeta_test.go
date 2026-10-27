package stream_test

import (
	"context"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestLedgerMetaSourceBasics(t *testing.T) {
	srv := fakerpc.NewLedgerServer([]protocol.LedgerInfo{
		consensusLedger(t, 10, 100, true),
		consensusLedger(t, 11, 105, true),
	})
	src := stream.NewLedgerMetaSource(srv, stream.LedgerMetaQuery{
		StartLedger: 10,
		PageLimit:   2,
		Consensus:   true,
	})

	if src.Name() != "ledger-meta" {
		t.Fatalf("expected name ledger-meta, got %s", src.Name())
	}

	seq, ok := src.CursorLedger("10")
	if !ok || seq != 10 {
		t.Fatalf("expected cursor ledger 10, got %d (ok=%v)", seq, ok)
	}

	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(batch.Records))
	}
	if batch.Records[0].Kind != stream.KindLedger {
		t.Fatalf("expected KindLedger, got %s", batch.Records[0].Kind)
	}

	src.Rewind(11)
	rewound, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(rewound.Records) == 0 || rewound.Records[0].Sequence != 11 {
		t.Fatalf("expected rewound sequence 11, got %+v", rewound)
	}
}
