package stream_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestHorizonSSESource(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "id: 12345-1\nevent: message\ndata: {\"id\":\"12345-1\",\"ledger_sequence\":12345,\"transaction_hash\":\"aabbcc\"}\n\n")
	}))
	defer ts.Close()

	src := stream.NewHorizonSSESource(ts.Client(), stream.HorizonSSEQuery{
		HorizonURL:  ts.URL,
		StartLedger: 12345,
		PageLimit:   10,
	})

	if src.Name() != "horizon-sse" {
		t.Fatalf("expected name horizon-sse, got %s", src.Name())
	}

	seq, ok := src.CursorLedger("12345-1")
	if !ok || seq != 12345 {
		t.Fatalf("expected cursor ledger 12345, got %d (ok=%v)", seq, ok)
	}

	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(batch.Records))
	}
	rec := batch.Records[0]
	if rec.Kind != stream.KindTransaction || rec.Transaction.Hash != "aabbcc" {
		t.Fatalf("unexpected record: %+v", rec)
	}

	src.Rewind(100)
}
