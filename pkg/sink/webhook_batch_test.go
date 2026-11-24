package sink_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func batchRecord(id string) stream.Record {
	return stream.Record{ID: id, Kind: stream.KindLedger, Close: &stream.LedgerClose{}}
}

func TestWebhookBatchSendsSignedEnvelope(t *testing.T) {
	var body []byte
	var header http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		body, err = io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		header = r.Header.Clone()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	secret := []byte("batch-secret")
	hook, err := sink.NewWebhookBatch(sink.WebhookBatchOptions{
		URL: server.URL, Secret: secret, BatchSize: 2, Concurrency: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hook.Close()
	if err := hook.Emit(batchRecord("one")); err != nil {
		t.Fatal(err)
	}
	if err := hook.Emit(batchRecord("two")); err != nil {
		t.Fatal(err)
	}
	if err := hook.Flush(); err != nil {
		t.Fatal(err)
	}

	var envelope struct {
		BatchID   string            `json:"batch_id"`
		Timestamp string            `json:"timestamp"`
		Events    []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("invalid envelope: %v", err)
	}
	if envelope.BatchID == "" || envelope.Timestamp == "" || len(envelope.Events) != 2 {
		t.Fatalf("unexpected envelope: %+v", envelope)
	}
	if got := header.Get("X-Prism-Batch-ID"); got != envelope.BatchID {
		t.Errorf("batch ID header = %q, want %q", got, envelope.BatchID)
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(header.Get("X-Prism-Timestamp") + "."))
	mac.Write(body)
	wantSignature := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if got := header.Get("X-Prism-Signature"); got != wantSignature {
		t.Errorf("signature = %q, want %q", got, wantSignature)
	}
}

func TestWebhookBatchBacksOffForRetryAfter(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	hook, err := sink.NewWebhookBatch(sink.WebhookBatchOptions{
		URL: server.URL, BatchSize: 1, Concurrency: 1, MaxRetries: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hook.Close()
	start := time.Now()
	if err := hook.Emit(batchRecord("limited")); err != nil {
		t.Fatal(err)
	}
	if err := hook.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests = %d, want 2", got)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Errorf("retry waited %s, want Retry-After backoff", elapsed)
	}
}
