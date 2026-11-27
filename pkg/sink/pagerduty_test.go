package sink

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestPagerDutySinkDispatch(t *testing.T) {
	var calls int32
	var payload PagerDutyPayload

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &payload)

		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status": "success", "message": "Event processed"}`))
	}))
	defer server.Close()

	opts := PagerDutyOptions{
		RoutingKey: "test-routing-key-12345",
		Endpoint:   server.URL,
		Timeout:    2 * time.Second,
		MaxRetries: 2,
	}

	sink, err := NewPagerDutySink(opts)
	if err != nil {
		t.Fatalf("NewPagerDutySink() error = %v", err)
	}
	defer sink.Close()

	alert := AlertNotification{
		RuleName:   "DrainExploitDetected",
		Severity:   "critical",
		LedgerSeq:  2048,
		TxHash:     "0123456789abcdef",
		ContractID: "CDRAINCONTRACT",
		EventIndex: 3,
		Message:    "Drain exploit detected on contract",
		Timestamp:  time.Now(),
	}

	if err := sink.Dispatch(context.Background(), alert); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}

	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}

	if payload.RoutingKey != "test-routing-key-12345" {
		t.Errorf("routing_key = %q, want test-routing-key-12345", payload.RoutingKey)
	}
	if payload.EventAction != "trigger" {
		t.Errorf("event_action = %q, want trigger", payload.EventAction)
	}
	if payload.Payload.Severity != "critical" {
		t.Errorf("severity = %q, want critical", payload.Payload.Severity)
	}
	if payload.DedupKey != "DrainExploitDetected-0123456789abcdef-3" {
		t.Errorf("dedup_key = %q, want DrainExploitDetected-0123456789abcdef-3", payload.DedupKey)
	}
}

func TestPagerDutySinkRetry(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := atomic.AddInt32(&attempts, 1)
		if att == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"status": "error"}`))
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status": "success"}`))
	}))
	defer server.Close()

	opts := PagerDutyOptions{
		RoutingKey: "test-routing-key-12345",
		Endpoint:   server.URL,
		Timeout:    2 * time.Second,
		MaxRetries: 2,
	}

	sink, err := NewPagerDutySink(opts)
	if err != nil {
		t.Fatalf("NewPagerDutySink() error = %v", err)
	}
	defer sink.Close()

	alert := AlertNotification{
		RuleName: "RetryIncident",
		Severity: "warning",
	}

	if err := sink.Dispatch(context.Background(), alert); err != nil {
		t.Fatalf("Dispatch() with retry error = %v", err)
	}

	if atomic.LoadInt32(&attempts) != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}
}
