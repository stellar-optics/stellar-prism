package sink

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestTelegramSinkDispatch(t *testing.T) {
	var calls int32
	var receivedPayload map[string]interface{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &receivedPayload)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok": true}`))
	}))
	defer server.Close()

	opts := TelegramOptions{
		Token:      "123456:TEST",
		ChatID:     "-100123456",
		Endpoint:   server.URL,
		Timeout:    2 * time.Second,
		MaxRetries: 2,
	}

	sink, err := NewTelegramSink(opts)
	if err != nil {
		t.Fatalf("NewTelegramSink() error = %v", err)
	}
	defer sink.Close()

	alert := AlertNotification{
		RuleName:   "SuspiciousTransfer",
		Severity:   "critical",
		LedgerSeq:  1000,
		TxHash:     "deadbeef1234",
		ContractID: "CCONTRACT123",
		Message:    "Transfer amount exceeds limit",
		Timestamp:  time.Now(),
	}

	if err := sink.Dispatch(context.Background(), alert); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}

	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}

	if receivedPayload["chat_id"] != "-100123456" {
		t.Errorf("chat_id = %v, want -100123456", receivedPayload["chat_id"])
	}
	text, _ := receivedPayload["text"].(string)
	if !strings.Contains(text, "CRITICAL") || !strings.Contains(text, "SuspiciousTransfer") {
		t.Errorf("text does not contain expected alert details: %s", text)
	}
}

func TestTelegramSinkRetry(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := atomic.AddInt32(&attempts, 1)
		if att == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"ok": false, "description": "rate limited"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok": true}`))
	}))
	defer server.Close()

	opts := TelegramOptions{
		Token:      "123456:TEST",
		ChatID:     "-100123456",
		Endpoint:   server.URL,
		Timeout:    2 * time.Second,
		MaxRetries: 2,
	}

	sink, err := NewTelegramSink(opts)
	if err != nil {
		t.Fatalf("NewTelegramSink() error = %v", err)
	}
	defer sink.Close()

	alert := AlertNotification{
		RuleName: "RetryTest",
		Severity: "info",
	}

	if err := sink.Dispatch(context.Background(), alert); err != nil {
		t.Fatalf("Dispatch() with retry error = %v", err)
	}

	if atomic.LoadInt32(&attempts) != 2 {
		t.Errorf("expected 2 attempts (1 retry), got %d", attempts)
	}
}
