package sink_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// webhookCapture is a test endpoint that records every delivered request.
type webhookCapture struct {
	mu       sync.Mutex
	server   *httptest.Server
	bodies   [][]byte
	headers  []http.Header
	attempts int
	respond  func(attempt int, w http.ResponseWriter, body []byte)
	notify   chan struct{}
}

type webhookRoundTripper func(*http.Request) (*http.Response, error)

func (roundTrip webhookRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func newWebhookCapture(t *testing.T, respond func(attempt int, w http.ResponseWriter, body []byte)) *webhookCapture {
	t.Helper()
	c := &webhookCapture{respond: respond}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading webhook body: %v", err)
		}
		c.mu.Lock()
		c.attempts++
		attempt := c.attempts
		c.bodies = append(c.bodies, body)
		c.headers = append(c.headers, r.Header.Clone())
		notify := c.notify
		c.mu.Unlock()

		if c.respond != nil {
			c.respond(attempt, w, body)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
		if notify != nil {
			select {
			case notify <- struct{}{}:
			default:
			}
		}
	}))
	t.Cleanup(srv.Close)
	c.server = srv
	return c
}

func TestWebhookEmitsStructuredJSONPayload(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_123, 0).UTC()
	var body []byte
	var headers http.Header
	client := &http.Client{Transport: webhookRoundTripper(func(request *http.Request) (*http.Response, error) {
		var err error
		body, err = io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		headers = request.Header.Clone()
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    request,
		}, nil
	})}
	hook, err := sink.NewWebhook(sink.WebhookOptions{
		URL:    "https://example.com/alerts",
		Secret: []byte("secret"),
		Client: client,
		Now:    func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewWebhook: %v", err)
	}

	payload := struct {
		Expression string `json:"expression"`
		Count      int    `json:"count"`
	}{Expression: `count() > 5 within 1m`, Count: 6}
	if err := hook.EmitJSON(payload); err != nil {
		t.Fatalf("EmitJSON: %v", err)
	}
	if err := hook.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var got struct {
		Expression string `json:"expression"`
		Count      int    `json:"count"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode emitted payload: %v", err)
	}
	if got != payload {
		t.Fatalf("payload = %+v, want %+v", got, payload)
	}
	if headers.Get("X-Prism-Signature") == "" {
		t.Fatal("structured payload was not signed")
	}
}

func (c *webhookCapture) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.bodies)
}

func (c *webhookCapture) request(i int) ([]byte, http.Header) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bodies[i], c.headers[i]
}

func TestWebhookDeliversSignedBatches(t *testing.T) {
	t.Parallel()

	capture := newWebhookCapture(t, nil)
	now := time.Unix(1_700_000_123, 0).UTC()
	secret := []byte("top-secret")

	hook, err := sink.NewWebhook(sink.WebhookOptions{
		URL:       capture.server.URL,
		Secret:    secret,
		BatchSize: 2,
		Decode:    true,
		Headers:   map[string]string{"X-Custom": "prism"},
		Now:       func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewWebhook() error = %v", err)
	}

	if err := hook.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if got := capture.count(); got != 0 {
		t.Fatalf("delivered %d requests before the batch filled, want 0", got)
	}
	if err := hook.Emit(ledgerRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if got := capture.count(); got != 1 {
		t.Fatalf("delivered %d requests, want 1", got)
	}
	if err := hook.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if got := capture.count(); got != 1 {
		t.Errorf("Flush after a full batch sent another request, total %d", got)
	}

	body, headers := capture.request(0)

	var envelope struct {
		Delivery string            `json:"delivery"`
		SentAt   string            `json:"sentAt"`
		Records  []json.RawMessage `json:"records"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("request body is not valid JSON: %v\n%s", err, body)
	}
	if len(envelope.Records) != 2 {
		t.Fatalf("batch has %d records, want 2\n%s", len(envelope.Records), body)
	}
	if envelope.Delivery == "" || headers.Get("X-Prism-Delivery") != envelope.Delivery {
		t.Errorf("delivery id header = %q, body = %q; want them equal and non-empty",
			headers.Get("X-Prism-Delivery"), envelope.Delivery)
	}
	if envelope.SentAt != "2023-11-14T22:15:23Z" {
		t.Errorf("sentAt = %q, want the injected clock", envelope.SentAt)
	}

	if got := headers.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	if got := headers.Get("X-Custom"); got != "prism" {
		t.Errorf("custom header = %q, want prism", got)
	}

	timestamp := headers.Get("X-Prism-Timestamp")
	if timestamp != "1700000123" {
		t.Errorf("X-Prism-Timestamp = %q, want 1700000123", timestamp)
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write(body)
	want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if got := headers.Get("X-Prism-Signature"); got != want {
		t.Errorf("X-Prism-Signature = %q, want %q", got, want)
	}

	var event struct {
		Kind     string          `json:"kind"`
		Value    json.RawMessage `json:"value"`
		ValueXDR string          `json:"valueXdr"`
	}
	if err := json.Unmarshal(envelope.Records[0], &event); err != nil {
		t.Fatalf("record is not valid JSON: %v", err)
	}
	if event.Kind != "event" || len(event.Value) == 0 {
		t.Errorf("decoded record = %+v, want a decoded event", event)
	}
}

func TestWebhookRawPayloadKeepsXDR(t *testing.T) {
	t.Parallel()

	capture := newWebhookCapture(t, nil)
	hook, err := sink.NewWebhook(sink.WebhookOptions{
		URL:       capture.server.URL,
		BatchSize: 1,
		Decode:    false,
	})
	if err != nil {
		t.Fatalf("NewWebhook() error = %v", err)
	}
	if err := hook.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	body, _ := capture.request(0)
	if !strings.Contains(string(body), `"valueXdr":"`+scValU32+`"`) {
		t.Errorf("raw webhook payload did not preserve base64 XDR\n%s", body)
	}
	if strings.Contains(string(body), `"value":{`) {
		t.Errorf("raw webhook payload decoded the value anyway\n%s", body)
	}
}

func TestWebhookRetriesAndRecovers(t *testing.T) {
	t.Parallel()

	capture := newWebhookCapture(t, func(attempt int, w http.ResponseWriter, _ []byte) {
		if attempt < 3 {
			http.Error(w, "try later", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	var delays []time.Duration
	hook, err := sink.NewWebhook(sink.WebhookOptions{
		URL:            capture.server.URL,
		BatchSize:      1,
		MaxRetries:     3,
		InitialBackoff: 100 * time.Millisecond,
		Sleep:          func(d time.Duration) { delays = append(delays, d) },
	})
	if err != nil {
		t.Fatalf("NewWebhook() error = %v", err)
	}

	if err := hook.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v, want the retry to succeed", err)
	}
	if capture.attempts != 3 {
		t.Errorf("attempts = %d, want 3", capture.attempts)
	}
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}
	if len(delays) != len(want) {
		t.Fatalf("delays = %v, want %v", delays, want)
	}
	for i := range want {
		if delays[i] != want[i] {
			t.Errorf("delay[%d] = %s, want %s", i, delays[i], want[i])
		}
	}
}

func TestWebhookDoesNotRetryPermanentFailure(t *testing.T) {
	t.Parallel()

	capture := newWebhookCapture(t, func(_ int, w http.ResponseWriter, _ []byte) {
		http.Error(w, "bad request", http.StatusBadRequest)
	})

	var delays []time.Duration
	hook, err := sink.NewWebhook(sink.WebhookOptions{
		URL:        capture.server.URL,
		BatchSize:  1,
		MaxRetries: 5,
		Sleep:      func(d time.Duration) { delays = append(delays, d) },
	})
	if err != nil {
		t.Fatalf("NewWebhook() error = %v", err)
	}

	err = hook.Emit(eventRecord())
	if err == nil {
		t.Fatal("Emit() error = nil, want a permanent failure")
	}
	if !strings.Contains(err.Error(), "Bad Request") {
		t.Errorf("error = %q, want it to mention the response status", err)
	}
	if capture.attempts != 1 {
		t.Errorf("attempts = %d, want a single attempt for a 4xx", capture.attempts)
	}
	if len(delays) != 0 {
		t.Errorf("retry delays = %v, want none", delays)
	}
}

func TestWebhookKeepsBatchAfterFailureForRetry(t *testing.T) {
	t.Parallel()

	var fail sync.Mutex
	failing := true
	capture := newWebhookCapture(t, func(_ int, w http.ResponseWriter, _ []byte) {
		fail.Lock()
		defer fail.Unlock()
		if failing {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	hook, err := sink.NewWebhook(sink.WebhookOptions{
		URL:        capture.server.URL,
		BatchSize:  1,
		MaxRetries: -1, // disable retries so the test can control recovery
		Sleep:      func(time.Duration) {},
	})
	if err != nil {
		t.Fatalf("NewWebhook() error = %v", err)
	}

	if err := hook.Emit(eventRecord()); err == nil {
		t.Fatal("Emit() error = nil, want the failed delivery")
	}

	fail.Lock()
	failing = false
	fail.Unlock()

	if err := hook.Flush(); err != nil {
		t.Fatalf("Flush() after recovery error = %v, want the buffered batch to be delivered", err)
	}
	if capture.attempts != 2 {
		t.Errorf("attempts = %d, want 2 (one failure, one recovery)", capture.attempts)
	}
	body, _ := capture.request(1)
	if !strings.Contains(string(body), `"kind":"event"`) {
		t.Errorf("recovered batch did not contain the original record\n%s", body)
	}
}

func TestWebhookHonoursRetryAfter(t *testing.T) {
	t.Parallel()

	capture := newWebhookCapture(t, func(attempt int, w http.ResponseWriter, _ []byte) {
		if attempt == 1 {
			w.Header().Set("Retry-After", "2")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	var delays []time.Duration
	hook, err := sink.NewWebhook(sink.WebhookOptions{
		URL:            capture.server.URL,
		BatchSize:      1,
		MaxRetries:     1,
		InitialBackoff: 10 * time.Millisecond,
		Sleep:          func(d time.Duration) { delays = append(delays, d) },
	})
	if err != nil {
		t.Fatalf("NewWebhook() error = %v", err)
	}
	if err := hook.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if len(delays) != 1 || delays[0] != 2*time.Second {
		t.Errorf("retry delays = %v, want the Retry-After value", delays)
	}
}

func TestWebhookFlushesPartialBatchOnDelay(t *testing.T) {
	t.Parallel()

	capture := newWebhookCapture(t, nil)
	capture.mu.Lock()
	capture.notify = make(chan struct{}, 1)
	capture.mu.Unlock()

	hook, err := sink.NewWebhook(sink.WebhookOptions{
		URL:       capture.server.URL,
		BatchSize: 100,
		MaxDelay:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewWebhook() error = %v", err)
	}
	defer func() {
		if err := hook.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	if err := hook.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	select {
	case <-capture.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("partial batch was not flushed after MaxDelay")
	}
	if capture.count() != 1 {
		t.Errorf("delivered %d requests, want 1", capture.count())
	}
}

func TestWebhookCloseFlushesPendingBatch(t *testing.T) {
	t.Parallel()

	capture := newWebhookCapture(t, nil)
	hook, err := sink.NewWebhook(sink.WebhookOptions{
		URL:       capture.server.URL,
		BatchSize: 100,
	})
	if err != nil {
		t.Fatalf("NewWebhook() error = %v", err)
	}
	if err := hook.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := hook.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if capture.count() != 1 {
		t.Errorf("delivered %d requests on close, want 1", capture.count())
	}
	if err := hook.Emit(eventRecord()); err == nil {
		t.Error("Emit() after Close error = nil, want an error")
	}
	if err := hook.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
}

func TestWebhookRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts sink.WebhookOptions
	}{
		{name: "empty URL", opts: sink.WebhookOptions{}},
		{name: "non-http scheme", opts: sink.WebhookOptions{URL: "ftp://example.com/hook"}},
		{name: "no host", opts: sink.WebhookOptions{URL: "http://"}},
		{name: "negative batch size", opts: sink.WebhookOptions{URL: "https://example.com", BatchSize: -1}},
		{name: "negative delay", opts: sink.WebhookOptions{URL: "https://example.com", MaxDelay: -time.Second}},
		{name: "jitter out of range", opts: sink.WebhookOptions{URL: "https://example.com", Jitter: 2}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := sink.NewWebhook(tc.opts); err == nil {
				t.Fatal("NewWebhook() error = nil, want a validation failure")
			}
		})
	}
}

func TestWebhookEmitBatchSplitsAtBatchSize(t *testing.T) {
	t.Parallel()

	capture := newWebhookCapture(t, nil)
	hook, err := sink.NewWebhook(sink.WebhookOptions{
		URL:       capture.server.URL,
		BatchSize: 2,
	})
	if err != nil {
		t.Fatalf("NewWebhook() error = %v", err)
	}

	records := []stream.Record{eventRecord(), eventRecord(), eventRecord(), eventRecord(), eventRecord()}
	if err := hook.EmitBatch(records); err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}
	if got := capture.count(); got != 2 {
		t.Errorf("delivered %d full batches, want 2; the remainder should wait for Flush", got)
	}
	if err := hook.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if got := capture.count(); got != 3 {
		t.Errorf("delivered %d requests after Flush, want 3", got)
	}
}
