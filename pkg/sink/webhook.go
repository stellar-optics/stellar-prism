package sink

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// Defaults for a Webhook. They are conservative: a delivery that is slightly
// late is preferable to one that floods a struggling endpoint.
const (
	defaultWebhookBatchSize     = 100
	defaultWebhookRetries       = 3
	defaultWebhookInitial       = 100 * time.Millisecond
	defaultWebhookMaxBackoff    = 5 * time.Second
	defaultWebhookTimeout       = 10 * time.Second
	defaultWebhookResponseBytes = 4 << 10
	webhookUserAgent            = "prism-webhook"
)

// WebhookOptions configures a Webhook sink.
type WebhookOptions struct {
	// URL is the endpoint every batch is POSTed to. Required; only http and
	// https are accepted.
	URL string
	// Secret, when non-empty, signs each request with HMAC-SHA256 over
	// "<timestamp>.<body>", sent as the X-Prism-Signature header. Receivers
	// should verify it before trusting the payload.
	Secret []byte
	// Headers are extra request headers. Reserved headers set by the sink
	// (Content-Type, User-Agent, X-Prism-Timestamp, X-Prism-Signature,
	// X-Prism-Delivery) cannot be overridden.
	Headers map[string]string
	// BatchSize is how many records accumulate before a delivery. Zero uses
	// 100; negative is invalid.
	BatchSize int
	// MaxDelay flushes a partial batch after this long, so low-volume streams
	// still deliver promptly. Zero disables the timer. It uses a timer rather
	// than a goroutine, so closing the sink leaves nothing behind.
	MaxDelay time.Duration
	// MaxRetries is how many times a failed delivery is retried after the
	// first attempt. Zero uses the default of 3; a negative value disables
	// retries.
	MaxRetries int
	// InitialBackoff is the delay before the first retry; it doubles for each
	// subsequent attempt. Zero uses 100ms.
	InitialBackoff time.Duration
	// MaxBackoff caps a single retry delay. Zero uses 5s.
	MaxBackoff time.Duration
	// Jitter randomises retry delays by this fraction (0.2 means +/-20%) so
	// many streams recovering together do not synchronise. Zero disables it.
	Jitter float64
	// Timeout bounds a single HTTP request. Zero uses 10s. Client, when set,
	// is used instead and Timeout is ignored.
	Timeout time.Duration
	// Client performs the requests. Tests and custom transports set it; nil
	// builds a client with Timeout.
	Client *http.Client
	// Decode embeds decoded XDR in the payload. When false the base64 XDR is
	// sent under the *Xdr fields, matching --raw.
	Decode bool
	// MaxResponseBytes bounds how much of an error response is read and kept
	// in the returned error. Zero uses 4KiB.
	MaxResponseBytes int64
	// Now is the clock used for timestamps. Tests set it; nil means time.Now.
	Now func() time.Time
	// Sleep waits between retries. Tests replace it to avoid real delays; nil
	// means time.Sleep.
	Sleep func(time.Duration)
	// OnError observes failures from the MaxDelay timer, which cannot return
	// an error to an Emit call. The same error is returned by the next Emit,
	// Flush or Close, so it is never lost.
	OnError func(error)
}

// Webhook delivers stream records to an HTTP endpoint as JSON batches.
//
// Delivery is at-least-once. A batch is removed from the buffer only after the
// endpoint answers 2xx; a failed delivery is retried with exponential backoff
// and, if still failing, remains buffered so Close can try again. A receiver
// should therefore treat X-Prism-Delivery as an idempotency key and tolerate a
// repeated batch after a lost response.
type Webhook struct {
	opts  WebhookOptions
	now   func() time.Time
	sleep func(time.Duration)
	http  *http.Client

	mu         sync.Mutex
	buf        []stream.Record
	timer      *time.Timer
	closed     bool
	asyncErr   error
	deliveries uint64
}

// webhookStatusError is a non-2xx response. It records whether retrying could
// possibly help, so a permanent client error does not burn the retry budget.
type webhookStatusError struct {
	code       int
	body       string
	retryAfter time.Duration
}

func (e *webhookStatusError) Error() string {
	if e.body == "" {
		return fmt.Sprintf("server responded %s", http.StatusText(e.code))
	}
	return fmt.Sprintf("server responded %s: %s", http.StatusText(e.code), e.body)
}

// retryable reports whether another attempt could succeed.
func (e *webhookStatusError) retryable() bool {
	switch {
	case e.code == http.StatusRequestTimeout,
		e.code == http.StatusTooEarly,
		e.code == http.StatusTooManyRequests,
		e.code >= 500:
		return true
	default:
		return false
	}
}

// NewWebhook validates opts and builds a webhook sink.
func NewWebhook(opts WebhookOptions) (*Webhook, error) {
	parsed, err := url.Parse(strings.TrimSpace(opts.URL))
	if err != nil {
		return nil, fmt.Errorf("webhook: invalid URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("webhook: URL %q must use http or https", opts.URL)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("webhook: URL %q has no host", opts.URL)
	}

	if opts.BatchSize < 0 {
		return nil, fmt.Errorf("webhook: BatchSize must not be negative")
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = defaultWebhookBatchSize
	}
	if opts.MaxDelay < 0 {
		return nil, fmt.Errorf("webhook: MaxDelay must not be negative")
	}
	if opts.MaxRetries < 0 {
		opts.MaxRetries = 0
	} else if opts.MaxRetries == 0 {
		opts.MaxRetries = defaultWebhookRetries
	}
	if opts.InitialBackoff <= 0 {
		opts.InitialBackoff = defaultWebhookInitial
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = defaultWebhookMaxBackoff
	}
	if opts.MaxBackoff < opts.InitialBackoff {
		opts.MaxBackoff = opts.InitialBackoff
	}
	if opts.Jitter < 0 || opts.Jitter > 1 {
		return nil, fmt.Errorf("webhook: Jitter must be between 0 and 1")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaultWebhookTimeout
	}
	if opts.MaxResponseBytes <= 0 {
		opts.MaxResponseBytes = defaultWebhookResponseBytes
	}

	now := opts.Now
	if now == nil {
		now = time.Now
	}
	sleep := opts.Sleep
	if sleep == nil {
		sleep = time.Sleep
	}
	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: opts.Timeout}
	}

	return &Webhook{opts: opts, now: now, sleep: sleep, http: client}, nil
}

// Emit implements stream.Sink.
func (w *Webhook) Emit(r stream.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.emitLocked(r)
}

// EmitBatch implements stream.BatchSink, splitting the input into deliverables
// no larger than BatchSize.
func (w *Webhook) EmitBatch(records []stream.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, r := range records {
		if err := w.emitLocked(r); err != nil {
			return err
		}
	}
	return nil
}

// EmitJSON sends one JSON payload immediately through the webhook's configured
// endpoint, signature and retry policy. It is suitable for structured alerts
// that are not stream records.
func (w *Webhook) EmitJSON(payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("webhook: encoding JSON payload: %w", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return fmt.Errorf("webhook: emit after close")
	}
	if w.asyncErr != nil {
		return w.asyncErr
	}
	if err := w.flushLocked(); err != nil {
		return err
	}
	return w.deliverJSON(body)
}

func (w *Webhook) emitLocked(r stream.Record) error {
	if w.closed {
		return fmt.Errorf("webhook: emit after close")
	}
	if w.asyncErr != nil {
		return w.asyncErr
	}

	w.buf = append(w.buf, r)
	if len(w.buf) >= w.opts.BatchSize {
		return w.flushLocked()
	}
	if w.opts.MaxDelay > 0 && w.timer == nil {
		w.timer = time.AfterFunc(w.opts.MaxDelay, w.flushOnTimer)
	}
	return nil
}

// Flush implements stream.Sink by delivering every buffered record now.
func (w *Webhook) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.flushLocked()
}

// Close flushes the pending batch and stops the timer. It is safe to call more
// than once.
func (w *Webhook) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Keep retrying a batch a previous Close could not deliver, while still
	// rejecting new records through Emit.
	w.closed = true
	return w.flushLocked()
}

func (w *Webhook) flushOnTimer() {
	w.mu.Lock()
	if w.closed || w.timer == nil {
		w.mu.Unlock()
		return
	}
	err := w.flushLocked()
	onError := w.opts.OnError
	w.mu.Unlock()

	if err != nil && onError != nil {
		onError(err)
	}
}

func (w *Webhook) flushLocked() error {
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	if len(w.buf) == 0 {
		return nil
	}

	batch := w.buf
	w.buf = nil

	if err := w.deliver(batch); err != nil {
		// Keep the batch: a transient failure must not lose records, and a
		// later Flush or Close gets another chance.
		w.buf = append(batch, w.buf...)
		w.asyncErr = err
		return err
	}
	w.asyncErr = nil
	return nil
}

func (w *Webhook) deliver(batch []stream.Record) error {
	body, deliveryID, err := w.payload(batch)
	if err != nil {
		return err
	}
	return w.deliverBody(deliveryID, body, len(batch))
}

func (w *Webhook) deliverJSON(body []byte) error {
	w.deliveries++
	now := w.now().UTC()
	deliveryID := fmt.Sprintf("%d-%d", now.UnixNano(), w.deliveries)
	return w.deliverBody(deliveryID, body, 1)
}

func (w *Webhook) deliverBody(deliveryID string, body []byte, recordCount int) error {
	var lastErr error
	for attempt := 0; attempt <= w.opts.MaxRetries; attempt++ {
		if attempt > 0 {
			w.sleep(w.retryDelay(attempt, lastErr))
		}
		lastErr = w.post(deliveryID, body)
		if lastErr == nil {
			return nil
		}
		var status *webhookStatusError
		if errors.As(lastErr, &status) && !status.retryable() {
			break
		}
	}

	return fmt.Errorf("webhook: delivering payload (%d records) to %s: %w", recordCount, w.opts.URL, lastErr)
}

func (w *Webhook) payload(batch []stream.Record) ([]byte, string, error) {
	records := make([]json.RawMessage, len(batch))
	for i, r := range batch {
		line, err := MarshalRecord(r, w.opts.Decode)
		if err != nil {
			return nil, "", fmt.Errorf("webhook: encoding record %s: %w", r.ID, err)
		}
		records[i] = line
	}

	w.deliveries++
	now := w.now().UTC()
	id := fmt.Sprintf("%d-%d", now.UnixNano(), w.deliveries)

	body, err := json.Marshal(struct {
		Delivery string            `json:"delivery"`
		SentAt   string            `json:"sentAt"`
		Records  []json.RawMessage `json:"records"`
	}{
		Delivery: id,
		SentAt:   now.Format(time.RFC3339),
		Records:  records,
	})
	if err != nil {
		return nil, "", fmt.Errorf("webhook: encoding batch: %w", err)
	}
	return body, id, nil
}

func (w *Webhook) post(deliveryID string, body []byte) error {
	req, err := http.NewRequest(http.MethodPost, w.opts.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}

	for name, value := range w.opts.Headers {
		req.Header.Set(name, value)
	}

	timestamp := strconv.FormatInt(w.now().UTC().Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", webhookUserAgent)
	req.Header.Set("X-Prism-Delivery", deliveryID)
	req.Header.Set("X-Prism-Timestamp", timestamp)
	if len(w.opts.Secret) > 0 {
		req.Header.Set("X-Prism-Signature", signWebhook(w.opts.Secret, timestamp, body))
	}

	resp, err := w.http.Do(req)
	if err != nil {
		return fmt.Errorf("posting: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, w.opts.MaxResponseBytes))
	if readErr != nil {
		return fmt.Errorf("reading response: %w", readErr)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return &webhookStatusError{
		code:       resp.StatusCode,
		body:       strings.TrimSpace(string(responseBody)),
		retryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), w.now()),
	}
}

// signWebhook returns the X-Prism-Signature value for a timestamp and body.
func signWebhook(secret []byte, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func (w *Webhook) retryDelay(attempt int, lastErr error) time.Duration {
	delay := backoffDelay(w.opts.InitialBackoff, w.opts.MaxBackoff, attempt, w.opts.Jitter)

	var status *webhookStatusError
	if errors.As(lastErr, &status) && status.retryAfter > delay {
		delay = status.retryAfter
		if delay > w.opts.MaxBackoff {
			delay = w.opts.MaxBackoff
		}
	}
	return delay
}

// parseRetryAfter understands both forms the header allows: delay-seconds and
// an HTTP date.
func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		if d := at.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}
