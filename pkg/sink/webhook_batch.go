package sink

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// WebhookBatchOptions configures a concurrent, batched webhook dispatcher.
// A zero-valued BatchSize, BatchTimeout, or Concurrency uses its documented
// default. Client may be supplied to share an application's transport.
type WebhookBatchOptions struct {
	URL          string
	Secret       []byte
	Headers      map[string]string
	BatchSize    int
	BatchTimeout time.Duration
	Concurrency  int
	MaxRetries   int
	Timeout      time.Duration
	Client       *http.Client
	Decode       bool
	Now          func() time.Time
	Sleep        func(time.Duration)
	OnError      func(error)
}

const (
	defaultWebhookBatchTimeout = 250 * time.Millisecond
	defaultWebhookConcurrency  = 4
)

// WebhookBatch sends batch envelopes using a bounded worker pool. Its default
// client enables HTTP/2 where the endpoint supports it and consequently reuses
// connections instead of opening one connection per event.
type WebhookBatch struct {
	opts  WebhookBatchOptions
	http  *http.Client
	now   func() time.Time
	sleep func(time.Duration)

	mu     sync.Mutex
	buf    []stream.Record
	timer  *time.Timer
	closed bool
	err    error
	seq    atomic.Uint64

	jobs    sync.WaitGroup
	work    chan []stream.Record
	workers sync.WaitGroup

	throttleMu  sync.Mutex
	nextAllowed time.Time
}

// NewWebhookBatch validates opts and starts its worker pool.
func NewWebhookBatch(opts WebhookBatchOptions) (*WebhookBatch, error) {
	parsed, err := url.Parse(strings.TrimSpace(opts.URL))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, fmt.Errorf("webhook batch: URL must be an absolute http(s) URL")
	}
	if opts.BatchSize < 0 || opts.BatchTimeout < 0 || opts.Concurrency < 0 {
		return nil, fmt.Errorf("webhook batch: batch size, timeout, and concurrency must not be negative")
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 50
	}
	if opts.BatchTimeout == 0 {
		opts.BatchTimeout = defaultWebhookBatchTimeout
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = defaultWebhookConcurrency
	}
	if opts.MaxRetries < 0 {
		opts.MaxRetries = 0
	} else if opts.MaxRetries == 0 {
		opts.MaxRetries = defaultWebhookRetries
	}
	if opts.Timeout == 0 {
		opts.Timeout = defaultWebhookTimeout
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Sleep == nil {
		opts.Sleep = time.Sleep
	}
	client := opts.Client
	if client == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.ForceAttemptHTTP2 = true
		client = &http.Client{Transport: transport, Timeout: opts.Timeout}
	}
	w := &WebhookBatch{opts: opts, http: client, now: opts.Now, sleep: opts.Sleep, work: make(chan []stream.Record, opts.Concurrency*2)}
	for range opts.Concurrency {
		w.workers.Add(1)
		go w.worker()
	}
	return w, nil
}

// Emit queues r and dispatches a full batch to a worker.
func (w *WebhookBatch) Emit(r stream.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("webhook batch: emit after close")
	}
	if w.err != nil {
		return w.err
	}
	w.buf = append(w.buf, r)
	if len(w.buf) >= w.opts.BatchSize {
		w.dispatchLocked()
	} else if w.timer == nil {
		w.timer = time.AfterFunc(w.opts.BatchTimeout, w.flushTimer)
	}
	return nil
}

// EmitBatch implements stream.BatchSink.
func (w *WebhookBatch) EmitBatch(records []stream.Record) error {
	for _, r := range records {
		if err := w.Emit(r); err != nil {
			return err
		}
	}
	return nil
}

// Flush submits pending records and waits for every submitted request.
func (w *WebhookBatch) Flush() error {
	w.mu.Lock()
	if w.err == nil {
		w.dispatchLocked()
	}
	w.mu.Unlock()
	w.jobs.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

// Close drains submitted work, stops workers, and is safe to call repeatedly.
func (w *WebhookBatch) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return w.Flush()
	}
	w.closed = true
	if w.err == nil {
		w.dispatchLocked()
	}
	w.mu.Unlock()
	w.jobs.Wait()
	close(w.work)
	w.workers.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *WebhookBatch) flushTimer() {
	w.mu.Lock()
	if !w.closed && w.err == nil {
		w.dispatchLocked()
	}
	err := w.err
	w.mu.Unlock()
	if err != nil && w.opts.OnError != nil {
		w.opts.OnError(err)
	}
}

func (w *WebhookBatch) dispatchLocked() {
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	if len(w.buf) == 0 {
		return
	}
	batch := w.buf
	w.buf = nil
	w.jobs.Add(1)
	w.work <- batch
}

func (w *WebhookBatch) worker() {
	defer w.workers.Done()
	for batch := range w.work {
		if err := w.deliver(batch); err != nil {
			w.mu.Lock()
			if w.err == nil {
				w.err = err
			}
			w.mu.Unlock()
			if w.opts.OnError != nil {
				w.opts.OnError(err)
			}
		}
		w.jobs.Done()
	}
}

func (w *WebhookBatch) deliver(batch []stream.Record) error {
	id := fmt.Sprintf("%d-%d", w.now().UTC().UnixNano(), w.seq.Add(1))
	records := make([]json.RawMessage, len(batch))
	for i, r := range batch {
		raw, err := MarshalRecord(r, w.opts.Decode)
		if err != nil {
			return fmt.Errorf("webhook batch: encoding record %s: %w", r.ID, err)
		}
		records[i] = raw
	}
	now := w.now().UTC()
	body, err := json.Marshal(struct {
		BatchID   string            `json:"batch_id"`
		Timestamp string            `json:"timestamp"`
		Events    []json.RawMessage `json:"events"`
	}{id, now.Format(time.RFC3339Nano), records})
	if err != nil {
		return fmt.Errorf("webhook batch: encoding envelope: %w", err)
	}
	var last error
	for attempt := 0; attempt <= w.opts.MaxRetries; attempt++ {
		if attempt > 0 {
			w.sleep(w.retryDelay(attempt, last))
		}
		w.waitForThrottle()
		last = w.post(id, body)
		if last == nil {
			return nil
		}
		var status *webhookStatusError
		if errors.As(last, &status) && !status.retryable() {
			break
		}
	}
	return fmt.Errorf("webhook batch: delivering %d events: %w", len(batch), last)
}

func (w *WebhookBatch) waitForThrottle() {
	for {
		w.throttleMu.Lock()
		until := w.nextAllowed
		w.throttleMu.Unlock()
		d := time.Until(until)
		if d <= 0 {
			return
		}
		w.sleep(d)
	}
}

func (w *WebhookBatch) post(id string, body []byte) (err error) {
	req, err := http.NewRequest(http.MethodPost, w.opts.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	for k, v := range w.opts.Headers {
		req.Header.Set(k, v)
	}
	timestamp := strconv.FormatInt(w.now().UTC().Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", webhookUserAgent)
	req.Header.Set("X-Prism-Batch-ID", id)
	req.Header.Set("X-Prism-Timestamp", timestamp)
	if len(w.opts.Secret) > 0 {
		req.Header.Set("X-Prism-Signature", signWebhook(w.opts.Secret, timestamp, body))
	}
	resp, err := w.http.Do(req)
	if err != nil {
		return fmt.Errorf("posting: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, defaultWebhookResponseBytes))
	if readErr != nil {
		return readErr
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	status := &webhookStatusError{code: resp.StatusCode, body: strings.TrimSpace(string(data)), retryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), w.now())}
	if status.code == http.StatusTooManyRequests && status.retryAfter > 0 {
		w.throttleMu.Lock()
		until := time.Now().Add(status.retryAfter)
		if until.After(w.nextAllowed) {
			w.nextAllowed = until
		}
		w.throttleMu.Unlock()
	}
	return status
}

func (w *WebhookBatch) retryDelay(attempt int, last error) time.Duration {
	d := backoffDelay(defaultWebhookInitial, defaultWebhookMaxBackoff, attempt, 0)
	var status *webhookStatusError
	if errors.As(last, &status) && status.retryAfter > d {
		return status.retryAfter
	}
	return d
}
