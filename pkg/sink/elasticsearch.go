package sink

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

const (
	defaultElasticsearchBatchSize = 100
	defaultElasticsearchTimeout   = 10 * time.Second
	elasticsearchUserAgent        = "prism-elasticsearch"
)

// ElasticsearchOptions configures an Elasticsearch or OpenSearch bulk sink.
type ElasticsearchOptions struct {
	// URL is the Elasticsearch/OpenSearch endpoint. Required; only http and
	// https are accepted.
	URL string
	// Index is the base index name. Documents are sent to Index-YYYY.MM.DD
	// using each record's UTC ledger-close date.
	Index string
	// Username and Password configure HTTP basic authentication.
	Username string
	Password string
	// APIKey configures Elasticsearch API key authentication. It cannot be
	// combined with basic authentication.
	APIKey string
	// BatchSize is how many records accumulate before a bulk request. Zero
	// uses 100; negative is invalid.
	BatchSize int
	// FlushInterval flushes a partial batch after this long. Zero disables
	// the timer; negative is invalid.
	FlushInterval time.Duration
	// Decode embeds decoded XDR in each indexed document.
	Decode bool
	// Timeout bounds a single bulk request. Zero uses 10s.
	Timeout time.Duration
	// Client performs requests. Tests and custom transports may set it; nil
	// builds a client with Timeout.
	Client *http.Client
	// OnError observes failures from the FlushInterval timer. The same error
	// is returned by the next Emit, Flush or Close.
	OnError func(error)
}

// Elasticsearch indexes stream records through the Elasticsearch-compatible
// _bulk API. The buffer is retained when a request fails, allowing a later
// Flush or Close to retry the same idempotent document IDs.
type Elasticsearch struct {
	opts ElasticsearchOptions
	http *http.Client
	url  string

	mu       sync.Mutex
	buf      []stream.Record
	timer    *time.Timer
	closed   bool
	asyncErr error
}

// NewElasticsearch validates opts and returns a bulk indexing sink.
func NewElasticsearch(opts ElasticsearchOptions) (*Elasticsearch, error) {
	parsed, err := url.Parse(strings.TrimSpace(opts.URL))
	if err != nil {
		return nil, fmt.Errorf("elasticsearch: invalid URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("elasticsearch: URL %q must use http or https", opts.URL)
	}
	if parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("elasticsearch: URL must have a host and no query or fragment")
	}
	if !validElasticsearchIndex(opts.Index) {
		return nil, fmt.Errorf("elasticsearch: invalid base index %q", opts.Index)
	}
	if opts.APIKey != "" && (opts.Username != "" || opts.Password != "") {
		return nil, fmt.Errorf("elasticsearch: APIKey cannot be combined with basic authentication")
	}
	if (opts.Username == "") != (opts.Password == "") {
		return nil, fmt.Errorf("elasticsearch: Username and Password must be provided together")
	}
	if opts.BatchSize < 0 {
		return nil, fmt.Errorf("elasticsearch: BatchSize must not be negative")
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = defaultElasticsearchBatchSize
	}
	if opts.FlushInterval < 0 {
		return nil, fmt.Errorf("elasticsearch: FlushInterval must not be negative")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaultElasticsearchTimeout
	}
	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: opts.Timeout}
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/_bulk"

	return &Elasticsearch{opts: opts, http: client, url: parsed.String()}, nil
}

func validElasticsearchIndex(index string) bool {
	if len(index) == 0 || len(index) > 244 || index == "." || index == ".." ||
		strings.HasPrefix(index, "_") || strings.HasPrefix(index, "-") || strings.HasPrefix(index, "+") {
		return false
	}
	for _, r := range index {
		if r >= 'A' && r <= 'Z' || strings.ContainsRune("\\/*?\"<>| ,#", r) || r == ':' {
			return false
		}
	}
	return true
}

// Emit implements stream.Sink.
func (e *Elasticsearch) Emit(r stream.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.emitLocked(r)
}

// EmitBatch implements stream.BatchSink.
func (e *Elasticsearch) EmitBatch(records []stream.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range records {
		if err := e.emitLocked(r); err != nil {
			return err
		}
	}
	return nil
}

func (e *Elasticsearch) emitLocked(r stream.Record) error {
	if e.closed {
		return fmt.Errorf("elasticsearch: emit after close")
	}
	if e.asyncErr != nil {
		return e.asyncErr
	}
	e.buf = append(e.buf, r)
	if len(e.buf) >= e.opts.BatchSize {
		return e.flushLocked()
	}
	if e.opts.FlushInterval > 0 && e.timer == nil {
		e.timer = time.AfterFunc(e.opts.FlushInterval, e.flushOnTimer)
	}
	return nil
}

// Flush sends all buffered records immediately.
func (e *Elasticsearch) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.flushLocked()
}

// Close stops the timer and flushes pending records. It is safe to call more
// than once.
func (e *Elasticsearch) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	return e.flushLocked()
}

func (e *Elasticsearch) flushOnTimer() {
	e.mu.Lock()
	if e.closed || e.timer == nil {
		e.mu.Unlock()
		return
	}
	err := e.flushLocked()
	onError := e.opts.OnError
	e.mu.Unlock()
	if err != nil && onError != nil {
		onError(err)
	}
}

func (e *Elasticsearch) flushLocked() error {
	if e.timer != nil {
		e.timer.Stop()
		e.timer = nil
	}
	if len(e.buf) == 0 {
		return nil
	}

	batch := e.buf
	body, err := e.bulkBody(batch)
	if err != nil {
		e.asyncErr = err
		return err
	}
	e.buf = nil
	if err := e.post(body); err != nil {
		e.buf = append(batch, e.buf...)
		e.asyncErr = err
		return err
	}
	e.asyncErr = nil
	return nil
}

func (e *Elasticsearch) bulkBody(records []stream.Record) ([]byte, error) {
	var body bytes.Buffer
	for _, record := range records {
		if record.ID == "" {
			return nil, fmt.Errorf("elasticsearch: record ID is required")
		}
		index := e.opts.Index + "-" + record.Time.UTC().Format("2006.01.02")
		meta, err := json.Marshal(struct {
			Index struct {
				Name string `json:"_index"`
				ID   string `json:"_id"`
			} `json:"index"`
		}{Index: struct {
			Name string `json:"_index"`
			ID   string `json:"_id"`
		}{Name: index, ID: record.ID}})
		if err != nil {
			return nil, fmt.Errorf("elasticsearch: encoding metadata for record %s: %w", record.ID, err)
		}
		document, err := MarshalRecord(record, e.opts.Decode)
		if err != nil {
			return nil, fmt.Errorf("elasticsearch: encoding record %s: %w", record.ID, err)
		}
		body.Write(meta)
		body.WriteByte('\n')
		body.Write(document)
		body.WriteByte('\n')
	}
	return body.Bytes(), nil
}

func (e *Elasticsearch) post(body []byte) error {
	req, err := http.NewRequest(http.MethodPost, e.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("elasticsearch: building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("User-Agent", elasticsearchUserAgent)
	if e.opts.APIKey != "" {
		req.Header.Set("Authorization", "ApiKey "+e.opts.APIKey)
	} else if e.opts.Username != "" {
		req.SetBasicAuth(e.opts.Username, e.opts.Password)
	}

	resp, err := e.http.Do(req)
	if err != nil {
		return fmt.Errorf("elasticsearch: sending bulk request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if readErr != nil {
			return fmt.Errorf("elasticsearch: reading error response: %w", readErr)
		}
		return fmt.Errorf("elasticsearch: bulk request returned %s: %s",
			http.StatusText(resp.StatusCode), strings.TrimSpace(string(responseBody)))
	}
	var result struct {
		Errors *bool `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("elasticsearch: decoding bulk response: %w", err)
	}
	if result.Errors == nil {
		return fmt.Errorf("elasticsearch: bulk response is missing the errors field")
	}
	if *result.Errors {
		return fmt.Errorf("elasticsearch: bulk request had item errors")
	}
	return nil
}
