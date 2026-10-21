package cli

import (
	"bytes"
	"compress/gzip"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
)

type webhookRequests struct {
	mu      sync.Mutex
	bodies  [][]byte
	headers []http.Header
}

func (w *webhookRequests) record(r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	w.mu.Lock()
	w.bodies = append(w.bodies, body)
	w.headers = append(w.headers, r.Header.Clone())
	w.mu.Unlock()
}

func (w *webhookRequests) snapshot() ([][]byte, []http.Header) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([][]byte(nil), w.bodies...), append([]http.Header(nil), w.headers...)
}

func TestWebhookSinkEndToEnd(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(11000, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	requests := &webhookRequests{}
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.record(r)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer hook.Close()

	stdout, _, err := run(t, "events", "--from", "11000", "--json",
		"--webhook-url", hook.URL,
		"--webhook-secret", "s3cret",
		"--webhook-batch-size", "2",
		"--webhook-timeout", "50ms")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(nonEmptyLines(stdout)); got != 3 {
		t.Errorf("stdout has %d records, want 3 alongside the webhook", got)
	}

	bodies, headers := requests.snapshot()
	if len(bodies) == 0 {
		t.Fatal("webhook received no requests")
	}
	total := 0
	for i, body := range bodies {
		var envelope struct {
			Delivery string            `json:"delivery"`
			Records  []json.RawMessage `json:"records"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			t.Fatalf("webhook body is not JSON: %v\n%s", err, body)
		}
		total += len(envelope.Records)

		timestamp := headers[i].Get("X-Prism-Timestamp")
		mac := hmac.New(sha256.New, []byte("s3cret"))
		mac.Write([]byte(timestamp))
		mac.Write([]byte("."))
		mac.Write(body)
		want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		if got := headers[i].Get("X-Prism-Signature"); got != want {
			t.Errorf("request %d signature = %q, want %q", i, got, want)
		}
	}
	if total != 3 {
		t.Errorf("webhook delivered %d records across %d requests, want 3", total, len(bodies))
	}
}

func TestElasticsearchSinkEndToEnd(t *testing.T) {
	t.Setenv(esAPIKeyEnvVar, "encoded-test-key")

	srv := fakerpc.NewServer(fakerpc.MakeEvents(11500, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	type bulkRequest struct {
		path          string
		authorization string
		contentType   string
		body          string
	}
	var (
		mu       sync.Mutex
		requests []bulkRequest
	)
	elasticsearch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading bulk request: %v", err)
		}
		mu.Lock()
		requests = append(requests, bulkRequest{
			path:          r.URL.Path,
			authorization: r.Header.Get("Authorization"),
			contentType:   r.Header.Get("Content-Type"),
			body:          string(body),
		})
		mu.Unlock()
		_, _ = io.WriteString(w, `{"errors":false,"items":[]}`)
	}))
	defer elasticsearch.Close()

	stdout, _, err := run(t, "events", "--from", "11500", "--json",
		"--sink-es-url", elasticsearch.URL,
		"--sink-es-index", "stellar-events",
		"--sink-es-batch-size", "2",
		"--sink-es-flush-interval", "0")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(nonEmptyLines(stdout)); got != 3 {
		t.Errorf("stdout has %d records, want 3 alongside Elasticsearch indexing", got)
	}
	mu.Lock()
	got := append([]bulkRequest(nil), requests...)
	mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("received %d bulk requests, want a full batch and final partial batch", len(got))
	}
	total := 0
	for i, request := range got {
		if request.path != "/_bulk" {
			t.Errorf("request %d path = %q, want /_bulk", i, request.path)
		}
		if request.authorization != "ApiKey encoded-test-key" {
			t.Errorf("request %d Authorization = %q, want API key auth", i, request.authorization)
		}
		if request.contentType != "application/x-ndjson" {
			t.Errorf("request %d Content-Type = %q, want application/x-ndjson", i, request.contentType)
		}
		lines := strings.Split(strings.TrimSuffix(request.body, "\n"), "\n")
		if len(lines)%2 != 0 {
			t.Errorf("request %d contains %d NDJSON lines, want metadata/document pairs", i, len(lines))
		}
		total += len(lines) / 2
	}
	if total != 3 {
		t.Errorf("indexed %d records, want 3", total)
	}
}

func TestCloudSinkEndToEnd(t *testing.T) {
	t.Setenv(gcsTokenEnvVar, "test-token")

	srv := fakerpc.NewServer(fakerpc.MakeEvents(12000, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	type upload struct {
		path        string
		query       string
		authorized  string
		contentType string
		body        []byte
	}
	var (
		mu      sync.Mutex
		uploads []upload
	)
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading upload: %v", err)
		}
		mu.Lock()
		uploads = append(uploads, upload{
			path:        r.URL.Path,
			query:       r.URL.RawQuery,
			authorized:  r.Header.Get("Authorization"),
			contentType: r.Header.Get("Content-Type"),
			body:        body,
		})
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer store.Close()

	stdout, _, err := run(t, "events", "--from", "12000", "--json",
		"--cloud-provider", "gcs",
		"--cloud-url", store.URL,
		"--cloud-bucket", "archive",
		"--cloud-prefix", "prism/test",
		"--cloud-batch-records", "2",
		"--cloud-compress=false")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(nonEmptyLines(stdout)); got != 3 {
		t.Errorf("stdout has %d records, want 3 alongside the upload", got)
	}

	mu.Lock()
	got := append([]upload(nil), uploads...)
	mu.Unlock()

	if len(got) != 2 {
		t.Fatalf("got %d uploads, want 2 (a full chunk and the remainder on close)", len(got))
	}
	total := 0
	for i, up := range got {
		if up.path != "/upload/storage/v1/b/archive/o" {
			t.Errorf("upload %d path = %q, want the GCS media endpoint", i, up.path)
		}
		if !strings.Contains(up.query, "uploadType=media") {
			t.Errorf("upload %d query = %q, want uploadType=media", i, up.query)
		}
		if !strings.Contains(up.query, "prism%2Ftest%2F") {
			t.Errorf("upload %d query = %q, want the configured key prefix", i, up.query)
		}
		if up.authorized != "Bearer test-token" {
			t.Errorf("upload %d Authorization = %q, want the environment token", i, up.authorized)
		}
		if up.contentType != "application/x-ndjson" {
			t.Errorf("upload %d content type = %q, want application/x-ndjson", i, up.contentType)
		}
		total += len(nonEmptyLines(string(up.body)))
	}
	if total != 3 {
		t.Errorf("archived %d records across %d chunks, want 3", total, len(got))
	}
}

func TestCloudSinkCompressesByDefault(t *testing.T) {
	t.Setenv(gcsTokenEnvVar, "test-token")

	srv := fakerpc.NewServer(fakerpc.MakeEvents(12500, 1), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	var (
		mu          sync.Mutex
		body        []byte
		contentType string
	)
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading upload: %v", err)
		}
		mu.Lock()
		body = raw
		contentType = r.Header.Get("Content-Type")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer store.Close()

	if _, _, err := run(t, "events", "--from", "12500", "--json",
		"--cloud-provider", "gcs",
		"--cloud-url", store.URL,
		"--cloud-bucket", "archive"); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	mu.Lock()
	compressed := body
	gotType := contentType
	mu.Unlock()

	if gotType != "application/gzip" {
		t.Errorf("content type = %q, want application/gzip", gotType)
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("upload is not gzip: %v", err)
	}
	defer func() { _ = reader.Close() }()
	plain, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("reading gzip: %v", err)
	}
	if !strings.Contains(string(plain), `"kind":"event"`) {
		t.Errorf("decompressed chunk does not contain the record\n%s", plain)
	}
}

func TestSyslogSinkEndToEnd(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(13000, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket() error = %v", err)
	}
	defer func() { _ = listener.Close() }()

	if _, _, err := run(t, "events", "--from", "13000", "--json",
		"--syslog-address", "udp://"+listener.LocalAddr().String(),
		"--syslog-facility", "local3"); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if err := listener.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}

	count := 0
	buf := make([]byte, 4096)
	for range 3 {
		n, _, readErr := listener.ReadFrom(buf)
		if readErr != nil {
			t.Fatalf("reading syslog datagram %d: %v", count, readErr)
		}
		message := string(buf[:n])
		// local3 (19) * 8 + info (6) = 158.
		if !strings.HasPrefix(message, "<158>1 ") {
			t.Errorf("message %d = %q, want a local3 informational RFC 5424 header", count, message)
		}
		if !strings.Contains(message, `"kind":"event"`) {
			t.Errorf("message %d does not carry the JSON record: %s", count, message)
		}
		count++
	}
	if count != 3 {
		t.Errorf("received %d syslog messages, want 3", count)
	}
}

func TestMetricsSinkServesAndReportsAddress(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(14000, 1), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, stderr, err := run(t, "events", "--from", "14000", "--json",
		"--metrics-addr", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(nonEmptyLines(stdout)); got != 1 {
		t.Errorf("stdout has %d records, want 1", got)
	}
	if !strings.Contains(stderr, "serving metrics on http://") || !strings.Contains(stderr, "/metrics") {
		t.Errorf("stderr does not report the metrics address\n%s", stderr)
	}
}

func TestSinkFlagValidation(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(15000, 1), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	tests := []struct {
		name string
		args []string
	}{
		{
			name: "webhook secret without url",
			args: []string{"events", "--from", "15000", "--webhook-secret", "s"},
		},
		{
			name: "Elasticsearch URL without index",
			args: []string{"events", "--from", "15000", "--sink-es-url", "http://localhost:9200"},
		},
		{
			name: "cloud bucket without url",
			args: []string{"events", "--from", "15000", "--cloud-bucket", "b"},
		},
		{
			name: "unknown cloud provider",
			args: []string{"events", "--from", "15000",
				"--cloud-url", "https://example.com", "--cloud-bucket", "b", "--cloud-provider", "ftp"},
		},
		{
			name: "unknown syslog format",
			args: []string{"events", "--from", "15000",
				"--syslog-address", "udp://127.0.0.1:514", "--syslog-format", "cef"},
		},
		{
			name: "unknown syslog facility",
			args: []string{"events", "--from", "15000",
				"--syslog-address", "udp://127.0.0.1:514", "--syslog-facility", "nowhere"},
		},
		{
			name: "negative webhook batch",
			args: []string{"events", "--from", "15000",
				"--webhook-url", "https://example.com/hook", "--webhook-batch-size", "-1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := run(t, tc.args...); err == nil {
				t.Fatal("Execute() error = nil, want a validation failure")
			}
		})
	}
}

func TestCloudS3NeedsCredentials(t *testing.T) {
	t.Setenv(awsAccessKeyEnvVar, "")
	t.Setenv(awsSecretKeyEnvVar, "")

	srv := fakerpc.NewServer(fakerpc.MakeEvents(16000, 1), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	_, _, err := run(t, "events", "--from", "16000",
		"--cloud-provider", "s3",
		"--cloud-url", "https://s3.example.com",
		"--cloud-bucket", "bucket")
	if err == nil {
		t.Fatal("Execute() error = nil, want a missing-credentials failure")
	}
	if !strings.Contains(err.Error(), awsAccessKeyEnvVar) {
		t.Errorf("error = %q, want it to name %s", err, awsAccessKeyEnvVar)
	}
}

func TestStreamingHelpAdvertisesSinkFlags(t *testing.T) {
	stdout, _, err := run(t, "events", "--help")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, flag := range []string{
		"--webhook-url", "--webhook-secret", "--cloud-provider", "--cloud-bucket",
		"--sink-es-url", "--sink-es-index", "--sink-es-username", "--sink-es-batch-size",
		"--metrics-addr", "--syslog-address", "--syslog-format", "--syslog-facility",
	} {
		if !strings.Contains(stdout, flag) {
			t.Errorf("help does not mention %s", flag)
		}
	}
}
