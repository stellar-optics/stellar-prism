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
	srv := fakerpc.NewServer(fakerpc.MakeEvents(11100, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	var (
		mu     sync.Mutex
		bodies [][]byte
	)
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_bulk" {
			t.Errorf("path = %q, want /_bulk", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "ApiKey test-key" {
			t.Errorf("Authorization = %q", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading request: %v", err)
		}
		mu.Lock()
		bodies = append(bodies, body)
		mu.Unlock()
		_, _ = w.Write([]byte(`{"errors":false}`))
	}))
	defer es.Close()

	stdout, _, err := run(t, "events", "--from", "11100", "--json", "--raw",
		"--sink-es-url", es.URL, "--sink-es-index", "prism-test",
		"--sink-es-api-key", "test-key", "--sink-es-batch-size", "2")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(nonEmptyLines(stdout)); got != 3 {
		t.Errorf("stdout has %d records, want 3", got)
	}

	mu.Lock()
	gotBodies := append([][]byte(nil), bodies...)
	mu.Unlock()
	if len(gotBodies) != 2 {
		t.Fatalf("bulk requests = %d, want 2", len(gotBodies))
	}
	indexed := 0
	for _, body := range gotBodies {
		lines := nonEmptyLines(string(body))
		if len(lines)%2 != 0 {
			t.Fatalf("bulk body has %d lines, want action/document pairs: %q", len(lines), body)
		}
		for i := 0; i < len(lines); i += 2 {
			var action struct {
				Index struct {
					ID    string `json:"_id"`
					Index string `json:"_index"`
				} `json:"index"`
			}
			if err := json.Unmarshal([]byte(lines[i]), &action); err != nil {
				t.Fatal(err)
			}
			if action.Index.ID == "" || !strings.HasPrefix(action.Index.Index, "prism-test-") {
				t.Errorf("bulk action = %+v", action)
			}
			var record map[string]any
			if err := json.Unmarshal([]byte(lines[i+1]), &record); err != nil {
				t.Fatal(err)
			}
			if record["id"] != action.Index.ID {
				t.Errorf("document id = %v, action id = %q", record["id"], action.Index.ID)
			}
			indexed++
		}
	}
	if indexed != 3 {
		t.Errorf("indexed %d records, want 3", indexed)
	}
}

func TestCSVOutputEndToEnd(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(11150, 2), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, _, err := run(t, "events", "--from", "11150", "--format", "csv",
		"--csv-fields", "id,ledger,contract")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	lines := nonEmptyLines(stdout)
	if len(lines) != 3 {
		t.Fatalf("CSV lines = %d, want header plus 2 records: %q", len(lines), stdout)
	}
	if lines[0] != "id,ledger,contract" {
		t.Errorf("header = %q", lines[0])
	}
	if !strings.Contains(lines[1], ",11150,") || !strings.Contains(lines[2], ",11151,") {
		t.Errorf("rows do not carry the expected ledgers: %q", stdout)
	}
}

func TestDelimitedOutputFlagValidation(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(11175, 1), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	for _, args := range [][]string{
		{"events", "--from", "11175", "--format", "json"},
		{"events", "--from", "11175", "--format", "csv", "--json"},
		{"events", "--from", "11175", "--csv-fields", "id,ledger"},
		{"events", "--from", "11175", "--format", "csv", "--csv-fields", "id,unknown"},
	} {
		if _, _, err := run(t, args...); err == nil {
			t.Errorf("run(%q) error = nil, want validation failure", args)
		}
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
		{
			name: "Elasticsearch credentials without URL",
			args: []string{"events", "--from", "15000", "--sink-es-api-key", "key"},
		},
		{
			name: "nonpositive Elasticsearch batch",
			args: []string{"events", "--from", "15000", "--sink-es-url", "https://example.com", "--sink-es-batch-size", "0"},
		},
		{
			name: "JetStream without NATS",
			args: []string{"events", "--from", "15000", "--nats-jetstream"},
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
		"--metrics-addr", "--syslog-address", "--syslog-format", "--syslog-facility",
		"--sink-es-url", "--sink-es-index", "--sink-nats", "--nats-subject", "--sink-zmq", "--zmq-topic",
		"--sink-audit", "--sink-audit-key", "--sink-audit-checkpoint",
	} {
		if !strings.Contains(stdout, flag) {
			t.Errorf("help does not mention %s", flag)
		}
	}
}
