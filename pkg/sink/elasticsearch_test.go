package sink_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
)

func TestElasticsearchBulkIndexUsesDailyIndicesAndAuthentication(t *testing.T) {
	t.Parallel()

	var (
		mu      sync.Mutex
		path    string
		auth    string
		content string
		payload string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading bulk request: %v", err)
		}
		mu.Lock()
		path, auth, content, payload = r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("Content-Type"), string(raw)
		mu.Unlock()
		_, _ = io.WriteString(w, `{"errors":false,"items":[]}`)
	}))
	t.Cleanup(server.Close)

	s, err := sink.NewElasticsearch(sink.ElasticsearchOptions{
		URL:       server.URL + "/proxy/",
		Index:     "stellar-events",
		Username:  "prism",
		Password:  "secret",
		BatchSize: 2,
		Decode:    false,
	})
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}
	first := eventRecord()
	first.Time = time.Date(2026, 10, 20, 23, 30, 0, 0, time.FixedZone("west", -7*60*60))
	second := eventRecord()
	second.ID += "-second"
	second.Time = time.Date(2026, 10, 21, 0, 30, 0, 0, time.FixedZone("east", 7*60*60))

	if err := s.Emit(first); err != nil {
		t.Fatalf("Emit(first) error = %v", err)
	}
	if err := s.Emit(second); err != nil {
		t.Fatalf("Emit(second) error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	mu.Lock()
	gotPath, gotAuth, gotContent, gotPayload := path, auth, content, payload
	mu.Unlock()
	if gotPath != "/proxy/_bulk" {
		t.Errorf("request path = %q, want /proxy/_bulk", gotPath)
	}
	if gotAuth != "Basic cHJpc206c2VjcmV0" {
		t.Errorf("Authorization = %q, want basic auth", gotAuth)
	}
	if gotContent != "application/x-ndjson" {
		t.Errorf("Content-Type = %q, want application/x-ndjson", gotContent)
	}

	lines := strings.Split(strings.TrimSuffix(gotPayload, "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("bulk request has %d lines, want 4:\n%s", len(lines), gotPayload)
	}
	var firstMeta, secondMeta struct {
		Index struct {
			Name string `json:"_index"`
			ID   string `json:"_id"`
		} `json:"index"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &firstMeta); err != nil {
		t.Fatalf("decoding first metadata line: %v", err)
	}
	if err := json.Unmarshal([]byte(lines[2]), &secondMeta); err != nil {
		t.Fatalf("decoding second metadata line: %v", err)
	}
	if firstMeta.Index.Name != "stellar-events-2026.10.21" || firstMeta.Index.ID != first.ID {
		t.Errorf("first bulk metadata = %+v, want UTC daily index and record ID", firstMeta.Index)
	}
	if secondMeta.Index.Name != "stellar-events-2026.10.20" || secondMeta.Index.ID != second.ID {
		t.Errorf("second bulk metadata = %+v, want UTC daily index and record ID", secondMeta.Index)
	}
	var doc struct {
		Kind string `json:"kind"`
		ID   string `json:"id"`
	}
	if err := json.Unmarshal([]byte(lines[1]), &doc); err != nil {
		t.Fatalf("decoding document line: %v", err)
	}
	if doc.Kind != "event" || doc.ID != first.ID {
		t.Errorf("indexed document = %+v, want the shared record JSON shape", doc)
	}
}

func TestElasticsearchAPIKeyAndFlushInterval(t *testing.T) {
	t.Parallel()

	requests := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "ApiKey encoded-key" {
			t.Errorf("Authorization = %q, want API key auth", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading bulk request: %v", err)
		}
		requests <- string(body)
		_, _ = io.WriteString(w, `{"errors":false}`)
	}))
	t.Cleanup(server.Close)

	s, err := sink.NewElasticsearch(sink.ElasticsearchOptions{
		URL:           server.URL,
		Index:         "events",
		APIKey:        "encoded-key",
		FlushInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}
	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	select {
	case body := <-requests:
		if !strings.Contains(body, `"kind":"event"`) {
			t.Errorf("timer request omitted document: %s", body)
		}
	case <-time.After(time.Second):
		t.Fatal("flush interval did not send a partial batch")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestElasticsearchRetainsFailedBatchForFlushRetry(t *testing.T) {
	t.Parallel()

	var (
		mu       sync.Mutex
		attempts int
		requests []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading bulk request: %v", err)
		}
		mu.Lock()
		attempts++
		requests = append(requests, string(body))
		attempt := attempts
		mu.Unlock()
		if attempt == 1 {
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `{"errors":false}`)
	}))
	t.Cleanup(server.Close)

	s, err := sink.NewElasticsearch(sink.ElasticsearchOptions{
		URL:       server.URL,
		Index:     "events",
		BatchSize: 1,
	})
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}
	if err := s.Emit(eventRecord()); err == nil {
		t.Fatal("Emit() error = nil, want server error")
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() retry error = %v", err)
	}
	mu.Lock()
	gotAttempts, gotRequests := attempts, append([]string(nil), requests...)
	mu.Unlock()
	if gotAttempts != 2 {
		t.Fatalf("made %d requests, want 2", gotAttempts)
	}
	if gotRequests[0] != gotRequests[1] {
		t.Errorf("retry body differs from original request")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewElasticsearchRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts sink.ElasticsearchOptions
	}{
		{name: "unsupported URL scheme", opts: sink.ElasticsearchOptions{URL: "ftp://localhost", Index: "events"}},
		{name: "invalid index", opts: sink.ElasticsearchOptions{URL: "http://localhost", Index: "Bad/Index"}},
		{name: "partial basic auth", opts: sink.ElasticsearchOptions{URL: "http://localhost", Index: "events", Username: "user"}},
		{name: "conflicting auth", opts: sink.ElasticsearchOptions{URL: "http://localhost", Index: "events", Username: "user", Password: "pass", APIKey: "key"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := sink.NewElasticsearch(tc.opts); err == nil {
				t.Fatal("NewElasticsearch() error = nil, want validation error")
			}
		})
	}
}
