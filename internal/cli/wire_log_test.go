package cli

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// wireCapture records what the RPC server actually received, so the test can
// prove the logger observed a real authorized request rather than a synthetic
// one.
type wireCapture struct {
	mu   sync.Mutex
	auth string
	body string
}

func (c *wireCapture) set(auth, body string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.auth = auth
	c.body = body
}

func (c *wireCapture) get() (auth, body string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.auth, c.body
}

// newWireRPCServer serves the one call an empty event stream makes. It is a
// real HTTP server, so the test exercises the SDK transport, the RoundTripper
// and the CLI wiring together.
func newWireRPCServer(t *testing.T, capture *wireCapture) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("server reading request body: %v", err)
			http.Error(w, "read failed", http.StatusInternalServerError)
			return
		}
		capture.set(r.Header.Get("Authorization"), string(raw))

		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.Unmarshal(raw, &request); err != nil {
			t.Errorf("server decoding JSON-RPC request: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if request.Method != "getEvents" {
			t.Errorf("server got method %q, want getEvents", request.Method)
			http.Error(w, "unexpected method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      request.ID,
			"result": map[string]any{
				"events":                []any{},
				"cursor":                "",
				"latestLedger":          9000,
				"oldestLedger":          9000,
				"latestLedgerCloseTime": "1700000000",
				"oldestLedgerCloseTime": "1700000000",
			},
		}); err != nil {
			t.Errorf("server writing response: %v", err)
		}
	}))
}

func authenticatedURL(serverURL string) string {
	return strings.Replace(serverURL, "http://", "http://prism:wire-secret@", 1)
}

type loggedExchange struct {
	Direction string              `json:"direction"`
	Status    int                 `json:"status"`
	URL       string              `json:"url"`
	Headers   map[string][]string `json:"headers"`
	Body      string              `json:"body"`
	Truncated bool                `json:"truncated"`
}

func readWireLog(t *testing.T, path string) []loggedExchange {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading wire log: %v", err)
	}
	if strings.TrimSpace(string(raw)) == "" {
		t.Fatal("wire log is empty")
	}

	var entries []loggedExchange
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var entry loggedExchange
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("wire log line is not valid JSON: %v\n%s", err, line)
		}
		entries = append(entries, entry)
	}
	return entries
}

func TestEventsCommandWireLogMasksAuthorization(t *testing.T) {
	var capture wireCapture
	srv := newWireRPCServer(t, &capture)
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "wire.ndjson")
	_, stderr, err := run(t, "events", "--from", "9000", "--json",
		"--rpc-url", authenticatedURL(srv.URL),
		"--wire-log", path)
	if err != nil {
		t.Fatalf("Execute() error = %v\nstderr:\n%s", err, stderr)
	}

	auth, requestBody := capture.get()
	if !strings.HasPrefix(auth, "Basic ") {
		t.Fatalf("server saw Authorization = %q, want basic auth from the URL", auth)
	}
	if !strings.Contains(requestBody, `"method":"getEvents"`) {
		t.Errorf("server did not receive getEvents: %s", requestBody)
	}

	entries := readWireLog(t, path)
	if len(entries) != 2 {
		t.Fatalf("got %d wire log entries, want a request and a response\n%+v", len(entries), entries)
	}
	request, response := entries[0], entries[1]
	if request.Direction != "request" || response.Direction != "response" {
		t.Fatalf("directions = %q, %q; want request, response", request.Direction, response.Direction)
	}
	if got := request.Headers["Authorization"]; len(got) != 1 || got[0] != "[REDACTED]" {
		t.Errorf("logged Authorization = %v, want [REDACTED]", got)
	}
	if !strings.Contains(request.Body, "getEvents") {
		t.Errorf("request payload was not dumped: %q", request.Body)
	}
	if response.Status != http.StatusOK {
		t.Errorf("logged status = %d, want 200", response.Status)
	}
	if !strings.Contains(response.Body, "latestLedger") {
		t.Errorf("response payload was not dumped: %q", response.Body)
	}
	if !strings.Contains(request.URL, "127.0.0.1") {
		t.Errorf("logged URL = %q, want the endpoint host", request.URL)
	}
	if strings.Contains(request.URL, "wire-secret") {
		t.Errorf("logged URL leaked the URL password: %q", request.URL)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("re-reading wire log: %v", err)
	}
	basic := base64.StdEncoding.EncodeToString([]byte("prism:wire-secret"))
	for _, secret := range []string{"wire-secret", basic, "Basic "} {
		if strings.Contains(string(raw), secret) {
			t.Errorf("wire log leaked %q\n%s", secret, raw)
		}
	}
}

func TestEventsCommandWireLogToStderr(t *testing.T) {
	var capture wireCapture
	srv := newWireRPCServer(t, &capture)
	defer srv.Close()

	_, stderr, err := run(t, "events", "--from", "9000", "--json",
		"--rpc-url", authenticatedURL(srv.URL),
		"--wire-log", "-")
	if err != nil {
		t.Fatalf("Execute() error = %v\nstderr:\n%s", err, stderr)
	}
	if !strings.Contains(stderr, `"direction":"request"`) {
		t.Errorf("--wire-log - did not write a request entry to stderr\n%s", stderr)
	}
	if strings.Contains(stderr, "wire-secret") {
		t.Errorf("--wire-log - leaked the URL password\n%s", stderr)
	}
}

func TestWireLogFlagsAppearInHelp(t *testing.T) {
	stdout, _, err := run(t, "events", "--help")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, flag := range []string{"--wire-log", "--wire-log-max-body"} {
		if !strings.Contains(stdout, flag) {
			t.Errorf("help does not mention %s\n%s", flag, stdout)
		}
	}
}

func TestWireLogRejectsInvalidOptions(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "max body without a destination",
			args: []string{"events", "--from", "9000", "--wire-log-max-body", "10"},
		},
		{
			name: "max body below minus one",
			args: []string{"events", "--from", "9000", "--wire-log", filepath.Join(t.TempDir(), "wire.log"),
				"--wire-log-max-body", "-2"},
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

func TestWireLogOpenFailureIsReported(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "wire.ndjson")
	_, _, err := run(t, "events", "--from", "9000", "--rpc-url", "http://127.0.0.1:1",
		"--wire-log", path)
	if err == nil {
		t.Fatal("Execute() error = nil, want an error opening the wire log")
	}
	if !strings.Contains(err.Error(), "wire log") {
		t.Errorf("error = %q, want it to name the wire log", err)
	}
}
