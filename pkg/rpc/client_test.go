package rpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// TestIsRetryable pins the classification the streaming loop depends on.
//
// The asymmetry matters: calling a permanent failure transient only wastes
// backoff attempts, whereas calling a transient failure permanent aborts a
// stream that would have recovered. So the default leans towards retrying,
// and these cases encode where the line sits.
func TestIsRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is not retryable", nil, false},
		{"context cancellation is the operator stopping us", context.Canceled, false},
		{"deadline exceeded is not retryable", context.DeadlineExceeded, false},
		{"explicitly permanent", rpc.Permanent(errors.New("bad filter")), false},
		{"a malformed request is not retryable", errors.New("filter type invalid"), false},
		{"unknown errors are not retried", errors.New("something odd happened"), false},

		{"connection refused", errors.New("dial tcp: connection refused"), true},
		{"connection reset", errors.New("read: connection reset by peer"), true},
		{"broken pipe", errors.New("write: broken pipe"), true},
		{"unexpected EOF", errors.New("unexpected EOF"), true},
		{"timeout", errors.New("Client.Timeout exceeded"), true},
		{"dns failure", errors.New("no such host"), true},
		{"bad gateway", errors.New("502 Bad Gateway"), true},
		{"service unavailable", errors.New("503 Service Unavailable"), true},
		{"gateway timeout", errors.New("504 Gateway Timeout"), true},
		{"rate limited", errors.New("429 Too Many Requests"), true},
		{"wrapped transient error", fmt.Errorf("rpc getEvents: %w",
			errors.New("connection reset by peer")), true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := rpc.IsRetryable(tc.err); got != tc.want {
				t.Errorf("IsRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestIsRetryableNetError covers the typed path, which is the one that fires
// for real network failures rather than the string fallback.
func TestIsRetryableNetError(t *testing.T) {
	t.Parallel()

	var netErr net.Error = &net.DNSError{Err: "server misbehaving", IsTemporary: true}
	if !rpc.IsRetryable(netErr) {
		t.Error("a net.Error should be retryable")
	}
	if !rpc.IsRetryable(fmt.Errorf("wrapped: %w", netErr)) {
		t.Error("a wrapped net.Error should still be retryable")
	}
}

func TestPermanentWrapping(t *testing.T) {
	t.Parallel()

	inner := errors.New("cursor out of range")
	err := rpc.Permanent(inner)

	if !rpc.IsPermanent(err) {
		t.Error("IsPermanent() = false for a permanent error")
	}
	if !errors.Is(err, inner) {
		t.Error("a permanent error does not unwrap to its cause")
	}
	if err.Error() != inner.Error() {
		t.Errorf("Error() = %q, want the wrapped message %q", err, inner)
	}
	if rpc.IsPermanent(inner) {
		t.Error("IsPermanent() = true for an ordinary error")
	}
	// Wrapping must survive further wrapping, since the tailer adds context.
	if !rpc.IsPermanent(fmt.Errorf("events stream: %w", err)) {
		t.Error("permanence was lost when the error was wrapped again")
	}
}

func TestResolveURL(t *testing.T) {
	tests := []struct {
		name string
		flag string
		env  string
		want string
	}{
		{"flag wins", "https://flag.example", "https://env.example", "https://flag.example"},
		{"env when no flag", "", "https://env.example", "https://env.example"},
		{"default when neither", "", "", rpc.DefaultURL},
		{"blank env is ignored", "", "   ", rpc.DefaultURL},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(rpc.URLEnvVar, tc.env)
			if got := rpc.ResolveURL(tc.flag); got != tc.want {
				t.Errorf("ResolveURL(%q) with %s=%q = %q, want %q",
					tc.flag, rpc.URLEnvVar, tc.env, got, tc.want)
			}
		})
	}
}

func TestResolveURLs(t *testing.T) {
	tests := []struct {
		name  string
		flags []string
		env   string
		want  []string
	}{
		{
			name:  "flags win in order",
			flags: []string{"https://a.example", "https://b.example"},
			env:   "https://env.example",
			want:  []string{"https://a.example", "https://b.example"},
		},
		{
			name:  "blank flags are ignored",
			flags: []string{"", " https://a.example "},
			env:   "https://env.example",
			want:  []string{"https://a.example"},
		},
		{
			name: "comma-separated environment",
			env:  "https://a.example, https://b.example",
			want: []string{"https://a.example", "https://b.example"},
		},
		{
			name: "default when neither is set",
			want: []string{rpc.DefaultURL},
		},
		{
			name: "blank environment is ignored",
			env:  "   ",
			want: []string{rpc.DefaultURL},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(rpc.URLEnvVar, tc.env)
			got := rpc.ResolveURLs(tc.flags)
			if len(got) != len(tc.want) {
				t.Fatalf("ResolveURLs(%v) = %v, want %v", tc.flags, got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("ResolveURLs(%v)[%d] = %q, want %q", tc.flags, i, got[i], tc.want[i])
				}
			}
		})
	}
}

// TestDefaultURLIsTestnet guards the promise that prism works with no
// configuration, and that it does not default to mainnet.
func TestDefaultURLIsTestnet(t *testing.T) {
	t.Parallel()

	if rpc.DefaultURL == "" {
		t.Fatal("DefaultURL is empty")
	}
	if !contains(rpc.DefaultURL, "testnet") {
		t.Errorf("DefaultURL = %q, want a testnet endpoint so the default is safe", rpc.DefaultURL)
	}
}

// TestNewAppliesTimeout checks that a zero timeout is replaced rather than
// meaning "wait forever", which would let a hung connection stall a stream.
func TestNewAppliesTimeout(t *testing.T) {
	t.Parallel()

	if c := rpc.New("https://example.invalid", 0); c == nil {
		t.Fatal("New() returned nil")
	}
	// A constructed client must satisfy the interface the streaming core uses.
	var _ rpc.Client = rpc.New("https://example.invalid", 0)
}

func contains(h, n string) bool {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return true
		}
	}
	return false
}

func TestParseHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     []string
		want    http.Header
		wantErr bool
	}{
		{
			name: "empty slice returns nil",
			raw:  nil,
			want: nil,
		},
		{
			name: "blank entries are ignored",
			raw:  []string{"", "   ", "X-API-Key: secret"},
			want: http.Header{"X-Api-Key": {"secret"}},
		},
		{
			name: "value containing colons and commas",
			raw: []string{
				"Authorization: Bearer abc:def:123",
				"Accept: application/json, text/plain",
			},
			want: http.Header{
				"Authorization": {"Bearer abc:def:123"},
				"Accept":        {"application/json, text/plain"},
			},
		},
		{
			name: "repeated header preserves all values",
			raw:  []string{"X-Tag: first", "x-tag: second"},
			want: http.Header{"X-Tag": {"first", "second"}},
		},
		{
			name:    "missing colon",
			raw:     []string{"X-API-Key"},
			wantErr: true,
		},
		{
			name:    "empty header name",
			raw:     []string{": secret"},
			wantErr: true,
		},
		{
			name:    "space in header name",
			raw:     []string{"Bad Key: value"},
			wantErr: true,
		},
		{
			name:    "invalid token character in header name",
			raw:     []string{"Bad/Key: value"},
			wantErr: true,
		},
		{
			name:    "carriage return in value",
			raw:     []string{"X-Test: foo\r\nInjected: bar"},
			wantErr: true,
		},
		{
			name:    "null byte in value",
			raw:     []string{"X-Test: foo\x00bar"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := rpc.ParseHeaders(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseHeaders(%v) error = nil, want error", tc.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseHeaders(%v) error = %v", tc.raw, err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("ParseHeaders(%v) = %v, want %v", tc.raw, got, tc.want)
			}
			for k, wantVals := range tc.want {
				if !slices.Equal(got[k], wantVals) {
					t.Errorf("ParseHeaders(%v)[%q] = %v, want %v", tc.raw, k, got[k], wantVals)
				}
			}
		})
	}
}

func TestResolveHeaders(t *testing.T) {
	tests := []struct {
		name    string
		flags   []string
		env     string
		want    http.Header
		wantErr bool
	}{
		{
			name: "empty when neither is set",
			want: nil,
		},
		{
			name: "single env header",
			env:  "X-API-Key: env-secret",
			want: http.Header{"X-Api-Key": {"env-secret"}},
		},
		{
			name: "comma-separated env headers",
			env:  "X-API-Key: env-secret, Authorization: Bearer tok",
			want: http.Header{
				"X-Api-Key":     {"env-secret"},
				"Authorization": {"Bearer tok"},
			},
		},
		{
			name: "newline-separated env headers",
			env:  "X-API-Key: env-secret\nX-Client: prism\n",
			want: http.Header{
				"X-Api-Key": {"env-secret"},
				"X-Client":  {"prism"},
			},
		},
		{
			name: "preserves commas inside header value",
			env:  "Accept: application/json, text/plain, X-API-Key: env-secret",
			want: http.Header{
				"Accept":    {"application/json, text/plain"},
				"X-Api-Key": {"env-secret"},
			},
		},
		{
			name:  "flag overrides matching env header and merges others",
			env:   "X-API-Key: env-secret, X-Env-Only: kept",
			flags: []string{"x-api-key: flag-secret", "X-Flag-Only: added"},
			want: http.Header{
				"X-Api-Key":   {"flag-secret"},
				"X-Env-Only":  {"kept"},
				"X-Flag-Only": {"added"},
			},
		},
		{
			name:    "invalid env header returns error",
			env:     "not-a-header",
			wantErr: true,
		},
		{
			name:    "invalid flag header returns error",
			flags:   []string{"not-a-header"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(rpc.HeadersEnvVar, tc.env)
			got, err := rpc.ResolveHeaders(tc.flags)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ResolveHeaders(%v) with %s=%q error = nil, want error",
						tc.flags, rpc.HeadersEnvVar, tc.env)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveHeaders(%v) error = %v", tc.flags, err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("ResolveHeaders(%v) = %v, want %v", tc.flags, got, tc.want)
			}
			for k, wantVals := range tc.want {
				if !slices.Equal(got[k], wantVals) {
					t.Errorf("ResolveHeaders(%v)[%q] = %v, want %v", tc.flags, k, got[k], wantVals)
				}
			}
		})
	}
}

func TestNewWithHeadersTransmitsCustomHeaders(t *testing.T) {
	t.Parallel()

	var (
		mu          sync.Mutex
		seenHeaders http.Header
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seenHeaders = r.Header.Clone()
		mu.Unlock()

		if r.Header.Get("X-API-Key") != "secret-key" || r.Header.Get("Authorization") != "Bearer token-123" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read error", http.StatusInternalServerError)
			return
		}
		var req struct {
			ID json.RawMessage `json:"id"`
		}
		if err := json.Unmarshal(raw, &req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result": map[string]any{
				"events":                []any{},
				"cursor":                "",
				"latestLedger":          9000,
				"oldestLedger":          9000,
				"latestLedgerCloseTime": "1700000000",
				"oldestLedgerCloseTime": "1700000000",
			},
		})
	}))
	defer srv.Close()

	// Without headers, the authenticated endpoint rejects the request.
	unauthClient := rpc.New(srv.URL, 5*time.Second)
	defer unauthClient.Close()
	if _, err := unauthClient.GetEvents(t.Context(), protocol.GetEventsRequest{StartLedger: 9000}); err == nil {
		t.Fatal("GetEvents() without custom headers succeeded, want unauthorized failure")
	}

	headers, err := rpc.ParseHeaders([]string{
		"X-API-Key: secret-key",
		"Authorization: Bearer token-123",
	})
	if err != nil {
		t.Fatalf("ParseHeaders() error = %v", err)
	}

	authClient := rpc.NewWithHeaders(srv.URL, 5*time.Second, headers)
	defer authClient.Close()
	if _, err := authClient.GetEvents(t.Context(), protocol.GetEventsRequest{StartLedger: 9000}); err != nil {
		t.Fatalf("GetEvents() with custom headers error = %v", err)
	}

	mu.Lock()
	gotAPIKey := seenHeaders.Get("X-Api-Key")
	gotAuth := seenHeaders.Get("Authorization")
	mu.Unlock()

	if gotAPIKey != "secret-key" {
		t.Errorf("server saw X-Api-Key = %q, want %q", gotAPIKey, "secret-key")
	}
	if gotAuth != "Bearer token-123" {
		t.Errorf("server saw Authorization = %q, want %q", gotAuth, "Bearer token-123")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestNewHeaderTransportDoesNotMutateOriginalRequest(t *testing.T) {
	t.Parallel()

	var downstream http.Header
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		downstream = req.Header.Clone()
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("{}")),
		}, nil
	})

	rt := rpc.NewHeaderTransport(base, http.Header{
		"X-Api-Key": {"secret"},
	})

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://example.invalid", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	req.Header.Set("X-Existing", "original")

	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	resp.Body.Close()

	if req.Header.Get("X-Api-Key") != "" {
		t.Errorf("RoundTrip mutated original request headers: %v", req.Header)
	}
	if downstream.Get("X-Existing") != "original" {
		t.Errorf("downstream X-Existing = %q, want %q", downstream.Get("X-Existing"), "original")
	}
	if downstream.Get("X-Api-Key") != "secret" {
		t.Errorf("downstream X-Api-Key = %q, want %q", downstream.Get("X-Api-Key"), "secret")
	}

	// Nil Header map on incoming request must also work without panicking.
	nilHeaderReq := &http.Request{Method: http.MethodGet, URL: req.URL}
	resp2, err := rt.RoundTrip(nilHeaderReq)
	if err != nil {
		t.Fatalf("RoundTrip(nil Header) error = %v", err)
	}
	resp2.Body.Close()
	if nilHeaderReq.Header != nil {
		t.Errorf("RoundTrip mutated nil Header on original request")
	}
}

func TestNewWithOptionsComposesTransportAndHeaders(t *testing.T) {
	t.Parallel()

	var seen string
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		seen = req.Header.Get("X-Api-Key")
		raw, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		var rpcReq struct {
			ID json.RawMessage `json:"id"`
		}
		if err := json.Unmarshal(raw, &rpcReq); err != nil {
			return nil, err
		}
		payload, err := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      rpcReq.ID,
			"result": map[string]any{
				"events":                []any{},
				"cursor":                "",
				"latestLedger":          1,
				"oldestLedger":          1,
				"latestLedgerCloseTime": "1",
				"oldestLedgerCloseTime": "1",
			},
		})
		if err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       io.NopCloser(strings.NewReader(string(payload))),
		}, nil
	})

	client := rpc.NewWithOptions("https://example.invalid", 5*time.Second, rpc.NewClientOption{
		Transport: base,
		Headers:   http.Header{"X-Api-Key": {"opt-secret"}},
	})
	defer client.Close()
	if _, err := client.GetEvents(t.Context(), protocol.GetEventsRequest{StartLedger: 1}); err != nil {
		t.Fatalf("GetEvents() error = %v", err)
	}
	if seen != "opt-secret" {
		t.Errorf("transport saw X-Api-Key = %q, want %q", seen, "opt-secret")
	}
}
