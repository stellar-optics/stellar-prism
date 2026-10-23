package rpc

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type failingWriter struct {
	err error
}

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

type flushWriter struct {
	mu       sync.Mutex
	buf      bytes.Buffer
	flushErr error
}

func (w *flushWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *flushWriter) Flush() error { return w.flushErr }

func (w *flushWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

type failingReadCloser struct {
	reader io.Reader
	err    error
	closed bool
}

func (r *failingReadCloser) Read(p []byte) (int, error) { return r.reader.Read(p) }
func (r *failingReadCloser) Close() error {
	r.closed = true
	return r.err
}

func fixedClock() time.Time { return time.Unix(1_700_000_000, 0).UTC() }

func closeResponse(t *testing.T, resp *http.Response) {
	t.Helper()
	if resp == nil || resp.Body == nil {
		return
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("closing response body: %v", err)
	}
}

func decodeWireLog(t *testing.T, raw string) []wireLogEntry {
	t.Helper()
	var entries []wireLogEntry
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		if line == "" {
			continue
		}
		var entry wireLogEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("wire log line is not valid JSON: %v\n%s", err, line)
		}
		entries = append(entries, entry)
	}
	return entries
}

func response(status int, header http.Header, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestNewWireLoggerDefaults(t *testing.T) {
	logger := NewWireLogger(nil, WireLogOptions{})

	if logger.w != io.Discard {
		t.Errorf("nil writer was not replaced by io.Discard: %T", logger.w)
	}
	if logger.base != http.DefaultTransport {
		t.Errorf("default transport = %T, want http.DefaultTransport", logger.base)
	}
	if logger.maxBody != DefaultWireLogMaxBodyBytes {
		t.Errorf("default max body = %d, want %d", logger.maxBody, DefaultWireLogMaxBodyBytes)
	}
	if logger.now == nil {
		t.Error("default clock is nil")
	}
	for _, name := range defaultRedactedHeaders {
		if _, ok := logger.redacted[http.CanonicalHeaderKey(name)]; !ok {
			t.Errorf("%s is not in the default redaction set", name)
		}
	}

	// A nil writer must not panic or fail the exchange it is observing.
	base := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, nil, "ok"), nil
	})
	logger = NewWireLogger(nil, WireLogOptions{Base: base})
	resp, err := logger.RoundTrip(httptest.NewRequest(http.MethodGet, "https://example.invalid", nil))
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("reading response: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("closing response: %v", err)
	}
	if err := logger.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestWireLoggerLogsAndRedactsExchange(t *testing.T) {
	var (
		gotBody []byte
		gotAuth string
	)
	base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotAuth = r.Header.Get("Authorization")
		var err error
		gotBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("transport reading request body: %v", err)
		}
		return response(http.StatusOK,
			http.Header{"Set-Cookie": {"session=response-secret"}},
			`{"jsonrpc":"2.0","result":{"ok":true}}`), nil
	})

	var buf bytes.Buffer
	logger := NewWireLogger(&buf, WireLogOptions{
		Base: base,
		Now:  fixedClock,
	})

	const requestBody = `{"jsonrpc":"2.0","method":"getEvents","id":1}`
	req := httptest.NewRequest(http.MethodPost,
		"https://prism:url-password@rpc.example/path?token=query-secret&limit=10",
		strings.NewReader(requestBody))
	req.Header.Set("Authorization", "Bearer header-secret")
	req.Header.Set("X-Api-Key", "api-secret")
	req.Header.Set("Accept", "application/json")

	resp, err := logger.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response body: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("closing response body: %v", err)
	}

	// The logger must be transparent: the transport sees the real credentials
	// and the caller sees the real response.
	if gotAuth != "Bearer header-secret" {
		t.Errorf("transport saw Authorization = %q, want the original value", gotAuth)
	}
	if string(gotBody) != requestBody {
		t.Errorf("transport saw body = %q, want %q", gotBody, requestBody)
	}
	if string(body) != `{"jsonrpc":"2.0","result":{"ok":true}}` {
		t.Errorf("caller got body = %q", body)
	}

	entries := decodeWireLog(t, buf.String())
	if len(entries) != 2 {
		t.Fatalf("got %d log entries, want 2\n%s", len(entries), buf.String())
	}

	reqEntry, respEntry := entries[0], entries[1]
	if reqEntry.Direction != "request" || respEntry.Direction != "response" {
		t.Fatalf("directions = %q, %q; want request, response", reqEntry.Direction, respEntry.Direction)
	}
	if reqEntry.Method != http.MethodPost {
		t.Errorf("request method = %q, want POST", reqEntry.Method)
	}
	if !strings.Contains(reqEntry.URL, "rpc.example/path") {
		t.Errorf("request URL = %q, want the endpoint preserved", reqEntry.URL)
	}
	if got := reqEntry.Headers.Get("Authorization"); got != RedactedValue {
		t.Errorf("logged Authorization = %q, want %q", got, RedactedValue)
	}
	if got := reqEntry.Headers.Get("X-Api-Key"); got != RedactedValue {
		t.Errorf("logged X-Api-Key = %q, want %q", got, RedactedValue)
	}
	if got := reqEntry.Headers.Get("Accept"); got != "application/json" {
		t.Errorf("logged Accept = %q, want the original value", got)
	}
	if reqEntry.Body != requestBody {
		t.Errorf("logged request body = %q, want %q", reqEntry.Body, requestBody)
	}
	if reqEntry.Truncated {
		t.Error("small request body was marked truncated")
	}
	if respEntry.Status != http.StatusOK {
		t.Errorf("logged status = %d, want 200", respEntry.Status)
	}
	if got := respEntry.Headers.Get("Set-Cookie"); got != RedactedValue {
		t.Errorf("logged Set-Cookie = %q, want %q", got, RedactedValue)
	}
	if respEntry.Body != `{"jsonrpc":"2.0","result":{"ok":true}}` {
		t.Errorf("logged response body = %q", respEntry.Body)
	}
	if respEntry.Time != fixedClock().Format(time.RFC3339Nano) {
		t.Errorf("timestamp = %q, want the injected clock", respEntry.Time)
	}

	raw := buf.String()
	for _, secret := range []string{"header-secret", "api-secret", "url-password", "query-secret", "response-secret"} {
		if strings.Contains(raw, secret) {
			t.Errorf("wire log leaked %q\n%s", secret, raw)
		}
	}
	if !strings.Contains(raw, RedactedValue) {
		t.Errorf("wire log does not contain %q\n%s", RedactedValue, raw)
	}

	if err := logger.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
	if err := logger.Err(); err != nil {
		t.Errorf("Err() = %v, want nil", err)
	}
}

func TestWireLoggerTruncatesAndReplaysBodies(t *testing.T) {
	const (
		requestBody  = "0123456789abcdef"
		responseBody = "abcdefghijklmnop"
	)

	var gotBody string
	base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("transport reading request body: %v", err)
		}
		gotBody = string(b)
		return response(http.StatusCreated, nil, responseBody), nil
	})

	var buf bytes.Buffer
	logger := NewWireLogger(&buf, WireLogOptions{Base: base, MaxBodyBytes: 8, Now: fixedClock})
	req := httptest.NewRequest(http.MethodPost, "https://rpc.example", strings.NewReader(requestBody))

	resp, err := logger.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("closing response: %v", err)
	}

	if gotBody != requestBody {
		t.Errorf("transport saw request body = %q, want the full body", gotBody)
	}
	if string(body) != responseBody {
		t.Errorf("caller saw response body = %q, want the full body", body)
	}

	entries := decodeWireLog(t, buf.String())
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	if entries[0].Body != requestBody[:8] || !entries[0].Truncated {
		t.Errorf("request body = %q truncated=%v, want the first 8 bytes and truncated",
			entries[0].Body, entries[0].Truncated)
	}
	if entries[1].Body != responseBody[:8] || !entries[1].Truncated {
		t.Errorf("response body = %q truncated=%v, want the first 8 bytes and truncated",
			entries[1].Body, entries[1].Truncated)
	}
	if err := logger.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestWireLoggerCanDisableBodyCapture(t *testing.T) {
	var gotBody string
	base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("transport reading request body: %v", err)
		}
		gotBody = string(b)
		return response(http.StatusOK, nil, "response payload"), nil
	})

	var buf bytes.Buffer
	logger := NewWireLogger(&buf, WireLogOptions{Base: base, MaxBodyBytes: -1, Now: fixedClock})
	req := httptest.NewRequest(http.MethodPost, "https://rpc.example", strings.NewReader("request payload"))

	resp, err := logger.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("closing response: %v", err)
	}
	if gotBody != "request payload" {
		t.Errorf("transport saw body = %q, want the original payload", gotBody)
	}
	if string(body) != "response payload" {
		t.Errorf("caller saw body = %q, want the original payload", body)
	}

	entries := decodeWireLog(t, buf.String())
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	for _, entry := range entries {
		if entry.Body != "" {
			t.Errorf("%s body = %q, want no body when capture is disabled", entry.Direction, entry.Body)
		}
		if !entry.BodyOmitted {
			t.Errorf("%s bodyOmitted = false, want true", entry.Direction)
		}
	}
}

func TestWireLoggerLogsTransportErrorAndRedactsIt(t *testing.T) {
	wantErr := errors.New("proxy rejected Bearer top-secret for https://user:pw@rpc.example?token=query-secret")
	base := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, wantErr
	})

	var buf bytes.Buffer
	logger := NewWireLogger(&buf, WireLogOptions{Base: base, Now: fixedClock})
	req := httptest.NewRequest(http.MethodPost, "https://user:pw@rpc.example?token=query-secret", nil)
	req.Header.Set("Authorization", "Bearer top-secret")

	resp, err := logger.RoundTrip(req)
	closeResponse(t, resp)
	if !errors.Is(err, wantErr) {
		t.Fatalf("RoundTrip() error = %v, want %v", err, wantErr)
	}

	entries := decodeWireLog(t, buf.String())
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want request and error\n%s", len(entries), buf.String())
	}
	if entries[1].Direction != "error" {
		t.Errorf("direction = %q, want error", entries[1].Direction)
	}
	if strings.Contains(entries[1].Error, "top-secret") ||
		strings.Contains(entries[1].Error, "user:pw") ||
		strings.Contains(entries[1].Error, "query-secret") {
		t.Errorf("error text leaked a credential: %q", entries[1].Error)
	}
	if !strings.Contains(entries[1].Error, RedactedValue) {
		t.Errorf("error text does not show a redaction marker: %q", entries[1].Error)
	}
}

func TestWireLoggerRejectsInvalidRequestsAndNilResponses(t *testing.T) {
	logger := NewWireLogger(io.Discard, WireLogOptions{
		Base: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, nil //nolint:nilnil // exercising the transport contract violation
		}),
	})

	resp, err := logger.RoundTrip(nil)
	closeResponse(t, resp)
	if err == nil {
		t.Error("RoundTrip(nil) error = nil, want an error")
	}
	resp, err = logger.RoundTrip(&http.Request{})
	closeResponse(t, resp)
	if err == nil {
		t.Error("RoundTrip(request without URL) error = nil, want an error")
	}
	resp, err = logger.RoundTrip(httptest.NewRequest(http.MethodGet, "https://rpc.example", nil))
	closeResponse(t, resp)
	if err == nil {
		t.Error("RoundTrip(nil response) error = nil, want an error")
	}
}

func TestWireLoggerReportsWriterFailuresWithoutFailingTheExchange(t *testing.T) {
	writeErr := errors.New("disk full")
	base := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, nil, "ok"), nil
	})
	logger := NewWireLogger(failingWriter{err: writeErr}, WireLogOptions{Base: base})

	resp, err := logger.RoundTrip(httptest.NewRequest(http.MethodGet, "https://rpc.example", nil))
	if err != nil {
		t.Fatalf("RoundTrip() error = %v, want the RPC exchange to succeed", err)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("reading response: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("closing response: %v", err)
	}

	err = logger.Err()
	if !errors.Is(err, writeErr) {
		t.Fatalf("Err() = %v, want it to wrap %v", err, writeErr)
	}
	if !errors.Is(logger.Close(), writeErr) {
		t.Errorf("Close() = %v, want it to wrap %v", logger.Close(), writeErr)
	}
	if !errors.Is(logger.Close(), writeErr) {
		t.Error("second Close() did not return the accumulated error")
	}
}

func TestWireLoggerFlushAndCloseAreIdempotent(t *testing.T) {
	flushErr := errors.New("flush failed")
	writer := &flushWriter{flushErr: flushErr}
	base := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, nil, "ok"), nil
	})
	logger := NewWireLogger(writer, WireLogOptions{Base: base})

	resp, err := logger.RoundTrip(httptest.NewRequest(http.MethodGet, "https://rpc.example", nil))
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("reading response: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("closing response: %v", err)
	}

	if err := logger.Flush(); !errors.Is(err, flushErr) {
		t.Errorf("Flush() = %v, want %v", err, flushErr)
	}
	if err := logger.Close(); !errors.Is(err, flushErr) {
		t.Errorf("Close() = %v, want %v", err, flushErr)
	}
	if writer.String() == "" {
		t.Error("nothing was written to the writer")
	}

	before := writer.String()
	// A request after Close still works, but must not append to the log.
	resp, err = logger.RoundTrip(httptest.NewRequest(http.MethodGet, "https://rpc.example", nil))
	closeResponse(t, resp)
	if err != nil {
		t.Fatalf("RoundTrip() after Close error = %v", err)
	}
	if after := writer.String(); after != before {
		t.Errorf("log was written after Close:\nbefore=%q\nafter=%q", before, after)
	}
}

func TestWireLoggerExtraRedactedHeaders(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWireLogger(&buf, WireLogOptions{
		Base: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return response(http.StatusOK, nil, ""), nil
		}),
		RedactHeaders: []string{" X-Custom-Secret ", ""},
		Now:           fixedClock,
	})
	req := httptest.NewRequest(http.MethodGet, "https://rpc.example", nil)
	req.Header.Set("X-Custom-Secret", "custom-value")
	req.Header.Set("X-Public", "public-value")

	resp, err := logger.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("closing response: %v", err)
	}

	entry := decodeWireLog(t, buf.String())[0]
	if got := entry.Headers.Get("X-Custom-Secret"); got != RedactedValue {
		t.Errorf("X-Custom-Secret = %q, want %q", got, RedactedValue)
	}
	if got := entry.Headers.Get("X-Public"); got != "public-value" {
		t.Errorf("X-Public = %q, want the original value", got)
	}
}

func TestWireLoggerEncodesNonUTF8BodyAsBase64(t *testing.T) {
	payload := []byte{0xff, 0xfe, 0x00, 0x01}
	var buf bytes.Buffer
	logger := NewWireLogger(&buf, WireLogOptions{
		Base: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				t.Errorf("transport reading request body: %v", err)
			}
			return response(http.StatusOK, nil, ""), nil
		}),
		Now: fixedClock,
	})

	req := httptest.NewRequest(http.MethodPost, "https://rpc.example", bytes.NewReader(payload))
	resp, err := logger.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("closing response: %v", err)
	}

	entry := decodeWireLog(t, buf.String())[0]
	if entry.BodyEncoding != "base64" {
		t.Errorf("bodyEncoding = %q, want base64", entry.BodyEncoding)
	}
	decoded, err := base64.StdEncoding.DecodeString(entry.Body)
	if err != nil {
		t.Fatalf("logged body is not valid base64: %v", err)
	}
	if !bytes.Equal(decoded, payload) {
		t.Errorf("decoded body = %v, want %v", decoded, payload)
	}
}

func TestWireLoggerReportsBodyReadFailures(t *testing.T) {
	readErr := errors.New("body exploded")
	closeErr := errors.New("close failed")

	t.Run("request", func(t *testing.T) {
		base := roundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Error("transport was called after the request body failed to read")
			return response(http.StatusOK, nil, ""), nil
		})
		var buf bytes.Buffer
		logger := NewWireLogger(&buf, WireLogOptions{Base: base})
		body := &failingReadCloser{
			reader: io.MultiReader(strings.NewReader("partial"), errorReader{err: readErr}),
			err:    closeErr,
		}
		req := httptest.NewRequest(http.MethodPost, "https://rpc.example", body)

		resp, err := logger.RoundTrip(req)
		closeResponse(t, resp)
		if !errors.Is(err, readErr) || !errors.Is(err, closeErr) {
			t.Fatalf("RoundTrip() error = %v, want both read and close failures", err)
		}
		if !body.closed {
			t.Error("failed request body was not closed")
		}
		if !errors.Is(logger.Err(), readErr) {
			t.Errorf("Err() = %v, want the read failure", logger.Err())
		}
		entries := decodeWireLog(t, buf.String())
		if len(entries) != 1 || entries[0].Direction != "error" {
			t.Fatalf("entries = %+v, want one error entry", entries)
		}
	})

	t.Run("response", func(t *testing.T) {
		body := &failingReadCloser{
			reader: io.MultiReader(strings.NewReader("partial"), errorReader{err: readErr}),
			err:    closeErr,
		}
		base := roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
		})
		var buf bytes.Buffer
		logger := NewWireLogger(&buf, WireLogOptions{Base: base})

		resp, err := logger.RoundTrip(httptest.NewRequest(http.MethodGet, "https://rpc.example", nil))
		closeResponse(t, resp)
		if !errors.Is(err, readErr) || !errors.Is(err, closeErr) {
			t.Fatalf("RoundTrip() error = %v, want both read and close failures", err)
		}
		if !body.closed {
			t.Error("failed response body was not closed")
		}
		entries := decodeWireLog(t, buf.String())
		if len(entries) != 2 || entries[1].Direction != "error" {
			t.Fatalf("entries = %+v, want request and error entries", entries)
		}
	})
}

func TestReadBodyPrefix(t *testing.T) {
	t.Run("nil reader", func(t *testing.T) {
		prefix, rest, truncated, err := readBodyPrefix(nil, 8)
		if err != nil || prefix != nil || rest != nil || truncated {
			t.Errorf("readBodyPrefix(nil) = %v, %v, %v, %v", prefix, rest, truncated, err)
		}
	})

	t.Run("disabled capture returns the original reader", func(t *testing.T) {
		reader := strings.NewReader("payload")
		prefix, rest, truncated, err := readBodyPrefix(reader, -1)
		if err != nil || prefix != nil || rest != reader || truncated {
			t.Errorf("readBodyPrefix(disabled) = %v, %v, %v, %v", prefix, rest, truncated, err)
		}
	})

	t.Run("zero limit marks any body truncated", func(t *testing.T) {
		prefix, rest, truncated, err := readBodyPrefix(strings.NewReader("payload"), 0)
		if err != nil {
			t.Fatalf("readBodyPrefix() error = %v", err)
		}
		if len(prefix) != 0 || !truncated {
			t.Errorf("prefix = %q, truncated = %v; want an empty prefix and truncation", prefix, truncated)
		}
		replayed, err := io.ReadAll(rest)
		if err != nil {
			t.Fatalf("reading rest: %v", err)
		}
		if string(replayed) != "payload" {
			t.Errorf("rest = %q, want the full remaining body", replayed)
		}
	})

	t.Run("short body is not truncated", func(t *testing.T) {
		prefix, rest, truncated, err := readBodyPrefix(strings.NewReader("abc"), 8)
		if err != nil {
			t.Fatalf("readBodyPrefix() error = %v", err)
		}
		if string(prefix) != "abc" || truncated {
			t.Errorf("prefix = %q, truncated = %v; want abc and false", prefix, truncated)
		}
		replayed, err := io.ReadAll(rest)
		if err != nil {
			t.Fatalf("reading rest: %v", err)
		}
		if len(replayed) != 0 {
			t.Errorf("rest = %q, want an exhausted reader", replayed)
		}
	})

	t.Run("read error is returned", func(t *testing.T) {
		want := errors.New("boom")
		_, _, _, err := readBodyPrefix(errorReader{err: want}, 8)
		if !errors.Is(err, want) {
			t.Errorf("readBodyPrefix() error = %v, want %v", err, want)
		}
	})

	t.Run("maximum limit does not overflow", func(t *testing.T) {
		prefix, _, truncated, err := readBodyPrefix(strings.NewReader("abc"), int64(^uint64(0)>>1))
		if err != nil || string(prefix) != "abc" || truncated {
			t.Errorf("readBodyPrefix(max) = %q, %v, %v", prefix, truncated, err)
		}
	})
}

func TestDurationMilliseconds(t *testing.T) {
	if got := durationMilliseconds(-time.Second); got != 0 {
		t.Errorf("durationMilliseconds(negative) = %v, want 0", got)
	}
	if got := durationMilliseconds(1500 * time.Microsecond); got != 1.5 {
		t.Errorf("durationMilliseconds(1.5ms) = %v, want 1.5", got)
	}
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }
