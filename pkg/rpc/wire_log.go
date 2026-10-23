package rpc

// A debug wire logger for RPC traffic.
//
// The logger is an http.RoundTripper: it wraps the transport the SDK client
// actually uses, records each request and response as one JSON object per
// line, and passes the exchange through untouched. Authorization headers are
// redacted before anything reaches the file, and request/response bodies are
// captured up to a configurable cap.
//
// It is deliberately synchronous. A background writer would need a queue, a
// flush protocol and a shutdown path, all of which can lose records or leak a
// goroutine when a command is interrupted. One mutex serialises whole lines
// instead; there is no goroutine to join and no buffer that can grow without
// bound.

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	// DefaultWireLogMaxBodyBytes is how many bytes of each request or
	// response body are recorded when WireLogOptions.MaxBodyBytes is zero.
	// JSON-RPC payloads are normally far smaller than this; the cap exists so
	// a large page cannot make the debug log unbounded.
	DefaultWireLogMaxBodyBytes int64 = 1 << 20

	// RedactedValue replaces a sensitive header value, URL credential or
	// query value in the wire log.
	RedactedValue = "[REDACTED]"
)

// defaultRedactedHeaders is the set of headers masked out of the box. It
// covers the standard authorization headers as well as the credential-bearing
// headers commonly used by hosted RPC providers.
var defaultRedactedHeaders = []string{
	"Authorization",
	"Proxy-Authorization",
	"Cookie",
	"Set-Cookie",
	"X-Api-Key",
	"X-Auth-Token",
	"X-Amz-Security-Token",
}

// sensitiveQueryParams are query parameter names whose values are masked in
// the logged URL. RPC providers sometimes put an API key in the endpoint URL
// rather than in a header.
var sensitiveQueryParams = map[string]struct{}{
	"access_token": {},
	"api_key":      {},
	"apikey":       {},
	"auth":         {},
	"key":          {},
	"password":     {},
	"secret":       {},
	"token":        {},
}

// WireLogOptions configures a WireLogger.
type WireLogOptions struct {
	// Base performs the actual request. It is the transport being observed;
	// nil means http.DefaultTransport. Tests inject a fake here.
	Base http.RoundTripper

	// MaxBodyBytes caps how much of each body is recorded. Zero uses
	// DefaultWireLogMaxBodyBytes. A negative value disables body capture,
	// which is useful when a caller only wants headers and timings.
	MaxBodyBytes int64

	// RedactHeaders adds header names to the default redaction set. Names
	// are matched case-insensitively. The defaults are Authorization,
	// Proxy-Authorization, Cookie, Set-Cookie, X-Api-Key, X-Auth-Token and
	// X-Amz-Security-Token.
	RedactHeaders []string

	// Now supplies the clock used for timestamps and durations. Nil uses
	// time.Now; tests set it for deterministic output.
	Now func() time.Time
}

// WireLogger is an http.RoundTripper that records RPC exchanges as NDJSON and
// redacts credentials before writing them.
//
// It is safe for concurrent use: the failover pool may have several requests
// in flight, and each complete line is written under a single mutex. The
// logger never starts a goroutine, so Close is about flushing and releasing
// the writer's state, not joining a worker.
type WireLogger struct {
	mu       sync.Mutex
	w        io.Writer
	base     http.RoundTripper
	maxBody  int64
	redacted map[string]struct{}
	now      func() time.Time
	closed   bool
	err      error
}

// NewWireLogger returns a logger writing NDJSON exchanges to w.
//
// A nil writer becomes io.Discard, so a misconfigured logger cannot panic the
// stream it is meant to diagnose.
func NewWireLogger(w io.Writer, opts WireLogOptions) *WireLogger {
	if w == nil {
		w = io.Discard
	}

	base := opts.Base
	if base == nil {
		base = http.DefaultTransport
	}

	maxBody := opts.MaxBodyBytes
	if maxBody == 0 {
		maxBody = DefaultWireLogMaxBodyBytes
	}

	now := opts.Now
	if now == nil {
		now = time.Now
	}

	redacted := make(map[string]struct{}, len(defaultRedactedHeaders)+len(opts.RedactHeaders))
	for _, name := range defaultRedactedHeaders {
		redacted[http.CanonicalHeaderKey(name)] = struct{}{}
	}
	for _, name := range opts.RedactHeaders {
		if name = strings.TrimSpace(name); name != "" {
			redacted[http.CanonicalHeaderKey(name)] = struct{}{}
		}
	}

	return &WireLogger{
		w:        w,
		base:     base,
		maxBody:  maxBody,
		redacted: redacted,
		now:      now,
	}
}

// wireLogEntry is one line in the wire log. Fields are omitted when they do
// not apply, so a request line stays readable next to a response line.
type wireLogEntry struct {
	Time         string      `json:"time"`
	Direction    string      `json:"direction"`
	Method       string      `json:"method,omitempty"`
	URL          string      `json:"url,omitempty"`
	Status       int         `json:"status,omitempty"`
	DurationMS   float64     `json:"durationMs,omitempty"`
	Headers      http.Header `json:"headers,omitempty"`
	Body         string      `json:"body,omitempty"`
	BodyEncoding string      `json:"bodyEncoding,omitempty"`
	BodyOmitted  bool        `json:"bodyOmitted,omitempty"`
	Truncated    bool        `json:"truncated,omitempty"`
	Error        string      `json:"error,omitempty"`
}

// RoundTrip implements http.RoundTripper.
//
// The exchange is recorded before it is performed and again when it returns.
// Logging failures are remembered and reported by Err or Close; they never
// replace the RPC result, because a full debug disk must not turn a working
// stream into a failing one.
func (l *WireLogger) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("wire log: nil request")
	}
	if req.URL == nil {
		return nil, errors.New("wire log: request has no URL")
	}

	reqEntry := wireLogEntry{
		Time:      l.timestamp(),
		Direction: "request",
		Method:    req.Method,
		URL:       l.redactURL(req.URL),
		Headers:   l.redactHeaders(req.Header),
	}

	if req.Body != nil && req.Body != http.NoBody {
		prefix, rest, truncated, err := readBodyPrefix(req.Body, l.maxBody)
		if err != nil {
			closeErr := req.Body.Close()
			joined := errors.Join(err, closeErr)
			l.recordError(fmt.Errorf("wire log: reading request body: %w", joined))
			l.emit(wireLogEntry{
				Time:      l.timestamp(),
				Direction: "error",
				Method:    req.Method,
				URL:       l.redactURL(req.URL),
				Error:     l.redactText(joined.Error(), req),
			})
			return nil, fmt.Errorf("wire log: reading request body: %w", joined)
		}
		// Rebuild the body from exactly the bytes that were inspected plus
		// whatever remains, so the transport sees the original payload.
		req.Body = &replayReadCloser{
			Reader: io.MultiReader(bytes.NewReader(prefix), rest),
			Closer: req.Body,
		}
		reqEntry.Body, reqEntry.BodyEncoding = encodeBody(prefix)
		reqEntry.Truncated = truncated
	}
	if l.maxBody < 0 {
		reqEntry.BodyOmitted = true
	}

	l.emit(reqEntry)

	start := l.now()
	resp, err := l.base.RoundTrip(req)
	elapsed := l.now().Sub(start)

	if err != nil {
		l.emit(wireLogEntry{
			Time:       l.timestamp(),
			Direction:  "error",
			Method:     req.Method,
			URL:        l.redactURL(req.URL),
			DurationMS: durationMilliseconds(elapsed),
			Error:      l.redactText(err.Error(), req),
		})
		return resp, err
	}
	if resp == nil {
		err := errors.New("wire log: transport returned a nil response with no error")
		l.emit(wireLogEntry{
			Time:       l.timestamp(),
			Direction:  "error",
			Method:     req.Method,
			URL:        l.redactURL(req.URL),
			DurationMS: durationMilliseconds(elapsed),
			Error:      err.Error(),
		})
		return nil, err
	}

	respEntry := wireLogEntry{
		Time:       l.timestamp(),
		Direction:  "response",
		Method:     req.Method,
		URL:        l.redactURL(req.URL),
		Status:     resp.StatusCode,
		DurationMS: durationMilliseconds(elapsed),
		Headers:    l.redactHeaders(resp.Header),
	}

	if resp.Body != nil {
		prefix, rest, truncated, readErr := readBodyPrefix(resp.Body, l.maxBody)
		if readErr != nil {
			closeErr := resp.Body.Close()
			joined := errors.Join(readErr, closeErr)
			l.recordError(fmt.Errorf("wire log: reading response body: %w", joined))
			l.emit(wireLogEntry{
				Time:       l.timestamp(),
				Direction:  "error",
				Method:     req.Method,
				URL:        l.redactURL(req.URL),
				Status:     resp.StatusCode,
				DurationMS: durationMilliseconds(elapsed),
				Error:      l.redactText(joined.Error(), req),
			})
			return nil, fmt.Errorf("wire log: reading response body: %w", joined)
		}
		resp.Body = &replayReadCloser{
			Reader: io.MultiReader(bytes.NewReader(prefix), rest),
			Closer: resp.Body,
		}
		respEntry.Body, respEntry.BodyEncoding = encodeBody(prefix)
		respEntry.Truncated = truncated
	}
	if l.maxBody < 0 {
		respEntry.BodyOmitted = true
	}

	l.emit(respEntry)
	return resp, nil
}

// Err returns the accumulated logging failure, or nil. It does not stop the
// stream; callers report it after the run, where it is useful rather than
// fatal.
func (l *WireLogger) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// Flush flushes the underlying writer when it supports flushing and returns
// any logging error accumulated so far.
func (l *WireLogger) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.flushLocked()
	return l.err
}

// Close stops further logging, flushes the writer if it supports it, and
// returns any logging error. It is idempotent, and every call returns the
// accumulated error rather than only the first one.
func (l *WireLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return l.err
	}
	l.closed = true
	l.flushLocked()
	return l.err
}

func (l *WireLogger) flushLocked() {
	flusher, ok := l.w.(interface{ Flush() error })
	if !ok {
		return
	}
	if err := flusher.Flush(); err != nil {
		l.setErrLocked(fmt.Errorf("wire log: flushing: %w", err))
	}
}

func (l *WireLogger) emit(entry wireLogEntry) {
	if entry.Time == "" {
		entry.Time = l.timestamp()
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(entry); err != nil {
		l.recordError(fmt.Errorf("wire log: encoding entry: %w", err))
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	if _, err := l.w.Write(buf.Bytes()); err != nil {
		l.setErrLocked(fmt.Errorf("wire log: writing entry: %w", err))
	}
}

func (l *WireLogger) recordError(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.setErrLocked(err)
}

func (l *WireLogger) setErrLocked(err error) {
	if err == nil {
		return
	}
	if l.err == nil {
		l.err = err
		return
	}
	l.err = errors.Join(l.err, err)
}

func (l *WireLogger) timestamp() string {
	return l.now().UTC().Format(time.RFC3339Nano)
}

func (l *WireLogger) redactHeaders(h http.Header) http.Header {
	if h == nil {
		return nil
	}
	out := make(http.Header, len(h))
	for name, values := range h {
		canonical := http.CanonicalHeaderKey(name)
		if _, ok := l.redacted[canonical]; ok {
			out[canonical] = []string{RedactedValue}
			continue
		}
		out[canonical] = append(out[canonical], values...)
	}
	return out
}

func (l *WireLogger) redactURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	clone := *u
	if clone.User != nil {
		clone.User = url.User(RedactedValue)
	}
	query := clone.Query()
	for name := range query {
		if _, ok := sensitiveQueryParams[strings.ToLower(name)]; ok {
			query[name] = []string{RedactedValue}
		}
	}
	clone.RawQuery = query.Encode()
	return clone.String()
}

// redactText removes credentials that a transport error message might echo
// back. The URL itself is normally included in those messages, so this is the
// difference between a useful debug log and a leaked token.
func (l *WireLogger) redactText(text string, req *http.Request) string {
	if req.URL != nil {
		if req.URL.User != nil {
			if userinfo := req.URL.User.String(); userinfo != "" {
				text = strings.ReplaceAll(text, userinfo, RedactedValue)
			}
			if password, ok := req.URL.User.Password(); ok && password != "" {
				text = strings.ReplaceAll(text, password, RedactedValue)
			}
		}
		for name, values := range req.URL.Query() {
			if _, ok := sensitiveQueryParams[strings.ToLower(name)]; !ok {
				continue
			}
			for _, value := range values {
				if value != "" {
					text = strings.ReplaceAll(text, value, RedactedValue)
				}
			}
		}
	}
	for name := range l.redacted {
		for _, value := range req.Header.Values(name) {
			if value != "" {
				text = strings.ReplaceAll(text, value, RedactedValue)
			}
		}
	}
	return text
}

// readBodyPrefix reads at most limit bytes from r and returns a reader that
// replays them followed by the rest. The returned rest always represents the
// unread portion, so the caller's reader observes exactly the original bytes.
func readBodyPrefix(r io.Reader, limit int64) (prefix []byte, rest io.Reader, truncated bool, err error) {
	if r == nil {
		return nil, nil, false, nil
	}
	if limit < 0 {
		return nil, r, false, nil
	}
	if limit == int64(^uint64(0)>>1) {
		// Avoid overflowing limit+1; no real body is this large.
		limit--
	}

	read, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, nil, false, err
	}
	if int64(len(read)) > limit {
		cut := int(limit)
		return read[:cut], io.MultiReader(bytes.NewReader(read[cut:]), r), true, nil
	}
	return read, r, false, nil
}

func encodeBody(body []byte) (text, encoding string) {
	if len(body) == 0 {
		return "", ""
	}
	if utf8.Valid(body) {
		return string(body), ""
	}
	return base64.StdEncoding.EncodeToString(body), "base64"
}

func durationMilliseconds(d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(d.Microseconds()) / 1000
}

// replayReadCloser lets a captured body be read again by the caller while the
// original body still owns Close.
type replayReadCloser struct {
	io.Reader
	io.Closer
}
