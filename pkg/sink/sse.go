package sink

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// SSE broadcasts NDJSON records at /events. Replay retains the latest 1024
// records in memory; an unknown/expired Last-Event-ID receives HTTP 409. Replay
// does not survive process restarts. Slow clients are disconnected and may replay.
//
// See docs/tutorials/web_streaming_sse_websocket.md for browser examples and
// the supported filtering and resume behavior.
type SSE struct {
	hub          *broadcastHub
	pingInterval time.Duration
}

// NewSSE constructs an SSE sink with bounded replay and 15-second keepalives.
func NewSSE() *SSE { return &SSE{hub: newBroadcastHub(1024), pingInterval: 15 * time.Second} }

// Emit implements stream.Sink.
func (s *SSE) Emit(r stream.Record) error {
	if strings.ContainsAny(r.ID, "\r\n\x00") || r.ID == "" {
		return fmt.Errorf("sse: invalid event ID")
	}
	return s.hub.emit(r)
}

// Flush implements stream.Sink; records are queued immediately.
func (s *SSE) Flush() error { return nil }

// Close disconnects clients and rejects future writes.
func (s *SSE) Close() error { return s.hub.close() }

// ServeHTTP implements http.Handler.
func (s *SSE) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/events" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if _, ok := w.(http.Flusher); !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	c, err := s.hub.subscribe(r.Header.Get("Last-Event-ID"), nil)
	if err != nil {
		code := http.StatusServiceUnavailable
		if errors.Is(err, errReplayExpired) {
			code = http.StatusConflict
		}
		http.Error(w, err.Error(), code)
		return
	}
	defer s.hub.remove(c)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	rc := http.NewResponseController(w)
	write := func(data string) error {
		if err := rc.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return err
		}
		if _, err := fmt.Fprint(w, data); err != nil {
			return err
		}
		return rc.Flush()
	}
	if err := write(": connected\n\n"); err != nil {
		return
	}
	ticker := time.NewTicker(s.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case m, ok := <-c.queue:
			if !ok {
				return
			}
			if err := write("id: " + m.id + "\ndata: " + string(m.data) + "\n\n"); err != nil {
				return
			}
		case <-ticker.C:
			if err := write(": ping\n\n"); err != nil {
				return
			}
		}
	}
}
