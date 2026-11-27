package sink

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// WebSocket broadcasts raw NDJSON-shaped records at /events (also /ws).
// Connections initially receive all events. The first subscription restricts
// that feed. Contract and topic subscriptions are ORed within a category and
// ANDed across categories. Topics match an exact base64 XDR topic segment.
// Same-origin browser requests are required by the standard upgrader.
//
// See docs/tutorials/web_streaming_sse_websocket.md for the subscription
// protocol and browser examples.
type WebSocket struct {
	hub         *broadcastHub
	mu          sync.Mutex
	connections map[*websocket.Conn]struct{}
	closed      bool
}

// NewWebSocket constructs a bounded, concurrent WebSocket sink.
func NewWebSocket() *WebSocket {
	return &WebSocket{hub: newBroadcastHub(0), connections: make(map[*websocket.Conn]struct{})}
}

// Emit implements stream.Sink.
func (s *WebSocket) Emit(r stream.Record) error { return s.hub.emit(r) }

// Flush implements stream.Sink.
func (s *WebSocket) Flush() error { return nil }

// Close sends a going-away control frame and releases every client connection.
func (s *WebSocket) Close() error {
	s.mu.Lock()
	s.closed = true
	deadline := time.Now().Add(time.Second)
	for c := range s.connections {
		if err := c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "server shutdown"), deadline); err != nil {
			// A disconnected peer still needs its transport released.
			_ = c.Close()
			continue
		}
		_ = c.Close()
	}
	s.mu.Unlock()
	return s.hub.close()
}

type subscriptions struct {
	mu                sync.RWMutex
	contracts, topics map[string]bool
}

func (s *subscriptions) match(r stream.Record) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.contracts) == 0 && len(s.topics) == 0 {
		return true
	}
	if r.Event == nil {
		return false
	}
	if len(s.contracts) > 0 && !s.contracts[r.Event.ContractID] {
		return false
	}
	if len(s.topics) > 0 {
		for _, t := range r.Event.TopicXDR {
			if s.topics[t] {
				return true
			}
		}
		return false
	}
	return true
}

// ServeHTTP upgrades a client and handles dynamic subscription messages.
func (s *WebSocket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/events" && r.URL.Path != "/ws" {
		http.NotFound(w, r)
		return
	}
	u := websocket.Upgrader{HandshakeTimeout: 5 * time.Second}
	conn, err := u.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.connections[conn] = struct{}{}
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.connections, conn); s.mu.Unlock() }()
	subs := &subscriptions{contracts: map[string]bool{}, topics: map[string]bool{}}
	c, err := s.hub.subscribe("", subs.match)
	if err != nil {
		return
	}
	defer s.hub.remove(c)
	done := make(chan struct{})
	replies := make(chan []byte, 16)
	conn.SetReadLimit(4096)
	if err := conn.SetReadDeadline(time.Now().Add(45 * time.Second)); err != nil {
		return
	}
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(45 * time.Second)) })
	go func() {
		defer close(done)
		for {
			var req struct {
				Subscribe string `json:"subscribe"`
				ID        string `json:"id"`
			}
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			reply := map[string]string{"subscribed": req.Subscribe, "id": req.ID}
			if req.ID == "" || (req.Subscribe != "contract" && req.Subscribe != "topic") {
				reply = map[string]string{"error": "expected subscribe=contract|topic and a nonempty id"}
			} else {
				subs.mu.Lock()
				if len(subs.contracts)+len(subs.topics) >= 128 {
					reply = map[string]string{"error": "subscription limit reached"}
				} else if req.Subscribe == "contract" {
					subs.contracts[req.ID] = true
				} else {
					subs.topics[req.ID] = true
				}
				subs.mu.Unlock()
			}
			data, err := json.Marshal(reply)
			if err != nil {
				return
			}
			select {
			case replies <- data:
			case <-r.Context().Done():
				return
			default:
				return
			}
		}
	}()
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	write := func(data []byte) error {
		if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
			return err
		}
		return conn.WriteMessage(websocket.TextMessage, data)
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case <-done:
			return
		case data := <-replies:
			if err := write(data); err != nil {
				return
			}
		case m, ok := <-c.queue:
			if !ok {
				return
			}
			if subs.match(m.record) {
				if err := write(m.data); err != nil {
					return
				}
			}
		case <-ping.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
				return
			}
		}
	}
}
