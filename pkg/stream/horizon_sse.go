package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DefaultHorizonSSEPageLimit is the page size requested for Horizon SSE streams.
const DefaultHorizonSSEPageLimit = 50

// HorizonSSEQuery describes which Horizon SSE events to stream.
type HorizonSSEQuery struct {
	// HorizonURL is the Horizon server URL to connect to.
	HorizonURL string
	// EventTypes specifies which event types to subscribe to.
	// Supported: "payment", "operation", "fee_bump"
	EventTypes []string
	// StartLedger is where a fresh stream begins. Zero means start from the
	// server's newest ledger.
	StartLedger uint32
	// PageLimit is the page size to request per fetch cycle.
	PageLimit uint
	// ReconnectAfter specifies how long to wait before reconnecting after a
	// connection loss. Defaults to 30 seconds.
	ReconnectAfter time.Duration
}

// HorizonSSESource is a push-based event source using Horizon Server-Sent Events.
// This provides a secondary stream of classic Stellar transactions and operations
// via Horizon's SSE endpoint, complementing the Soroban RPC event streams.
type HorizonSSESource struct {
	client       *http.Client
	horizonURL   string
	eventTypes   []string
	startLedger  uint32
	pageLimit    uint
	reconnect    time.Duration
	lastSequence uint32
	connected    bool
}

// NewHorizonSSESource returns a Source over the given Horizon URL.
func NewHorizonSSESource(client *http.Client, q HorizonSSEQuery) *HorizonSSESource {
	if client == nil {
		client = http.DefaultClient
	}
	if q.PageLimit == 0 {
		q.PageLimit = DefaultHorizonSSEPageLimit
	}
	if q.ReconnectAfter == 0 {
		q.ReconnectAfter = 30 * time.Second
	}

	return &HorizonSSESource{
		client:      client,
		horizonURL:  strings.TrimRight(q.HorizonURL, "/"),
		eventTypes:  q.EventTypes,
		startLedger: q.StartLedger,
		pageLimit:   q.PageLimit,
		reconnect:   q.ReconnectAfter,
	}
}

// Name implements Source.
func (s *HorizonSSESource) Name() string { return "horizon-sse" }

// Rewind implements Rewinder, repositioning a fresh fetch at ledger.
func (s *HorizonSSESource) Rewind(ledger uint32) {
	s.startLedger = ledger
	s.lastSequence = ledger
	s.connected = false
}

// CursorLedger implements Source by parsing the Horizon SSE cursor format.
func (s *HorizonSSESource) CursorLedger(cursor string) (uint32, bool) {
	if cursor == "" {
		return 0, false
	}
	// Horizon SSE cursors are in the format "ledger-sequence" or just a ledger sequence
	parts := strings.Split(cursor, "-")
	ledger, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(ledger), true
}

// Fetch implements Source - for Horizon SSE, this initiates the SSE connection
// and returns the initial batch of events.
func (s *HorizonSSESource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	endpoint := s.horizonURL + "/events"

	params := url.Values{}
	if cursor != "" {
		params.Set("cursor", cursor)
	} else if s.startLedger > 0 {
		params.Set("start_ledger", fmt.Sprintf("%d", s.startLedger))
	}
	if s.pageLimit > 0 {
		params.Set("limit", fmt.Sprintf("%d", s.pageLimit))
	}
	for _, et := range s.eventTypes {
		params.Add("types", et)
	}

	fullURL := endpoint
	if len(params) > 0 {
		fullURL += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return Batch{}, fmt.Errorf("creating SSE request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := s.client.Do(req)
	if err != nil {
		return Batch{}, fmt.Errorf("SSE connection error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Batch{}, fmt.Errorf("SSE server returned status %d", resp.StatusCode)
	}

	batch := Batch{
		Records: make([]Record, 0),
		Latest:  s.startLedger,
		Oldest:  s.startLedger,
		Full:    false,
	}

	decoder := NewSSEDecoder(resp.Body)
	for uint(len(batch.Records)) < s.pageLimit {
		event, readErr := decoder.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			break
		}

		record := parseHorizonSSEEvent(event)
		if record != nil {
			batch.Records = append(batch.Records, *record)
			batch.NextCursor = event.ID
			if record.Sequence > s.lastSequence {
				s.lastSequence = record.Sequence
			}
		}
	}

	sort.Slice(batch.Records, func(i, j int) bool {
		return batch.Records[i].ID < batch.Records[j].ID
	})

	return batch, nil
}

// SSEEvent represents a single Server-Sent Event.
type SSEEvent struct {
	ID    string
	Data  string
	Event string
	Retry *int
}

// SSEDecoder reads Server-Sent Events from an io.ReadCloser.
type SSEDecoder struct {
	reader io.ReadCloser
	buffer []byte
	closed bool
}

// NewSSEDecoder creates a new SSE decoder from an io.ReadCloser.
func NewSSEDecoder(r io.ReadCloser) *SSEDecoder {
	return &SSEDecoder{reader: r}
}

// Read reads the next SSE event from the stream.
func (d *SSEDecoder) Read() (*SSEEvent, error) {
	if d.closed {
		return nil, io.EOF
	}

	for {
		if idx := strings.Index(string(d.buffer), "\n\n"); idx >= 0 {
			eventBuf := string(d.buffer[:idx])
			d.buffer = d.buffer[idx+2:]
			if event, ok := d.parseEvent(eventBuf); ok {
				return event, nil
			}
			continue
		}

		b := make([]byte, 4096)
		n, err := d.reader.Read(b)
		if n > 0 {
			d.buffer = append(d.buffer, b[:n]...)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				d.closed = true
				if len(d.buffer) > 0 {
					event, ok := d.parseEvent(string(d.buffer))
					d.buffer = nil
					if ok {
						return event, nil
					}
				}
				return nil, io.EOF
			}
			return nil, err
		}
	}
}

func (d *SSEDecoder) parseEvent(buf string) (*SSEEvent, bool) {
	event := &SSEEvent{}
	lines := strings.Split(buf, "\n")
	var dataLines []string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		key := strings.TrimSpace(parts[0])
		var val string
		if len(parts) == 2 {
			val = strings.TrimSpace(parts[1])
		}

		switch key {
		case "id":
			event.ID = val
		case "event":
			event.Event = val
		case "data":
			dataLines = append(dataLines, val)
		case "retry":
			if n, err := strconv.Atoi(val); err == nil {
				event.Retry = &n
			}
		}
	}

	event.Data = strings.Join(dataLines, "\n")
	if event.ID == "" && event.Data == "" {
		return nil, false
	}
	return event, true
}

func parseHorizonSSEEvent(event *SSEEvent) *Record {
	if event.ID == "" && event.Data == "" {
		return nil
	}

	var evt map[string]interface{}
	if err := json.Unmarshal([]byte(event.Data), &evt); err != nil {
		return nil
	}

	var recordID64 uint64
	if id, ok := evt["id"].(float64); ok {
		recordID64 = uint64(id)
	} else if idStr, ok := evt["id"].(string); ok {
		if n, err := strconv.ParseUint(idStr, 10, 64); err == nil {
			recordID64 = n
		}
	}

	var ledgerSeq uint64
	if lp, ok := evt["ledger_sequence"].(float64); ok {
		ledgerSeq = uint64(lp)
	} else if lp, ok := evt["lp"].(float64); ok {
		ledgerSeq = uint64(lp)
	} else if lpStr, ok := evt["lp"].(string); ok {
		if n, err := strconv.ParseUint(lpStr, 10, 64); err == nil {
			ledgerSeq = n
		}
	}

	var txHash string
	if tx, ok := evt["transaction_hash"].(string); ok {
		txHash = tx
	} else if tx, ok := evt["tx"].(string); ok {
		txHash = tx
	}

	var closed time.Time
	if createdAt, ok := evt["created_at"].(string); ok {
		if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
			closed = t.UTC()
		}
	} else if tc, ok := evt["tc"].(float64); ok && tc > 0 {
		closed = time.Unix(int64(tc), 0).UTC()
	}

	id := event.ID
	if id == "" {
		id = fmt.Sprintf("%010d-%010d", ledgerSeq, recordID64)
	}

	return &Record{
		ID:       id,
		Sequence: uint32(ledgerSeq),
		Time:     closed,
		Kind:     KindTransaction,
		Transaction: &Transaction{
			Hash: txHash,
		},
	}
}
