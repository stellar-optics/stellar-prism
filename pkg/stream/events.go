package stream

import (
	"context"
	"fmt"
	"strings"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-xdr-lens/pkg/lens"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// MaxContractIDs, MaxTopicFilters and MaxTopicSegments mirror the server's
// documented limits. They are enforced here so an over-long filter fails with
// a clear local message instead of a generic RPC rejection.
const (
	MaxContractIDs   = protocol.MaxContractIDsLimit
	MaxTopicFilters  = protocol.MaxTopicsLimit
	MaxTopicSegments = protocol.MaxTopicCount
)

// DefaultPageLimit is the page size requested from the server. Larger pages
// mean fewer round trips when backfilling.
const DefaultPageLimit = 200

// EventQuery describes which events to stream.
type EventQuery struct {
	// ContractIDs restricts the stream to these contracts. At most
	// MaxContractIDs are allowed by the server.
	ContractIDs []string
	// Topics are server-side topic filters. Each is a comma-separated list of
	// base64 ScVal segments, where "*" matches exactly one segment and "**"
	// matches zero or more and must come last.
	Topics []string
	// EventTypes restricts to "contract", "system" or "diagnostic". Empty
	// means all types.
	EventTypes []string
	// StartLedger is where a fresh stream begins. Zero means start from the
	// server's newest ledger.
	StartLedger uint32
	// PageLimit is the page size to request.
	PageLimit uint

	// topics holds the parsed form of Topics, built once by NewEventSource so
	// that a malformed filter fails at construction rather than on every poll.
	topics []protocol.TopicFilter
}

// Validate checks the query against the server's documented limits so that
// mistakes surface before a request is made.
func (q EventQuery) Validate() error {
	if len(q.ContractIDs) > MaxContractIDs {
		return fmt.Errorf("at most %d contract IDs may be given, got %d", MaxContractIDs, len(q.ContractIDs))
	}
	if len(q.Topics) > MaxTopicFilters {
		return fmt.Errorf("at most %d topic filters may be given, got %d", MaxTopicFilters, len(q.Topics))
	}
	for _, t := range q.Topics {
		segs := strings.Split(t, ",")
		if len(segs) == 0 || t == "" {
			return fmt.Errorf("empty topic filter")
		}
		if len(segs) > MaxTopicSegments+1 { // +1 allows a trailing "**"
			return fmt.Errorf(
				"topic filter %q has %d segments; at most %d are allowed (plus a trailing \"**\")",
				t, len(segs), MaxTopicSegments)
		}
		for i, s := range segs {
			if s == protocol.WildCardZeroOrMore && i != len(segs)-1 {
				return fmt.Errorf("in topic filter %q, %q must be the last segment", t, protocol.WildCardZeroOrMore)
			}
		}
	}
	for _, et := range q.EventTypes {
		switch et {
		case protocol.EventTypeContract, protocol.EventTypeSystem, protocol.EventTypeDiagnostic:
		default:
			return fmt.Errorf(
				"unknown event type %q; expected one of contract, system, diagnostic", et)
		}
	}
	return nil
}

// EventSource streams Soroban contract events from RPC.
type EventSource struct {
	client rpc.Client
	query  EventQuery
}

// NewEventSource returns a Source over the given RPC client.
func NewEventSource(client rpc.Client, q EventQuery) (*EventSource, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if q.PageLimit == 0 {
		q.PageLimit = DefaultPageLimit
	}
	for _, t := range q.Topics {
		parsed, err := topicSegments(t)
		if err != nil {
			return nil, err
		}
		q.topics = append(q.topics, parsed)
	}
	return &EventSource{client: client, query: q}, nil
}

// Name implements Source.
func (s *EventSource) Name() string { return "events" }

// CursorLedger implements Source by parsing the RPC cursor format.
func (s *EventSource) CursorLedger(cursor string) (uint32, bool) {
	if cursor == "" {
		return 0, false
	}
	c, err := protocol.ParseCursor(cursor)
	if err != nil {
		return 0, false
	}
	return c.Ledger, true
}

// Fetch implements Source.
//
// The RPC rejects a request carrying both a ledger range and a cursor, so the
// first call uses StartLedger and every later one uses the cursor alone.
func (s *EventSource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	req := protocol.GetEventsRequest{
		Filters: s.filters(),
		Pagination: &protocol.PaginationOptions{
			Limit: s.query.PageLimit,
		},
	}

	if cursor == "" {
		req.StartLedger = s.query.StartLedger
	} else {
		parsed, err := protocol.ParseCursor(cursor)
		if err != nil {
			return Batch{}, rpc.Permanent(fmt.Errorf("invalid cursor %q: %w", cursor, err))
		}
		req.Pagination.Cursor = &parsed
	}

	resp, err := s.client.GetEvents(ctx, req)
	if err != nil {
		return Batch{}, err
	}

	batch := Batch{
		Records:    make([]Record, 0, len(resp.Events)),
		NextCursor: resp.Cursor,
		Latest:     resp.LatestLedger,
		Oldest:     resp.OldestLedger,
		Full:       uint(len(resp.Events)) >= s.query.PageLimit,
	}
	for _, e := range resp.Events {
		batch.Records = append(batch.Records, eventRecord(e))
	}
	return batch, nil
}

// eventRecord converts an RPC event into a neutral Record.
func eventRecord(e protocol.EventInfo) Record {
	var closed time.Time
	if e.LedgerClosedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, e.LedgerClosedAt); err == nil {
			closed = parsed.UTC()
		}
	}
	return Record{
		ID:       e.ID,
		Sequence: uint32(e.Ledger), //nolint:gosec // ledger sequences are non-negative
		Time:     closed,
		Kind:     KindEvent,
		Event: &Event{
			Type:       e.EventType,
			ContractID: e.ContractID,
			TxHash:     e.TransactionHash,
			TxIndex:    e.TxIndex,
			OpIndex:    e.OpIndex,
			TopicXDR:   e.TopicXDR,
			ValueXDR:   e.ValueXDR,
		},
	}
}

// filters builds the single server-side filter prism uses.
//
// The RPC supports several filter objects, but combining contract IDs and
// topics into one filter matches what `stellar events` does and keeps the
// matching semantics identical between the two tools.
func (s *EventSource) filters() []protocol.EventFilter {
	f := protocol.EventFilter{}
	if len(s.query.ContractIDs) > 0 {
		f.ContractIDs = s.query.ContractIDs
	}
	if len(s.query.EventTypes) > 0 {
		set := protocol.EventTypeSet{}
		for _, t := range s.query.EventTypes {
			set[t] = nil
		}
		f.EventType = set
	}
	for _, t := range s.query.topics {
		f.Topics = append(f.Topics, t)
	}
	if len(f.ContractIDs) == 0 && len(f.Topics) == 0 && len(f.EventType) == 0 {
		return []protocol.EventFilter{}
	}
	return []protocol.EventFilter{f}
}

// topicSegments parses one comma-separated topic filter into the server's
// segment representation.
//
// Non-wildcard segments are base64 ScVals, decoded through lens so that an
// invalid segment produces the same error wording a user would see from
// `lens decode`, and so a typo is caught locally rather than as a generic
// server rejection.
func topicSegments(filter string) (protocol.TopicFilter, error) {
	parts := strings.Split(filter, ",")
	segs := make(protocol.TopicFilter, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("topic filter %q has an empty segment", filter)
		}

		if part == protocol.WildCardExactOne || part == protocol.WildCardZeroOrMore {
			wildcard := part
			segs = append(segs, protocol.SegmentFilter{Wildcard: &wildcard})
			continue
		}

		value, err := lens.DecodeAs(part, "ScVal")
		if err != nil {
			return nil, fmt.Errorf("topic segment %q is not a base64 ScVal: %w", part, err)
		}
		scv, ok := value.Raw.(*xdr.ScVal)
		if !ok {
			return nil, fmt.Errorf("topic segment %q did not decode to an ScVal", part)
		}
		segs = append(segs, protocol.SegmentFilter{ScVal: scv})
	}
	return segs, nil
}
