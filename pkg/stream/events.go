package stream

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
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
	// Watchlist, when set, supplies a live contract set and may contain more
	// addresses than the RPC's per-request limit.
	Watchlist ContractWatchlist
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

// InvocationFrame is one contract in a transaction's nested invocation trace.
type InvocationFrame struct {
	ContractID string
	Function   string
}

// ApplyInvocationTrace annotates an event from an ordered root-to-leaf call
// trace. A trace must come from transaction metadata that preserves invocation
// nesting; the getEvents RPC response itself does not currently include it.
func ApplyInvocationTrace(event *Event, trace []InvocationFrame) {
	if event == nil || len(trace) == 0 {
		return
	}
	leaf := -1
	for i, frame := range trace {
		if frame.ContractID == event.ContractID {
			leaf = i
		}
	}
	if leaf < 0 {
		return
	}
	event.CallDepth = leaf
	event.InvocationPath = make([]string, leaf+1)
	for i := 0; i <= leaf; i++ {
		event.InvocationPath[i] = trace[i].ContractID
	}
	if leaf > 0 {
		event.ParentContractID = trace[leaf-1].ContractID
	}
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

// Rewind implements Rewinder, repositioning a fresh fetch at ledger.
func (s *EventSource) Rewind(ledger uint32) { s.query.StartLedger = ledger }

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
	ids := append([]string(nil), s.query.ContractIDs...)
	if s.query.Watchlist != nil {
		ids = s.query.Watchlist.ContractIDs()
		if len(ids) == 0 {
			return Batch{Records: []Record{}, NextCursor: cursor}, nil
		}
	}
	groups := splitContractIDs(ids)
	initialCapacity := int(s.query.PageLimit)
	if s.query.PageLimit > DefaultPageLimit {
		initialCapacity = DefaultPageLimit
	}
	merged := Batch{Records: getRecordBuffer(initialCapacity)}
	seen := getEventIDSet()
	defer putEventIDSet(seen)
	for _, group := range groups {
		batch, err := s.fetchGroup(ctx, cursor, group)
		if err != nil {
			putRecordBuffer(merged.Records)
			return Batch{}, err
		}
		if batch.Latest > merged.Latest {
			merged.Latest = batch.Latest
			merged.LatestCloseTime = batch.LatestCloseTime
		}
		if merged.Oldest == 0 || (batch.Oldest > 0 && batch.Oldest < merged.Oldest) {
			merged.Oldest = batch.Oldest
		}
		merged.Full = merged.Full || batch.Full
		for _, record := range batch.Records {
			if _, ok := seen[record.ID]; ok {
				continue
			}
			seen[record.ID] = struct{}{}
			merged.Records = append(merged.Records, record)
		}
		if batch.NextCursor != "" && (merged.NextCursor == "" || batch.NextCursor < merged.NextCursor) {
			merged.NextCursor = batch.NextCursor
		}
	}
	sort.Slice(merged.Records, func(i, j int) bool { return merged.Records[i].ID < merged.Records[j].ID })
	if merged.NextCursor == "" {
		merged.NextCursor = cursor
	}
	return merged, nil
}

func (s *EventSource) releaseRecords(records []Record) {
	putRecordBuffer(records)
}

func splitContractIDs(ids []string) [][]string {
	if len(ids) == 0 {
		return [][]string{nil}
	}
	groups := make([][]string, 0, (len(ids)+MaxContractIDs-1)/MaxContractIDs)
	for len(ids) > 0 {
		n := len(ids)
		if n > MaxContractIDs {
			n = MaxContractIDs
		}
		groups = append(groups, append([]string(nil), ids[:n]...))
		ids = ids[n:]
	}
	return groups
}

func (s *EventSource) fetchGroup(ctx context.Context, cursor string, contractIDs []string) (Batch, error) {
	req := protocol.GetEventsRequest{
		Filters: s.filters(contractIDs),
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
		Records:         make([]Record, 0, len(resp.Events)),
		NextCursor:      resp.Cursor,
		Latest:          resp.LatestLedger,
		LatestCloseTime: unixCloseTime(resp.LatestLedgerCloseTime),
		Oldest:          resp.OldestLedger,
		Full:            uint(len(resp.Events)) >= s.query.PageLimit,
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
			EventIndex: eventIndex(e.ID),
			TopicXDR:   e.TopicXDR,
			ValueXDR:   e.ValueXDR,
		},
	}
}

func eventIndex(id string) uint32 {
	toid, event, ok := strings.Cut(id, "-")
	if !ok || strings.Contains(event, "-") {
		return 0
	}
	if _, err := strconv.ParseInt(toid, 10, 64); err != nil {
		return 0
	}
	index, err := strconv.ParseUint(event, 10, 32)
	if err != nil {
		return 0
	}
	return uint32(index)
}

// filters builds the single server-side filter prism uses.
//
// The RPC supports several filter objects, but combining contract IDs and
// topics into one filter matches what `stellar events` does and keeps the
// matching semantics identical between the two tools.
func (s *EventSource) filters(contractIDs []string) []protocol.EventFilter {
	f := protocol.EventFilter{}
	if len(contractIDs) > 0 {
		f.ContractIDs = contractIDs
	}
	if len(s.query.EventTypes) > 0 {
		set := protocol.EventTypeSet{}
		for _, t := range s.query.EventTypes {
			set[t] = nil
		}
		f.EventType = set
	}
	f.Topics = append(f.Topics, s.query.topics...)
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

// -----------------------------------------------------------------------------
// Zero-Copy & Object Pooling Optimizations for Event Extraction (Issue #346)
// -----------------------------------------------------------------------------

var (
	recordPool = sync.Pool{
		New: func() any {
			return &Record{
				Kind: KindEvent,
				Event: &Event{
					TopicXDR: make([]string, 0, 4),
				},
			}
		},
	}

	eventPool = sync.Pool{
		New: func() any {
			return &Event{
				TopicXDR: make([]string, 0, 4),
			}
		},
	}

	topicSlicePool = sync.Pool{
		New: func() any {
			s := make([]string, 0, 8)
			return &s
		},
	}
)

// GetRecord borrows a Record from the pool.
func GetRecord() *Record {
	r, ok := recordPool.Get().(*Record)
	if !ok {
		r = &Record{
			Kind:  KindEvent,
			Event: GetEvent(),
		}
		return r
	}
	if r.Event == nil {
		r.Event = GetEvent()
	}
	r.Kind = KindEvent
	return r
}

// PutRecord returns a Record and its nested Event to the pool.
func PutRecord(r *Record) {
	if r == nil {
		return
	}
	if r.Event != nil {
		PutEvent(r.Event)
		r.Event = nil
	}
	*r = Record{}
	recordPool.Put(r)
}

// GetEvent borrows an Event from the pool.
func GetEvent() *Event {
	e, ok := eventPool.Get().(*Event)
	if !ok {
		return &Event{
			TopicXDR: make([]string, 0, 4),
		}
	}
	e.TopicXDR = e.TopicXDR[:0]
	e.InvocationPath = nil
	return e
}

// PutEvent returns an Event to the pool.
func PutEvent(e *Event) {
	if e == nil {
		return
	}
	if cap(e.TopicXDR) > 64 {
		e.TopicXDR = make([]string, 0, 4)
	} else {
		e.TopicXDR = e.TopicXDR[:0]
	}
	e.Type = ""
	e.ContractID = ""
	e.TxHash = ""
	e.TxIndex = 0
	e.OpIndex = 0
	e.EventIndex = 0
	e.ValueXDR = ""
	e.CallDepth = 0
	e.ParentContractID = ""
	e.InvocationPath = nil
	eventPool.Put(e)
}

// GetTopicSlice borrows a string slice for topics from the pool.
func GetTopicSlice(capacity int) []string {
	ptr, ok := topicSlicePool.Get().(*[]string)
	if !ok {
		return make([]string, 0, capacity)
	}
	s := *ptr
	if cap(s) < capacity {
		s = make([]string, 0, capacity)
	} else {
		s = s[:0]
	}
	return s
}

// PutTopicSlice returns a string slice to the pool.
func PutTopicSlice(s []string) {
	if cap(s) > 64 {
		return
	}
	s = s[:0]
	topicSlicePool.Put(&s)
}

// EventPageArena provides a zero-copy memory arena for extracting events
// across a ledger or page with bounded lifetime.
type EventPageArena struct {
	buf []byte
	enc *xdr.EncodingBuffer
}

// NewEventPageArena creates a new EventPageArena instance.
func NewEventPageArena() *EventPageArena {
	return &EventPageArena{
		buf: make([]byte, 0, 64*1024),
		enc: xdr.NewEncodingBuffer(),
	}
}

var eventPageArenaPool = sync.Pool{
	New: func() any {
		return NewEventPageArena()
	},
}

// Reset clears the arena buffer for reuse.
func (a *EventPageArena) Reset() {
	a.buf = a.buf[:0]
}

// EncodeScValBase64 encodes an ScVal into base64 directly inside the arena buffer,
// returning a zero-copy string reference.
func (a *EventPageArena) EncodeScValBase64(val xdr.EncoderTo) (string, error) {
	if val == nil {
		return "", nil
	}
	raw, err := a.enc.UnsafeMarshalBinary(val)
	if err != nil {
		return "", err
	}
	encLen := base64.StdEncoding.EncodedLen(len(raw))
	start := len(a.buf)
	if cap(a.buf)-start < encLen {
		newCap := cap(a.buf) * 2
		if newCap < start+encLen {
			newCap = start + encLen + 1024
		}
		newBuf := make([]byte, start, newCap)
		copy(newBuf, a.buf)
		a.buf = newBuf
	}
	a.buf = a.buf[:start+encLen]
	base64.StdEncoding.Encode(a.buf[start:], raw)
	return unsafe.String(&a.buf[start], encLen), nil
}

// FormatID formats a record ID directly into the arena buffer.
func (a *EventPageArena) FormatID(seq uint32, txIdx, evtIdx int) string {
	start := len(a.buf)
	needed := 24 // "0001234567-000000-000000"
	if cap(a.buf)-start < needed {
		a.buf = append(a.buf, make([]byte, needed)...)
		a.buf = a.buf[:start]
	}
	formatted := fmt.Sprintf("%010d-%06d-%06d", seq, txIdx, evtIdx)
	a.buf = append(a.buf, formatted...)
	return unsafe.String(&a.buf[start], len(formatted))
}

// FormatTxHash formats a transaction hash hex directly into the arena buffer.
func (a *EventPageArena) FormatTxHash(h xdr.Hash) string {
	start := len(a.buf)
	needed := hex.EncodedLen(len(h))
	if cap(a.buf)-start < needed {
		a.buf = append(a.buf, make([]byte, needed)...)
		a.buf = a.buf[:start]
	}
	curLen := len(a.buf)
	a.buf = a.buf[:curLen+needed]
	hex.Encode(a.buf[curLen:], h[:])
	return unsafe.String(&a.buf[start], needed)
}

// EncodeScValBase64ZeroCopy serializes an ScVal to base64 using pooled buffers
// and zero-copy byte-to-string slicing where lifetime is bounded by page processing.
func EncodeScValBase64ZeroCopy(val *xdr.ScVal) (string, error) {
	if val == nil {
		return "", nil
	}
	arena, ok := eventPageArenaPool.Get().(*EventPageArena)
	if !ok {
		arena = NewEventPageArena()
	}
	arena.Reset()
	defer eventPageArenaPool.Put(arena)
	return arena.EncodeScValBase64(val)
}

// ExtractEventsFromLedgerCloseMeta extracts Soroban events from LedgerCloseMeta
// using zero-copy byte slicing and sync.Pool optimizations.
func ExtractEventsFromLedgerCloseMeta(lcm *xdr.LedgerCloseMeta) []Record {
	if lcm == nil {
		return nil
	}

	seq := lcm.LedgerSequence()
	closeTime := lcm.ClosedAt()
	txCount := lcm.CountTransactions()

	arena, ok := eventPageArenaPool.Get().(*EventPageArena)
	if !ok {
		arena = NewEventPageArena()
	}
	arena.Reset()
	defer eventPageArenaPool.Put(arena)

	records := make([]Record, 0, txCount*4)

	for txIdx := 0; txIdx < txCount; txIdx++ {
		txHash := arena.FormatTxHash(lcm.TransactionHash(txIdx))
		meta := lcm.TxApplyProcessing(txIdx)

		var events []xdr.ContractEvent
		if meta.V == 3 && meta.V3 != nil && meta.V3.SorobanMeta != nil {
			events = meta.V3.SorobanMeta.Events
		} else if meta.V == 4 && meta.V4 != nil {
			for _, ce := range meta.V4.Events {
				events = append(events, ce.Event)
			}
		}

		for evtIdx, evt := range events {
			var contractID string
			if evt.ContractId != nil {
				if id, err := strkey.Encode(strkey.VersionByteContract, evt.ContractId[:]); err == nil {
					contractID = id
				}
			}

			var eventType string
			switch evt.Type {
			case xdr.ContractEventTypeContract:
				eventType = protocol.EventTypeContract
			case xdr.ContractEventTypeSystem:
				eventType = protocol.EventTypeSystem
			case xdr.ContractEventTypeDiagnostic:
				eventType = protocol.EventTypeDiagnostic
			default:
				eventType = "unknown"
			}

			topicCount := 0
			if evt.Body.V0 != nil {
				topicCount = len(evt.Body.V0.Topics)
			}

			topics := GetTopicSlice(topicCount)
			if evt.Body.V0 != nil {
				for i := range evt.Body.V0.Topics {
					topicStr, err := arena.EncodeScValBase64(&evt.Body.V0.Topics[i])
					if err == nil {
						topics = append(topics, topicStr)
					}
				}
			}

			var valueStr string
			if evt.Body.V0 != nil {
				if valStr, err := arena.EncodeScValBase64(&evt.Body.V0.Data); err == nil {
					valueStr = valStr
				}
			}

			id := arena.FormatID(seq, txIdx, evtIdx)
			r := Record{
				ID:       id,
				Sequence: seq,
				Time:     closeTime,
				Kind:     KindEvent,
				Event: &Event{
					Type:       eventType,
					ContractID: contractID,
					TxHash:     txHash,
					TxIndex:    uint32(txIdx),
					OpIndex:    0,
					EventIndex: uint32(evtIdx),
					TopicXDR:   topics,
					ValueXDR:   valueStr,
				},
			}
			records = append(records, r)
		}
	}

	return records
}

// ExtractEventsFromLedgerCloseMetaPooled extracts events returning pooled *Record instances.
// Callers can process records within page lifetime and call ReleaseEvents when finished.
func ExtractEventsFromLedgerCloseMetaPooled(lcm *xdr.LedgerCloseMeta) []*Record {
	if lcm == nil {
		return nil
	}

	seq := lcm.LedgerSequence()
	closeTime := lcm.ClosedAt()
	txCount := lcm.CountTransactions()

	arena, ok := eventPageArenaPool.Get().(*EventPageArena)
	if !ok {
		arena = NewEventPageArena()
	}
	arena.Reset()

	records := make([]*Record, 0, txCount*4)

	for txIdx := 0; txIdx < txCount; txIdx++ {
		txHash := arena.FormatTxHash(lcm.TransactionHash(txIdx))
		meta := lcm.TxApplyProcessing(txIdx)

		var events []xdr.ContractEvent
		if meta.V == 3 && meta.V3 != nil && meta.V3.SorobanMeta != nil {
			events = meta.V3.SorobanMeta.Events
		} else if meta.V == 4 && meta.V4 != nil {
			for _, ce := range meta.V4.Events {
				events = append(events, ce.Event)
			}
		}

		for evtIdx, evt := range events {
			var contractID string
			if evt.ContractId != nil {
				if id, err := strkey.Encode(strkey.VersionByteContract, evt.ContractId[:]); err == nil {
					contractID = id
				}
			}

			var eventType string
			switch evt.Type {
			case xdr.ContractEventTypeContract:
				eventType = protocol.EventTypeContract
			case xdr.ContractEventTypeSystem:
				eventType = protocol.EventTypeSystem
			case xdr.ContractEventTypeDiagnostic:
				eventType = protocol.EventTypeDiagnostic
			default:
				eventType = "unknown"
			}

			topicCount := 0
			if evt.Body.V0 != nil {
				topicCount = len(evt.Body.V0.Topics)
			}

			topics := GetTopicSlice(topicCount)
			if evt.Body.V0 != nil {
				for i := range evt.Body.V0.Topics {
					topicStr, err := arena.EncodeScValBase64(&evt.Body.V0.Topics[i])
					if err == nil {
						topics = append(topics, topicStr)
					}
				}
			}

			var valueStr string
			if evt.Body.V0 != nil {
				if valStr, err := arena.EncodeScValBase64(&evt.Body.V0.Data); err == nil {
					valueStr = valStr
				}
			}

			r := GetRecord()
			r.ID = arena.FormatID(seq, txIdx, evtIdx)
			r.Sequence = seq
			r.Time = closeTime
			r.Kind = KindEvent
			r.Event.Type = eventType
			r.Event.ContractID = contractID
			r.Event.TxHash = txHash
			r.Event.TxIndex = uint32(txIdx)
			r.Event.OpIndex = 0
			r.Event.EventIndex = uint32(evtIdx)
			r.Event.TopicXDR = topics
			r.Event.ValueXDR = valueStr

			records = append(records, r)
		}
	}

	eventPageArenaPool.Put(arena)
	return records
}

// ReleaseEvents returns a batch of pooled *Record instances back to the pool.
func ReleaseEvents(records []*Record) {
	for _, r := range records {
		if r != nil {
			if r.Event != nil && len(r.Event.TopicXDR) > 0 {
				PutTopicSlice(r.Event.TopicXDR)
				r.Event.TopicXDR = nil
			}
			PutRecord(r)
		}
	}
}

// ExtractEventsFromLedgerCloseMetaStandard extracts events using standard unoptimized allocations,
// used as a baseline to benchmark the zero-copy and pooling improvements.
func ExtractEventsFromLedgerCloseMetaStandard(lcm *xdr.LedgerCloseMeta) []Record {
	if lcm == nil {
		return nil
	}

	seq := lcm.LedgerSequence()
	closeTime := lcm.ClosedAt()
	txCount := lcm.CountTransactions()

	var records []Record

	for txIdx := 0; txIdx < txCount; txIdx++ {
		txHashBytes := lcm.TransactionHash(txIdx)
		txHash := hex.EncodeToString(txHashBytes[:])
		meta := lcm.TxApplyProcessing(txIdx)

		var events []xdr.ContractEvent
		if meta.V == 3 && meta.V3 != nil && meta.V3.SorobanMeta != nil {
			events = meta.V3.SorobanMeta.Events
		} else if meta.V == 4 && meta.V4 != nil {
			for _, ce := range meta.V4.Events {
				events = append(events, ce.Event)
			}
		}

		for evtIdx, evt := range events {
			var contractID string
			if evt.ContractId != nil {
				if id, err := strkey.Encode(strkey.VersionByteContract, evt.ContractId[:]); err == nil {
					contractID = id
				}
			}

			var eventType string
			switch evt.Type {
			case xdr.ContractEventTypeContract:
				eventType = protocol.EventTypeContract
			case xdr.ContractEventTypeSystem:
				eventType = protocol.EventTypeSystem
			case xdr.ContractEventTypeDiagnostic:
				eventType = protocol.EventTypeDiagnostic
			default:
				eventType = "unknown"
			}

			var topics []string
			if evt.Body.V0 != nil {
				topics = make([]string, 0, len(evt.Body.V0.Topics))
				for _, t := range evt.Body.V0.Topics {
					if s, err := xdr.MarshalBase64(t); err == nil {
						topics = append(topics, s)
					}
				}
			}

			var valueStr string
			if evt.Body.V0 != nil {
				if valStr, err := xdr.MarshalBase64(evt.Body.V0.Data); err == nil {
					valueStr = valStr
				}
			}

			r := Record{
				ID:       fmt.Sprintf("%010d-%06d-%06d", seq, txIdx, evtIdx),
				Sequence: seq,
				Time:     closeTime,
				Kind:     KindEvent,
				Event: &Event{
					Type:       eventType,
					ContractID: contractID,
					TxHash:     txHash,
					TxIndex:    uint32(txIdx),
					OpIndex:    0,
					EventIndex: uint32(evtIdx),
					TopicXDR:   topics,
					ValueXDR:   valueStr,
				},
			}
			records = append(records, r)
		}
	}

	return records
}
