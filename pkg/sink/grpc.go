package sink

import (
	"context"
	"fmt"
	"sync"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
	prismpb "github.com/stellar-optics/stellar-prism/proto"
)

// GRPCSink is both a stream sink and the generated PrismStream service. Each
// subscriber has a bounded queue; a slow disconnected client is never allowed
// to stall the tailer.
type GRPCSink struct {
	prismpb.UnimplementedPrismStreamServer
	prismpb.UnimplementedEventStreamServiceServer
	mu      sync.Mutex
	next    uint64
	events  map[uint64]chan stream.Record
	ledgers map[uint64]chan stream.Record
}

func NewGRPCSink() *GRPCSink {
	return &GRPCSink{events: map[uint64]chan stream.Record{}, ledgers: map[uint64]chan stream.Record{}}
}
func (s *GRPCSink) Emit(r stream.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var clients map[uint64]chan stream.Record
	switch r.Kind {
	case stream.KindEvent:
		clients = s.events
	case stream.KindLedger:
		clients = s.ledgers
	default:
		return nil
	}
	for _, ch := range clients {
		select {
		case ch <- r:
		default:
		}
	}
	return nil
}
func (*GRPCSink) Flush() error { return nil }
func (s *GRPCSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, set := range []map[uint64]chan stream.Record{s.events, s.ledgers} {
		for id, ch := range set {
			close(ch)
			delete(set, id)
		}
	}
	return nil
}
func (s *GRPCSink) subscribe(set map[uint64]chan stream.Record) (uint64, <-chan stream.Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.next
	s.next++
	ch := make(chan stream.Record, 128)
	set[id] = ch
	return id, ch
}
func (s *GRPCSink) remove(set map[uint64]chan stream.Record, id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch := set[id]; ch != nil {
		delete(set, id)
		close(ch)
	}
}
func (s *GRPCSink) StreamEvents(req *prismpb.EventRequest, out prismpb.PrismStream_StreamEventsServer) error {
	id, ch := s.subscribe(s.events)
	defer s.remove(s.events, id)
	for {
		select {
		case <-out.Context().Done():
			return out.Context().Err()
		case r, ok := <-ch:
			if !ok {
				return nil
			}
			if req.GetContractId() != "" && (r.Event == nil || r.Event.ContractID != req.GetContractId()) {
				continue
			}
			b, e := MarshalRecord(r, false)
			if e != nil {
				return e
			}
			if e = out.Send(&prismpb.EventResponse{Id: r.ID, Ledger: r.Sequence, ClosedAtUnixMs: r.Time.UnixMilli(), RecordJson: b}); e != nil {
				return e
			}
		}
	}
}

// Subscribe is the issue-named alias for StreamEvents.
func (s *GRPCSink) Subscribe(req *prismpb.FilterRequest, out prismpb.EventStreamService_SubscribeServer) error {
	return s.StreamEvents(req, out)
}

func (s *GRPCSink) StreamLedgers(_ *prismpb.LedgerRequest, out prismpb.PrismStream_StreamLedgersServer) error {
	id, ch := s.subscribe(s.ledgers)
	defer s.remove(s.ledgers, id)
	for {
		select {
		case <-out.Context().Done():
			return out.Context().Err()
		case r, ok := <-ch:
			if !ok {
				return nil
			}
			b, e := MarshalRecord(r, false)
			if e != nil {
				return fmt.Errorf("grpc: marshal record: %w", e)
			}
			if e = out.Send(&prismpb.LedgerResponse{Id: r.ID, Ledger: r.Sequence, ClosedAtUnixMs: r.Time.UnixMilli(), RecordJson: b}); e != nil {
				return e
			}
		}
	}
}

var _ context.Context
