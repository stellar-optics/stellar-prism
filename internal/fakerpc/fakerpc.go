// Package fakerpc provides a scriptable, in-memory Soroban RPC used to test
// the streaming state machine without a network.
//
// The fake holds a fixed universe of events and serves them through the same
// cursor semantics as the real server, so tests describe failures ("drop the
// third call", "replay the last page") rather than hand-assembling responses.
// That keeps the scenarios readable and makes it hard to accidentally write a
// test that passes against a fake the real server would never produce.
package fakerpc

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

// ErrDropped simulates a connection failure. It matches prism's retryable
// classification, so the tailer will back off and retry.
var ErrDropped = errors.New("connection reset by peer")

// Behaviour scripts how the fake misbehaves on a given call. The zero value
// serves the request normally.
type Behaviour struct {
	// Err is returned instead of a response.
	Err error
	// ReplayLast re-serves the previous page, simulating a server that
	// resends after a reconnect.
	ReplayLast bool
	// Reorder shuffles the page into descending order, simulating a server
	// that violates its ordering contract.
	Reorder bool
	// Empty serves an empty page while still reporting bounds, simulating a
	// caught-up stream.
	Empty bool
	// OldestLedger overrides the reported retention floor, which is how a
	// retention overrun (a real gap) is simulated.
	OldestLedger uint32
	// LatestLedger overrides the reported head, which is how a lagging
	// backend in a load-balanced pool is simulated.
	LatestLedger uint32
}

// Server is a fake Soroban RPC.
type Server struct {
	mu sync.Mutex

	// events is the full ordered universe of events the server knows about.
	events []protocol.EventInfo
	// ledgers is the full ordered universe of ledgers.
	ledgers []protocol.LedgerInfo

	// pageLimit caps how many records a single call returns, mimicking the
	// server-side limit independently of what the client asks for.
	pageLimit int
	// oldest and latest are the reported retention bounds.
	oldest, latest uint32

	// script maps a call index (starting at 0) to a Behaviour.
	script map[int]Behaviour
	// calls counts GetEvents/GetLedgers invocations.
	calls int
	// lastPage remembers what was served, for ReplayLast.
	lastEvents  []protocol.EventInfo
	lastLedgers []protocol.LedgerInfo

	// tx maps a hash to a canned transaction response.
	tx map[string]protocol.GetTransactionResponse
	// txs is the canned response for getTransactions.
	txs protocol.GetTransactionsResponse
}

// Option configures a Server.
type Option func(*Server)

// WithPageLimit caps the number of records returned per call.
func WithPageLimit(n int) Option {
	return func(s *Server) { s.pageLimit = n }
}

// WithScript installs per-call behaviours, keyed by zero-based call index.
func WithScript(script map[int]Behaviour) Option {
	return func(s *Server) { s.script = script }
}

// WithRetention sets the reported oldest retained ledger.
func WithRetention(oldest uint32) Option {
	return func(s *Server) { s.oldest = oldest }
}

// NewServer returns a fake serving the given events.
func NewServer(events []protocol.EventInfo, opts ...Option) *Server {
	s := &Server{
		events:    events,
		pageLimit: 100,
		script:    map[int]Behaviour{},
		tx:        map[string]protocol.GetTransactionResponse{},
	}
	for _, o := range opts {
		o(s)
	}
	if len(events) > 0 {
		s.latest = uint32(events[len(events)-1].Ledger) //nolint:gosec // test data
		if s.oldest == 0 {
			s.oldest = uint32(events[0].Ledger) //nolint:gosec // test data
		}
	}
	return s
}

// NewLedgerServer returns a fake serving the given ledgers.
func NewLedgerServer(ledgers []protocol.LedgerInfo, opts ...Option) *Server {
	s := &Server{
		ledgers:   ledgers,
		pageLimit: 100,
		script:    map[int]Behaviour{},
		tx:        map[string]protocol.GetTransactionResponse{},
	}
	for _, o := range opts {
		o(s)
	}
	if len(ledgers) > 0 {
		s.latest = ledgers[len(ledgers)-1].Sequence
		if s.oldest == 0 {
			s.oldest = ledgers[0].Sequence
		}
	}
	return s
}

// SetTransaction installs a canned response for GetTransaction.
func (s *Server) SetTransaction(hash string, resp protocol.GetTransactionResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tx[hash] = resp
}

// SetTransactions installs a canned response for GetTransactions.
func (s *Server) SetTransactions(resp protocol.GetTransactionsResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.txs = resp
}

// Calls reports how many stream calls have been made, so tests can assert on
// retry and drain behaviour.
func (s *Server) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// GetEvents implements rpc.Client.
func (s *Server) GetEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return protocol.GetEventsResponse{}, err
	}

	call := s.calls
	s.calls++
	b := s.script[call]

	if b.Err != nil {
		return protocol.GetEventsResponse{}, b.Err
	}

	// Mirror the real server's rule that a cursor and a ledger range are
	// mutually exclusive. A tailer bug that sets both would otherwise pass
	// silently against the fake and fail only in production.
	hasCursor := req.Pagination != nil && req.Pagination.Cursor != nil
	if hasCursor && req.StartLedger != 0 {
		return protocol.GetEventsResponse{}, errors.New("ledger ranges and cursor cannot both be set")
	}

	oldest, latest := s.oldest, s.latest
	if b.OldestLedger != 0 {
		oldest = b.OldestLedger
	}
	if b.LatestLedger != 0 {
		latest = b.LatestLedger
	}

	respond := func(page []protocol.EventInfo) protocol.GetEventsResponse {
		s.lastEvents = page
		resp := protocol.GetEventsResponse{
			Events:       page,
			LatestLedger: latest,
			OldestLedger: oldest,
		}
		if len(page) > 0 {
			resp.Cursor = page[len(page)-1].ID
		} else if hasCursor {
			resp.Cursor = req.Pagination.Cursor.String()
		}
		return resp
	}

	if b.Empty {
		return respond(nil), nil
	}
	if b.ReplayLast {
		return respond(s.lastEvents), nil
	}

	// Select everything strictly after the cursor, or from the start ledger.
	var out []protocol.EventInfo
	for _, e := range s.events {
		if hasCursor {
			if e.ID > req.Pagination.Cursor.String() {
				out = append(out, e)
			}
			continue
		}
		if req.StartLedger == 0 || uint32(e.Ledger) >= req.StartLedger { //nolint:gosec // test data
			out = append(out, e)
		}
	}

	limit := s.pageLimit
	if req.Pagination != nil && req.Pagination.Limit > 0 && int(req.Pagination.Limit) < limit {
		limit = int(req.Pagination.Limit)
	}
	if len(out) > limit {
		out = out[:limit]
	}

	if b.Reorder {
		reversed := make([]protocol.EventInfo, len(out))
		copy(reversed, out)
		sort.Slice(reversed, func(i, j int) bool { return reversed[i].ID > reversed[j].ID })
		// Cursor still reflects the true high-water mark; only the page order
		// is wrong, which is exactly the hazard being simulated.
		s.lastEvents = reversed
		resp := protocol.GetEventsResponse{
			Events:       reversed,
			LatestLedger: latest,
			OldestLedger: oldest,
		}
		if len(out) > 0 {
			resp.Cursor = out[len(out)-1].ID
		}
		return resp, nil
	}

	return respond(out), nil
}

// GetLedgers implements rpc.Client.
func (s *Server) GetLedgers(ctx context.Context, req protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return protocol.GetLedgersResponse{}, err
	}

	call := s.calls
	s.calls++
	b := s.script[call]

	if b.Err != nil {
		return protocol.GetLedgersResponse{}, b.Err
	}

	oldest, latest := s.oldest, s.latest
	if b.OldestLedger != 0 {
		oldest = b.OldestLedger
	}
	if b.LatestLedger != 0 {
		latest = b.LatestLedger
	}

	hasCursor := req.Pagination != nil && req.Pagination.Cursor != ""

	respond := func(page []protocol.LedgerInfo) protocol.GetLedgersResponse {
		s.lastLedgers = page
		resp := protocol.GetLedgersResponse{
			Ledgers:      page,
			LatestLedger: latest,
			OldestLedger: oldest,
		}
		if len(page) > 0 {
			resp.Cursor = fmt.Sprintf("%d", page[len(page)-1].Sequence)
		} else if hasCursor {
			resp.Cursor = req.Pagination.Cursor
		}
		return resp
	}

	if b.Empty {
		return respond(nil), nil
	}
	if b.ReplayLast {
		return respond(s.lastLedgers), nil
	}

	var after uint32
	if hasCursor {
		if _, err := fmt.Sscanf(req.Pagination.Cursor, "%d", &after); err != nil {
			return protocol.GetLedgersResponse{}, fmt.Errorf("bad cursor: %w", err)
		}
	}

	var out []protocol.LedgerInfo
	for _, l := range s.ledgers {
		if hasCursor {
			if l.Sequence > after {
				out = append(out, l)
			}
			continue
		}
		if req.StartLedger == 0 || l.Sequence >= req.StartLedger {
			out = append(out, l)
		}
	}

	limit := s.pageLimit
	if req.Pagination != nil && req.Pagination.Limit > 0 && int(req.Pagination.Limit) < limit {
		limit = int(req.Pagination.Limit)
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return respond(out), nil
}

// GetTransaction implements rpc.Client.
func (s *Server) GetTransaction(ctx context.Context, req protocol.GetTransactionRequest) (protocol.GetTransactionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return protocol.GetTransactionResponse{}, err
	}
	resp, ok := s.tx[req.Hash]
	if !ok {
		return protocol.GetTransactionResponse{
			TransactionDetails: protocol.TransactionDetails{
				Status: protocol.TransactionStatusNotFound,
			},
		}, nil
	}
	return resp, nil
}

// GetTransactions implements rpc.Client.
func (s *Server) GetTransactions(ctx context.Context, req protocol.GetTransactionsRequest) (protocol.GetTransactionsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return protocol.GetTransactionsResponse{}, err
	}
	return s.txs, nil
}

// GetLatestLedger implements rpc.Client.
func (s *Server) GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return protocol.GetLatestLedgerResponse{}, err
	}
	return protocol.GetLatestLedgerResponse{Sequence: s.latest}, nil
}

// MakeEvents builds a contiguous run of events, one per ledger starting at
// startLedger, for use as a fake's universe.
func MakeEvents(startLedger uint32, count int) []protocol.EventInfo {
	out := make([]protocol.EventInfo, 0, count)
	for i := range count {
		ledger := startLedger + uint32(i) //nolint:gosec // test data
		cur := protocol.Cursor{Ledger: ledger, Tx: 1, Op: 0, Event: 0}
		out = append(out, protocol.EventInfo{
			EventType:       protocol.EventTypeContract,
			Ledger:          int32(ledger), //nolint:gosec // test data
			LedgerClosedAt:  time.Unix(int64(1700000000+ledger), 0).UTC().Format(time.RFC3339),
			ContractID:      "CBGSBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKKY3",
			ID:              cur.String(),
			TxIndex:         1,
			TransactionHash: fmt.Sprintf("%064x", ledger),
			// A minimal but genuinely decodable ScVal: the u32 value 7.
			TopicXDR: []string{"AAAAAwAAAAc="},
			ValueXDR: "AAAAAwAAAAc=",
		})
	}
	return out
}

// MakeLedgers builds a contiguous run of ledgers.
func MakeLedgers(startLedger uint32, count int) []protocol.LedgerInfo {
	out := make([]protocol.LedgerInfo, 0, count)
	for i := range count {
		seq := startLedger + uint32(i) //nolint:gosec // test data
		out = append(out, protocol.LedgerInfo{
			Hash:            fmt.Sprintf("%064x", seq),
			Sequence:        seq,
			LedgerCloseTime: int64(1700000000 + seq),
		})
	}
	return out
}
