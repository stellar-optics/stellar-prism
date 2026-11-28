package prismtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/toid"
)

// MockRPC is a scriptable, in-memory Soroban RPC used to test streaming
// state machines without a network. It holds a fixed universe of events and
// serves them through the same cursor semantics as the real server, so tests
// can describe failures ("drop the third call", "replay the last page")
// rather than hand-assembling responses. This keeps scenarios readable and
// makes it hard to accidentally write a test that passes against a fake the
// real server would never produce.
type MockRPC struct {
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
	lastEvents       []protocol.EventInfo
	lastLedgers      []protocol.LedgerInfo
	lastTransactions []protocol.TransactionInfo

	// tx maps a hash to a canned transaction response.
	tx map[string]protocol.GetTransactionResponse
	// transactions is the ordered universe served by a transaction fake.
	transactions []protocol.TransactionInfo
	// txs is a canned response for getTransactions, used when transactions
	// is empty.
	txs protocol.GetTransactionsResponse
	// entries maps an encoded ledger key to its canned result.
	entries map[string]protocol.LedgerEntryResult
	// simulations returned in order by simulateTransaction.
	simulations []protocol.SimulateTransactionResponse
	// simulationCalls tracks position in simulations.
	simulationCalls int

	// health is the explicit getHealth response; healthSet distinguishes it
	// from an unconfigured zero value.
	health      protocol.GetHealthResponse
	healthSet   bool
	healthErr   error
	healthCalls int
	// healthScript stages a sequence of responses for periodic-check tests.
	healthScript []HealthBehaviour
}

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

// HealthBehaviour scripts one getHealth response. A non-nil Err takes
// precedence over Response.
type HealthBehaviour struct {
	// Response is returned as-is when Err is nil.
	Response protocol.GetHealthResponse
	// Err is returned instead of Response.
	Err error
}

// NewMockRPC returns a new empty MockRPC ready for scripting.
func NewMockRPC() *MockRPC {
	return &MockRPC{
		script:    map[int]Behaviour{},
		pageLimit: 100,
		entries:   map[string]protocol.LedgerEntryResult{},
		tx:        map[string]protocol.GetTransactionResponse{},
	}
}

// AddEvent appends an event to the universe served by GetEvents. Events
// are served in order, paginated by pageLimit.
func (m *MockRPC) AddEvent(e protocol.EventInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
	if m.latest == 0 {
		m.latest = uint32(e.Ledger)
	}
	if m.oldest == 0 {
		m.oldest = uint32(e.Ledger)
	}
}

// InjectError returns an error instead of a response on the call at callIndex.
// After the error is injected, subsequent calls at the same index also fail.
// This simulates a connection failure that matches prism's retryable
// classification, so the tailer will back off and retry.
func (m *MockRPC) InjectError(callIndex int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.script[callIndex] = Behaviour{Err: err}
}

// SimulateGap configures the fake to simulate a retention gap: the reported
// oldest ledger is advanced to `oldest`, making it appear that ledgers from
// `from` to `oldest-1` have been pruned. This is how a real gap is simulated
// in tests.
func (m *MockRPC) SimulateGap(from, oldest uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.oldest = oldest
}

// GetEvents implements rpc.Client.
func (m *MockRPC) GetEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	m.mu.Lock()
	_ = ctx // reserved for context checks in production
	defer m.mu.Unlock()

	call := m.calls
	m.calls++
	b := m.script[call]

	if b.Err != nil {
		return protocol.GetEventsResponse{}, b.Err
	}

	hasCursor := req.Pagination != nil && req.Pagination.Cursor != nil
	if hasCursor && req.StartLedger != 0 {
		return protocol.GetEventsResponse{}, errors.New("ledger ranges and cursor cannot both be set")
	}

	oldest, latest := m.oldest, m.latest
	if b.OldestLedger != 0 {
		oldest = b.OldestLedger
	}
	if b.LatestLedger != 0 {
		latest = b.LatestLedger
	}

	respond := func(page []protocol.EventInfo) protocol.GetEventsResponse {
		m.lastEvents = page
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
		return respond(m.lastEvents), nil
	}

	var out []protocol.EventInfo
	for _, e := range m.events {
		if hasCursor {
			if e.ID > req.Pagination.Cursor.String() {
				out = append(out, e)
			}
			continue
		}
		if req.StartLedger == 0 || uint32(e.Ledger) >= req.StartLedger {
			out = append(out, e)
		}
	}

	limit := m.pageLimit
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
		m.lastEvents = reversed
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

// GetEventsBatch implements rpc.Client, serving every request in order and
// stopping at the first scripted failure — the responses gathered before it
// are returned with the error so the caller can see how far the batch got.
func (m *MockRPC) GetEventsBatch(ctx context.Context, reqs []protocol.GetEventsRequest) ([]protocol.GetEventsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(reqs) == 0 {
		return nil, nil
	}

	responses := make([]protocol.GetEventsResponse, 0, len(reqs))
	for _, req := range reqs {
		// Each entry in the batch is one call as far as the script is
		// concerned, so the indices advance in step with the requests.
		call := m.calls
		m.calls++
		b := m.script[call]

		if b.Err != nil {
			return responses, b.Err
		}

		hasCursor := req.Pagination != nil && req.Pagination.Cursor != nil
		if hasCursor && req.StartLedger != 0 {
			return responses, errors.New("ledger ranges and cursor cannot both be set")
		}

		oldest, latest := m.oldest, m.latest
		if b.OldestLedger != 0 {
			oldest = b.OldestLedger
		}
		if b.LatestLedger != 0 {
			latest = b.LatestLedger
		}

		respond := func(page []protocol.EventInfo) protocol.GetEventsResponse {
			m.lastEvents = page
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
			responses = append(responses, respond(nil))
			continue
		}
		if b.ReplayLast {
			responses = append(responses, respond(m.lastEvents))
			continue
		}

		var out []protocol.EventInfo
		for _, e := range m.events {
			if hasCursor {
				if e.ID > req.Pagination.Cursor.String() {
					out = append(out, e)
				}
				continue
			}
			if req.StartLedger == 0 || uint32(e.Ledger) >= req.StartLedger {
				out = append(out, e)
			}
		}

		limit := m.pageLimit
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
			m.lastEvents = reversed
			resp := protocol.GetEventsResponse{
				Events:       reversed,
				LatestLedger: latest,
				OldestLedger: oldest,
			}
			if len(out) > 0 {
				resp.Cursor = out[len(out)-1].ID
			}
			responses = append(responses, resp)
			continue
		}

		responses = append(responses, respond(out))
	}
	return responses, nil
}

// GetLedgers implements rpc.Client.
func (m *MockRPC) GetLedgers(ctx context.Context, req protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error) {
	m.mu.Lock()
	_ = ctx // reserved for context checks in production
	defer m.mu.Unlock()

	call := m.calls
	m.calls++
	b := m.script[call]

	if b.Err != nil {
		return protocol.GetLedgersResponse{}, b.Err
	}

	oldest, latest := m.oldest, m.latest
	if b.OldestLedger != 0 {
		oldest = b.OldestLedger
	}
	if b.LatestLedger != 0 {
		latest = b.LatestLedger
	}

	hasCursor := req.Pagination != nil && req.Pagination.Cursor != ""

	respond := func(page []protocol.LedgerInfo) protocol.GetLedgersResponse {
		m.lastLedgers = page
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
		return respond(m.lastLedgers), nil
	}

	var after uint32
	if hasCursor {
		if _, err := fmt.Sscanf(req.Pagination.Cursor, "%d", &after); err != nil {
			return protocol.GetLedgersResponse{}, fmt.Errorf("bad cursor: %w", err)
		}
	}

	var out []protocol.LedgerInfo
	for _, l := range m.ledgers {
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

	limit := m.pageLimit
	if req.Pagination != nil && req.Pagination.Limit > 0 && int(req.Pagination.Limit) < limit {
		limit = int(req.Pagination.Limit)
	}
	if len(out) > limit {
		out = out[:limit]
	}

	return respond(out), nil
}

// GetTransaction implements rpc.Client.
func (m *MockRPC) GetTransaction(ctx context.Context, req protocol.GetTransactionRequest) (protocol.GetTransactionResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return protocol.GetTransactionResponse{}, err
	}
	resp, ok := m.tx[req.Hash]
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
func (m *MockRPC) GetTransactions(ctx context.Context, req protocol.GetTransactionsRequest) (protocol.GetTransactionsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return protocol.GetTransactionsResponse{}, err
	}
	if len(m.transactions) == 0 {
		return m.txs, nil
	}

	call := m.calls
	m.calls++
	b := m.script[call]
	if b.Err != nil {
		return protocol.GetTransactionsResponse{}, b.Err
	}

	oldest, latest := m.oldest, m.latest
	if b.OldestLedger != 0 {
		oldest = b.OldestLedger
	}
	if b.LatestLedger != 0 {
		latest = b.LatestLedger
	}

	hasCursor := req.Pagination != nil && req.Pagination.Cursor != ""
	// Mirror the real server's rule that a cursor and a ledger range are
	// mutually exclusive.
	if hasCursor && req.StartLedger != 0 {
		return protocol.GetTransactionsResponse{}, errors.New("ledger ranges and cursor cannot both be set")
	}

	respond := func(page []protocol.TransactionInfo) protocol.GetTransactionsResponse {
		m.lastTransactions = page
		resp := protocol.GetTransactionsResponse{
			Transactions: page,
			LatestLedger: latest,
			OldestLedger: oldest,
		}
		if len(page) > 0 {
			resp.Cursor = transactionCursor(page[len(page)-1])
		} else if hasCursor {
			resp.Cursor = req.Pagination.Cursor
		}
		return resp
	}

	if b.Empty {
		return respond(nil), nil
	}
	if b.ReplayLast {
		return respond(m.lastTransactions), nil
	}

	var after int64 = -1
	if hasCursor {
		parsed, err := strconv.ParseInt(req.Pagination.Cursor, 10, 64)
		if err != nil {
			return protocol.GetTransactionsResponse{}, fmt.Errorf("bad cursor: %w", err)
		}
		after = parsed
	}

	var out []protocol.TransactionInfo
	for _, tx := range m.transactions {
		if hasCursor {
			if transactionToid(tx) > after {
				out = append(out, tx)
			}
			continue
		}
		if req.StartLedger == 0 || tx.Ledger >= req.StartLedger {
			out = append(out, tx)
		}
	}

	limit := m.pageLimit
	if req.Pagination != nil && req.Pagination.Limit > 0 && int(req.Pagination.Limit) < limit {
		limit = int(req.Pagination.Limit)
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return respond(out), nil
}

// SetLedgers installs the full ordered universe of ledgers.
func (m *MockRPC) SetLedgers(ledgers []protocol.LedgerInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ledgers = append([]protocol.LedgerInfo(nil), ledgers...)
	if len(ledgers) > 0 {
		if m.oldest == 0 {
			m.oldest = ledgers[0].Sequence
		}
		if m.latest == 0 {
			m.latest = ledgers[len(ledgers)-1].Sequence
		}
	}
}

// SetTransaction installs a canned response for GetTransaction.
func (m *MockRPC) SetTransaction(hash string, resp protocol.GetTransactionResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tx[hash] = resp
}

// SetTransactions installs a canned response for GetTransactions.
func (m *MockRPC) SetTransactions(resp protocol.GetTransactionsResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.txs = resp
}

// SetSimulations installs responses returned in order by simulateTransaction.
// The final response is reused after the sequence is exhausted.
func (m *MockRPC) SetSimulations(responses []protocol.SimulateTransactionResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.simulations = append([]protocol.SimulateTransactionResponse(nil), responses...)
	m.simulationCalls = 0
}

// SetLedgerEntries installs canned results keyed by encoded ledger key.
func (m *MockRPC) SetLedgerEntries(entries map[string]protocol.LedgerEntryResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = entries
}

// SetHealth installs an explicit getHealth response.
func (m *MockRPC) SetHealth(resp protocol.GetHealthResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.health = resp
	m.healthSet = true
	m.healthErr = nil
	m.healthScript = nil
}

// SetHealthError makes every subsequent GetHealth call fail with err.
func (m *MockRPC) SetHealthError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.healthErr = err
	m.healthSet = false
	m.healthScript = nil
}

// HealthCalls reports how many getHealth calls the fake has served.
func (m *MockRPC) HealthCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.healthCalls
}

// Health implements rpc.HealthClient.
func (m *MockRPC) Health(ctx context.Context) (protocol.GetHealthResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return protocol.GetHealthResponse{}, err
	}

	call := m.healthCalls
	m.healthCalls++

	if len(m.healthScript) > 0 {
		index := call
		if index >= len(m.healthScript) {
			index = len(m.healthScript) - 1
		}
		behaviour := m.healthScript[index]
		if behaviour.Err != nil {
			return protocol.GetHealthResponse{}, behaviour.Err
		}
		return behaviour.Response, nil
	}
	if m.healthErr != nil {
		return protocol.GetHealthResponse{}, m.healthErr
	}
	if m.healthSet {
		return m.health, nil
	}

	now := time.Now()
	var retention uint32
	if m.latest >= m.oldest {
		retention = m.latest - m.oldest
	}
	return protocol.GetHealthResponse{
		Status:                "healthy",
		LatestLedger:          m.latest,
		LatestLedgerCloseTime: now.Unix(),
		OldestLedger:          m.oldest,
		OldestLedgerCloseTime: now.Add(-time.Duration(retention) * 5 * time.Second).Unix(),
		LedgerRetentionWindow: retention,
	}, nil
}

// SimulateTransaction implements rpc.Client.
func (m *MockRPC) SimulateTransaction(ctx context.Context, req protocol.SimulateTransactionRequest) (protocol.SimulateTransactionResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return protocol.SimulateTransactionResponse{}, err
	}
	if len(m.simulations) == 0 {
		return protocol.SimulateTransactionResponse{}, nil
	}
	index := m.simulationCalls
	if index >= len(m.simulations) {
		index = len(m.simulations) - 1
	}
	m.simulationCalls++
	return m.simulations[index], nil
}

// GetLatestLedger implements rpc.Client.
func (m *MockRPC) GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return protocol.GetLatestLedgerResponse{}, err
	}
	return protocol.GetLatestLedgerResponse{Sequence: m.latest}, nil
}

// MakeEvents builds a contiguous run of events, one per ledger starting at
// startLedger, for use as a fake's universe.
func MakeEvents(startLedger uint32, count int) []protocol.EventInfo {
	out := make([]protocol.EventInfo, 0, count)
	for i := range count {
		ledger := startLedger + uint32(i)
		cur := protocol.Cursor{Ledger: ledger, Tx: 1, Op: 0, Event: 0}
		out = append(out, protocol.EventInfo{
			EventType:       protocol.EventTypeContract,
			Ledger:          int32(ledger),
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
		seq := startLedger + uint32(i)
		out = append(out, protocol.LedgerInfo{
			Hash:            fmt.Sprintf("%064x", seq),
			Sequence:        seq,
			LedgerCloseTime: int64(1700000000 + seq),
		})
	}
	return out
}

// transactionCursor returns a string cursor for a transaction.
func transactionCursor(tx protocol.TransactionInfo) string {
	return strconv.FormatInt(transactionToid(tx), 10)
}

// transactionToid returns the SEP-35 total-order position the real server
// encodes in its cursor.
func transactionToid(tx protocol.TransactionInfo) int64 {
	return toid.New(int32(tx.Ledger), tx.ApplicationOrder, 1).ToInt64() //nolint:gosec // test data
}

// Behaviour constructs a Behaviour with the given options. The zero value
// serves the request normally.
func BehaviourOpts() Behaviour {
	return Behaviour{}
}

// WithErr returns a Behaviour that always returns the given error instead
// of a response.
func WithErr(err error) func(*Behaviour) {
	return func(b *Behaviour) {
		b.Err = err
	}
}

// WithReplayLast returns a Behaviour that re-serves the previous page.
func WithReplayLast() func(*Behaviour) {
	return func(b *Behaviour) {
		b.ReplayLast = true
	}
}

// WithReorder returns a Behaviour that shuffles the page into descending order.
func WithReorder() func(*Behaviour) {
	return func(b *Behaviour) {
		b.Reorder = true
	}
}

// WithEmpty returns a Behaviour that serves an empty page while still
// reporting bounds.
func WithEmpty() func(*Behaviour) {
	return func(b *Behaviour) {
		b.Empty = true
	}
}

// WithOldestLedger overrides the reported retention floor.
func WithOldestLedger(oldest uint32) func(*Behaviour) {
	return func(b *Behaviour) {
		b.OldestLedger = oldest
	}
}

// WithLatestLedger overrides the reported head.
func WithLatestLedger(latest uint32) func(*Behaviour) {
	return func(b *Behaviour) {
		b.LatestLedger = latest
	}
}

// MockServer is a lightweight HTTP mock Soroban RPC server testkit.
type MockServer struct {
	*MockRPC
	Server *httptest.Server
	URL    string

	simMu     sync.Mutex
	simStatus int
	simCount  int
}

type jsonrpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonrpcResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id"`
	Result  any    `json:"result,omitempty"`
	Error   any    `json:"error,omitempty"`
}

// NewServer starts an HTTP mock server backed by m.
func (m *MockRPC) NewServer() *MockServer {
	return NewMockServer(m)
}

// NewMockServer starts an HTTP mock server backed by m.
func NewMockServer(m *MockRPC) *MockServer {
	s := &MockServer{
		MockRPC: m,
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.simMu.Lock()
		if s.simCount > 0 && s.simStatus > 0 {
			s.simCount--
			status := s.simStatus
			s.simMu.Unlock()
			http.Error(w, fmt.Sprintf("simulated HTTP error %d", status), status)
			return
		}
		s.simMu.Unlock()

		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req jsonrpcRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		ctx := r.Context()

		var (
			result any
			rpcErr error
		)

		switch req.Method {
		case "getEvents":
			var p protocol.GetEventsRequest
			if rpcErr = decodeParams(req.Params, &p); rpcErr == nil {
				result, rpcErr = s.GetEvents(ctx, p)
			}
		case "getTransaction":
			var p protocol.GetTransactionRequest
			if rpcErr = decodeParams(req.Params, &p); rpcErr == nil {
				result, rpcErr = s.GetTransaction(ctx, p)
			}
		case "getTransactions":
			var p protocol.GetTransactionsRequest
			if rpcErr = decodeParams(req.Params, &p); rpcErr == nil {
				result, rpcErr = s.GetTransactions(ctx, p)
			}
		case "getLedgers":
			var p protocol.GetLedgersRequest
			if rpcErr = decodeParams(req.Params, &p); rpcErr == nil {
				result, rpcErr = s.GetLedgers(ctx, p)
			}
		case "getHealth":
			result, rpcErr = s.Health(ctx)
		case "getLatestLedger":
			result, rpcErr = s.GetLatestLedger(ctx)
		case "getNetwork":
			result = protocol.GetNetworkResponse{
				Passphrase: "Test SDF Network ; September 2015",
			}
		default:
			rpcErr = fmt.Errorf("method %q not implemented", req.Method)
		}

		resp := jsonrpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
		}
		if rpcErr != nil {
			resp.Error = map[string]any{
				"code":    -32603,
				"message": rpcErr.Error(),
			}
		} else {
			resp.Result = result
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	s.Server = httptest.NewServer(handler)
	s.URL = s.Server.URL
	return s
}

// Simulate500 simulates an HTTP 500 error for the next n calls.
func (s *MockServer) Simulate500(count int) {
	s.SimulateStatus(http.StatusInternalServerError, count)
}

// SimulateRateLimit simulates an HTTP 429 rate limit for the next n calls.
func (s *MockServer) SimulateRateLimit(count int) {
	s.SimulateStatus(http.StatusTooManyRequests, count)
}

// SimulateStatus sets the HTTP status code for the next n calls.
func (s *MockServer) SimulateStatus(statusCode, count int) {
	s.simMu.Lock()
	defer s.simMu.Unlock()
	s.simStatus = statusCode
	s.simCount = count
}

// Close closes the test HTTP server.
func (s *MockServer) Close() {
	if s.Server != nil {
		s.Server.Close()
	}
}

// decodeParams unmarshals JSON-RPC params into dst. Absent params leave dst at
// its zero value.
func decodeParams(params json.RawMessage, dst any) error {
	if len(params) == 0 {
		return nil
	}
	if err := json.Unmarshal(params, dst); err != nil {
		return fmt.Errorf("invalid params: %w", err)
	}
	return nil
}
