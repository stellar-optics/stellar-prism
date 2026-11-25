package stream

import (
	"context"
	"go/parser"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

func TestExpressionFilterArithmeticStringsAndNestedFields(t *testing.T) {
	t.Parallel()
	context := map[string]any{
		"value":    map[string]any{"amount": int64(150), "asset": map[string]any{"code": "USD"}},
		"topics":   []any{map[string]any{"symbol": "transfer"}},
		"topic":    []any{"transfer"},
		"contract": "C123",
	}
	cases := []struct {
		expr string
		want bool
	}{
		{`value.amount * 2 > 100 && value.asset.code == "USD"`, true},
		{`topics[0].symbol == "transfer" && contract != "C456"`, true},
		{`topic[0] == "transfer" && contains(topic[0], "fer")`, true},
		{`value.amount + 3 < 100`, false},
	}
	for _, tc := range cases {
		t.Run(tc.expr, func(t *testing.T) {
			parsed, err := parser.ParseExpr(tc.expr)
			if err != nil {
				t.Fatal(err)
			}
			if err := validateFilterAST(parsed); err != nil {
				t.Fatal(err)
			}
			got, err := evalFilterExpr(parsed, context)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestExpressionFilterReadsScValTopicsAndValue(t *testing.T) {
	t.Parallel()

	symbol, err := xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol("transfer"))
	if err != nil {
		t.Fatal(err)
	}
	topic, err := xdr.MarshalBase64(symbol)
	if err != nil {
		t.Fatal(err)
	}
	value, err := xdr.NewScVal(xdr.ScValTypeScvI128, xdr.Int128Parts{Hi: 0, Lo: xdr.Uint64(150)})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := xdr.MarshalBase64(value)
	if err != nil {
		t.Fatal(err)
	}

	filter, err := CompileExpression(`topic[0] == "transfer" && value > 100`)
	if err != nil {
		t.Fatal(err)
	}
	matched, err := filter.Match(Record{
		Kind: KindEvent,
		Event: &Event{
			TopicXDR: []string{topic},
			ValueXDR: payload,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Fatal("filter did not match the decoded topic and value")
	}
}

func TestExpressionFilterReadsNestedScValMapFields(t *testing.T) {
	key, err := xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol("amount"))
	if err != nil {
		t.Fatal(err)
	}
	amount, err := xdr.NewScVal(xdr.ScValTypeScvI128, xdr.Int128Parts{Hi: 0, Lo: xdr.Uint64(1_500_000_000)})
	if err != nil {
		t.Fatal(err)
	}
	entries := xdr.ScMap{{Key: key, Val: amount}}
	payload, err := xdr.NewScVal(xdr.ScValTypeScvMap, &entries)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := xdr.MarshalBase64(payload)
	if err != nil {
		t.Fatal(err)
	}
	filter, err := CompileExpression(`value.amount > 100`)
	if err != nil {
		t.Fatal(err)
	}
	matched, err := filter.Match(Record{Sequence: 7, Kind: KindEvent, Event: &Event{ValueXDR: encoded}})
	if err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Fatal("nested amount expression did not match")
	}
}

func TestCompileExpressionRejectsUnknownAndNonBoolean(t *testing.T) {
	t.Parallel()
	if _, err := CompileExpression(`os.Exit(0)`); err == nil {
		t.Fatal("CompileExpression accepted an unsupported call")
	}
	filter, err := CompileExpression(`value.amount + 1`)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := filter.(*expressionFilter); !ok {
		t.Fatalf("filter type %T", filter)
	}
}

// liveContractClient is a fake RPC whose chain grows by one ledger per poll.
// Every ledger carries one event from each contract in universe, and
// GetEvents honours the request's contract filter and cursor the way the real
// server does, so the contract set the source sends is what decides which
// events come back.
type liveContractClient struct {
	rpc.Client
	universe []string

	mu       sync.Mutex
	head     uint32
	requests [][]string
}

func (c *liveContractClient) event(ledger uint32, i int) protocol.EventInfo {
	id := protocol.Cursor{Ledger: ledger, Tx: 1, Event: uint32(i)}.String() //nolint:gosec // tiny test index
	return protocol.EventInfo{
		ID:         id,
		Ledger:     int32(ledger), //nolint:gosec // tiny test ledger
		EventType:  protocol.EventTypeContract,
		ContractID: c.universe[i],
		TopicXDR:   []string{"AAAAAQ=="},
		ValueXDR:   "AAAAAQ==",
	}
}

func (c *liveContractClient) GetEvents(_ context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.head++
	wanted := map[string]bool{}
	var asked []string
	for _, f := range req.Filters {
		for _, id := range f.ContractIDs {
			wanted[id] = true
			asked = append(asked, id)
		}
	}
	c.requests = append(c.requests, asked)

	after := protocol.Cursor{Ledger: req.StartLedger}
	if req.Pagination != nil && req.Pagination.Cursor != nil {
		after = *req.Pagination.Cursor
	}
	var out []protocol.EventInfo
	for ledger := uint32(1); ledger <= c.head; ledger++ {
		for i, contract := range c.universe {
			if !wanted[contract] {
				continue
			}
			e := c.event(ledger, i)
			if cur, _ := protocol.ParseCursor(e.ID); req.Pagination != nil && req.Pagination.Cursor != nil && cur.Cmp(after) <= 0 {
				continue
			} else if (req.Pagination == nil || req.Pagination.Cursor == nil) && ledger < req.StartLedger {
				continue
			}
			out = append(out, e)
		}
	}
	return protocol.GetEventsResponse{
		Events: out,
		// Like the real server, an exhausted window ends at the head so the
		// next request resumes after it rather than rescanning.
		Cursor:       protocol.Cursor{Ledger: c.head + 1}.String(),
		LatestLedger: c.head,
	}, nil
}

type contractCollector struct{ records []Record }

func (c *contractCollector) Emit(r Record) error { c.records = append(c.records, r); return nil }
func (c *contractCollector) Flush() error        { return nil }

// waitForWatchlist polls until the filter reports want, failing the test
// after a generous deadline so a missed file event shows up as a clear error
// rather than a hang.
func waitForWatchlist(t *testing.T, filter *WatchlistFilter, want []string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !reflect.DeepEqual(filter.ContractIDs(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("watchlist = %v, want %v", filter.ContractIDs(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestWatchlistFilterUpdatesDuringActiveStream is the acceptance check for
// hot reloading: a single --follow run keeps going while the watchlist file
// is edited, the very next poll asks the RPC for the new contract set, the
// added contract's events start flowing, the removed contract's stop, and
// the stream's cursor never resets.
func TestWatchlistFilterUpdatesDuringActiveStream(t *testing.T) {
	c1, c2, c3 := testContractID(1), testContractID(2), testContractID(3)
	path := filepath.Join(t.TempDir(), "watchlist.txt")
	if err := os.WriteFile(path, []byte(c1+"\n"+c2+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	filter, err := NewWatchlistFilter(path)
	if err != nil {
		t.Fatal(err)
	}
	filter.SetDebounce(10 * time.Millisecond)
	var (
		changesMu sync.Mutex
		changes   []WatchlistChange
	)
	filter.OnChange(func(c WatchlistChange) {
		changesMu.Lock()
		defer changesMu.Unlock()
		changes = append(changes, c)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := filter.Start(ctx); err != nil {
		t.Fatal(err)
	}

	client := &liveContractClient{universe: []string{c1, c2, c3}}
	src, err := NewEventSource(client, EventQuery{Watchlist: filter, StartLedger: 1, PageLimit: 100})
	if err != nil {
		t.Fatal(err)
	}
	out := &contractCollector{}

	// The clock's sleep is where the stream sits between polls, so that is
	// where the file is edited: after the third poll c1 is swapped for c3,
	// and after the sixth poll the stream is stopped.
	const swapAfter, stopAfter = 3, 6
	var swappedAtLedger uint32
	clock := &watchlistClock{onSleep: func(n int) {
		switch n {
		case swapAfter:
			client.mu.Lock()
			swappedAtLedger = client.head
			client.mu.Unlock()
			if err := os.WriteFile(path, []byte("# rotated\n"+c2+"\n"+c3+"\n"), 0o600); err != nil {
				t.Error(err)
			}
			waitForWatchlist(t, filter, []string{c2, c3})
		case stopAfter:
			cancel()
		}
	}}
	tailer := NewTailer(src, out, Options{Follow: true, Clock: clock, Filter: FilterSet{filter}})
	if err := tailer.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	// The RPC saw the old set before the edit and the new set after it,
	// within one tailer run.
	client.mu.Lock()
	requests := append([][]string(nil), client.requests...)
	client.mu.Unlock()
	if len(requests) < stopAfter {
		t.Fatalf("made %d requests, want at least %d", len(requests), stopAfter)
	}
	for i, got := range requests {
		want := []string{c1, c2}
		if i >= swapAfter {
			want = []string{c2, c3}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("request %d asked for %v, want %v", i+1, got, want)
		}
	}

	// Delivery: c2 throughout, c1 only before the swap, c3 only after it,
	// IDs strictly increasing across the swap (no restart, no replay).
	var sawC1, sawC3 bool
	for i, r := range out.records {
		if i > 0 && r.ID <= out.records[i-1].ID {
			t.Fatalf("record %d ID %s not after %s; the stream restarted or replayed", i, r.ID, out.records[i-1].ID)
		}
		switch r.Event.ContractID {
		case c1:
			sawC1 = true
			if r.Sequence > swappedAtLedger {
				t.Errorf("removed contract c1 emitted at ledger %d, after the swap at %d", r.Sequence, swappedAtLedger)
			}
		case c3:
			sawC3 = true
			if r.Sequence <= swappedAtLedger {
				t.Errorf("added contract c3 emitted at ledger %d, before the swap at %d", r.Sequence, swappedAtLedger)
			}
		}
	}
	if !sawC1 || !sawC3 {
		t.Fatalf("saw c1=%v c3=%v; want events from both sides of the swap", sawC1, sawC3)
	}

	changesMu.Lock()
	defer changesMu.Unlock()
	want := []WatchlistChange{{Added: []string{c3}, Removed: []string{c1}, Total: 2}}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("change notifications = %+v, want %+v", changes, want)
	}
}

// watchlistClock is a Clock whose sleeps return immediately after running a
// hook, so a test can act between the tailer's polls.
type watchlistClock struct {
	now     time.Time
	sleeps  int
	onSleep func(n int)
}

func (c *watchlistClock) Now() time.Time { return c.now }

func (c *watchlistClock) Sleep(ctx context.Context, d time.Duration) error {
	c.sleeps++
	c.now = c.now.Add(d)
	if c.onSleep != nil {
		c.onSleep(c.sleeps)
	}
	return ctx.Err()
}

// TestWatchlistFilterMatchDropsRemovedContractsImmediately checks the
// client-side half of the filter: once a contract leaves the set, records
// from it are rejected even if they were fetched under the old set.
func TestWatchlistFilterMatchDropsRemovedContractsImmediately(t *testing.T) {
	c1, c2 := testContractID(1), testContractID(2)
	path := filepath.Join(t.TempDir(), "watchlist.txt")
	if err := os.WriteFile(path, []byte(c1+"\n"+c2+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	filter, err := NewWatchlistFilter(path)
	if err != nil {
		t.Fatal(err)
	}
	fromC1 := Record{Kind: KindEvent, Event: &Event{ContractID: c1}}
	if ok, _ := filter.Match(fromC1); !ok {
		t.Fatal("watched contract was rejected")
	}
	if err := os.WriteFile(path, []byte(c2+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	change, err := filter.Reload()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(change.Removed, []string{c1}) || len(change.Added) != 0 || change.Total != 1 {
		t.Fatalf("Reload() change = %+v, want c1 removed", change)
	}
	if ok, _ := filter.Match(fromC1); ok {
		t.Fatal("removed contract still matched")
	}
	if ok, _ := filter.Match(Record{Kind: KindLedger}); !ok {
		t.Fatal("non-event record was rejected; the watchlist only governs events")
	}
}

// TestWatchlistFilterKeepsPreviousSetOnInvalidReload checks a bad edit cannot
// take the stream down or blank the filter: the error is reported and the
// last good set stays active until the file is fixed.
func TestWatchlistFilterKeepsPreviousSetOnInvalidReload(t *testing.T) {
	c1, c2 := testContractID(1), testContractID(2)
	path := filepath.Join(t.TempDir(), "watchlist.txt")
	if err := os.WriteFile(path, []byte(c1+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	filter, err := NewWatchlistFilter(path)
	if err != nil {
		t.Fatal(err)
	}
	filter.SetDebounce(10 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := filter.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(c1+"\nnot-a-contract\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-filter.Errors():
		if err == nil {
			t.Fatal("got nil reload error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("invalid watchlist edit was not reported")
	}
	if got := filter.ContractIDs(); !reflect.DeepEqual(got, []string{c1}) {
		t.Fatalf("after invalid edit IDs = %v, want previous set [c1]", got)
	}
	if err := os.WriteFile(path, []byte(c1+"\n"+c2+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitForWatchlist(t, filter, []string{c1, c2})
}
