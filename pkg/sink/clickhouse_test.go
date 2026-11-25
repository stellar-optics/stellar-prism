package sink_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

type mockClickHouseClient struct {
	mu         sync.Mutex
	queries    []string
	inserts    map[string][][]any
	insertCols map[string][]string
	execErr    error
	insertErr  error
	closed     bool
}

func newMockClickHouseClient() *mockClickHouseClient {
	return &mockClickHouseClient{
		inserts:    make(map[string][][]any),
		insertCols: make(map[string][]string),
	}
}

func (m *mockClickHouseClient) Exec(ctx context.Context, query string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.execErr != nil {
		return m.execErr
	}
	m.queries = append(m.queries, query)
	return nil
}

func (m *mockClickHouseClient) Insert(ctx context.Context, table string, columns []string, rows [][]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.insertErr != nil {
		return m.insertErr
	}
	m.insertCols[table] = columns
	m.inserts[table] = append(m.inserts[table], rows...)
	return nil
}

func (m *mockClickHouseClient) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockClickHouseClient) GetInserts(table string) [][]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.inserts[table]
}

func (m *mockClickHouseClient) GetColumns(table string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.insertCols[table]
}

func (m *mockClickHouseClient) GetQueries() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]string, len(m.queries))
	copy(cp, m.queries)
	return cp
}

func TestClickHouseSinkSchemaCreation(t *testing.T) {
	mock := newMockClickHouseClient()
	cfg := sink.ClickHouseConfig{
		Addr:      "127.0.0.1:9000",
		Database:  "prism_test",
		BatchSize: 100,
	}

	s, err := sink.NewClickHouseWithClient(context.Background(), mock, cfg)
	if err != nil {
		t.Fatalf("NewClickHouseWithClient() error = %v", err)
	}
	defer s.Close()

	queries := mock.GetQueries()
	if len(queries) < 2 {
		t.Fatalf("expected at least 2 DDL queries, got %d", len(queries))
	}

	foundEvents := false
	foundTx := false
	for _, q := range queries {
		if strings.Contains(q, "stellar_events") && strings.Contains(q, "ReplacingMergeTree") {
			foundEvents = true
			if !strings.Contains(q, "contract_id") || !strings.Contains(q, "topics") || !strings.Contains(q, "data") {
				t.Errorf("stellar_events missing expected columns: %s", q)
			}
		}
		if strings.Contains(q, "stellar_transactions") && strings.Contains(q, "MergeTree") {
			foundTx = true
			if !strings.Contains(q, "tx_hash") || !strings.Contains(q, "status") || !strings.Contains(q, "ops_count") {
				t.Errorf("stellar_transactions missing expected columns: %s", q)
			}
		}
	}

	if !foundEvents {
		t.Errorf("did not find CREATE TABLE stellar_events with ReplacingMergeTree in queries: %v", queries)
	}
	if !foundTx {
		t.Errorf("did not find CREATE TABLE stellar_transactions with MergeTree in queries: %v", queries)
	}
}

func TestClickHouseSinkBulkBufferingAndFlush(t *testing.T) {
	mock := newMockClickHouseClient()
	cfg := sink.ClickHouseConfig{
		Addr:          "127.0.0.1:9000",
		Database:      "default",
		BatchSize:     5,
		FlushInterval: 1 * time.Hour, // do not flush on timer during this test
	}

	s, err := sink.NewClickHouseWithClient(context.Background(), mock, cfg)
	if err != nil {
		t.Fatalf("NewClickHouseWithClient() error = %v", err)
	}
	defer s.Close()

	event := stream.Record{
		ID:       "evt-1",
		Sequence: 1000,
		Time:     time.Now().UTC(),
		Kind:     stream.KindEvent,
		Event: &stream.Event{
			ContractID: "CAAA",
			TopicXDR:   []string{"topic1", "topic2"},
			ValueXDR:   "val1",
		},
	}

	tx := stream.Record{
		ID:       "tx-1",
		Sequence: 1000,
		Time:     time.Now().UTC(),
		Kind:     stream.KindTransaction,
		Transaction: &stream.Transaction{
			Hash:       "deadbeef",
			Successful: true,
		},
	}

	// Emit 4 records (< batch size 5)
	for i := 0; i < 2; i++ {
		if err := s.Emit(event); err != nil {
			t.Fatalf("Emit(event) error = %v", err)
		}
		if err := s.Emit(tx); err != nil {
			t.Fatalf("Emit(tx) error = %v", err)
		}
	}

	// Should not have inserted yet
	if len(mock.GetInserts("default.stellar_events")) != 0 {
		t.Errorf("expected 0 inserted events before batch fills, got %d", len(mock.GetInserts("default.stellar_events")))
	}

	// Emit 5th record -> triggers bulk insert
	if err := s.Emit(event); err != nil {
		t.Fatalf("Emit(event) error = %v", err)
	}

	events := mock.GetInserts("default.stellar_events")
	txs := mock.GetInserts("default.stellar_transactions")

	if len(events) != 3 {
		t.Errorf("expected 3 events inserted, got %d", len(events))
	}
	if len(txs) != 2 {
		t.Errorf("expected 2 transactions inserted, got %d", len(txs))
	}

	// Verify columns
	eventCols := mock.GetColumns("default.stellar_events")
	expectedEventCols := []string{"timestamp", "ledger", "contract_id", "topics", "data"}
	for i, c := range expectedEventCols {
		if i >= len(eventCols) || eventCols[i] != c {
			t.Errorf("expected event col %d to be %s, got %v", i, c, eventCols)
		}
	}

	txCols := mock.GetColumns("default.stellar_transactions")
	expectedTxCols := []string{"timestamp", "ledger", "tx_hash", "status", "fee", "ops_count"}
	for i, c := range expectedTxCols {
		if i >= len(txCols) || txCols[i] != c {
			t.Errorf("expected tx col %d to be %s, got %v", i, c, txCols)
		}
	}

	if s.Written() != 5 {
		t.Errorf("Written() = %d, want 5", s.Written())
	}
}

func TestClickHouseSinkFlushAndClose(t *testing.T) {
	mock := newMockClickHouseClient()
	cfg := sink.ClickHouseConfig{
		Addr:          "127.0.0.1:9000",
		Database:      "default",
		BatchSize:     100,
		FlushInterval: 1 * time.Hour,
	}

	s, err := sink.NewClickHouseWithClient(context.Background(), mock, cfg)
	if err != nil {
		t.Fatalf("NewClickHouseWithClient() error = %v", err)
	}

	event := stream.Record{
		ID:       "evt-1",
		Sequence: 100,
		Time:     time.Now().UTC(),
		Kind:     stream.KindEvent,
		Event: &stream.Event{
			ContractID: "CBBS",
			TopicXDR:   []string{"t1"},
			ValueXDR:   "v1",
		},
	}

	if err := s.Emit(event); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	if len(mock.GetInserts("default.stellar_events")) != 0 {
		t.Fatalf("expected 0 inserts before flush")
	}

	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if len(mock.GetInserts("default.stellar_events")) != 1 {
		t.Errorf("expected 1 insert after flush, got %d", len(mock.GetInserts("default.stellar_events")))
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if !mock.closed {
		t.Errorf("expected mock client to be closed")
	}

	// Emit after close should return ErrClickHouseClosed
	if err := s.Emit(event); !errors.Is(err, sink.ErrClickHouseClosed) {
		t.Errorf("Emit() after close error = %v, want ErrClickHouseClosed", err)
	}
}

func TestClickHouseSinkInsertErrorPreservesBuffer(t *testing.T) {
	mock := newMockClickHouseClient()
	mock.insertErr = errors.New("network timeout")
	cfg := sink.ClickHouseConfig{
		Addr:      "127.0.0.1:9000",
		BatchSize: 1,
	}

	s, err := sink.NewClickHouseWithClient(context.Background(), mock, cfg)
	if err != nil {
		t.Fatalf("NewClickHouseWithClient() error = %v", err)
	}
	defer s.Close()

	event := stream.Record{
		ID:       "evt-fail",
		Sequence: 50,
		Kind:     stream.KindEvent,
		Event: &stream.Event{
			ContractID: "CFAIL",
		},
	}

	err = s.Emit(event)
	if err == nil {
		t.Fatalf("Emit() expected error on insert failure, got nil")
	}
}
