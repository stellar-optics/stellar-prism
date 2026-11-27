package sink_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// fakeTimescale is a TimescaleDatabase that records what it was asked to do.
// It stands in for PostgreSQL so the batching and shutdown behaviour can be
// tested without a database or a network.
type fakeTimescale struct {
	mu sync.Mutex

	execs    []string
	execFn   func(sql string) error
	copyFn   func(attempt int) error
	shortBy  int
	attempts int
	copies   int
	rows     [][]any
	tables   []string
	columns  [][]string
}

func (f *fakeTimescale) Exec(_ context.Context, sql string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execs = append(f.execs, sql)
	if f.execFn != nil {
		return f.execFn(sql)
	}
	return nil
}

func (f *fakeTimescale) CopyRows(_ context.Context, table string, columns []string, rows [][]any) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	attempt := f.attempts
	f.attempts++
	if f.copyFn != nil {
		if err := f.copyFn(attempt); err != nil {
			return 0, err
		}
	}
	for _, r := range rows {
		f.rows = append(f.rows, append([]any(nil), r...))
	}
	f.copies++
	f.tables = append(f.tables, table)
	f.columns = append(f.columns, append([]string(nil), columns...))
	return int64(len(rows) - f.shortBy), nil
}

func (f *fakeTimescale) snapshot() (copies int, rows [][]any, execs []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.copies, append([][]any(nil), f.rows...), append([]string(nil), f.execs...)
}

// newTestTimescale builds a sink over the fake, closing it at cleanup.
func newTestTimescale(t *testing.T, db *fakeTimescale, cfg sink.TimescaleConfig) *sink.Timescale {
	t.Helper()
	s, err := sink.NewTimescaleWithDatabase(context.Background(), db, cfg)
	if err != nil {
		t.Fatalf("NewTimescaleWithDatabase() error = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestTimescaleCreatesSchemaAndHypertable(t *testing.T) {
	t.Parallel()

	db := &fakeTimescale{}
	s := newTestTimescale(t, db, sink.TimescaleConfig{})

	_, _, execs := db.snapshot()
	if len(execs) != 2 {
		t.Fatalf("issued %d statements, want 2 (create table, create hypertable):\n%s",
			len(execs), strings.Join(execs, "\n"))
	}
	if !strings.Contains(execs[0], "CREATE TABLE IF NOT EXISTS \"prism_records\"") {
		t.Errorf("first statement is not the table DDL:\n%s", execs[0])
	}
	if !strings.Contains(execs[0], "PRIMARY KEY (id, ledger)") {
		t.Errorf("table DDL has no idempotency key:\n%s", execs[0])
	}
	if !strings.Contains(execs[1], "create_hypertable('prism_records', 'ledger'") {
		t.Errorf("second statement does not create a hypertable partitioned by ledger:\n%s", execs[1])
	}
	if !s.Hypertable() {
		t.Error("Hypertable() = false after a successful create_hypertable")
	}
}

// TestTimescaleFallsBackOnPlainPostgres covers the documented deployment
// without the TimescaleDB extension: the sink must still work.
func TestTimescaleFallsBackOnPlainPostgres(t *testing.T) {
	t.Parallel()

	db := &fakeTimescale{}
	db.execFn = func(sql string) error {
		if strings.Contains(sql, "create_hypertable") {
			return &pgconn.PgError{Code: "42883", Message: "function create_hypertable does not exist"}
		}
		return nil
	}

	s := newTestTimescale(t, db, sink.TimescaleConfig{})
	if s.Hypertable() {
		t.Error("Hypertable() = true on a server without TimescaleDB")
	}

	// A fallback table must still accept rows.
	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if copies, _, _ := db.snapshot(); copies != 1 {
		t.Errorf("copied %d batches, want 1", copies)
	}
}

func TestTimescaleSurfacesRealSchemaErrors(t *testing.T) {
	t.Parallel()

	boom := errors.New("permission denied for database")
	db := &fakeTimescale{}
	db.execFn = func(sql string) error {
		if strings.Contains(sql, "create_hypertable") {
			return &pgconn.PgError{Code: "42P01", Message: "relation does not exist"}
		}
		return boom
	}

	_, err := sink.NewTimescaleWithDatabase(context.Background(), db, sink.TimescaleConfig{})
	if err == nil {
		t.Fatal("NewTimescaleWithDatabase() error = nil, want the schema failure")
	}
	if !errors.Is(err, boom) {
		t.Errorf("error = %v, want it to wrap the underlying failure", err)
	}
}

func TestTimescaleRejectsBadConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  sink.TimescaleConfig
	}{
		{name: "too many qualifiers", cfg: sink.TimescaleConfig{Table: "a.b.c"}},
		{name: "empty segment", cfg: sink.TimescaleConfig{Table: "public..records"}},
		{name: "sql injection", cfg: sink.TimescaleConfig{Table: "x; DROP TABLE y"}},
		{name: "leading digit", cfg: sink.TimescaleConfig{Table: "1records"}},
		{name: "negative batch", cfg: sink.TimescaleConfig{BatchSize: -1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := sink.NewTimescaleWithDatabase(context.Background(), &fakeTimescale{}, tc.cfg); err == nil {
				t.Fatal("NewTimescaleWithDatabase() error = nil, want a validation failure")
			}
		})
	}
}

func TestTimescaleRequiresConnectionString(t *testing.T) {
	t.Parallel()

	if _, err := sink.NewTimescale(context.Background(), "  ", sink.TimescaleConfig{}); err == nil {
		t.Fatal("NewTimescale(empty dsn) error = nil, want an error")
	}
}

// TestTimescaleFlushesOnBatchSize covers the count trigger and the flattened
// row shape, which is the part analytical queries depend on.
func TestTimescaleFlushesOnBatchSize(t *testing.T) {
	t.Parallel()

	db := &fakeTimescale{}
	s := newTestTimescale(t, db, sink.TimescaleConfig{BatchSize: 3, Table: "analytics.prism"})

	event := eventRecord()
	ledger := ledgerRecord()
	tx := stream.Record{
		ID:       "0000000100-0001",
		Sequence: 100,
		Time:     time.Unix(1700000100, 0).UTC(),
		Kind:     stream.KindTransaction,
		Transaction: &stream.Transaction{
			Hash:          "feedface",
			Index:         1,
			Successful:    false,
			EnvelopeXDR:   "env",
			ResultXDR:     "res",
			ResultMetaXDR: "meta",
		},
	}

	if err := s.Emit(event); err != nil {
		t.Fatalf("Emit(event) error = %v", err)
	}
	if err := s.Emit(ledger); err != nil {
		t.Fatalf("Emit(ledger) error = %v", err)
	}
	if copies, _, _ := db.snapshot(); copies != 0 {
		t.Fatalf("copied %d batches before the batch filled, want 0", copies)
	}

	if err := s.Emit(tx); err != nil {
		t.Fatalf("Emit(tx) error = %v", err)
	}

	copies, rows, _ := db.snapshot()
	if copies != 1 {
		t.Fatalf("copied %d batches after the batch filled, want 1", copies)
	}
	if len(rows) != 3 {
		t.Fatalf("copied %d rows, want 3", len(rows))
	}
	if got := db.tables[0]; got != "analytics.prism" {
		t.Errorf("copied into %q, want analytics.prism", got)
	}
	if got := s.Written(); got != 3 {
		t.Errorf("Written() = %d, want 3", got)
	}
	if got := s.Flushes(); got != 1 {
		t.Errorf("Flushes() = %d, want 1", got)
	}

	// Event row: kind-specific columns set, others NULL.
	if rows[0][0] != event.ID || rows[0][2] != "event" {
		t.Errorf("event row id/kind = %v/%v, want %v/event", rows[0][0], rows[0][2], event.ID)
	}
	if rows[0][4] != nil {
		t.Errorf("event row ledger hash = %v, want NULL", rows[0][4])
	}
	if rows[0][5] != event.Event.ContractID {
		t.Errorf("event row contract = %v, want %v", rows[0][5], event.Event.ContractID)
	}
	if topics, ok := rows[0][10].([]string); !ok || len(topics) != 1 {
		t.Errorf("event row topics = %#v, want the base64 topic slice", rows[0][10])
	}
	if rows[0][9] != nil {
		t.Errorf("event row successful = %v, want NULL", rows[0][9])
	}

	// Ledger row: close hash and metadata set, event fields NULL.
	if rows[1][4] != "deadbeef" {
		t.Errorf("ledger row hash = %v, want deadbeef", rows[1][4])
	}
	if rows[1][11] != nil {
		t.Errorf("ledger row value = %v, want NULL", rows[1][11])
	}

	// Transaction row: index and outcome set, false is preserved as false.
	if rows[2][6] != "feedface" {
		t.Errorf("transaction row hash = %v, want feedface", rows[2][6])
	}
	if rows[2][9] != false {
		t.Errorf("transaction row successful = %v, want false (not NULL)", rows[2][9])
	}
	if rows[2][13] != "res" {
		t.Errorf("transaction row result = %v, want res", rows[2][13])
	}
}

// TestTimescaleFlushesOnInterval covers the timer path: a slow stream must not
// hold rows in memory indefinitely.
func TestTimescaleFlushesOnInterval(t *testing.T) {
	t.Parallel()

	db := &fakeTimescale{}
	s := newTestTimescale(t, db, sink.TimescaleConfig{
		BatchSize:     1000,
		FlushInterval: 5 * time.Millisecond,
	})

	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	waitFor(t, "the interval flush", func() bool {
		copies, _, _ := db.snapshot()
		return copies == 1
	})
	if got := s.Written(); got != 1 {
		t.Errorf("Written() = %d, want 1", got)
	}
}

// TestTimescaleKeepsBatchOnCopyFailure is the retry contract: a failed COPY
// inserted nothing, so the rows stay buffered for the next attempt.
func TestTimescaleKeepsBatchOnCopyFailure(t *testing.T) {
	t.Parallel()

	boom := errors.New("server closed the connection")
	db := &fakeTimescale{}
	db.copyFn = func(call int) error {
		if call == 0 {
			return boom
		}
		return nil
	}

	s := newTestTimescale(t, db, sink.TimescaleConfig{BatchSize: 2})

	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	err := s.Emit(ledgerRecord())
	if err == nil {
		t.Fatal("Emit() error = nil, want the copy failure")
	}
	if !errors.Is(err, boom) {
		t.Errorf("Emit() error = %v, want it to wrap the copy failure", err)
	}

	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() after the failure error = %v, want the retained batch to retry", err)
	}
	copies, rows, _ := db.snapshot()
	if copies != 1 {
		t.Fatalf("copy succeeded %d times, want 1", copies)
	}
	db.mu.Lock()
	attempts := db.attempts
	db.mu.Unlock()
	if attempts != 2 {
		t.Fatalf("made %d copy attempts, want 2 (one failed, one succeeded)", attempts)
	}
	if len(rows) != 2 {
		t.Errorf("copied %d rows in total, want 2 — the failed batch must be retried, not dropped", len(rows))
	}
}

// TestTimescaleBackgroundFailureReachesNextEmit covers the timer path, which
// has no caller to return to: the error must surface on the next Emit rather
// than be swallowed.
func TestTimescaleBackgroundFailureReachesNextEmit(t *testing.T) {
	t.Parallel()

	boom := errors.New("connection refused")
	db := &fakeTimescale{}
	db.copyFn = func(int) error { return boom }

	s := newTestTimescale(t, db, sink.TimescaleConfig{
		BatchSize:     100,
		FlushInterval: 5 * time.Millisecond,
	})
	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	// A failed COPY keeps the batch buffered and every tick retries it, so by
	// the time this poll runs the timer may have made several attempts. Wait
	// for at least one rather than exactly one: on a loaded runner the count
	// can step past 1 between polls, and "== 1" would then never hold.
	waitFor(t, "the background flush", func() bool {
		db.mu.Lock()
		defer db.mu.Unlock()
		return db.attempts >= 1
	})

	if err := s.Emit(eventRecord()); !errors.Is(err, boom) {
		t.Errorf("Emit() error = %v, want the background flush failure", err)
	}
}

// TestTimescaleShortCopyIsAnError covers a driver that reports fewer rows than
// it was given: silent data loss must fail the stream instead of being counted
// as success.
func TestTimescaleShortCopyIsAnError(t *testing.T) {
	t.Parallel()

	db := &fakeTimescale{shortBy: 1}
	s := newTestTimescale(t, db, sink.TimescaleConfig{BatchSize: 100})

	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	r := eventRecord()
	r.ID = "0000000101"
	if err := s.Emit(r); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	err := s.Flush()
	if err == nil {
		t.Fatal("Flush() error = nil, want a short-copy failure")
	}
	if !strings.Contains(err.Error(), "wrote 1 of 2 rows") {
		t.Errorf("error = %q, want it to report the short copy", err)
	}
	if got := s.Written(); got != 0 {
		t.Errorf("Written() = %d after a short copy, want 0", got)
	}

	// The rows were retained, so a recovered database still receives them.
	db.mu.Lock()
	db.shortBy = 0
	db.mu.Unlock()
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() after recovery error = %v", err)
	}
	if got := s.Written(); got != 2 {
		t.Errorf("Written() = %d after recovery, want 2", got)
	}
}

func TestTimescaleFlushIsIdempotent(t *testing.T) {
	t.Parallel()

	db := &fakeTimescale{}
	s := newTestTimescale(t, db, sink.TimescaleConfig{BatchSize: 100})

	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() on an empty batch error = %v", err)
	}
	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("second Flush() error = %v", err)
	}

	copies, rows, _ := db.snapshot()
	if copies != 1 || len(rows) != 1 {
		t.Errorf("copies = %d, rows = %d, want 1 and 1 — a duplicate flush wrote nothing twice", copies, len(rows))
	}
	if got := s.Flushes(); got != 1 {
		t.Errorf("Flushes() = %d, want 1 — an empty flush is not a COPY", got)
	}
}

func TestTimescaleCloseDrainsStopsAndIsIdempotent(t *testing.T) {
	t.Parallel()

	db := &fakeTimescale{}
	s, err := sink.NewTimescaleWithDatabase(context.Background(), db, sink.TimescaleConfig{BatchSize: 100})
	if err != nil {
		t.Fatalf("NewTimescaleWithDatabase() error = %v", err)
	}
	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	copies, rows, _ := db.snapshot()
	if copies != 1 || len(rows) != 1 {
		t.Errorf("after Close copies = %d, rows = %d, want 1 and 1 — Close must flush the final batch", copies, len(rows))
	}

	// A second Close must not deadlock or flush again.
	if err := s.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if copies, _, _ := db.snapshot(); copies != 1 {
		t.Errorf("copies after second Close = %d, want 1", copies)
	}

	if err := s.Emit(eventRecord()); !errors.Is(err, sink.ErrTimescaleClosed) {
		t.Errorf("Emit() after Close error = %v, want ErrTimescaleClosed", err)
	}
	if err := s.Flush(); err != nil {
		t.Errorf("Flush() after Close error = %v, want nil", err)
	}
	if got := s.Flushes(); got != 1 {
		t.Errorf("Flushes() = %d after Close, want 1", got)
	}
	if got := s.Written(); got != 1 {
		t.Errorf("Written() = %d after Close, want 1", got)
	}
}

func TestTimescaleRejectsUnknownKind(t *testing.T) {
	t.Parallel()

	db := &fakeTimescale{}
	s := newTestTimescale(t, db, sink.TimescaleConfig{})

	bad := stream.Record{ID: "x", Kind: stream.Kind("nonsense")}
	if err := s.Emit(bad); err == nil {
		t.Error("Emit(unknown kind) error = nil, want an error")
	}
	if err := s.Emit(stream.Record{ID: "x", Kind: stream.KindEvent}); err == nil {
		t.Error("Emit(event without payload) error = nil, want an error")
	}
	if err := s.Emit(stream.Record{ID: "x", Kind: stream.KindLedger}); err == nil {
		t.Error("Emit(ledger without payload) error = nil, want an error")
	}
	if err := s.Emit(stream.Record{ID: "x", Kind: stream.KindTransaction}); err == nil {
		t.Error("Emit(transaction without payload) error = nil, want an error")
	}
}

func TestTimescaleZeroTimeIsNull(t *testing.T) {
	t.Parallel()

	db := &fakeTimescale{}
	s := newTestTimescale(t, db, sink.TimescaleConfig{BatchSize: 1})

	r := eventRecord()
	r.Time = time.Time{}
	if err := s.Emit(r); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	_, rows, _ := db.snapshot()
	if len(rows) != 1 {
		t.Fatalf("copied %d rows, want 1", len(rows))
	}
	if rows[0][3] != nil {
		t.Errorf("closed_at = %v, want NULL for an undated record", rows[0][3])
	}
}

// TestTimescaleHonoursNoHypertable covers the explicit opt-out.
func TestTimescaleHonoursNoHypertable(t *testing.T) {
	t.Parallel()

	db := &fakeTimescale{}
	s := newTestTimescale(t, db, sink.TimescaleConfig{NoHypertable: true})

	_, _, execs := db.snapshot()
	if len(execs) != 1 {
		t.Fatalf("issued %d statements, want only the table DDL", len(execs))
	}
	if s.Hypertable() {
		t.Error("Hypertable() = true with NoHypertable set")
	}
}
