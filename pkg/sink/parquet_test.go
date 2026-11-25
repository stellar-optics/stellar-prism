package sink_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2" // reads the written parts back in tests

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func newTestParquet(t *testing.T, cfg sink.ParquetConfig) *sink.Parquet {
	t.Helper()
	s, err := sink.NewParquet(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewParquet() error = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// partRows is what a reader sees after the sink's work: the part files read
// back through DuckDB, which is exactly how a user would query them.
type partRow struct {
	ID       string
	Ledger   uint64
	Kind     string
	ClosedAt sql.NullTime
	Topics   []string
	Value    sql.NullString
	OK       sql.NullBool
}

func readParts(t *testing.T, dir string) []partRow {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("opening duckdb to read parts: %v", err)
	}
	defer func() { _ = db.Close() }()

	pattern := strings.ReplaceAll(filepath.Join(dir, "*.parquet"), "'", "''")
	rows, err := db.Query(fmt.Sprintf(
		`SELECT id, ledger, kind, closed_at, topics_xdr, value_xdr, successful
		 FROM read_parquet('%s') ORDER BY id`, pattern))
	if err != nil {
		t.Fatalf("reading parts: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var out []partRow
	for rows.Next() {
		var (
			r      partRow
			topics any
		)
		if err := rows.Scan(&r.ID, &r.Ledger, &r.Kind, &r.ClosedAt, &topics, &r.Value, &r.OK); err != nil {
			t.Fatalf("scanning part row: %v", err)
		}
		r.Topics = stringsOf(topics)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating parts: %v", err)
	}
	return out
}

func stringsOf(v any) []string {
	switch got := v.(type) {
	case nil:
		return nil
	case []string:
		return got
	case []any:
		out := make([]string, 0, len(got))
		for _, e := range got {
			s, ok := e.(string)
			if !ok {
				return nil
			}
			out = append(out, s)
		}
		return out
	default:
		return nil
	}
}

func partFiles(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
	if err != nil {
		t.Fatalf("globbing parts: %v", err)
	}
	return matches
}

func transactionRecord() stream.Record {
	return stream.Record{
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
}

// TestParquetWritesQueryablePartFile is the end-to-end contract: records go in,
// a real Parquet file comes out, and DuckDB can read it back with the right
// schema and values.
func TestParquetWritesQueryablePartFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s := newTestParquet(t, sink.ParquetConfig{Dir: dir, BatchSize: 100})

	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit(event) error = %v", err)
	}
	if err := s.Emit(ledgerRecord()); err != nil {
		t.Fatalf("Emit(ledger) error = %v", err)
	}
	if err := s.Emit(transactionRecord()); err != nil {
		t.Fatalf("Emit(transaction) error = %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	files := partFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("wrote %d part files, want 1: %v", len(files), files)
	}
	if got := filepath.Base(files[0]); got != "part-000001.parquet" {
		t.Errorf("part file = %q, want part-000001.parquet", got)
	}
	if info, err := os.Stat(files[0]); err != nil || info.Size() == 0 {
		t.Fatalf("part file is missing or empty: %v", err)
	}

	got := readParts(t, dir)
	if len(got) != 3 {
		t.Fatalf("read back %d rows, want 3", len(got))
	}

	byKind := map[string]partRow{}
	for _, r := range got {
		byKind[r.Kind] = r
	}
	event := byKind["event"]
	if event.Kind != "event" {
		t.Errorf("row 0 kind = %q, want event", event.Kind)
	}
	if event.Ledger != 100 {
		t.Errorf("row 0 ledger = %d, want 100", event.Ledger)
	}
	if !event.ClosedAt.Valid || !event.ClosedAt.Time.Equal(time.Unix(1700000100, 0)) {
		t.Errorf("row 0 closed_at = %v, want the record time", event.ClosedAt)
	}
	if len(event.Topics) != 1 || event.Topics[0] != scValU32 {
		t.Errorf("row 0 topics = %v, want [%s]", event.Topics, scValU32)
	}
	if event.Value.String != scValU32 {
		t.Errorf("row 0 value = %q, want %q", event.Value.String, scValU32)
	}
	if event.OK.Valid {
		t.Error("row 0 successful is set for an event, want NULL")
	}

	if ledger := byKind["ledger"]; ledger.ID != "0000000100" {
		t.Errorf("ledger row = %+v, want id 0000000100", ledger)
	}
	if tx := byKind["transaction"]; !tx.OK.Valid || tx.OK.Bool {
		t.Errorf("transaction row = %+v, want a failed transaction", tx)
	}
}

func TestParquetBatchSizeSplitsParts(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s := newTestParquet(t, sink.ParquetConfig{Dir: dir, BatchSize: 2})

	for i := range 5 {
		r := eventRecord()
		r.ID = fmt.Sprintf("%010d-0000", 100+i)
		if err := s.Emit(r); err != nil {
			t.Fatalf("Emit(%d) error = %v", i, err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if files := partFiles(t, dir); len(files) != 3 {
		t.Fatalf("wrote %d part files, want 3 (2+2+1): %v", len(files), files)
	}
	if got := len(readParts(t, dir)); got != 5 {
		t.Errorf("read back %d rows, want 5 — batches must not lose or duplicate rows", got)
	}
	if got := s.Parts(); got != 3 {
		t.Errorf("Parts() = %d, want 3", got)
	}
	if got := s.Written(); got != 5 {
		t.Errorf("Written() = %d, want 5", got)
	}
}

// TestParquetFlushesOnInterval covers the timer path, which is what makes the
// sink usable for a live tail rather than only for a bounded backfill.
func TestParquetFlushesOnInterval(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s := newTestParquet(t, sink.ParquetConfig{
		Dir:           dir,
		BatchSize:     1000,
		FlushInterval: 5 * time.Millisecond,
	})

	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	waitFor(t, "the interval flush", func() bool {
		return len(partFiles(t, dir)) == 1
	})
	if got := len(readParts(t, dir)); got != 1 {
		t.Errorf("read back %d rows, want 1", got)
	}
}

func TestParquetCloseFlushesAndIsIdempotent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := sink.NewParquet(context.Background(), sink.ParquetConfig{Dir: dir, BatchSize: 100})
	if err != nil {
		t.Fatalf("NewParquet() error = %v", err)
	}
	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if files := partFiles(t, dir); len(files) != 1 {
		t.Fatalf("after Close wrote %d part files, want 1 — Close must flush", len(files))
	}

	if err := s.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if files := partFiles(t, dir); len(files) != 1 {
		t.Errorf("second Close wrote another part (%d files), want 1", len(files))
	}

	if err := s.Emit(eventRecord()); !errors.Is(err, sink.ErrParquetClosed) {
		t.Errorf("Emit() after Close error = %v, want ErrParquetClosed", err)
	}
	if err := s.Flush(); err != nil {
		t.Errorf("Flush() after Close error = %v, want nil", err)
	}
}

// TestParquetNeverOverwritesExistingParts covers a restarted run: it must
// continue the numbering rather than clobbering earlier data.
func TestParquetNeverOverwritesExistingParts(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	existing := filepath.Join(dir, "part-000001.parquet")
	if err := os.WriteFile(existing, []byte("previous run"), 0o600); err != nil {
		t.Fatalf("seeding existing part: %v", err)
	}

	s := newTestParquet(t, sink.ParquetConfig{Dir: dir, BatchSize: 100})
	if err := s.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	contents, err := os.ReadFile(existing)
	if err != nil {
		t.Fatalf("reading existing part: %v", err)
	}
	if string(contents) != "previous run" {
		t.Error("the existing part file was overwritten")
	}
	if _, err := os.Stat(filepath.Join(dir, "part-000002.parquet")); err != nil {
		t.Errorf("the new part was not written after the existing one: %v", err)
	}
}

func TestParquetRejectsBadConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  sink.ParquetConfig
	}{
		{name: "no directory"},
		{name: "prefix with separator", cfg: sink.ParquetConfig{Dir: t.TempDir(), Prefix: "nested/part"}},
		{name: "prefix is dot dot", cfg: sink.ParquetConfig{Dir: t.TempDir(), Prefix: ".."}},
		{name: "unknown compression", cfg: sink.ParquetConfig{Dir: t.TempDir(), Compression: "brotli"}},
		{name: "negative batch", cfg: sink.ParquetConfig{Dir: t.TempDir(), BatchSize: -5}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := sink.NewParquet(context.Background(), tc.cfg); err == nil {
				t.Fatal("NewParquet() error = nil, want a validation failure")
			}
		})
	}
}

func TestParquetRejectsUnknownKind(t *testing.T) {
	t.Parallel()

	s := newTestParquet(t, sink.ParquetConfig{Dir: t.TempDir()})

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

func TestParquetZeroTimeIsNull(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s := newTestParquet(t, sink.ParquetConfig{Dir: dir, BatchSize: 100})

	r := eventRecord()
	r.Time = time.Time{}
	if err := s.Emit(r); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	got := readParts(t, dir)
	if len(got) != 1 {
		t.Fatalf("read back %d rows, want 1", len(got))
	}
	if got[0].ClosedAt.Valid {
		t.Errorf("closed_at = %v, want NULL for an undated record", got[0].ClosedAt)
	}
}

func TestParquetHonoursCompression(t *testing.T) {
	t.Parallel()

	for _, codec := range []string{"snappy", "zstd", "gzip", "uncompressed"} {
		t.Run(codec, func(t *testing.T) {
			dir := t.TempDir()
			s := newTestParquet(t, sink.ParquetConfig{Dir: dir, Compression: codec, BatchSize: 100})
			if err := s.Emit(eventRecord()); err != nil {
				t.Fatalf("Emit() error = %v", err)
			}
			if err := s.Flush(); err != nil {
				t.Fatalf("Flush() error = %v", err)
			}
			if got := len(readParts(t, dir)); got != 1 {
				t.Errorf("read back %d rows with %s, want 1", got, codec)
			}
		})
	}
}

func TestParquetCustomRowGroupSizeAndZstdRoundtrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s := newTestParquet(t, sink.ParquetConfig{
		Dir:          dir,
		Compression:  "zstd",
		RowGroupSize: 64 * 1024 * 1024,
		BatchSize:    10,
	})

	records := []stream.Record{
		eventRecord(),
		ledgerRecord(),
		transactionRecord(),
	}

	for _, r := range records {
		if err := s.Emit(r); err != nil {
			t.Fatalf("Emit() error = %v", err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	parts := readParts(t, dir)
	if len(parts) != len(records) {
		t.Fatalf("read back %d rows, want %d", len(parts), len(records))
	}
}

func TestParquetSinkOptions(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ps, err := sink.NewParquetSink(dir,
		sink.WithParquetSinkCompression("zstd"),
		sink.WithParquetSinkRowGroupSize(64*1024*1024),
	)
	if err != nil {
		t.Fatalf("NewParquetSink() error = %v", err)
	}
	defer ps.Close()

	if err := ps.Emit(eventRecord()); err != nil {
		t.Fatalf("ParquetSink.Emit() error = %v", err)
	}
	if err := ps.Flush(); err != nil {
		t.Fatalf("ParquetSink.Flush() error = %v", err)
	}
}

func TestParquetEmptyFlushWritesNothing(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s := newTestParquet(t, sink.ParquetConfig{Dir: dir})
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if files := partFiles(t, dir); len(files) != 0 {
		t.Errorf("an empty flush wrote %d files, want 0", len(files))
	}
}
