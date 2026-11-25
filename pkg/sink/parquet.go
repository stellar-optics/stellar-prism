package sink

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2" // registers the duckdb SQL driver
	"github.com/parquet-go/parquet-go"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// ParquetSink stores the stable record envelope in date partitions. Rows are
// written incrementally; Flush closes the current row group without retaining
// stream data in memory.
type ParquetSink struct {
	dir          string
	mu           sync.Mutex
	day          string
	file         *os.File
	writer       *parquet.GenericWriter[parquetSinkRow]
	compression  string
	rowGroupSize int64
}

type parquetSinkRow struct {
	ID      string `parquet:"id"`
	Kind    string `parquet:"kind"`
	Ledger  uint32 `parquet:"ledger"`
	Time    int64  `parquet:"time"`
	Payload string `parquet:"payload"`
}

type ParquetSinkOption func(*ParquetSink)

func WithParquetSinkCompression(codec string) ParquetSinkOption {
	return func(s *ParquetSink) { s.compression = codec }
}

func WithParquetSinkRowGroupSize(size int64) ParquetSinkOption {
	return func(s *ParquetSink) { s.rowGroupSize = size }
}

func NewParquetSink(dir string, opts ...ParquetSinkOption) (*ParquetSink, error) {
	if dir == "" {
		return nil, fmt.Errorf("parquet: directory is required")
	}
	s := &ParquetSink{
		dir:          dir,
		compression:  DefaultParquetCompression,
		rowGroupSize: DefaultParquetRowGroupSize,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

func (s *ParquetSink) Emit(r stream.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	day := r.Time.UTC().Format("2006-01-02")
	if day == "0001-01-01" {
		day = time.Now().UTC().Format("2006-01-02")
	}
	if day != s.day {
		if err := s.closeLocked(); err != nil {
			return err
		}
		path := filepath.Join(s.dir, "dt="+day)
		if err := os.MkdirAll(path, 0o755); err != nil {
			return err
		}
		f, err := os.Create(filepath.Join(path, "records.parquet"))
		if err != nil {
			return err
		}
		s.file = f
		var writerOpts []parquet.WriterOption
		switch strings.ToLower(s.compression) {
		case "snappy":
			writerOpts = append(writerOpts, parquet.Compression(&parquet.Snappy))
		case "zstd":
			writerOpts = append(writerOpts, parquet.Compression(&parquet.Zstd))
		case "gzip":
			writerOpts = append(writerOpts, parquet.Compression(&parquet.Gzip))
		case "uncompressed":
			writerOpts = append(writerOpts, parquet.Compression(&parquet.Uncompressed))
		}
		if s.rowGroupSize > 0 {
			writerOpts = append(writerOpts, parquet.WriteBufferSize(int(s.rowGroupSize)))
		}
		s.writer = parquet.NewGenericWriter[parquetSinkRow](f, writerOpts...)
		s.day = day
	}
	b, err := MarshalRecord(r, false)
	if err != nil {
		return err
	}
	_, err = s.writer.Write([]parquetSinkRow{{r.ID, string(r.Kind), r.Sequence, r.Time.UnixMilli(), string(b)}})
	return err
}

func (s *ParquetSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writer != nil {
		return s.writer.Flush()
	}
	return nil
}

func (s *ParquetSink) Close() error { s.mu.Lock(); defer s.mu.Unlock(); return s.closeLocked() }

func (s *ParquetSink) closeLocked() error {
	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			return err
		}
		s.writer = nil
	}
	if s.file != nil {
		err := s.file.Close()
		s.file = nil
		return err
	}
	return nil
}

// Defaults for the Parquet sink.
const (
	// DefaultParquetBatchSize is the number of rows per part file. Large
	// enough that files are worth reading, small enough that a live stream
	// still produces parts as it goes.
	DefaultParquetBatchSize = 5000
	// DefaultParquetFlushInterval bounds how long a partial part waits.
	DefaultParquetFlushInterval = 5 * time.Second
	// DefaultParquetFlushTimeout bounds a single COPY.
	DefaultParquetFlushTimeout = 30 * time.Second
	// DefaultParquetPrefix is the part file name prefix.
	DefaultParquetPrefix = "part"
	// DefaultParquetCompression is the Parquet compression codec.
	DefaultParquetCompression = "snappy"
	// DefaultParquetRowGroupSize is the row group size in bytes (128MB).
	DefaultParquetRowGroupSize int64 = 128 * 1024 * 1024
)

// ErrParquetClosed is returned by Emit once the sink has been closed.
var ErrParquetClosed = errors.New("parquet: sink is closed")

// parquetCompressions is the allow-list of codecs. The codec is interpolated
// into SQL, so it is validated rather than passed through.
var parquetCompressions = map[string]bool{
	"zstd":         true,
	"snappy":       true,
	"gzip":         true,
	"uncompressed": true,
}

// ParquetConfig configures a Parquet sink. The zero value is usable provided
// Dir is set.
type ParquetConfig struct {
	// Dir is the directory part files are written into. It is created if it
	// does not exist.
	Dir string
	// Prefix is the part file name prefix; files are named
	// <prefix>-000001.parquet. Existing files are never overwritten.
	Prefix string
	// BatchSize is how many rows make up one part file.
	BatchSize int
	// FlushInterval bounds how long a partial part waits before a background
	// flush.
	FlushInterval time.Duration
	// FlushTimeout bounds a single COPY.
	FlushTimeout time.Duration
	// Compression is the Parquet codec: zstd, snappy, gzip or uncompressed.
	Compression string
	// RowGroupSize controls the row group buffer size before writing columnar chunks.
	RowGroupSize int64
}

func (c ParquetConfig) withDefaults() ParquetConfig {
	if c.Prefix == "" {
		c.Prefix = DefaultParquetPrefix
	}
	if c.BatchSize == 0 {
		c.BatchSize = DefaultParquetBatchSize
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = DefaultParquetFlushInterval
	}
	if c.FlushTimeout <= 0 {
		c.FlushTimeout = DefaultParquetFlushTimeout
	}
	if c.Compression == "" {
		c.Compression = DefaultParquetCompression
	}
	if c.RowGroupSize <= 0 {
		c.RowGroupSize = DefaultParquetRowGroupSize
	}
	return c
}

func (c ParquetConfig) validate() error {
	if strings.TrimSpace(c.Dir) == "" {
		return fmt.Errorf("parquet: an output directory is required")
	}
	if c.Prefix != filepath.Base(c.Prefix) || c.Prefix == "." || c.Prefix == ".." {
		return fmt.Errorf("parquet: prefix %q must be a plain file name", c.Prefix)
	}
	if c.BatchSize < 0 {
		return fmt.Errorf("parquet: batch size must not be negative, got %d", c.BatchSize)
	}
	if c.RowGroupSize < 0 {
		return fmt.Errorf("parquet: row group size must not be negative, got %d", c.RowGroupSize)
	}
	if !parquetCompressions[strings.ToLower(c.Compression)] {
		return fmt.Errorf(
			"parquet: unknown compression %q; expected one of zstd, snappy, gzip, uncompressed",
			c.Compression)
	}
	return nil
}

// Parquet writes records as columnar Parquet part files, encoded by an
// embedded DuckDB instance.
//
// Rows are staged in an in-memory DuckDB table and copied out with
// `COPY … TO`. Each flush becomes one part file, so memory stays bounded even
// on a long backfill and a matching glob (`read_parquet('dir/*.parquet')`)
// reads the whole stream back. Part files are written to a temporary name and
// renamed, so an interrupted COPY cannot leave a truncated part behind.
//
// A background goroutine performs the timed flushes; Close stops it and waits
// for it to finish.
type Parquet struct {
	db   *sql.DB
	stmt *sql.Stmt
	cfg  ParquetConfig

	mu         sync.Mutex
	pending    int
	pendingErr error
	closed     bool
	partSeq    int
	parts      int
	written    int64

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// NewParquet starts a Parquet sink writing part files into cfg.Dir.
//
// It uses DuckDB in memory purely as the Parquet encoder; nothing is
// persisted outside the part files themselves.
func NewParquet(ctx context.Context, cfg ParquetConfig) (*Parquet, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	dir, err := filepath.Abs(cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("parquet: resolving %s: %w", cfg.Dir, err)
	}
	cfg.Dir = dir
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("parquet: creating %s: %w", dir, err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("parquet: opening duckdb: %w", err)
	}
	// One connection serialises access to the in-memory staging table, which
	// keeps the table visible to every call regardless of pooling.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("parquet: opening duckdb: %w", err)
	}
	if _, err := db.ExecContext(ctx, "SET preserve_insertion_order=false"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("parquet: configuring duckdb: %w", err)
	}
	if _, err := db.ExecContext(ctx, createParquetTableSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("parquet: preparing staging table: %w", err)
	}
	stmt, err := db.PrepareContext(ctx, insertParquetRowSQL)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("parquet: preparing insert: %w", err)
	}

	t := &Parquet{
		db:   db,
		stmt: stmt,
		cfg:  cfg,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	t.partSeq = highestPart(t.cfg.Dir, t.cfg.Prefix)
	go t.run()
	return t, nil
}

// Emit implements stream.Sink by staging the row in DuckDB and writing a part
// file once the batch fills.
func (t *Parquet) Emit(r stream.Record) error {
	row, err := parquetRow(r)
	if err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrParquetClosed
	}
	if t.pendingErr != nil {
		err := t.pendingErr
		t.pendingErr = nil
		return err
	}

	if _, err := t.stmt.ExecContext(context.Background(), row...); err != nil {
		return fmt.Errorf("parquet: staging record %s: %w", r.ID, err)
	}
	t.pending++
	if t.pending < t.cfg.BatchSize {
		return nil
	}
	return t.flushLocked(context.Background())
}

// Flush implements stream.Sink by writing any staged rows as a part file now.
func (t *Parquet) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}
	err := t.flushLocked(context.Background())
	if err == nil {
		t.pendingErr = nil
	}
	return err
}

// Close writes the final part, stops the background flusher and releases
// DuckDB. It is safe to call more than once, and always waits for the
// goroutine it starts.
func (t *Parquet) Close() error {
	t.closeOnce.Do(func() {
		close(t.stop)
		<-t.done

		t.mu.Lock()
		t.closed = true
		err := t.flushLocked(context.Background())
		t.mu.Unlock()

		t.closeErr = errors.Join(err, t.stmt.Close(), t.db.Close())
	})
	return t.closeErr
}

// Parts reports how many part files have been written.
func (t *Parquet) Parts() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.parts
}

// Written reports how many rows have been written to part files.
func (t *Parquet) Written() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.written
}

// run performs the periodic flush, and exits as soon as Close signals it.
func (t *Parquet) run() {
	defer close(t.done)

	ticker := time.NewTicker(t.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.stop:
			return
		case <-ticker.C:
			t.mu.Lock()
			if err := t.flushLocked(context.Background()); err != nil && t.pendingErr == nil {
				t.pendingErr = err
			}
			t.mu.Unlock()
		}
	}
}

// flushLocked copies the staged rows into a new part file. The caller must
// hold t.mu.
func (t *Parquet) flushLocked(ctx context.Context) error {
	if t.pending == 0 {
		return nil
	}

	path, err := t.nextPartPath()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, t.cfg.FlushTimeout)
	defer cancel()

	tmp := path + ".tmp"
	if _, err := t.db.ExecContext(ctx, copyParquetSQL(tmp, t.cfg.Compression, t.cfg.RowGroupSize)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("parquet: writing %s: %w", path, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("parquet: finalising %s: %w", path, err)
	}

	rows := t.pending
	// The part is on disk before the staging table is cleared, so a failure
	// below stops the stream without re-writing rows that already landed.
	t.pending = 0
	t.parts++
	t.written += int64(rows)

	if _, err := t.db.ExecContext(ctx, `DELETE FROM prism_records`); err != nil {
		return fmt.Errorf("parquet: clearing staging table after %s: %w", path, err)
	}
	return nil
}

// nextPartPath picks the next free part file name, continuing after any files
// a previous run left in the directory.
func (t *Parquet) nextPartPath() (string, error) {
	for {
		t.partSeq++
		name := fmt.Sprintf("%s-%06d.parquet", t.cfg.Prefix, t.partSeq)
		path := filepath.Join(t.cfg.Dir, name)
		_, err := os.Stat(path)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			return path, nil
		case err != nil:
			return "", fmt.Errorf("parquet: checking %s: %w", path, err)
		}
	}
}

// highestPart returns the highest part number already present in dir, so a
// restarted run appends rather than overwriting.
func highestPart(dir, prefix string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	highest := 0
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, prefix+"-") || !strings.HasSuffix(name, ".parquet") {
			continue
		}
		var n int
		if _, err := fmt.Sscanf(strings.TrimSuffix(strings.TrimPrefix(name, prefix+"-"), ".parquet"), "%d", &n); err != nil {
			continue
		}
		if n > highest {
			highest = n
		}
	}
	return highest
}

const createParquetTableSQL = `CREATE TABLE IF NOT EXISTS prism_records (
	id              VARCHAR   NOT NULL,
	ledger          UBIGINT   NOT NULL,
	kind            VARCHAR   NOT NULL,
	closed_at       TIMESTAMP,
	hash            VARCHAR,
	contract_id     VARCHAR,
	tx_hash         VARCHAR,
	tx_index        UINTEGER,
	op_index        UINTEGER,
	successful      BOOLEAN,
	topics_xdr      VARCHAR[],
	value_xdr       VARCHAR,
	envelope_xdr    VARCHAR,
	result_xdr      VARCHAR,
	result_meta_xdr VARCHAR,
	header_xdr      VARCHAR,
	metadata_xdr    VARCHAR
)`

const insertParquetRowSQL = `INSERT INTO prism_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`

// copyParquetSQL renders the COPY that turns the staging table into one part
// file, ordered by id so output is deterministic.
func copyParquetSQL(path, compression string, rowGroupSize int64) string {
	if rowGroupSize > 0 {
		return fmt.Sprintf(
			`COPY (SELECT * FROM prism_records ORDER BY id) TO '%s' (FORMAT PARQUET, COMPRESSION %s, ROW_GROUP_SIZE_BYTES %d)`,
			escapeLiteral(path), compression, rowGroupSize)
	}
	return fmt.Sprintf(
		`COPY (SELECT * FROM prism_records ORDER BY id) TO '%s' (FORMAT PARQUET, COMPRESSION %s)`,
		escapeLiteral(path), compression)
}

// parquetRow flattens a record into the staging table's column order.
//
// Absent fields are passed as nil so Parquet reads NULL, not a zero value.
// A zero time.Time would otherwise be stored as year 1 rather than NULL.
func parquetRow(r stream.Record) ([]any, error) {
	row := make([]any, 17)
	row[0] = r.ID
	row[1] = uint64(r.Sequence)
	row[2] = string(r.Kind)
	if !r.Time.IsZero() {
		row[3] = r.Time.UTC()
	}

	switch r.Kind {
	case stream.KindEvent:
		e := r.Event
		if e == nil {
			return nil, fmt.Errorf("parquet: event record %s has no payload", r.ID)
		}
		row[5] = nullString(e.ContractID)
		row[6] = nullString(e.TxHash)
		row[7] = e.TxIndex
		row[8] = e.OpIndex
		if len(e.TopicXDR) > 0 {
			row[10] = e.TopicXDR
		}
		row[11] = nullString(e.ValueXDR)

	case stream.KindLedger:
		c := r.Close
		if c == nil {
			return nil, fmt.Errorf("parquet: ledger record %s has no payload", r.ID)
		}
		row[4] = nullString(c.Hash)
		row[15] = nullString(c.HeaderXDR)
		row[16] = nullString(c.MetadataXDR)

	case stream.KindTransaction:
		tx := r.Transaction
		if tx == nil {
			return nil, fmt.Errorf("parquet: transaction record %s has no payload", r.ID)
		}
		row[6] = nullString(tx.Hash)
		row[7] = tx.Index
		row[9] = tx.Successful
		row[12] = nullString(tx.EnvelopeXDR)
		row[13] = nullString(tx.ResultXDR)
		row[14] = nullString(tx.ResultMetaXDR)

	default:
		return nil, fmt.Errorf("parquet: unknown record kind %q", r.Kind)
	}

	return row, nil
}
