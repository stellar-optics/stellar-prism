package sink

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// Defaults for the ClickHouse sink.
const (
	// DefaultClickHouseBatchSize is the number of records buffered before a bulk insert.
	DefaultClickHouseBatchSize = 5000
	// DefaultClickHouseFlushInterval bounds how long a partial batch waits.
	DefaultClickHouseFlushInterval = 2 * time.Second
	// DefaultClickHouseFlushTimeout bounds a single insert operation.
	DefaultClickHouseFlushTimeout = 30 * time.Second
	// DefaultClickHouseDatabase is the default ClickHouse database.
	DefaultClickHouseDatabase = "default"
	// DefaultClickHouseUser is the default ClickHouse user.
	DefaultClickHouseUser = "default"
	// DefaultClickHousePort is the native TCP protocol port.
	DefaultClickHousePort = "9000"
)

// ErrClickHouseClosed is returned by Emit once the sink has been closed.
var ErrClickHouseClosed = errors.New("clickhouse: sink is closed")

// ClickHouseClient is the database client interface used by ClickHouseSink.
// Keeping it an interface allows unit and integration testing with a mock client.
type ClickHouseClient interface {
	Exec(ctx context.Context, query string) error
	Insert(ctx context.Context, table string, columns []string, rows [][]any) error
	Close() error
}

// ClickHouseConfig configures a ClickHouse native sink.
type ClickHouseConfig struct {
	// Addr is the host:port of the ClickHouse native TCP interface (port 9000).
	Addr string
	// Database is the database name (default: default).
	Database string
	// User is the username (default: default).
	User string
	// Password is the password for authentication.
	Password string
	// BatchSize is how many records are buffered before flushing (default: 5000).
	BatchSize int
	// FlushInterval bounds how long a partial batch waits before background flush.
	FlushInterval time.Duration
	// FlushTimeout bounds a single insert operation.
	FlushTimeout time.Duration
}

func (c ClickHouseConfig) withDefaults() ClickHouseConfig {
	if c.Database == "" {
		c.Database = DefaultClickHouseDatabase
	}
	if c.User == "" {
		c.User = DefaultClickHouseUser
	}
	if c.BatchSize <= 0 {
		c.BatchSize = DefaultClickHouseBatchSize
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = DefaultClickHouseFlushInterval
	}
	if c.FlushTimeout <= 0 {
		c.FlushTimeout = DefaultClickHouseFlushTimeout
	}
	return c
}

func (c ClickHouseConfig) validate() error {
	if strings.TrimSpace(c.Addr) == "" {
		return fmt.Errorf("clickhouse: address is required (--clickhouse-addr)")
	}
	if c.BatchSize < 0 {
		return fmt.Errorf("clickhouse: batch size must not be negative, got %d", c.BatchSize)
	}
	return nil
}

// ClickHouseSink buffers events and transactions and inserts them into ClickHouse
// tables in bulk.
type ClickHouseSink struct {
	client ClickHouseClient
	cfg    ClickHouseConfig

	mu         sync.Mutex
	eventsBuf  [][]any
	txBuf      [][]any
	closed     bool
	pendingErr error
	written    int64
	flushes    int

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// NewClickHouse connects to the ClickHouse native TCP port and returns a ClickHouseSink.
func NewClickHouse(ctx context.Context, cfg ClickHouseConfig) (*ClickHouseSink, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	client, err := dialNativeTCP(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: connecting to %s: %w", cfg.Addr, err)
	}
	return newClickHouseWithClient(ctx, client, cfg)
}

// NewClickHouseWithClient creates a ClickHouseSink using the provided ClickHouseClient.
func NewClickHouseWithClient(ctx context.Context, client ClickHouseClient, cfg ClickHouseConfig) (*ClickHouseSink, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return newClickHouseWithClient(ctx, client, cfg)
}

func newClickHouseWithClient(ctx context.Context, client ClickHouseClient, cfg ClickHouseConfig) (*ClickHouseSink, error) {
	if client == nil {
		return nil, fmt.Errorf("clickhouse: client is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if err := ensureClickHouseSchema(ctx, client, cfg.Database); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("clickhouse: initializing schema: %w", err)
	}

	s := &ClickHouseSink{
		client: client,
		cfg:    cfg,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go s.run()
	return s, nil
}

// ensureClickHouseSchema automatically creates the required tables:
// stellar_events: timestamp, ledger, contract_id, topics, data.
// stellar_transactions: timestamp, ledger, tx_hash, status, fee, ops_count.
func ensureClickHouseSchema(ctx context.Context, client ClickHouseClient, db string) error {
	createEventsSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.stellar_events (
	timestamp DateTime64(3, 'UTC'),
	ledger UInt32,
	contract_id String,
	topics Array(String),
	data String
) ENGINE = ReplacingMergeTree()
ORDER BY (ledger, contract_id, timestamp)`, db)

	if err := client.Exec(ctx, createEventsSQL); err != nil {
		return fmt.Errorf("creating stellar_events table: %w", err)
	}

	createTxSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.stellar_transactions (
	timestamp DateTime64(3, 'UTC'),
	ledger UInt32,
	tx_hash String,
	status String,
	fee UInt64,
	ops_count UInt32
) ENGINE = MergeTree()
ORDER BY (ledger, tx_hash, timestamp)`, db)

	if err := client.Exec(ctx, createTxSQL); err != nil {
		return fmt.Errorf("creating stellar_transactions table: %w", err)
	}
	return nil
}

// Emit stages a record for bulk insert.
func (s *ClickHouseSink) Emit(r stream.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClickHouseClosed
	}
	if s.pendingErr != nil {
		err := s.pendingErr
		s.pendingErr = nil
		return err
	}

	recordTime := r.Time
	if recordTime.IsZero() {
		recordTime = time.Now()
	}

	switch r.Kind {
	case stream.KindEvent:
		if r.Event != nil {
			topics := r.Event.TopicXDR
			if topics == nil {
				topics = []string{}
			}
			row := []any{
				recordTime.UTC(),
				r.Sequence,
				r.Event.ContractID,
				topics,
				r.Event.ValueXDR,
			}
			s.eventsBuf = append(s.eventsBuf, row)
		}
	case stream.KindTransaction:
		if r.Transaction != nil {
			status := "SUCCESS"
			if !r.Transaction.Successful {
				status = "FAILED"
			}
			opsCount := uint32(1)
			fee := uint64(0)
			row := []any{
				recordTime.UTC(),
				r.Sequence,
				r.Transaction.Hash,
				status,
				fee,
				opsCount,
			}
			s.txBuf = append(s.txBuf, row)
		}
	default:
		// Other kinds can be recorded as transactions or events if applicable
	}

	if len(s.eventsBuf)+len(s.txBuf) >= s.cfg.BatchSize {
		return s.flushLocked(context.Background())
	}
	return nil
}

// Flush writes any buffered records to ClickHouse immediately.
func (s *ClickHouseSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	err := s.flushLocked(context.Background())
	if err == nil {
		s.pendingErr = nil
	}
	return err
}

// Close flushes remaining buffered records, stops the background timer, and closes the client.
func (s *ClickHouseSink) Close() error {
	s.closeOnce.Do(func() {
		close(s.stop)
		<-s.done

		s.mu.Lock()
		s.closed = true
		err := s.flushLocked(context.Background())
		s.mu.Unlock()

		s.closeErr = errors.Join(err, s.client.Close())
	})
	return s.closeErr
}

// Written returns the total number of records successfully written.
func (s *ClickHouseSink) Written() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.written
}

// Flushes returns how many batch flushes have occurred.
func (s *ClickHouseSink) Flushes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushes
}

func (s *ClickHouseSink) run() {
	defer close(s.done)

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.mu.Lock()
			if err := s.flushLocked(context.Background()); err != nil && s.pendingErr == nil {
				s.pendingErr = err
			}
			s.mu.Unlock()
		}
	}
}

func (s *ClickHouseSink) flushLocked(ctx context.Context) error {
	if len(s.eventsBuf) == 0 && len(s.txBuf) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, s.cfg.FlushTimeout)
	defer cancel()

	var errs []error
	var inserted int64

	if len(s.eventsBuf) > 0 {
		cols := []string{"timestamp", "ledger", "contract_id", "topics", "data"}
		table := fmt.Sprintf("%s.stellar_events", s.cfg.Database)
		if err := s.client.Insert(ctx, table, cols, s.eventsBuf); err != nil {
			errs = append(errs, fmt.Errorf("bulk insert stellar_events (%d rows): %w", len(s.eventsBuf), err))
		} else {
			inserted += int64(len(s.eventsBuf))
			s.eventsBuf = nil
		}
	}

	if len(s.txBuf) > 0 {
		cols := []string{"timestamp", "ledger", "tx_hash", "status", "fee", "ops_count"}
		table := fmt.Sprintf("%s.stellar_transactions", s.cfg.Database)
		if err := s.client.Insert(ctx, table, cols, s.txBuf); err != nil {
			errs = append(errs, fmt.Errorf("bulk insert stellar_transactions (%d rows): %w", len(s.txBuf), err))
		} else {
			inserted += int64(len(s.txBuf))
			s.txBuf = nil
		}
	}

	s.written += inserted
	if inserted > 0 {
		s.flushes++
	}

	return errors.Join(errs...)
}

// --- Native TCP Client implementation ---

const (
	clientHelloPacket = 0
	clientQueryPacket = 1
	clientDataPacket  = 2
	clientPingPacket  = 4

	serverHelloPacket     = 0
	serverDataPacket      = 1
	serverExceptionPacket = 2
	serverProgressPacket  = 3
	serverPongPacket      = 4
	serverEndOfStream     = 5

	clientVersionMajor = 23
	clientVersionMinor = 8
	clientVersionPatch = 1
	clientTCPProtocol  = 54460
)

type nativeTCPClient struct {
	conn net.Conn
	rw   *bufio.ReadWriter
	cfg  ClickHouseConfig
	mu   sync.Mutex
}

func dialNativeTCP(ctx context.Context, cfg ClickHouseConfig) (*nativeTCPClient, error) {
	d := net.Dialer{Timeout: 10 * time.Second}
	addr := cfg.Addr
	if !strings.Contains(addr, ":") {
		addr = net.JoinHostPort(addr, DefaultClickHousePort)
	}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	client := &nativeTCPClient{
		conn: conn,
		rw:   bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
		cfg:  cfg,
	}

	if err := client.handshake(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("handshake failed: %w", err)
	}
	return client, nil
}

func (c *nativeTCPClient) handshake(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// ClientHello packet:
	// packet_type (varint 0)
	// client_name (string "prism")
	// client_version_major (varint)
	// client_version_minor (varint)
	// client_protocol_version (varint)
	// database (string)
	// user (string)
	// password (string)
	if err := writeVarUint(c.rw, clientHelloPacket); err != nil {
		return err
	}
	if err := writeString(c.rw, "prism"); err != nil {
		return err
	}
	if err := writeVarUint(c.rw, clientVersionMajor); err != nil {
		return err
	}
	if err := writeVarUint(c.rw, clientVersionMinor); err != nil {
		return err
	}
	if err := writeVarUint(c.rw, clientTCPProtocol); err != nil {
		return err
	}
	if err := writeString(c.rw, c.cfg.Database); err != nil {
		return err
	}
	if err := writeString(c.rw, c.cfg.User); err != nil {
		return err
	}
	if err := writeString(c.rw, c.cfg.Password); err != nil {
		return err
	}
	if err := c.rw.Flush(); err != nil {
		return err
	}

	// Read Server response
	packetType, err := readVarUint(c.rw)
	if err != nil {
		return err
	}
	if packetType == serverExceptionPacket {
		return c.readException()
	}
	if packetType != serverHelloPacket {
		return fmt.Errorf("unexpected handshake packet type: %d", packetType)
	}

	// Consume ServerHello fields; errors are non-fatal since the handshake
	// packet has already been validated by the type check above.
	if _, err = readString(c.rw); err != nil { // server_name
		return fmt.Errorf("reading server hello: %w", err)
	}
	if _, err = readVarUint(c.rw); err != nil { // major
		return fmt.Errorf("reading server hello: %w", err)
	}
	if _, err = readVarUint(c.rw); err != nil { // minor
		return fmt.Errorf("reading server hello: %w", err)
	}
	if _, err = readVarUint(c.rw); err != nil { // protocol version
		return fmt.Errorf("reading server hello: %w", err)
	}
	if _, err = readString(c.rw); err != nil { // timezone
		return fmt.Errorf("reading server hello: %w", err)
	}
	if _, err = readString(c.rw); err != nil { // display_name
		return fmt.Errorf("reading server hello: %w", err)
	}
	if _, err = readVarUint(c.rw); err != nil { // patch version
		return fmt.Errorf("reading server hello: %w", err)
	}

	return nil
}

func (c *nativeTCPClient) Exec(ctx context.Context, query string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.sendQuery(query); err != nil {
		return err
	}
	return c.drainServerResponse()
}

func (c *nativeTCPClient) Insert(ctx context.Context, table string, columns []string, rows [][]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(rows) == 0 {
		return nil
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", "))
	for i, row := range rows {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		for j, val := range row {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(formatClickHouseValue(val))
		}
		sb.WriteString(")")
	}

	if err := c.sendQuery(sb.String()); err != nil {
		return err
	}
	return c.drainServerResponse()
}

func (c *nativeTCPClient) sendQuery(query string) error {
	if err := writeVarUint(c.rw, clientQueryPacket); err != nil {
		return err
	}
	if err := writeString(c.rw, ""); err != nil { // query_id
		return err
	}
	// Client info (empty stage / settings)
	if err := writeVarUint(c.rw, 1); err != nil { // initial query
		return err
	}
	if err := writeString(c.rw, ""); err != nil { // initial user
		return err
	}
	if err := writeString(c.rw, ""); err != nil { // initial query id
		return err
	}
	if err := writeString(c.rw, "[::ffff:127.0.0.1]:0"); err != nil { // client address
		return err
	}
	if err := writeVarUint(c.rw, 0); err != nil { // stage: Complete (2) or Default (0)
		return err
	}
	if err := writeVarUint(c.rw, 0); err != nil { // compression: off (0)
		return err
	}
	if err := writeString(c.rw, query); err != nil {
		return err
	}
	// Empty data block (end of query data)
	if err := writeVarUint(c.rw, clientDataPacket); err != nil {
		return err
	}
	if err := writeString(c.rw, ""); err != nil { // temp table
		return err
	}
	if err := writeVarUint(c.rw, 0); err != nil { // columns
		return err
	}
	if err := writeVarUint(c.rw, 0); err != nil { // rows
		return err
	}
	return c.rw.Flush()
}

func (c *nativeTCPClient) drainServerResponse() error {
	for {
		packetType, err := readVarUint(c.rw)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		switch packetType {
		case serverEndOfStream:
			return nil
		case serverExceptionPacket:
			return c.readException()
		case serverProgressPacket:
			if _, err := readVarUint(c.rw); err != nil { // rows
				return fmt.Errorf("reading progress: %w", err)
			}
			if _, err := readVarUint(c.rw); err != nil { // bytes
				return fmt.Errorf("reading progress: %w", err)
			}
			if _, err := readVarUint(c.rw); err != nil { // total_rows
				return fmt.Errorf("reading progress: %w", err)
			}
		case serverDataPacket:
			// Read block
			if _, err := readString(c.rw); err != nil { // temp table
				return fmt.Errorf("reading data block: %w", err)
			}
			if _, err := readVarUint(c.rw); err != nil { // cols
				return fmt.Errorf("reading data block: %w", err)
			}
			if _, err := readVarUint(c.rw); err != nil { // rows
				return fmt.Errorf("reading data block: %w", err)
			}
		default:
			return nil
		}
	}
}

func (c *nativeTCPClient) readException() error {
	code, err := readVarUint(c.rw)
	if err != nil {
		return fmt.Errorf("reading exception code: %w", err)
	}
	name, err := readString(c.rw)
	if err != nil {
		return fmt.Errorf("reading exception name: %w", err)
	}
	msg, err := readString(c.rw)
	if err != nil {
		return fmt.Errorf("reading exception message: %w", err)
	}
	if _, err := readString(c.rw); err != nil { // stack
		return fmt.Errorf("reading exception stack: %w", err)
	}
	hasNested, err := readVarUint(c.rw)
	if err != nil {
		return fmt.Errorf("reading exception nested flag: %w", err)
	}
	if hasNested != 0 {
		if nestedErr := c.readException(); nestedErr != nil {
			return fmt.Errorf("clickhouse exception %d (%s): %s (nested: %w)", code, name, msg, nestedErr)
		}
	}
	return fmt.Errorf("clickhouse exception %d (%s): %s", code, name, msg)
}

func (c *nativeTCPClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func formatClickHouseValue(val any) string {
	switch v := val.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(strings.ReplaceAll(v, "\\", "\\\\"), "'", "\\'") + "'"
	case time.Time:
		return fmt.Sprintf("'%s'", v.UTC().Format("2006-01-02 15:04:05.000"))
	case []string:
		items := make([]string, len(v))
		for i, s := range v {
			items[i] = "'" + strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "'", "\\'") + "'"
		}
		return "[" + strings.Join(items, ", ") + "]"
	case uint32, uint64, int, int64, int32:
		return fmt.Sprintf("%d", v)
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprintf("'%v'", v)
	}
}

func writeVarUint(w io.Writer, v uint64) error {
	for v >= 0x80 {
		if _, err := w.Write([]byte{byte(v) | 0x80}); err != nil {
			return err
		}
		v >>= 7
	}
	_, err := w.Write([]byte{byte(v)})
	return err
}

func readVarUint(r io.Reader) (uint64, error) {
	var v uint64
	var shift uint
	buf := make([]byte, 1)
	for {
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		b := buf[0]
		v |= uint64(b&0x7f) << shift
		if b < 0x80 {
			break
		}
		shift += 7
		if shift >= 64 {
			return 0, errors.New("clickhouse: varint overflow")
		}
	}
	return v, nil
}

func writeString(w io.Writer, s string) error {
	if err := writeVarUint(w, uint64(len(s))); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}

func readString(r io.Reader) (string, error) {
	n, err := readVarUint(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}
