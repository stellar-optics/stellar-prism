package sink

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// The audit trail is a hash chain. Every record is hashed together with the
// hash of the record before it:
//
//	Hash_n = SHA256(Hash_{n-1} + Record_n)
//
// so a change to any record changes every hash after it. On top of that
// chain, a checkpoint is written every so often and signed with an Ed25519
// key: the signature covers the chain hash at that point, so a log that was
// edited and re-hashed end to end still fails to verify against the
// checkpoints written before the edit.
const (
	// AuditEntryTypeHeader identifies the first line of an audit log. It
	// carries the public key the checkpoints are signed with.
	AuditEntryTypeHeader = "audit_header"
	// AuditEntryTypeRecord identifies one audited stream record.
	AuditEntryTypeRecord = "audit_record"
	// AuditEntryTypeCheckpoint identifies a signed attestation of the chain.
	AuditEntryTypeCheckpoint = "audit_checkpoint"

	// AuditVersion is the version of the audit log format.
	AuditVersion = 1
	// AuditChainAlgorithm names the hash chain algorithm.
	AuditChainAlgorithm = "sha256"
	// AuditSignatureAlgorithm names the checkpoint signature algorithm.
	AuditSignatureAlgorithm = "ed25519"

	// DefaultAuditCheckpointRecords is how many records pass between signed
	// checkpoints when the caller does not choose.
	DefaultAuditCheckpointRecords = 100
)

// auditKeySuffix is appended to the audit log path when no key path is given,
// so the signing key sits next to the trail it signs.
const auditKeySuffix = ".key"

// AuditHeader is the first entry of an audit log.
//
// The genesis hash covers the header, so the header cannot be swapped for
// another one without breaking every record that follows.
type AuditHeader struct {
	Type      string `json:"type"`
	Version   int    `json:"version"`
	Chain     string `json:"chain"`
	Signature string `json:"signature"`
	PublicKey string `json:"publicKey"`
	CreatedAt string `json:"createdAt"`
	Hash      string `json:"hash"`
}

// content returns the part of the header the genesis hash covers.
func (h AuditHeader) content() auditHeaderContent {
	return auditHeaderContent{
		Type:      h.Type,
		Version:   h.Version,
		Chain:     h.Chain,
		Signature: h.Signature,
		PublicKey: h.PublicKey,
		CreatedAt: h.CreatedAt,
	}
}

// AuditRecord is one audited stream record.
//
// The record itself is stored under "record" in the same shape the NDJSON
// sink emits, so an auditor reads the trail without a second decoder.
type AuditRecord struct {
	Type       string          `json:"type"`
	Seq        uint32          `json:"seq"`
	Ledger     uint32          `json:"ledger"`
	Time       string          `json:"time"`
	RecordID   string          `json:"recordId"`
	RecordKind string          `json:"recordKind"`
	PrevHash   string          `json:"prevHash"`
	Hash       string          `json:"hash"`
	Record     json.RawMessage `json:"record"`
}

// content returns the part of the entry the chain hash covers.
//
// The previous hash is not repeated here: it is already the prefix of the
// hash input, so including it again would add nothing.
func (r AuditRecord) content() auditRecordContent {
	return auditRecordContent{
		Seq:        r.Seq,
		Ledger:     r.Ledger,
		Time:       r.Time,
		RecordID:   r.RecordID,
		RecordKind: r.RecordKind,
		Record:     r.Record,
	}
}

// AuditCheckpoint is a signed attestation of the chain up to a point.
//
// Seq is the number of records the checkpoint covers, so a checkpoint whose
// count no longer matches the log shows that records were removed.
type AuditCheckpoint struct {
	Type      string `json:"type"`
	Seq       uint32 `json:"seq"`
	Ledger    uint32 `json:"ledger"`
	Time      string `json:"time"`
	Hash      string `json:"hash"`
	Signature string `json:"signature"`
}

// auditHeaderContent is the part of the header the genesis hash covers.
type auditHeaderContent struct {
	Type      string `json:"type"`
	Version   int    `json:"version"`
	Chain     string `json:"chain"`
	Signature string `json:"signature"`
	PublicKey string `json:"publicKey"`
	CreatedAt string `json:"createdAt"`
}

// auditRecordContent is the part of a record entry the chain hash covers.
type auditRecordContent struct {
	Seq        uint32          `json:"seq"`
	Ledger     uint32          `json:"ledger"`
	Time       string          `json:"time"`
	RecordID   string          `json:"recordId"`
	RecordKind string          `json:"recordKind"`
	Record     json.RawMessage `json:"record"`
}

// AuditOptions configures an AuditSink.
type AuditOptions struct {
	// Path is the audit log file. Required.
	Path string
	// KeyPath is the Ed25519 private key that signs the checkpoints. Empty
	// uses Path plus ".key". The key file is created with mode 0600 when it
	// does not exist yet, so a trail can be started without ceremony.
	KeyPath string
	// CheckpointRecords is how many records pass between signed
	// checkpoints. Zero writes a checkpoint after every record.
	CheckpointRecords int
	// Decode embeds decoded XDR in the audited records, matching a stream
	// run without --raw.
	Decode bool
	// Now is the clock used for the entry timestamps. Tests set it; nil
	// means time.Now.
	Now func() time.Time
}

// AuditSink writes a tamper-evident audit trail.
//
// Each record is hashed together with the hash of the record before it, so
// a modified, deleted or inserted line breaks the chain and is reported by
// VerifyAuditLog. Checkpoints are signed, so a trail that was edited and
// re-hashed from the beginning still fails against the checkpoints written
// before the edit.
//
// The trail is append-only across runs: a new sink continues the chain of an
// existing log instead of starting a new one, and refuses to continue a log
// that was signed with a different key.
type AuditSink struct {
	opts   AuditOptions
	file   *os.File
	writer *bufio.Writer
	key    ed25519.PrivateKey
	public ed25519.PublicKey
	now    func() time.Time

	mu              sync.Mutex
	chain           []byte
	records         uint32
	lastLedger      uint32
	sinceCheckpoint int
	closed          bool
}

// NewAuditSink opens the audit log and returns a sink that appends to it.
//
// An existing log is continued: its header is read, its key is checked
// against the signing key, and the chain picks up from the last entry. A log
// left with a half-written final line by an interrupted run is truncated to
// the last complete entry before the chain continues.
func NewAuditSink(opts AuditOptions) (*AuditSink, error) {
	if strings.TrimSpace(opts.Path) == "" {
		return nil, fmt.Errorf("audit: Path is required")
	}

	keyPath := opts.KeyPath
	if keyPath == "" {
		keyPath = opts.Path + auditKeySuffix
	}

	key, err := loadOrCreateAuditKey(keyPath)
	if err != nil {
		return nil, err
	}
	public := AuditPublicKey(key)
	if public == nil {
		return nil, fmt.Errorf("audit: %s does not hold an Ed25519 key", keyPath)
	}

	now := opts.Now
	if now == nil {
		now = time.Now
	}

	a := &AuditSink{opts: opts, key: key, public: public, now: now}

	file, err := os.OpenFile(opts.Path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("audit: opening %s: %w", opts.Path, err)
	}
	a.file = file
	a.writer = bufio.NewWriter(file)

	if err := a.resume(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return a, nil
}

// resume prepares the chain for a log that may already hold entries.
func (a *AuditSink) resume() error {
	info, err := a.file.Stat()
	if err != nil {
		return fmt.Errorf("audit: inspecting %s: %w", a.opts.Path, err)
	}
	if info.Size() == 0 {
		return a.writeHeader()
	}

	header, err := a.readHeader()
	if err != nil {
		return err
	}
	// The trail is a chain of trust: a log signed with another key cannot be
	// continued by this one without breaking the checkpoints in between.
	want := a.publicKey()
	if header.PublicKey != want {
		return fmt.Errorf("audit: %s was written with a different signing key", a.opts.Path)
	}

	if err := a.truncatePartialLine(); err != nil {
		return err
	}

	last, err := a.readLastEntry()
	if err != nil {
		return err
	}
	if last == nil {
		return fmt.Errorf("audit: %s has no complete entry to continue from", a.opts.Path)
	}

	switch entry := last.(type) {
	case AuditRecord:
		chain, decodeErr := hex.DecodeString(entry.Hash)
		if decodeErr != nil {
			return fmt.Errorf("audit: %s ends with an unreadable hash: %w", a.opts.Path, decodeErr)
		}
		a.chain = chain
		a.records = entry.Seq
		a.lastLedger = entry.Ledger
	case AuditCheckpoint:
		chain, decodeErr := hex.DecodeString(entry.Hash)
		if decodeErr != nil {
			return fmt.Errorf("audit: %s ends with an unreadable hash: %w", a.opts.Path, decodeErr)
		}
		a.chain = chain
		a.records = entry.Seq
		a.lastLedger = entry.Ledger
	default:
		return fmt.Errorf("audit: %s ends with an entry of unknown type %q", a.opts.Path, last)
	}
	return nil
}

// readHeader reads the first entry of an existing log.
func (a *AuditSink) readHeader() (*AuditHeader, error) {
	reader := bufio.NewReader(a.file)
	line, err := reader.ReadBytes('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("audit: reading %s: %w", a.opts.Path, err)
	}
	if len(bytes.TrimSpace(line)) == 0 {
		return nil, fmt.Errorf("audit: %s has no header", a.opts.Path)
	}

	var header AuditHeader
	if err := json.Unmarshal(bytes.TrimSpace(line), &header); err != nil {
		return nil, fmt.Errorf("audit: %s has an unreadable header: %w", a.opts.Path, err)
	}
	if header.Type != AuditEntryTypeHeader {
		return nil, fmt.Errorf("audit: %s does not start with an audit header", a.opts.Path)
	}
	return &header, nil
}

// truncatePartialLine drops a final line that was not terminated, which is
// what an interrupted run leaves behind.
func (a *AuditSink) truncatePartialLine() error {
	info, err := a.file.Stat()
	if err != nil {
		return fmt.Errorf("audit: inspecting %s: %w", a.opts.Path, err)
	}
	if info.Size() == 0 {
		return nil
	}

	tail := make([]byte, 1)
	if _, err := a.file.ReadAt(tail, info.Size()-1); err != nil {
		return fmt.Errorf("audit: inspecting %s: %w", a.opts.Path, err)
	}
	if tail[0] == '\n' {
		return nil
	}

	// Find the end of the last complete line and cut the file there.
	offset, err := a.lastCompleteLineOffset()
	if err != nil {
		return err
	}
	if err := a.file.Truncate(offset); err != nil {
		return fmt.Errorf("audit: truncating %s: %w", a.opts.Path, err)
	}
	return nil
}

// lastCompleteLineOffset returns the offset just past the last complete line.
func (a *AuditSink) lastCompleteLineOffset() (int64, error) {
	info, err := a.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("audit: inspecting %s: %w", a.opts.Path, err)
	}
	const window = 64 << 10
	start := info.Size() - window
	if start < 0 {
		start = 0
	}
	buf := make([]byte, info.Size()-start)
	if _, err := a.file.ReadAt(buf, start); err != nil && !errors.Is(err, io.EOF) {
		return 0, fmt.Errorf("audit: reading %s: %w", a.opts.Path, err)
	}
	if idx := bytes.LastIndexByte(buf, '\n'); idx >= 0 {
		return start + int64(idx) + 1, nil
	}
	if start > 0 {
		return 0, fmt.Errorf("audit: %s ends with an entry longer than %d bytes", a.opts.Path, window)
	}
	return 0, nil
}

// readLastEntry reads the final entry of the log.
func (a *AuditSink) readLastEntry() (any, error) {
	info, err := a.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("audit: inspecting %s: %w", a.opts.Path, err)
	}
	line, err := readLastLine(a.file, info.Size())
	if err != nil {
		return nil, fmt.Errorf("audit: reading %s: %w", a.opts.Path, err)
	}
	if len(line) == 0 {
		return nil, nil
	}

	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &envelope); err != nil {
		return nil, fmt.Errorf("audit: %s ends with an unreadable entry: %w", a.opts.Path, err)
	}

	switch envelope.Type {
	case AuditEntryTypeRecord:
		var entry AuditRecord
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, fmt.Errorf("audit: %s ends with an unreadable entry: %w", a.opts.Path, err)
		}
		return entry, nil
	case AuditEntryTypeCheckpoint:
		var entry AuditCheckpoint
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, fmt.Errorf("audit: %s ends with an unreadable entry: %w", a.opts.Path, err)
		}
		return entry, nil
	default:
		return nil, fmt.Errorf("audit: %s ends with an entry of unknown type %q", a.opts.Path, envelope.Type)
	}
}

// readLastLine returns the final line of a file, without its newline.
//
// Only the tail of the file is read, so a long trail does not have to be
// loaded into memory to find where it stopped.
func readLastLine(f *os.File, size int64) ([]byte, error) {
	const window = 64 << 10
	start := size - window
	if start < 0 {
		start = 0
	}
	buf := make([]byte, size-start)
	if _, err := f.ReadAt(buf, start); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	data := bytes.TrimSuffix(buf, []byte("\n"))
	if len(data) == 0 {
		return nil, nil
	}
	idx := bytes.LastIndexByte(data, '\n')
	if idx < 0 {
		if start > 0 {
			return nil, fmt.Errorf("the last line is longer than %d bytes", window)
		}
		return data, nil
	}
	return data[idx+1:], nil
}

// publicKey returns the base64 encoded public key of the signing key.
func (a *AuditSink) publicKey() string {
	return base64.StdEncoding.EncodeToString(a.public)
}

// writeHeader writes the header and starts the chain at the genesis hash.
func (a *AuditSink) writeHeader() error {
	content := auditHeaderContent{
		Type:      AuditEntryTypeHeader,
		Version:   AuditVersion,
		Chain:     AuditChainAlgorithm,
		Signature: AuditSignatureAlgorithm,
		PublicKey: a.publicKey(),
		CreatedAt: a.now().UTC().Format(time.RFC3339Nano),
	}
	body, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("audit: encoding header: %w", err)
	}
	genesis := sha256.Sum256(body)

	header := AuditHeader{
		Type:      content.Type,
		Version:   content.Version,
		Chain:     content.Chain,
		Signature: content.Signature,
		PublicKey: content.PublicKey,
		CreatedAt: content.CreatedAt,
		Hash:      hex.EncodeToString(genesis[:]),
	}
	if err := a.writeEntry(header); err != nil {
		return err
	}
	if err := a.writer.Flush(); err != nil {
		return fmt.Errorf("audit: flushing header: %w", err)
	}
	a.chain = genesis[:]
	return nil
}

// Emit implements stream.Sink.
func (a *AuditSink) Emit(r stream.Record) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return fmt.Errorf("audit: emit after close: %w", os.ErrClosed)
	}
	return a.emitLocked(r)
}

// EmitBatch implements stream.BatchSink, writing a whole batch before a
// single flush.
func (a *AuditSink) EmitBatch(records []stream.Record) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return fmt.Errorf("audit: emit after close: %w", os.ErrClosed)
	}
	for _, r := range records {
		if err := a.emitLocked(r); err != nil {
			return err
		}
	}
	return a.Flush()
}

// emitLocked writes one record and, when due, the checkpoint that follows it.
func (a *AuditSink) emitLocked(r stream.Record) error {
	payload, err := MarshalRecord(r, a.opts.Decode)
	if err != nil {
		return fmt.Errorf("audit: encoding record %s: %w", r.ID, err)
	}

	a.records++
	content := auditRecordContent{
		Seq:        a.records,
		Ledger:     r.Sequence,
		Time:       formatTime(r.Time),
		RecordID:   r.ID,
		RecordKind: string(r.Kind),
		Record:     payload,
	}
	hash, err := auditChainHash(a.chain, content)
	if err != nil {
		return err
	}

	entry := AuditRecord{
		Type:       AuditEntryTypeRecord,
		Seq:        content.Seq,
		Ledger:     content.Ledger,
		Time:       content.Time,
		RecordID:   content.RecordID,
		RecordKind: content.RecordKind,
		PrevHash:   hex.EncodeToString(a.chain),
		Hash:       hex.EncodeToString(hash),
		Record:     payload,
	}
	if err := a.writeEntry(entry); err != nil {
		return err
	}

	a.chain = hash
	a.lastLedger = content.Ledger
	a.sinceCheckpoint++
	if a.checkpointDue() {
		if err := a.writeCheckpointLocked(); err != nil {
			return err
		}
	}
	return nil
}

// checkpointDue reports whether a checkpoint is due after the record that
// was just written.
func (a *AuditSink) checkpointDue() bool {
	if a.opts.CheckpointRecords <= 0 {
		return true
	}
	return a.sinceCheckpoint >= a.opts.CheckpointRecords
}

// writeCheckpointLocked signs the chain hash and writes the checkpoint.
func (a *AuditSink) writeCheckpointLocked() error {
	checkpoint := AuditCheckpoint{
		Type:      AuditEntryTypeCheckpoint,
		Seq:       a.records,
		Ledger:    a.lastLedger,
		Time:      a.now().UTC().Format(time.RFC3339Nano),
		Hash:      hex.EncodeToString(a.chain),
		Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(a.key, a.chain)),
	}
	if err := a.writeEntry(checkpoint); err != nil {
		return err
	}
	a.sinceCheckpoint = 0
	return nil
}

// writeEntry writes one JSON entry as a single line.
func (a *AuditSink) writeEntry(entry any) error {
	body, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("audit: encoding entry: %w", err)
	}
	if _, err := a.writer.Write(body); err != nil {
		return fmt.Errorf("audit: writing to %s: %w", a.opts.Path, err)
	}
	if err := a.writer.WriteByte('\n'); err != nil {
		return fmt.Errorf("audit: writing to %s: %w", a.opts.Path, err)
	}
	return nil
}

// Flush implements stream.Sink. It flushes the buffer and syncs the file, so
// a record the tailer has emitted survives a crash.
func (a *AuditSink) Flush() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil
	}
	if err := a.writer.Flush(); err != nil {
		return fmt.Errorf("audit: flushing %s: %w", a.opts.Path, err)
	}
	if err := a.file.Sync(); err != nil {
		return fmt.Errorf("audit: syncing %s: %w", a.opts.Path, err)
	}
	return nil
}

// Close writes a final checkpoint, flushes the log and closes the file. It
// is safe to call more than once.
func (a *AuditSink) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil
	}
	a.closed = true

	var errs []error
	if a.records > 0 && a.sinceCheckpoint > 0 {
		if err := a.writeCheckpointLocked(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := a.writer.Flush(); err != nil {
		errs = append(errs, fmt.Errorf("audit: flushing %s: %w", a.opts.Path, err))
	}
	if err := a.file.Sync(); err != nil {
		errs = append(errs, fmt.Errorf("audit: syncing %s: %w", a.opts.Path, err))
	}
	if err := a.file.Close(); err != nil {
		errs = append(errs, fmt.Errorf("audit: closing %s: %w", a.opts.Path, err))
	}
	return errors.Join(errs...)
}

// auditChainHash returns the rolling hash of a record: the SHA-256 of the
// previous hash followed by the record content.
func auditChainHash(prev []byte, content auditRecordContent) ([]byte, error) {
	body, err := json.Marshal(content)
	if err != nil {
		return nil, fmt.Errorf("audit: encoding record content: %w", err)
	}
	digest := sha256.New()
	digest.Write(prev)
	digest.Write(body)
	return digest.Sum(nil), nil
}

// loadOrCreateAuditKey reads the signing key, creating it when it is not
// there yet.
//
// The key is created exclusively, so two processes starting at the same time
// cannot each write a key and leave the trail signed by one of them.
func loadOrCreateAuditKey(path string) (ed25519.PrivateKey, error) {
	key, err := readAuditKey(path)
	if err == nil {
		return key, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	_, private, genErr := ed25519.GenerateKey(rand.Reader)
	if genErr != nil {
		return nil, fmt.Errorf("audit: generating signing key: %w", genErr)
	}
	encoded, marshalErr := marshalAuditKey(private)
	if marshalErr != nil {
		return nil, marshalErr
	}

	file, createErr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if createErr != nil {
		if errors.Is(createErr, os.ErrExist) {
			// Another process won the race; use the key it wrote.
			return readAuditKey(path)
		}
		return nil, fmt.Errorf("audit: creating key %s: %w", path, createErr)
	}
	if _, writeErr := file.Write(encoded); writeErr != nil {
		_ = file.Close()
		return nil, fmt.Errorf("audit: writing key %s: %w", path, writeErr)
	}
	if closeErr := file.Close(); closeErr != nil {
		return nil, fmt.Errorf("audit: writing key %s: %w", path, closeErr)
	}
	return private, nil
}

// readAuditKey reads a PEM encoded Ed25519 private key.
func readAuditKey(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("audit: key %s does not exist: %w", path, os.ErrNotExist)
		}
		return nil, fmt.Errorf("audit: reading key %s: %w", path, err)
	}
	return parseAuditKey(data)
}

// parseAuditKey parses a PEM encoded Ed25519 private key.
func parseAuditKey(data []byte) (ed25519.PrivateKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("audit: signing key is not PEM encoded")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("audit: parsing signing key: %w", err)
	}
	private, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("audit: signing key is a %T, want an Ed25519 key", key)
	}
	return private, nil
}

// marshalAuditKey encodes an Ed25519 private key as PEM.
func marshalAuditKey(private ed25519.PrivateKey) ([]byte, error) {
	encoded, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return nil, fmt.Errorf("audit: encoding signing key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded}), nil
}

// AuditPublicKey returns the public key of a signing key.
func AuditPublicKey(private ed25519.PrivateKey) ed25519.PublicKey {
	public, ok := private.Public().(ed25519.PublicKey)
	if !ok {
		return nil
	}
	return public
}

// MarshalAuditPublicKey encodes an Ed25519 public key as PEM, so an auditor
// can pin the key the trail must be signed with.
func MarshalAuditPublicKey(public ed25519.PublicKey) ([]byte, error) {
	if len(public) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("audit: public key is %d bytes, want %d", len(public), ed25519.PublicKeySize)
	}
	encoded, err := x509.MarshalPKIXPublicKey(public)
	if err != nil {
		return nil, fmt.Errorf("audit: encoding public key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded}), nil
}

// ParseAuditPublicKey reads a PEM encoded Ed25519 public key.
func ParseAuditPublicKey(data []byte) (ed25519.PublicKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("audit: public key is not PEM encoded")
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("audit: parsing public key: %w", err)
	}
	public, ok := key.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("audit: public key is a %T, want an Ed25519 key", key)
	}
	return public, nil
}
