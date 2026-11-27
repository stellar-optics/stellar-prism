package sink

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// Stage names recorded on a dead letter, saying where the record failed.
const (
	// DLQStageValidation means the record itself was malformed and never
	// reached the wrapped sink.
	DLQStageValidation = "validation"
	// DLQStageDelivery means the wrapped sink rejected the record.
	DLQStageDelivery = "delivery"
	// DLQStageFlush means the wrapped sink failed to flush buffered output,
	// which cannot be attributed to one record.
	DLQStageFlush = "flush"
)

// DLQKind tags every dead letter line, so a queue file can be told apart from
// the NDJSON records a stream normally emits.
const DLQKind = "dlq"

// DLQEntry is one JSON line in the dead letter queue.
//
// The original record is preserved field-for-field, so a dead letter can be
// replayed by decoding Record and handing it back to a sink. Record is nil
// only for a flush failure, which is not about one record.
type DLQEntry struct {
	Kind     string         `json:"kind"`
	FailedAt string         `json:"failedAt,omitempty"`
	Stage    string         `json:"stage"`
	Error    string         `json:"error"`
	Record   *stream.Record `json:"record,omitempty"`
}

// DLQOptions configures a DLQ.
type DLQOptions struct {
	// MaxRecordRetries retries a failed delivery before quarantining it. Zero
	// preserves the immediate-quarantine behaviour.
	MaxRecordRetries int
	// Now supplies the FailedAt timestamp. Tests inject a fixed clock; nil
	// means time.Now.
	Now func() time.Time
	// CloseWriter closes the writer passed to NewDLQ when the DLQ is closed,
	// but only when that writer implements io.Closer. It is off by default so
	// wrapping a shared writer (stdout, a test buffer) cannot close it
	// unexpectedly.
	CloseWriter bool
	// OnDeadLetter, when set, observes every successfully recorded failure.
	// It is called synchronously, so it must not block for long.
	OnDeadLetter func(DLQEntry)
}

// DLQStats counts what a DLQ has seen. It is a snapshot, not a live view.
type DLQStats struct {
	// Delivered counts records the wrapped sink accepted.
	Delivered int
	// Malformed counts records rejected by ValidateRecord.
	Malformed int
	// Failed counts records the wrapped sink rejected.
	Failed int
	// FlushFailures counts flush errors that were captured as dead letters.
	FlushFailures int
}

// DLQ wraps a sink and captures records it cannot deliver, or records that are
// structurally malformed, as NDJSON lines in a dead letter writer.
//
// A captured failure is not returned to the caller: the stream keeps going, a
// long-running tail is not killed by one bad record or one transient write
// failure, and the failure is not silently dropped either — it is on disk.
// Only a failure to write the dead letter itself is returned, because at that
// point the record would otherwise be lost without a trace.
//
// A DLQ owns its wrapped sink if that sink implements io.Closer, and closes it
// from Close. It starts no goroutines, so there is nothing to leak and nothing
// to join on termination.
//
// See docs/operations/dead_letter_queue.md for CLI configuration, the on-disk
// format, and replay procedures.
type DLQ struct {
	primary stream.Sink
	writer  *bufio.Writer
	enc     *json.Encoder
	closer  io.Closer
	opts    DLQOptions
	now     func() time.Time

	mu     sync.Mutex
	stats  DLQStats
	closed bool
}

// NewDLQ returns a sink that forwards to primary and writes failures to out.
//
// primary must not be nil; a DLQ with nothing to wrap would hide the real
// error. out must not be nil either, because a DLQ that cannot record
// anything is not a dead letter queue.
func NewDLQ(primary stream.Sink, out io.Writer, opts DLQOptions) (*DLQ, error) {
	if primary == nil {
		return nil, fmt.Errorf("dlq: primary sink is required")
	}
	if out == nil {
		return nil, fmt.Errorf("dlq: output writer is required")
	}

	now := opts.Now
	if now == nil {
		now = time.Now
	}

	bw := bufio.NewWriter(out)
	enc := json.NewEncoder(bw)
	enc.SetEscapeHTML(false)

	d := &DLQ{
		primary: primary,
		writer:  bw,
		enc:     enc,
		opts:    opts,
		now:     now,
	}
	if opts.CloseWriter {
		if closer, ok := out.(io.Closer); ok {
			d.closer = closer
		}
	}
	return d, nil
}

// ValidateRecord reports whether a record has the structure its Kind requires.
//
// It deliberately checks only structure: an XDR payload that will not decode
// is already handled inline by the built-in sinks, and rejecting it here would
// turn a display problem into a delivery failure. What it catches is the class
// of record that cannot be rendered at all — an unknown kind, a missing ID, or
// a payload pointer that does not match the kind.
func ValidateRecord(r stream.Record) error {
	if r.ID == "" {
		return errors.New("record has no ID")
	}

	switch r.Kind {
	case stream.KindEvent:
		if r.Event == nil {
			return fmt.Errorf("record %s is an event with no payload", r.ID)
		}
	case stream.KindLedger:
		if r.Close == nil {
			return fmt.Errorf("record %s is a ledger with no payload", r.ID)
		}
	case stream.KindTransaction:
		if r.Transaction == nil {
			return fmt.Errorf("record %s is a transaction with no payload", r.ID)
		}
	case stream.KindStorageChange:
		if r.StorageChange == nil {
			return fmt.Errorf("record %s is a storage change with no payload", r.ID)
		}
	case stream.KindWASMDeployment:
		if r.WASMDeployment == nil {
			return fmt.Errorf("record %s is a wasm deployment with no payload", r.ID)
		}
	default:
		return fmt.Errorf("record %s has unknown kind %q", r.ID, r.Kind)
	}
	return nil
}

// Emit implements stream.Sink.
//
// A malformed record or a failed delivery is captured and swallowed; nil means
// the record is accounted for, either by the wrapped sink or by the queue.
func (d *DLQ) Emit(r stream.Record) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fmt.Errorf("dlq: emit after close: %w", os.ErrClosed)
	}
	return d.emitLocked(r)
}

// EmitBatch implements stream.BatchSink by validating and delivering one
// record at a time. A batch sink could fail part-way through, which would make
// it impossible to say which records were delivered; per-record isolation is
// what lets every failure be attributed to the record that caused it.
func (d *DLQ) EmitBatch(records []stream.Record) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fmt.Errorf("dlq: emit after close: %w", os.ErrClosed)
	}
	for _, r := range records {
		if err := d.emitLocked(r); err != nil {
			return err
		}
	}
	return d.flushLocked()
}

func (d *DLQ) emitLocked(r stream.Record) error {
	if err := ValidateRecord(r); err != nil {
		d.stats.Malformed++
		return d.recordLocked(r, DLQStageValidation, err)
	}

	var err error
	for attempt := 0; attempt <= d.opts.MaxRecordRetries; attempt++ {
		err = d.primary.Emit(r)
		if err == nil {
			d.stats.Delivered++
			return nil
		}
	}
	if err != nil {
		d.stats.Failed++
		return d.recordLocked(r, DLQStageDelivery, err)
	}
	return nil
}

// recordLocked writes one dead letter and reports only a failure to write it.
func (d *DLQ) recordLocked(r stream.Record, stage string, cause error) error {
	entry := DLQEntry{
		Kind:     DLQKind,
		FailedAt: d.now().UTC().Format(time.RFC3339Nano),
		Stage:    stage,
		Error:    cause.Error(),
		Record:   &r,
	}
	if err := d.enc.Encode(entry); err != nil {
		return fmt.Errorf("dlq: writing %s entry for record %s: %w", stage, r.ID, err)
	}
	// Flush per failure, like the stream sinks flush per record: a dead letter
	// that is still sitting in a buffer when the process is killed is not a
	// dead letter at all.
	if err := d.writer.Flush(); err != nil {
		return fmt.Errorf("dlq: flushing %s entry for record %s: %w", stage, r.ID, err)
	}
	d.notifyLocked(entry)
	return nil
}

func (d *DLQ) notifyLocked(entry DLQEntry) {
	if d.opts.OnDeadLetter != nil {
		d.opts.OnDeadLetter(entry)
	}
}

// Flush implements stream.Sink.
//
// A primary flush failure is captured as a DLQStageFlush entry rather than
// returned: at that point every individual record has already been accounted
// for, and failing the command at shutdown would defeat the point of a dead
// letter queue. A failure to flush the queue itself is returned, because no
// durable record of the loss exists.
func (d *DLQ) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}
	return d.flushLocked()
}

func (d *DLQ) flushLocked() error {
	var errs []error

	// Flush failures captured by earlier emits first, so the queue order
	// matches the order in which the failures happened.
	if err := d.writer.Flush(); err != nil {
		errs = append(errs, fmt.Errorf("dlq: flushing dead letters: %w", err))
	}

	if err := d.primary.Flush(); err != nil {
		d.stats.FlushFailures++
		if wErr := d.writeFlushEntryLocked(err); wErr != nil {
			errs = append(errs, wErr)
		}
	}

	// Flush the flush entry, if one was just written.
	if err := d.writer.Flush(); err != nil {
		errs = append(errs, fmt.Errorf("dlq: flushing dead letters: %w", err))
	}
	return errors.Join(errs...)
}

func (d *DLQ) writeFlushEntryLocked(cause error) error {
	entry := DLQEntry{
		Kind:     DLQKind,
		FailedAt: d.now().UTC().Format(time.RFC3339Nano),
		Stage:    DLQStageFlush,
		Error:    cause.Error(),
	}
	if err := d.enc.Encode(entry); err != nil {
		return fmt.Errorf("dlq: writing flush entry: %w", err)
	}
	if err := d.writer.Flush(); err != nil {
		return fmt.Errorf("dlq: flushing flush entry: %w", err)
	}
	d.notifyLocked(entry)
	return nil
}

// Stats returns a snapshot of what the DLQ has captured.
func (d *DLQ) Stats() DLQStats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stats
}

// Close flushes the queue, then closes the wrapped sink and, when CloseWriter
// was set, the writer passed to NewDLQ. It is safe to call more than once, and
// every Close waits for the flush it needs rather than returning early.
func (d *DLQ) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}
	d.closed = true

	var errs []error
	if err := d.flushLocked(); err != nil {
		errs = append(errs, err)
	}

	if closer, ok := d.primary.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("dlq: closing primary sink: %w", err))
		}
	}
	if d.closer != nil {
		if err := d.closer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("dlq: closing dead letter writer: %w", err))
		}
	}
	return errors.Join(errs...)
}
