package sink_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

var errBoom = errors.New("boom")

// queueSink is a controllable stream.Sink for exercising the DLQ.
type queueSink struct {
	failErr  error
	flushErr error
	closeErr error

	emitted []stream.Record
	flushes int
	closed  int
}

func (s *queueSink) Emit(r stream.Record) error {
	if s.failErr != nil {
		return s.failErr
	}
	s.emitted = append(s.emitted, r)
	return nil
}

func (s *queueSink) Flush() error {
	s.flushes++
	return s.flushErr
}

func (s *queueSink) Close() error {
	s.closed++
	return s.closeErr
}

// failingWriter fails every write, which is how a full disk looks.
type failingWriter struct {
	err    error
	writes int
}

func (w *failingWriter) Write(p []byte) (int, error) {
	w.writes++
	return 0, w.err
}

// closeTrackingWriter records whether the DLQ closed a writer it does not own.
type closeTrackingWriter struct {
	bytes.Buffer
	closed int
}

func (w *closeTrackingWriter) Close() error {
	w.closed++
	return nil
}

func fixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

func decodeDeadLetters(t *testing.T, buf *bytes.Buffer) []sink.DLQEntry {
	t.Helper()

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return nil
	}

	entries := make([]sink.DLQEntry, 0, len(lines))
	for _, line := range lines {
		var entry sink.DLQEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("dead letter is not valid JSON: %v\n%s", err, line)
		}
		entries = append(entries, entry)
	}
	return entries
}

func TestNewDLQRequiresSinkAndWriter(t *testing.T) {
	t.Parallel()

	if _, err := sink.NewDLQ(nil, &bytes.Buffer{}, sink.DLQOptions{}); err == nil {
		t.Error("NewDLQ(nil sink) error = nil, want an error")
	}
	if _, err := sink.NewDLQ(stream.SinkFunc(func(stream.Record) error { return nil }), nil, sink.DLQOptions{}); err == nil {
		t.Error("NewDLQ(nil writer) error = nil, want an error")
	}
}

func TestDLQDeliversValidRecords(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 4, 14, 35, 13, 0, time.UTC)
	primary := &queueSink{}
	var queue bytes.Buffer

	var observed []sink.DLQEntry
	dlq, err := sink.NewDLQ(primary, &queue, sink.DLQOptions{
		Now:          fixedClock(now),
		OnDeadLetter: func(entry sink.DLQEntry) { observed = append(observed, entry) },
	})
	if err != nil {
		t.Fatalf("NewDLQ() error = %v", err)
	}

	rec := eventRecord()
	if err := dlq.Emit(rec); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if len(primary.emitted) != 1 || primary.emitted[0].ID != rec.ID {
		t.Fatalf("primary received %d records, want the valid record", len(primary.emitted))
	}
	if queue.Len() != 0 {
		t.Errorf("valid record was dead-lettered: %s", queue.String())
	}
	if got := dlq.Stats(); got.Delivered != 1 || got.Failed != 0 || got.Malformed != 0 {
		t.Errorf("stats = %+v, want one delivery and no failures", got)
	}
	if len(observed) != 0 {
		t.Errorf("OnDeadLetter called %d times for a valid record", len(observed))
	}
}

func TestDLQCapturesMalformedRecords(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 4, 14, 35, 13, 0, time.UTC)

	tests := []struct {
		name string
		rec  stream.Record
		want string
	}{
		{
			name: "missing ID",
			rec:  stream.Record{Kind: stream.KindLedger, Close: &stream.LedgerClose{}},
			want: "no ID",
		},
		{
			name: "unknown kind",
			rec:  stream.Record{ID: "0000000001", Kind: stream.Kind("nonsense")},
			want: "unknown kind",
		},
		{
			name: "event without payload",
			rec:  stream.Record{ID: "0000000002", Kind: stream.KindEvent},
			want: "no payload",
		},
		{
			name: "ledger without payload",
			rec:  stream.Record{ID: "0000000003", Kind: stream.KindLedger},
			want: "no payload",
		},
		{
			name: "transaction without payload",
			rec:  stream.Record{ID: "0000000004", Kind: stream.KindTransaction},
			want: "no payload",
		},
		{
			name: "storage change without payload",
			rec:  stream.Record{ID: "0000000005", Kind: stream.KindStorageChange},
			want: "no payload",
		},
		{
			name: "wasm deployment without payload",
			rec:  stream.Record{ID: "0000000006", Kind: stream.KindWASMDeployment},
			want: "no payload",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			primary := &queueSink{}
			var queue bytes.Buffer
			var observed []sink.DLQEntry
			dlq, err := sink.NewDLQ(primary, &queue, sink.DLQOptions{
				Now:          fixedClock(now),
				OnDeadLetter: func(entry sink.DLQEntry) { observed = append(observed, entry) },
			})
			if err != nil {
				t.Fatalf("NewDLQ() error = %v", err)
			}

			if err := dlq.Emit(tc.rec); err != nil {
				t.Fatalf("Emit() error = %v, want the malformed record to be captured", err)
			}
			if len(primary.emitted) != 0 {
				t.Errorf("malformed record reached the primary sink")
			}

			entries := decodeDeadLetters(t, &queue)
			if len(entries) != 1 {
				t.Fatalf("got %d dead letters, want 1\n%s", len(entries), queue.String())
			}
			entry := entries[0]
			if entry.Kind != sink.DLQKind || entry.Stage != sink.DLQStageValidation {
				t.Errorf("entry = %+v, want kind %q stage %q", entry, sink.DLQKind, sink.DLQStageValidation)
			}
			if !strings.Contains(entry.Error, tc.want) {
				t.Errorf("entry error = %q, want it to mention %q", entry.Error, tc.want)
			}
			if entry.Record == nil || entry.Record.ID != tc.rec.ID || entry.Record.Kind != tc.rec.Kind {
				t.Errorf("entry record = %+v, want the original record preserved", entry.Record)
			}
			if entry.FailedAt != now.Format(time.RFC3339Nano) {
				t.Errorf("failedAt = %q, want the injected clock", entry.FailedAt)
			}
			if len(observed) != 1 || observed[0].Stage != sink.DLQStageValidation {
				t.Errorf("OnDeadLetter observed %d entries, want the validation failure", len(observed))
			}
			if got := dlq.Stats(); got.Malformed != 1 || got.Delivered != 0 || got.Failed != 0 {
				t.Errorf("stats = %+v, want one malformed record", got)
			}
		})
	}
}

func TestValidateRecordAcceptsEveryKind(t *testing.T) {
	t.Parallel()

	records := []stream.Record{
		{ID: "0000000001", Kind: stream.KindEvent, Event: &stream.Event{}},
		{ID: "0000000002", Kind: stream.KindLedger, Close: &stream.LedgerClose{}},
		{ID: "0000000003", Kind: stream.KindTransaction, Transaction: &stream.Transaction{}},
		{ID: "0000000004", Kind: stream.KindStorageChange, StorageChange: &stream.StorageChange{}},
		{ID: "0000000005", Kind: stream.KindWASMDeployment, WASMDeployment: &stream.WASMEvent{}},
	}
	for _, rec := range records {
		if err := sink.ValidateRecord(rec); err != nil {
			t.Errorf("ValidateRecord(%s) error = %v, want nil", rec.Kind, err)
		}
	}
}

func TestDLQCapturesDeliveryFailure(t *testing.T) {
	t.Parallel()

	primary := &queueSink{failErr: errBoom}
	var queue bytes.Buffer
	dlq, err := sink.NewDLQ(primary, &queue, sink.DLQOptions{})
	if err != nil {
		t.Fatalf("NewDLQ() error = %v", err)
	}

	rec := eventRecord()
	if err := dlq.Emit(rec); err != nil {
		t.Fatalf("Emit() error = %v, want the delivery failure to be captured", err)
	}

	entries := decodeDeadLetters(t, &queue)
	if len(entries) != 1 {
		t.Fatalf("got %d dead letters, want 1\n%s", len(entries), queue.String())
	}
	entry := entries[0]
	if entry.Stage != sink.DLQStageDelivery {
		t.Errorf("stage = %q, want %q", entry.Stage, sink.DLQStageDelivery)
	}
	if !strings.Contains(entry.Error, errBoom.Error()) {
		t.Errorf("error = %q, want the underlying failure", entry.Error)
	}
	if entry.Record == nil || entry.Record.ID != rec.ID {
		t.Errorf("record = %+v, want the failed record", entry.Record)
	}
	if got := dlq.Stats(); got.Failed != 1 || got.Delivered != 0 || got.Malformed != 0 {
		t.Errorf("stats = %+v, want one delivery failure", got)
	}
}

func TestDLQReturnsErrorWhenTheQueueCannotBeWritten(t *testing.T) {
	t.Parallel()

	writeErr := errors.New("disk full")
	writer := &failingWriter{err: writeErr}
	primary := &queueSink{failErr: errBoom}
	dlq, err := sink.NewDLQ(primary, writer, sink.DLQOptions{})
	if err != nil {
		t.Fatalf("NewDLQ() error = %v", err)
	}

	// A payload larger than bufio's buffer forces the encoder to write
	// through to the failing writer during Emit rather than at Flush.
	rec := eventRecord()
	rec.Event.ValueXDR = strings.Repeat("A", 20_000)

	err = dlq.Emit(rec)
	if err == nil {
		t.Fatal("Emit() error = nil, want the queue write failure")
	}
	if !errors.Is(err, writeErr) {
		t.Errorf("Emit() error = %v, want it to wrap %v", err, writeErr)
	}
	if writer.writes == 0 {
		t.Error("the failing writer was never written to")
	}
}

func TestDLQFlushCapturesPrimaryFlushFailure(t *testing.T) {
	t.Parallel()

	primary := &queueSink{flushErr: errBoom}
	var queue bytes.Buffer
	dlq, err := sink.NewDLQ(primary, &queue, sink.DLQOptions{})
	if err != nil {
		t.Fatalf("NewDLQ() error = %v", err)
	}

	if err := dlq.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := dlq.Flush(); err != nil {
		t.Fatalf("Flush() error = %v, want a flush failure to be captured", err)
	}
	if primary.flushes != 1 {
		t.Errorf("primary flushed %d times, want 1", primary.flushes)
	}

	entries := decodeDeadLetters(t, &queue)
	if len(entries) != 1 {
		t.Fatalf("got %d dead letters, want the flush failure\n%s", len(entries), queue.String())
	}
	if entries[0].Stage != sink.DLQStageFlush || entries[0].Record != nil {
		t.Errorf("entry = %+v, want a flush entry with no record", entries[0])
	}
	if !strings.Contains(entries[0].Error, errBoom.Error()) {
		t.Errorf("error = %q, want the flush failure", entries[0].Error)
	}
	if got := dlq.Stats(); got.FlushFailures != 1 {
		t.Errorf("FlushFailures = %d, want 1", got.FlushFailures)
	}
}

func TestDLQFlushReturnsQueueFailure(t *testing.T) {
	t.Parallel()

	writeErr := errors.New("disk full")
	writer := &failingWriter{err: writeErr}
	dlq, err := sink.NewDLQ(&queueSink{}, writer, sink.DLQOptions{})
	if err != nil {
		t.Fatalf("NewDLQ() error = %v", err)
	}

	bad := stream.Record{ID: "0000000001", Kind: stream.Kind("nonsense")}
	if err := dlq.Emit(bad); !errors.Is(err, writeErr) {
		t.Errorf("Emit() error = %v, want it to wrap %v", err, writeErr)
	}
	if err := dlq.Flush(); !errors.Is(err, writeErr) {
		t.Errorf("Flush() error = %v, want it to wrap %v", err, writeErr)
	}
}

func TestDLQEmitBatchAccountsForEveryRecord(t *testing.T) {
	t.Parallel()

	good := eventRecord()
	bad := stream.Record{ID: "0000000002", Kind: stream.Kind("nonsense")}
	undeliverable := eventRecord()
	undeliverable.ID = "0000000003"
	last := eventRecord()
	last.ID = "0000000004"

	primary := &selectiveSink{failID: undeliverable.ID}
	var queue bytes.Buffer
	dlq, err := sink.NewDLQ(primary, &queue, sink.DLQOptions{})
	if err != nil {
		t.Fatalf("NewDLQ() error = %v", err)
	}

	if err := dlq.EmitBatch([]stream.Record{good, bad, undeliverable, last}); err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}

	if len(primary.emitted) != 2 {
		t.Fatalf("primary received %d records, want the two deliverable ones", len(primary.emitted))
	}
	if primary.flushes != 1 {
		t.Errorf("primary flushed %d times, want 1 at the end of the batch", primary.flushes)
	}

	entries := decodeDeadLetters(t, &queue)
	if len(entries) != 2 {
		t.Fatalf("got %d dead letters, want 2\n%s", len(entries), queue.String())
	}
	stages := map[string]int{}
	for _, entry := range entries {
		stages[entry.Stage]++
		if entry.Record == nil {
			t.Errorf("batch entry has no record: %+v", entry)
		}
	}
	if stages[sink.DLQStageValidation] != 1 || stages[sink.DLQStageDelivery] != 1 {
		t.Errorf("stages = %v, want one validation and one delivery failure", stages)
	}
	if got := dlq.Stats(); got.Delivered != 2 || got.Malformed != 1 || got.Failed != 1 {
		t.Errorf("stats = %+v, want two delivered, one malformed and one failed", got)
	}
}

// selectiveSink fails only for one record ID, so a batch test can prove that
// the failure is attributed to the right record.
type selectiveSink struct {
	failID  string
	emitted []stream.Record
	flushes int
}

func (s *selectiveSink) Emit(r stream.Record) error {
	if r.ID == s.failID {
		return errBoom
	}
	s.emitted = append(s.emitted, r)
	return nil
}

func (s *selectiveSink) Flush() error {
	s.flushes++
	return nil
}

func TestDLQClosesWriterAndPrimaryIdempotently(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "dead.ndjson")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("opening queue file: %v", err)
	}

	primary := &queueSink{}
	dlq, err := sink.NewDLQ(primary, file, sink.DLQOptions{CloseWriter: true})
	if err != nil {
		t.Fatalf("NewDLQ() error = %v", err)
	}
	if err := dlq.Emit(stream.Record{ID: "0000000001", Kind: stream.Kind("nonsense")}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	if err := dlq.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := dlq.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if primary.closed != 1 {
		t.Errorf("primary closed %d times, want 1", primary.closed)
	}

	// The file must be closed and complete: reading it back is what proves
	// the DLQ flushed before closing rather than dropping the buffer.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading queue file: %v", err)
	}
	if got := len(decodeDeadLetters(t, bytes.NewBuffer(data))); got != 1 {
		t.Errorf("queue file has %d entries, want 1", got)
	}
}

func TestDLQDoesNotCloseWriterByDefault(t *testing.T) {
	t.Parallel()

	writer := &closeTrackingWriter{}
	dlq, err := sink.NewDLQ(&queueSink{}, writer, sink.DLQOptions{})
	if err != nil {
		t.Fatalf("NewDLQ() error = %v", err)
	}
	if err := dlq.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if writer.closed != 0 {
		t.Errorf("writer closed %d times, want it left alone", writer.closed)
	}
}

func TestDLQCloseJoinsErrors(t *testing.T) {
	t.Parallel()

	writer := &closeTrackingWriter{}
	primary := &queueSink{closeErr: errBoom}
	dlq, err := sink.NewDLQ(primary, writer, sink.DLQOptions{CloseWriter: true})
	if err != nil {
		t.Fatalf("NewDLQ() error = %v", err)
	}

	if err := dlq.Close(); !errors.Is(err, errBoom) {
		t.Errorf("Close() error = %v, want it to include the primary close failure", err)
	}
}

func TestDLQRejectsUseAfterClose(t *testing.T) {
	t.Parallel()

	dlq, err := sink.NewDLQ(&queueSink{}, &bytes.Buffer{}, sink.DLQOptions{})
	if err != nil {
		t.Fatalf("NewDLQ() error = %v", err)
	}
	if err := dlq.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := dlq.Emit(eventRecord()); !errors.Is(err, os.ErrClosed) {
		t.Errorf("Emit() after Close() error = %v, want os.ErrClosed", err)
	}
	if err := dlq.EmitBatch([]stream.Record{eventRecord()}); !errors.Is(err, os.ErrClosed) {
		t.Errorf("EmitBatch() after Close() error = %v, want os.ErrClosed", err)
	}
	if err := dlq.Flush(); err != nil {
		t.Errorf("Flush() after Close() error = %v, want nil", err)
	}
}

// attemptSink fails a fixed number of deliveries, counting every attempt.
type attemptSink struct {
	failures int
	attempts int
	emitted  []stream.Record
}

func (s *attemptSink) Emit(r stream.Record) error {
	s.attempts++
	if s.attempts <= s.failures {
		return errBoom
	}
	s.emitted = append(s.emitted, r)
	return nil
}

func (s *attemptSink) Flush() error { return nil }

func TestDLQDirectoryWritesOneFilePerRecordAfterBackoff(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "dlq")
	primary := &attemptSink{failures: 1 << 30}
	now := time.Unix(1_700_000_000, 0).UTC()

	dlq, err := sink.NewDLQ(primary, nil, sink.DLQOptions{
		Directory:        dir,
		MaxRecordRetries: 3,
		RetryBackoff:     time.Millisecond,
		MaxRetryBackoff:  2 * time.Millisecond,
		Now:              func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewDLQ() error = %v", err)
	}

	record := eventRecord()
	if err := dlq.Emit(record); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if primary.attempts != 4 {
		t.Fatalf("delivery attempts = %d, want the initial attempt plus 3 retries", primary.attempts)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading queue directory: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("dead letters = %d, want 1", len(entries))
	}

	name := entries[0].Name()
	if !strings.HasPrefix(name, "20231114T221320.000000000Z_") || !strings.HasSuffix(name, ".json") {
		t.Errorf("dead letter file %q is not <timestamp>_<id>.json", name)
	}
	if !strings.Contains(name, record.ID) {
		t.Errorf("dead letter file %q does not name record %s", name, record.ID)
	}

	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("reading dead letter: %v", err)
	}
	var entry struct {
		Kind   string         `json:"kind"`
		Stage  string         `json:"stage"`
		Error  string         `json:"error"`
		Stack  string         `json:"stack"`
		Record *stream.Record `json:"record"`
	}
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("decoding dead letter: %v", err)
	}
	if entry.Kind != sink.DLQKind {
		t.Errorf("kind = %q, want %q", entry.Kind, sink.DLQKind)
	}
	if entry.Stage != sink.DLQStageDelivery {
		t.Errorf("stage = %q, want %q", entry.Stage, sink.DLQStageDelivery)
	}
	if !strings.Contains(entry.Error, errBoom.Error()) {
		t.Errorf("error = %q, want it to contain %q", entry.Error, errBoom)
	}
	if !strings.Contains(entry.Stack, "goroutine") {
		t.Errorf("stack = %q, want a goroutine trace", entry.Stack)
	}
	if entry.Record == nil || entry.Record.ID != record.ID {
		t.Errorf("record = %+v, want the failed record preserved", entry.Record)
	}
}

func TestDLQDirectoryRetriesTransientFailuresWithoutSpilling(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "dlq")
	primary := &attemptSink{failures: 2}

	dlq, err := sink.NewDLQ(primary, nil, sink.DLQOptions{
		Directory:        dir,
		MaxRecordRetries: 3,
		RetryBackoff:     time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewDLQ() error = %v", err)
	}

	if err := dlq.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if primary.attempts != 3 {
		t.Errorf("delivery attempts = %d, want 3", primary.attempts)
	}
	if len(primary.emitted) != 1 {
		t.Errorf("delivered records = %d, want 1", len(primary.emitted))
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading queue directory: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("dead letters = %d, want none after a recovered retry", len(entries))
	}
}
