package stream

import (
	"context"
	"fmt"
	"io"
)

// Batch is one page of records returned by a Source, together with the
// server-reported bounds the state machine needs to detect a gap.
type Batch struct {
	// Records are in ascending ID order.
	Records []Record
	// NextCursor resumes the stream immediately after the last record. An
	// empty value means the Source has no further position to offer, and the
	// tailer will keep using the previous cursor.
	NextCursor string
	// Latest is the newest ledger the server has ingested.
	Latest uint32
	// Oldest is the oldest ledger the server still retains. Data before this
	// point has been pruned and is unrecoverable — the only way a gap can
	// occur.
	Oldest uint32
	// Full reports that the page hit the server's limit, meaning more records
	// are already available and the tailer should fetch again without waiting.
	Full bool
}

// Source produces a stream of Records from some upstream, one page at a time.
//
// Implementations must honour three rules, which the tailer relies on for its
// delivery guarantee:
//
//  1. Records within a Batch are ordered by ascending ID.
//  2. Record.ID is fixed-width and zero-padded, so lexicographic order equals
//     chronological order across the whole stream, not just within a page.
//  3. Fetching with the same cursor twice yields the same records. The tailer
//     retries a failed fetch with the unchanged cursor, so a Source that
//     advances internal state on a failed call would create a gap.
//
// To add a new stream source, implement this interface and wire it into a
// command. Nothing in the tailer or in any Sink needs to change. See
// docs/architecture.md.
type Source interface {
	// Name identifies the source in errors and diagnostics.
	Name() string

	// Fetch returns the next page. On the first call cursor is empty, and the
	// Source starts from whatever position it was configured with.
	Fetch(ctx context.Context, cursor string) (Batch, error)

	// CursorLedger reports which ledger a cursor refers to. It returns false
	// when the cursor cannot be interpreted, in which case the tailer skips
	// gap detection for that step rather than guessing.
	CursorLedger(cursor string) (uint32, bool)
}

// Sink consumes records produced by a stream.
//
// Emit is called synchronously from the tailer's single goroutine, so a slow
// Sink applies natural backpressure to polling. That is deliberate: there is
// no internal queue to grow without bound when a downstream consumer stalls.
//
// To add a new output format, implement this interface. See
// docs/architecture.md.
type Sink interface {
	// Emit writes a single record.
	Emit(r Record) error
	// Flush releases any buffered output. It is called when the stream ends,
	// including on cancellation.
	Flush() error
}

// SinkFunc adapts a plain function to the Sink interface, for tests and for
// trivial sinks with nothing to flush.
type SinkFunc func(r Record) error

// Emit implements Sink.
func (f SinkFunc) Emit(r Record) error { return f(r) }

// Flush implements Sink and does nothing.
func (f SinkFunc) Flush() error { return nil }

// WriterFlusher is the subset of bufio.Writer that sinks use, so that a sink
// can be constructed over any writer in tests.
type WriterFlusher interface {
	io.Writer
	Flush() error
}

// GapError reports that records were irrecoverably missed because the stream
// fell behind the server's retention window.
//
// This is the only way prism can lose records, and it is always reported
// rather than silently skipped: a stream with an unannounced hole is worse
// than one that stops.
type GapError struct {
	// Source names the stream that gapped.
	Source string
	// From is the ledger the stream had reached.
	From uint32
	// Oldest is the earliest ledger the server still retains.
	Oldest uint32
}

func (e *GapError) Error() string {
	return fmt.Sprintf(
		"gap in %s stream: resumed at ledger %d but the server has pruned everything before ledger %d, so ledgers %d-%d were missed",
		e.Source, e.From, e.Oldest, e.From, e.Oldest-1,
	)
}
