package stream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// DefaultPollInterval is roughly one Stellar ledger close. Polling faster
// than ledgers close only burns request quota.
const DefaultPollInterval = 5 * time.Second

// Stats records what a tail run did, for diagnostics and for tests to assert
// on.
type Stats struct {
	// Emitted counts records handed to the Sink.
	Emitted int
	// Duplicates counts records the server re-sent that were suppressed.
	// A non-zero value after a reconnect is expected and healthy.
	Duplicates int
	// Fetches counts successful upstream calls.
	Fetches int
	// Retries counts fetches that failed and were retried.
	Retries int
}

// Options configures a Tailer.
type Options struct {
	// Follow keeps the stream open, polling for new records. When false the
	// tailer drains what is already available and returns.
	Follow bool
	// Limit stops the stream after this many records. Zero means unlimited.
	Limit int
	// PollInterval is the wait between polls once caught up.
	PollInterval time.Duration
	// Backoff governs retry delays after a failed fetch.
	Backoff Backoff
	// MaxRetries bounds consecutive failed fetches before giving up. Zero
	// means retry indefinitely, which is usually what a long-running tail
	// wants.
	MaxRetries int
	// Clock is injectable for tests. Defaults to RealClock.
	Clock Clock
	// OnRetry, when set, is called before each backoff wait so the CLI can
	// tell the user what is happening rather than appearing to hang.
	OnRetry func(attempt int, delay time.Duration, err error)
}

// Tailer drives a Source, emitting records to a Sink.
//
// # Delivery guarantee
//
// A Tailer provides at-least-once delivery with in-process de-duplication,
// and explicit gap detection. Concretely:
//
//   - No duplicates within a run. Cursor paging is exact, and every record is
//     checked against the highest ID already emitted, so a page replayed after
//     a reconnect is suppressed.
//
//   - No silent gaps, ever. A failed fetch is retried with the cursor
//     unchanged, so nothing is skipped by a dropped connection. The one way
//     records can genuinely be lost is falling behind the server's retention
//     window, and that is detected against the server-reported oldest ledger
//     and returned as a *GapError rather than passed over.
//
//   - Not exactly-once across restarts. The tailer cannot atomically emit a
//     record and commit its cursor, so a process killed between the two will
//     re-emit on restart. Durable cursors would narrow this and are on the
//     roadmap; consumers that care should key off Record.ID.
type Tailer struct {
	src  Source
	sink Sink
	opts Options

	// cursor is the position to resume from. Empty means "start where the
	// Source was configured to start".
	cursor string
	// lastID is the highest record ID emitted so far. Because IDs are
	// fixed-width and zero-padded, a plain string comparison orders them
	// correctly, which is what makes de-duplication a single comparison.
	lastID string

	stats Stats
}

// NewTailer returns a Tailer wired to src and sink.
func NewTailer(src Source, sink Sink, opts Options) *Tailer {
	if opts.PollInterval <= 0 {
		opts.PollInterval = DefaultPollInterval
	}
	if opts.Clock == nil {
		opts.Clock = RealClock{}
	}
	if opts.Backoff.Initial == 0 && opts.Backoff.Max == 0 {
		opts.Backoff = DefaultBackoff()
	}
	return &Tailer{src: src, sink: sink, opts: opts}
}

// Stats returns a snapshot of what the run has done so far.
func (t *Tailer) Stats() Stats { return t.stats }

// Cursor returns the position the stream has reached, suitable for resuming
// a later run.
func (t *Tailer) Cursor() string { return t.cursor }

// Run drives the stream until it completes, hits an error, or ctx is
// cancelled.
//
// Cancellation is a clean stop, not a failure: Run flushes the sink and
// returns nil so that Ctrl-C on a tail does not look like a crash.
func (t *Tailer) Run(ctx context.Context) error {
	defer func() {
		// Best-effort flush; a flush error on an already-failing path should
		// not mask the original cause.
		_ = t.sink.Flush()
	}()

	for {
		if err := ctx.Err(); err != nil {
			return t.finish(nil)
		}

		batch, err := t.fetchWithRetry(ctx)
		if err != nil {
			if isCancellation(err) {
				return t.finish(nil)
			}
			return err
		}

		if err := t.checkGap(batch); err != nil {
			return err
		}

		done, err := t.emit(batch)
		if err != nil {
			return err
		}
		if done {
			return t.finish(nil)
		}

		// Advance only after a fully successful emit. If emitting failed we
		// return above with the cursor untouched, so a caller retrying the
		// whole run resumes from the same place rather than past the failure.
		if batch.NextCursor != "" {
			t.cursor = batch.NextCursor
		}

		// A full page means the server already has more for us; keep draining
		// before sleeping, so backfill runs at network speed rather than at
		// one page per poll interval.
		if batch.Full {
			continue
		}

		if !t.opts.Follow {
			return t.finish(nil)
		}

		if err := t.opts.Clock.Sleep(ctx, t.opts.PollInterval); err != nil {
			return t.finish(nil)
		}
	}
}

// finish flushes and returns err, converting a flush failure into the error
// when there is no more pressing one to report.
func (t *Tailer) finish(err error) error {
	if flushErr := t.sink.Flush(); flushErr != nil && err == nil {
		return fmt.Errorf("flushing output: %w", flushErr)
	}
	return err
}

// fetchWithRetry calls the Source, retrying transient failures with backoff
// and always reusing the same cursor so that no record can be skipped.
func (t *Tailer) fetchWithRetry(ctx context.Context) (Batch, error) {
	var attempt int
	for {
		batch, err := t.src.Fetch(ctx, t.cursor)
		if err == nil {
			t.stats.Fetches++
			return batch, nil
		}

		if isCancellation(err) {
			return Batch{}, err
		}
		if !rpc.IsRetryable(err) {
			return Batch{}, fmt.Errorf("%s stream: %w", t.src.Name(), err)
		}

		attempt++
		t.stats.Retries++
		if t.opts.MaxRetries > 0 && attempt > t.opts.MaxRetries {
			return Batch{}, fmt.Errorf(
				"%s stream: giving up after %d consecutive failures: %w",
				t.src.Name(), attempt-1, err)
		}

		delay := t.opts.Backoff.Delay(attempt)
		if t.opts.OnRetry != nil {
			t.opts.OnRetry(attempt, delay, err)
		}
		if sleepErr := t.opts.Clock.Sleep(ctx, delay); sleepErr != nil {
			return Batch{}, sleepErr
		}
	}
}

// checkGap reports whether the stream has fallen behind the server's
// retention window, which is the only circumstance under which records are
// irrecoverably lost.
//
// It only applies once a cursor exists: the first fetch has not resumed from
// anywhere, and an out-of-range start ledger is rejected by the server with a
// clearer message than this check could produce.
func (t *Tailer) checkGap(b Batch) error {
	if t.cursor == "" || b.Oldest == 0 {
		return nil
	}
	at, ok := t.src.CursorLedger(t.cursor)
	if !ok {
		return nil
	}
	if at < b.Oldest {
		return &GapError{Source: t.src.Name(), From: at, Oldest: b.Oldest}
	}
	return nil
}

// emit writes a batch's records, suppressing any the server replayed, and
// reports whether the configured limit has been reached.
func (t *Tailer) emit(b Batch) (done bool, err error) {
	for _, r := range b.Records {
		// IDs are fixed-width and zero-padded, so this comparison is a
		// chronological one. Anything at or before the high-water mark is a
		// replay after a reconnect, or an out-of-order server response.
		if t.lastID != "" && r.ID <= t.lastID {
			t.stats.Duplicates++
			continue
		}

		if err := t.sink.Emit(r); err != nil {
			return false, fmt.Errorf("writing record %s: %w", r.ID, err)
		}
		t.lastID = r.ID
		t.stats.Emitted++

		if t.opts.Limit > 0 && t.stats.Emitted >= t.opts.Limit {
			return true, nil
		}
	}
	return false, nil
}

func isCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
