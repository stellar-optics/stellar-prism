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
	// Filtered counts records rejected by a configured client-side filter.
	Filtered int
	// Duplicates counts records the server re-sent that were suppressed.
	// A non-zero value after a reconnect is expected and healthy.
	Duplicates int
	// Fetches counts successful upstream calls.
	Fetches int
	// Retries counts fetches that failed and were retried.
	Retries int
	// Gaps counts retention gaps the stream detected.
	Gaps int
	// Recovered counts gaps the stream skipped past under GapResume.
	Recovered int
	// Rewinds counts rewinds the stream applied. Each one re-emitted the
	// records between the old and the new position.
	Rewinds int
}

// Page describes the progress of one page that was emitted in full.
//
// It carries the position a later run could resume from, which is what makes
// a durable cursor possible without the tailer knowing anything about
// storage.
type Page struct {
	// Source names the stream that produced the page.
	Source string
	// Cursor is the position to resume from after this page. It is empty
	// when the source offers no further position.
	Cursor string
	// Ledger is the ledger the cursor refers to, or zero when the source
	// cannot say.
	Ledger uint32
	// Records counts the records emitted in the page.
	Records int
}

// Options configures a Tailer.
type Options struct {
	// Follow keeps the stream open, polling for new records. When false the
	// tailer drains what is already available and returns.
	Follow bool
	// Limit stops the stream after this many records. Zero means unlimited.
	Limit int
	// Filter runs before the sink. Rejected records still advance the source
	// cursor, but do not count toward Limit or Emitted.
	Filter RecordFilter
	// PollInterval is the wait between polls once caught up.
	PollInterval time.Duration
	// Backoff governs retry delays after a failed fetch.
	Backoff Backoff
	// MaxRetries bounds consecutive failed fetches before giving up. Zero
	// means retry indefinitely, which is usually what a long-running tail
	// wants.
	MaxRetries int
	// Adaptive switches the follow loop's poll wait to the adaptive poller in
	// backoff.go, which tunes the interval from how many new ledgers close
	// during each wait instead of holding PollInterval fixed. With it on,
	// PollInterval is not consulted; the bounds below are.
	Adaptive bool
	// AdaptiveMin and AdaptiveMax bound the tuned interval. Zero values fall
	// back to DefaultAdaptiveMinPoll and DefaultAdaptiveMaxPoll, and a Max
	// below Min is pulled back up, so out-of-range configuration degrades to
	// clamping rather than failing the stream mid-flight.
	AdaptiveMin time.Duration
	// AdaptiveMax is the longest interval the poller may take.
	AdaptiveMax time.Duration
	// Clock is injectable for tests. Defaults to RealClock.
	Clock Clock
	// StartCursor resumes the stream from a position an earlier run
	// recorded. The empty value means "start where the Source was
	// configured to start".
	//
	// A resumed stream is gap-checked like any other, because the cursor is
	// a real position and the server may have pruned past it.
	StartCursor string
	// OnRetry, when set, is called before each backoff wait so the CLI can
	// tell the user what is happening rather than appearing to hang.
	OnRetry func(attempt int, delay time.Duration, err error)
	// Store, when set, persists the stream position so a later run resumes
	// where this one stopped. Nil — the default — keeps the position in
	// memory only, which is exactly the behaviour prism had before durable
	// cursors existed.
	Store CursorStore
	// StoreKey names the row the position is saved under. Empty means the
	// Source's name; set it when one Source name covers several distinct
	// queries, or two streams would resume from each other's position.
	StoreKey string

	// GapStrategy selects what to do when the server has pruned records.
	// The empty value means GapFail.
	GapStrategy GapStrategy
	// MaxGapRecoveries bounds how many times GapResume may skip ahead. Zero
	// means no bound.
	MaxGapRecoveries int
	// OnGap, when set, is called for every detected gap.
	OnGap func(GapEvent)
	// Rewind, when set, lets a caller step the stream back while it runs.
	//
	// The tailer applies a pending request before its next fetch: the source
	// is repositioned, the cursor is cleared and the de-duplication mark is
	// reset, so the records in between are emitted again. That is deliberate
	// reprocessing, and it is the only way a stream re-emits a record within
	// a single run.
	Rewind *RewindController
	// OnRewind, when set, is called after a rewind has been applied.
	OnRewind func(RewindTarget)
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
//
//   - A rewind re-emits on purpose. Stepping the cursor back reprocesses the
//     records in between, so they are emitted a second time. It is the only
//     way a record is emitted twice within a run, and it happens only when
//     the caller asks for one.
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
	// lastLedger is the ledger of the most recently emitted record. It is
	// the fallback anchor for a rewind when the cursor cannot be read.
	lastLedger uint32
	// rewoundTo is the ledger the last rewind landed on. It is what makes a
	// rewind that reached past the server's retention window detectable.
	rewoundTo uint32

	// persisted is the last position successfully written to the Store, and
	// persistedOK records whether anything has been written or restored yet.
	// Re-writing an unchanged position would turn every idle poll of a
	// caught-up follow into a transaction for nothing.
	persisted   CursorState
	persistedOK bool

	stats    Stats
	recovery GapRecovery
	reorder  *ReorderBuffer
	// Checkpoints wait until all records through their page have left the
	// reorder buffer and reached the sink. The fetch cursor can run ahead.
	pages           []pendingPage
	deliveredCursor string

	// adaptive is non-nil only when Options.Adaptive is set, and then drives
	// the wait between polls instead of the fixed interval.
	adaptive *AdaptivePoller
	// latestSeen tracks the head the previous poll reported, so the adaptive
	// poller can count how many ledgers closed during each wait.
	latestSeen uint32
	// currentLedger is the newest ledger the stream has emitted, which is
	// what the lag reported to the health endpoints is measured from.
	currentLedger uint32
	// leading records that this instance holds the lease, so a change of
	// leadership can be told from an ordinary poll.
	leading bool
	// leaderEpoch is the lease epoch this instance led in. A different
	// epoch means the lease changed hands, so the work in flight belongs
	// to a term that has ended and has to be discarded.
	leaderEpoch uint64
	// flushGate admits one flush at a time. A flush that overran its
	// deadline is abandoned, and a second one must not start alongside
	// it: a sink that buffers would otherwise write the same batch
	// twice.
	flushGate chan struct{}
}

type pendingPage struct {
	page   Page
	lastID string
	// records identifies the unique records first seen on this page, so
	// client-side rejections can be removed from its delivered count.
	records map[string]struct{}
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
	strategy, err := ParseGapStrategy(string(opts.GapStrategy))
	if err != nil {
		strategy = GapFail
	}
	t := &Tailer{
		src:             src,
		sink:            sink,
		opts:            opts,
		cursor:          opts.StartCursor,
		deliveredCursor: opts.StartCursor,
		recovery: GapRecovery{
			Strategy:      strategy,
			MaxRecoveries: opts.MaxGapRecoveries,
			OnGap:         opts.OnGap,
		},
		reorder:   NewReorderBuffer(),
		flushGate: make(chan struct{}, 1),
	}
	if opts.Adaptive {
		t.adaptive = NewAdaptivePoller(opts.AdaptiveMin, opts.AdaptiveMax, 2)
	}
	return t
}

// wait returns how long to hold before the next poll, given what the last
// batch reported. The fixed interval is the default; the adaptive poller
// replaces it by observing how many new ledgers closed during the previous
// wait.
func (t *Tailer) wait(b Batch) time.Duration {
	if t.adaptive == nil {
		return t.opts.PollInterval
	}
	return t.adaptive.Observe(t.newLedgers(b))
}

// newLedgers reports how many ledgers closed since the previous poll, or
// zero when the head did not advance or was reported out of order by a
// lagging backend, which is both indistinguishable from idle for tuning
// purposes and safe to treat as such.
func (t *Tailer) newLedgers(b Batch) int {
	if b.Latest == 0 || t.latestSeen == 0 || b.Latest <= t.latestSeen {
		t.latestSeen = b.Latest
		return 0
	}
	n := int64(b.Latest) - int64(t.latestSeen)
	t.latestSeen = b.Latest
	// The delta is only consumed against thresholds, so saturate at int32 to
	// stay portable without losing the decision.
	const int32Max = int64(1)<<31 - 1
	if n > int32Max {
		n = int32Max
	}
	return int(n)
}

// Stats returns a snapshot of what the run has done so far.
func (t *Tailer) Stats() Stats { return t.stats }

// Cursor returns the position the stream has reached, suitable for resuming
// a later run.
func (t *Tailer) Cursor() string { return t.deliveredCursor }

// Run drives the stream until it completes, hits an error, or ctx is
// cancelled.
//
// Cancellation is a clean stop, not a failure: Run flushes the sink and
// returns nil so that Ctrl-C on a tail does not look like a crash.
func (t *Tailer) Run(ctx context.Context) (err error) {
	defer func() {
		// The records still in the sink's buffer are flushed on the way
		// out. A failure there is reported when the run otherwise
		// succeeded, and is left out when it did not: the cause of the
		// failure is the more useful of the two.
		if flushErr := t.flushOutput(context.WithoutCancel(ctx)); flushErr != nil && err == nil {
			err = fmt.Errorf("flushing output: %w", flushErr)
		}
	}()

	if err := t.restore(ctx); err != nil {
		if IsCancellation(err) {
			return t.finish(nil)
		}
		return err
	}
	if err := t.restoreState(); err != nil {
		return err
	}

	for {
		if err := ctx.Err(); err != nil {
			return t.finish(nil)
		}

		// A rewind is applied before the fetch, so a request made while the
		// stream is running takes effect at a page boundary rather than in
		// the middle of a page.
		if err := t.applyRewind(); err != nil {
			return t.finish(err)
		}

		batch, err := t.fetchWithRetry(ctx)
		if err != nil {
			if IsCancellation(err) {
				return t.finish(nil)
			}
			return err
		}

		gapErr := t.checkGap(batch)
		if gapErr == nil {
			gapErr = t.checkRewindGap(batch)
		}
		// The rewind check applies to the page that follows the rewind.
		// Once that page has been seen the target has been passed, and
		// comparing it with a later retention floor would report a gap
		// that is not one: the server's floor moves forward with time.
		t.rewoundTo = 0

		if gapErr != nil {
			t.stats.Gaps++
			recovered, recoverErr := t.recovery.Handle(t.src, gapErr)
			if recoverErr != nil {
				return recoverErr
			}
			if recovered {
				t.stats.Recovered = t.recovery.Recovered()
				t.cursor = ""
				continue
			}
		}

		t.queuePage(batch)
		done, _, err := t.emit(ctx, batch)
		if err != nil {
			if IsCancellation(err) {
				return t.finish(nil)
			}
			return err
		}
		t.reportProgress(batch)
		// A bounded follow has all it needs once the available pages are
		// drained and the buffered suffix satisfies the limit. Do not wait
		// for another ledger just to finish an explicitly bounded request.
		if !done && !batch.Full && t.opts.Limit > 0 && t.stats.Emitted+len(t.reorder.pending) >= t.opts.Limit {
			done, _, err = t.emitReady(ctx, t.reorder.Flush())
			if err != nil {
				return err
			}
		}
		if done {
			return t.finish(nil)
		}

		t.reportReadyPages()

		// Advance only after a fully successful emit. If emitting failed we
		// return above with the cursor untouched, so a caller retrying the
		// whole run resumes from the same place rather than past the failure.
		if batch.NextCursor != "" {
			t.cursor = batch.NextCursor
			// The page boundary is the first point at which the resume
			// position may move; writing it here is what lets a later run
			// fetch from here instead of from the start of the page.
			if err := t.persist(ctx); err != nil {
				if IsCancellation(err) {
					return t.finish(nil)
				}
				return err
			}
			// Re-persist with the server's own token now that the batch has
			// fully drained. It is equivalent to the last record's ID but in
			// the exact form the server issued it, which is the most
			// conservative position to resume from.
			if ledger, ok := t.src.CursorLedger(t.cursor); ok {
				if err := t.persistState(t.cursor, ledger); err != nil {
					return err
				}
			}
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

		if err := t.opts.Clock.Sleep(ctx, t.wait(batch)); err != nil {
			return t.finish(nil)
		}
	}
}

// leadership waits until this instance holds the lease and reports whether
// the stream may continue.
//
// The first time leadership is taken the stream is rewound to the cursor
// the previous leader committed, so a standby that takes over picks up the
// work that was left behind instead of continuing from its own fetch
// cursor, which may be ahead of what was actually delivered.
//
// The same happens after a handover in the other direction: an instance that
// was fenced and later takes the lease back starts a new term, so it
// rewinds again rather than continuing from a cursor the new leader has
// already moved past.
func (t *Tailer) leadership(ctx context.Context) (bool, error) {
	if t.opts.Leader == nil {
		return true, nil
	}
	leader, err := t.opts.Leader.Ensure(ctx)
	if err != nil {
		return false, err
	}
	if !leader {
		return false, nil
	}
	epoch := t.opts.Leader.Epoch()
	if t.leading && epoch == t.leaderEpoch {
		return true, nil
	}
	t.leading = true
	t.leaderEpoch = epoch
	// The pages queued before the handover describe work the previous
	// leader did, so they are dropped rather than committed.
	t.pages = nil
	if t.opts.LeaderResume == nil {
		return true, nil
	}
	cursor, err := t.opts.LeaderResume(ctx)
	if err != nil {
		return false, fmt.Errorf("resuming from the committed cursor: %w", err)
	}
	if cursor != "" {
		t.cursor = cursor
		t.deliveredCursor = cursor
	}
	return true, nil
}

// reportProgress tells the caller how far the stream has got.
func (t *Tailer) reportProgress(b Batch) {
	for _, r := range b.Records {
		if r.Sequence > t.currentLedger {
			t.currentLedger = r.Sequence
		}
	}
	if t.opts.OnProgress == nil {
		return
	}
	t.opts.OnProgress(Progress{
		CurrentLedger: t.currentLedger,
		LatestLedger:  b.Latest,
	})
}

// Drain flushes everything the stream is holding and stops cleanly.
//
// It is what a graceful shutdown calls. The records still held in the
// reorder buffer are written to the sink, the sink is flushed, and the
// pages that were delivered are reported, so the final cursor is committed
// before the process goes away.
//
// The timeout bounds how long the drain may take. A drain that overruns it
// is reported as a failure rather than waited out: a process that is about
// to be killed cannot be helped by a longer flush.
//
// Drain must be called after the run has stopped. It is not safe to call
// while the tailer is still fetching.
func (t *Tailer) Drain(ctx context.Context, timeout time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	done := make(chan error, 1)
	go func() {
		done <- t.drain(ctx)
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("draining the stream: %w", ctx.Err())
	}
}

// drain performs the flush without a deadline of its own.
//
// It runs on its own goroutine so that Drain can honour its timeout: a sink
// that is stuck on a slow network cannot be interrupted, but the process
// waiting for it can give up.
//
// A configured limit is respected: the records the limit left buffered are
// dropped rather than emitted, because a request for N records must not
// quietly become N plus whatever the run happened to be holding.
func (t *Tailer) drain(ctx context.Context) error {
	if t.opts.Limit == 0 || t.stats.Emitted < t.opts.Limit {
		if _, _, err := t.emitReady(ctx, t.reorder.Flush()); err != nil {
			return err
		}
	}
	t.reportReadyPages()
	if err := t.flushOutput(ctx); err != nil {
		return fmt.Errorf("flushing output: %w", err)
	}
	return nil
}

// finish emits whatever the stream is still holding and commits the final
// cursor.
//
// The sink is not flushed here: Run flushes it once, on the way out, so a
// run that ends flushes exactly once whatever path it ended by.
func (t *Tailer) finish(err error) error {
	if err == nil && (t.opts.Limit == 0 || t.stats.Emitted < t.opts.Limit) {
		if _, _, flushErr := t.emitReady(context.Background(), t.reorder.Flush()); flushErr != nil {
			return flushErr
		}
	}
	t.reportReadyPages()
	return err
}

// flushOutput flushes the sink.
//
// Only one flush runs at a time. A flush that overran its deadline is
// abandoned, and a second one waits for it rather than starting alongside
// it: a sink that buffers would otherwise write the same batch twice.
//
// The whole operation, waiting included, is bounded by Options.DrainTimeout
// when one is configured.
func (t *Tailer) flushOutput(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if t.opts.DrainTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.opts.DrainTimeout)
		defer cancel()
	}

	select {
	case t.flushGate <- struct{}{}:
	case <-ctx.Done():
		return fmt.Errorf("waiting for the output to flush: %w", ctx.Err())
	}

	done := make(chan error, 1)
	go func() {
		// The gate is released by the goroutine that holds it, so an
		// abandoned flush keeps it and no second flush starts until it
		// is done.
		defer func() { <-t.flushGate }()
		done <- t.sink.Flush()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
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

		if IsCancellation(err) {
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
func (t *Tailer) checkGap(b Batch) *GapError {
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

// applyRewind consumes a pending rewind request and repositions the stream.
//
// Repositioning clears the cursor and the de-duplication mark, which is what
// makes the records in between be emitted again: a rewind is deliberate
// reprocessing, and it is the only way a stream re-emits a record within a
// single run.
func (t *Tailer) applyRewind() error {
	if t.opts.Rewind == nil {
		return nil
	}
	req, ok := t.opts.Rewind.Take()
	if !ok {
		return nil
	}

	anchor, ok := t.rewindAnchor(req)
	if !ok {
		return fmt.Errorf(
			"cannot rewind the %s stream: it has not reached a position yet", t.src.Name())
	}
	target, err := req.Distance.Target(anchor)
	if err != nil {
		return fmt.Errorf("rewinding the %s stream: %w", t.src.Name(), err)
	}

	rewinder, ok := t.src.(Rewinder)
	if !ok {
		return fmt.Errorf(
			"source %q cannot be rewound because it does not implement Rewinder", t.src.Name())
	}

	rewinder.Rewind(target.Ledger)
	t.cursor = ""
	t.lastID = ""
	t.rewoundTo = target.Ledger
	t.stats.Rewinds++

	if t.opts.OnRewind != nil {
		t.opts.OnRewind(target)
	}
	return nil
}

// rewindAnchor reports the position a rewind is measured from.
//
// An explicit anchor wins, because the caller knows where it wants the step
// measured from. Otherwise the step is measured from the stream's own
// position: the cursor it reached, or the last record it emitted.
func (t *Tailer) rewindAnchor(req RewindRequest) (uint32, bool) {
	if req.Anchor != 0 {
		return req.Anchor, true
	}
	if t.cursor != "" {
		if at, ok := t.src.CursorLedger(t.cursor); ok {
			return at, true
		}
	}
	if t.lastLedger != 0 {
		return t.lastLedger, true
	}
	return 0, false
}

// checkRewindGap reports whether a rewind reached past the server's
// retention window.
//
// Rewinding is a request to reprocess records, so landing before the oldest
// retained ledger means the records that were wanted are gone. That is a
// gap, and it is reported like any other rather than skipped over.
func (t *Tailer) checkRewindGap(b Batch) *GapError {
	if t.rewoundTo == 0 || b.Oldest == 0 {
		return nil
	}
	if t.rewoundTo < b.Oldest {
		return &GapError{Source: t.src.Name(), From: t.rewoundTo, Oldest: b.Oldest}
	}
	return nil
}

// emit writes a batch's records, suppressing any the server replayed, and
// reports whether the configured limit has been reached.
func (t *Tailer) emit(b Batch) (done bool, err error) {
	for _, r := range b.Records {
		if r.ID > p.lastID {
			p.lastID = r.ID
		}
		_, buffered := t.reorder.seen[r.ID]
		if r.ID > t.lastID && !buffered && !seen[r.ID] {
			p.page.Records++
			p.records[r.ID] = struct{}{}
		}
		seen[r.ID] = true
	}
	if ledger, ok := t.src.CursorLedger(b.NextCursor); ok {
		p.page.Ledger = ledger
	}
	t.pages = append(t.pages, p)
}

func (t *Tailer) reportReadyPages() {
	for len(t.pages) > 0 && t.pages[0].lastID <= t.lastID {
		p := t.pages[0].page
		t.pages[0] = pendingPage{}
		t.pages = t.pages[1:]
		if p.Cursor != "" {
			t.deliveredCursor = p.Cursor
		}
		if t.opts.OnPage != nil {
			t.opts.OnPage(p)
		}
	}
}

// emit writes a batch's records, suppressing any the server replayed, and
// reports whether the configured limit has been reached and records written.
func (t *Tailer) emit(ctx context.Context, b Batch) (done bool, emitted int, err error) {
	if t.opts.Store != nil {
		return t.emitReady(ctx, b.Records)
	}
	return t.emitReady(ctx, t.reorder.Add(b.Records))
}

func (t *Tailer) emitReady(ctx context.Context, records []Record) (done bool, emitted int, err error) {
	if len(records) == 0 {
		return false, 0, nil
	}
	if t.opts.Process != nil {
		records, err = t.opts.Process(records)
		if err != nil {
			return false, 0, err
		}
	}
	for _, r := range records {
		// IDs are fixed-width and zero-padded, so this comparison is a
		// chronological one. Anything at or before the high-water mark is a
		// replay after a reconnect, or an out-of-order server response.
		if t.lastID != "" && r.ID <= t.lastID {
			t.stats.Duplicates++
			continue
		}
		if t.opts.Filter != nil {
			matched, err := t.opts.Filter.Match(r)
			if err != nil {
				return false, emitted, fmt.Errorf("filtering record %s: %w", r.ID, err)
			}
			if !matched {
				t.lastID = r.ID
				t.stats.Filtered++
				for i := range t.pages {
					if _, ok := t.pages[i].records[r.ID]; ok {
						t.pages[i].page.Records--
						delete(t.pages[i].records, r.ID)
						break
					}
				}
				t.reportReadyPages()
				continue
			}
		}

		if err := t.sink.Emit(r); err != nil {
			return false, emitted, fmt.Errorf("writing record %s: %w", r.ID, err)
		}
		t.lastID = r.ID
		t.lastLedger = r.Sequence
		t.stats.Emitted++
		emitted++
		t.reportReadyPages()

		// Persist at the cursor the record was fetched from: the record is
		// out, and the next run must both resume at or before it and know
		// what to suppress.
		if err := t.persist(ctx); err != nil {
			return false, emitted, err
		}

		// The position becomes durable only once the record is out: the
		// state file is written after the emit, never before, so it can
		// never run ahead of the stream. A crash in between re-emits this
		// one record on restart — at-least-once, as documented — and the
		// restored high-water mark suppresses anything already delivered.
		if err := t.persistState(r.ID, r.Sequence); err != nil {
			return false, emitted, err
		}

		if t.opts.Limit > 0 && t.stats.Emitted >= t.opts.Limit {
			return true, emitted, nil
		}
	}
	return false, emitted, nil
}

// restoreState resumes the in-memory position from the persisted one.
//
// The saved position wins over a freshly configured start, because that is
// what "durable" means. A state file written by a different stream is
// refused rather than misapplied, and a token the Source cannot interpret is
// reported before any request is made rather than surfacing as a confusing
// server rejection mid-run.
func (t *Tailer) restoreState() error {
	if t.opts.State == nil {
		return nil
	}
	st, err := t.opts.State.Load()
	if err != nil {
		return fmt.Errorf("loading stream state: %w", err)
	}
	if st.Source == "" {
		// Nothing persisted yet: start where the Source was configured.
		return nil
	}
	if st.Source != t.src.Name() {
		return fmt.Errorf(
			"state holds a position for the %q stream, but this run follows %q; use a separate state file per stream",
			st.Source, t.src.Name())
	}
	if st.PagingToken != "" {
		if _, ok := t.src.CursorLedger(st.PagingToken); !ok {
			return fmt.Errorf(
				"state file cursor %q is not a valid %s cursor", st.PagingToken, t.src.Name())
		}
		t.cursor = st.PagingToken
	}
	t.lastID = st.LastID
	return nil
}

// persistState saves the current position, recording pagingToken as the resume
// point and the tailer's high-water mark. It is a no-op when no StateStore
// is configured, which keeps the non-durable path free of extra work.
func (t *Tailer) persistState(pagingToken string, ledger uint32) error {
	if t.opts.State == nil {
		return nil
	}
	st := State{
		Version:     StateVersion,
		Source:      t.src.Name(),
		Ledger:      ledger,
		PagingToken: pagingToken,
		Cursor:      pagingToken,
		LastID:      t.lastID,
		UpdatedAt:   t.opts.Clock.Now(),
	}
	if err := t.opts.State.Save(st); err != nil {
		return fmt.Errorf("persisting stream position: %w", err)
	}
	return nil
}
