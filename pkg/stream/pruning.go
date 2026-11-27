package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// DefaultPruneInterval is how often the background pruner compacts persisted
// cursor metadata when no other interval is configured.
//
// Five minutes is frequent enough that the file never grows far beyond its
// retention limit, and rare enough that a long-running stream spends
// effectively no time on it.
const DefaultPruneInterval = 5 * time.Minute

// DefaultPruneKeep is how many cursor metadata entries are retained per
// stream key when no other limit is configured.
//
// A hundred checkpoints is hours of polling at one checkpoint per page, which
// is far more than a resume needs while keeping the file small.
const DefaultPruneKeep = 100

// ErrPrunerStarted is returned when a Pruner that is already running is
// started again.
var ErrPrunerStarted = errors.New("pruner: already started")

// ErrPrunerClosed is returned when a closed Pruner is started again.
var ErrPrunerClosed = errors.New("pruner: already closed")

// CursorMetadata is one persisted cursor checkpoint.
//
// It records where a stream reached, so a later run can pick the stream up
// where the last one stopped. The cursor itself is opaque to prism: only the
// source that produced it can interpret it.
type CursorMetadata struct {
	// Key identifies the stream the checkpoint belongs to. Two runs share a
	// key only when they watch the same thing, so a recorded cursor is never
	// applied to a different set of filters.
	Key string `json:"key"`
	// Cursor is the position to resume from.
	Cursor string `json:"cursor"`
	// Ledger is the ledger the cursor refers to, or zero when the source
	// cannot say.
	Ledger uint32 `json:"ledger,omitempty"`
	// RecordedAt is when the checkpoint was written. It is what the age
	// based retention policy is measured against.
	RecordedAt time.Time `json:"recorded_at"`
}

// MetadataStore persists cursor metadata and drops the entries that are no
// longer needed.
//
// It is the seam between the streaming core and wherever the metadata is
// kept: a file today, a database tomorrow. Implementations must be safe for
// concurrent use, because the stream appends checkpoints while the
// background pruner rewrites them.
type MetadataStore interface {
	// Append records one checkpoint.
	Append(ctx context.Context, meta CursorMetadata) error

	// Latest returns the most recent checkpoint recorded for key. The second
	// result is false when the key has no checkpoint at all.
	Latest(ctx context.Context, key string) (CursorMetadata, bool, error)

	// Prune removes the entries the policy no longer calls for and reports
	// what it kept and what it removed.
	Prune(ctx context.Context, policy PrunePolicy) (PruneStats, error)
}

// PrunePolicy says which entries a prune pass should keep.
//
// The two limits are independent: an entry is kept when it is both within the
// per-key count and within the age limit.
type PrunePolicy struct {
	// Keep is the number of entries retained per key, counting the newest
	// first. Zero keeps every entry.
	Keep int
	// Cutoff removes entries recorded before it. The zero time keeps entries
	// regardless of age.
	Cutoff time.Time
}

// PruneStats reports the outcome of a prune pass.
type PruneStats struct {
	// Kept counts the entries that were retained.
	Kept int
	// Removed counts the entries that were deleted.
	Removed int
}

// PruneOptions configures a Pruner.
type PruneOptions struct {
	// Interval is how often the background loop prunes. A value of zero or
	// less makes the loop compact once and stop, which is what a caller that
	// only wants the file tidied at startup and shutdown wants.
	Interval time.Duration
	// Keep is the number of entries retained per key. Zero keeps every
	// entry.
	Keep int
	// MaxAge drops entries recorded longer ago than this. Zero keeps entries
	// regardless of age.
	MaxAge time.Duration
	// Clock is injectable for tests. Defaults to RealClock.
	Clock Clock
	// OnPrune, when set, is called after every pass that ran, with what the
	// pass kept and removed.
	OnPrune func(PruneStats)
	// OnError, when set, is called when a pass fails. The loop keeps
	// running: a metadata file that cannot be compacted right now is not a
	// reason to stop streaming.
	OnError func(error)
}

// PrunerStats summarises the work a Pruner has done.
type PrunerStats struct {
	// Passes counts the prune passes that ran.
	Passes int
	// Removed counts the entries deleted across all passes.
	Removed int
	// Errors counts the passes that failed.
	Errors int
}

// Pruner compacts persisted cursor metadata in the background.
//
// The metadata a long-running stream accumulates is unbounded by nature: one
// checkpoint per page, one page per ledger close, for as long as the stream
// runs. Left alone the file grows without limit, which is the failure this
// type exists to prevent.
//
// A Pruner owns exactly one goroutine. Start begins the loop and Close stops
// it and waits for it, so a terminated stream leaves nothing running behind
// it.
type Pruner struct {
	store MetadataStore
	opts  PruneOptions
	clock Clock

	mu      sync.Mutex
	started bool
	closed  bool
	cancel  context.CancelFunc
	done    chan struct{}
	stats   PrunerStats
}

// NewPruner returns a Pruner that compacts store according to opts.
func NewPruner(store MetadataStore, opts PruneOptions) (*Pruner, error) {
	if store == nil {
		return nil, errors.New("pruner: a metadata store is required")
	}
	if opts.Clock == nil {
		opts.Clock = RealClock{}
	}
	if opts.Interval < 0 {
		opts.Interval = 0
	}
	if opts.Keep < 0 {
		opts.Keep = 0
	}
	if opts.MaxAge < 0 {
		opts.MaxAge = 0
	}
	return &Pruner{store: store, opts: opts, clock: opts.Clock}, nil
}

// Start begins pruning in the background and returns immediately.
//
// The loop runs until Close is called or the context is cancelled. Starting a
// Pruner twice is an error: two loops would race for the same file.
func (p *Pruner) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrPrunerClosed
	}
	if p.started {
		return ErrPrunerStarted
	}
	p.started = true

	loopCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.done = make(chan struct{})

	go func() {
		defer close(p.done)
		p.loop(loopCtx)
	}()
	return nil
}

// Close stops the background loop and waits for it to finish.
//
// It is safe to call more than once and safe to call on a Pruner that was
// never started. Closing is what makes the Pruner leak-free: the goroutine it
// owns is joined here, not merely asked to stop.
//
// Every caller waits for the goroutine, not only the first one. A second
// Close that returned while the loop was still running would report the
// pruner as stopped when it is not, and a caller that trusted that report
// could exit with the goroutine still alive.
func (p *Pruner) Close() error {
	p.mu.Lock()
	if p.closed {
		done := p.done
		p.mu.Unlock()
		if done != nil {
			<-done
		}
		return nil
	}
	p.closed = true
	cancel := p.cancel
	done := p.done
	p.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	return nil
}

// Stats reports the work the Pruner has done so far.
func (p *Pruner) Stats() PrunerStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

// PruneOnce runs a single prune pass and reports what it did.
//
// A pass that fails leaves the metadata untouched: the store is responsible
// for that, and a half-written file would be worse than a large one.
func (p *Pruner) PruneOnce(ctx context.Context) (PruneStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return PruneStats{}, err
	}

	policy := PrunePolicy{Keep: p.opts.Keep}
	if p.opts.MaxAge > 0 {
		policy.Cutoff = p.clock.Now().Add(-p.opts.MaxAge)
	}

	stats, err := p.store.Prune(ctx, policy)
	if err != nil {
		p.mu.Lock()
		p.stats.Errors++
		p.mu.Unlock()
		return PruneStats{}, fmt.Errorf("pruning cursor metadata: %w", err)
	}

	p.mu.Lock()
	p.stats.Passes++
	p.stats.Removed += stats.Removed
	p.mu.Unlock()

	if p.opts.OnPrune != nil {
		p.opts.OnPrune(stats)
	}
	return stats, nil
}

// loop prunes on every interval until the context is cancelled.
//
// A failed pass is reported and the loop continues, because a metadata file
// that cannot be compacted right now is not a reason to stop streaming.
func (p *Pruner) loop(ctx context.Context) {
	for {
		p.runPass(ctx)

		if p.opts.Interval <= 0 {
			return
		}
		if err := p.clock.Sleep(ctx, p.opts.Interval); err != nil {
			// Cancellation is a clean stop, not a failure.
			return
		}
	}
}

// runPass prunes once and reports a failure.
func (p *Pruner) runPass(ctx context.Context) {
	if _, err := p.PruneOnce(ctx); err != nil {
		if IsCancellation(err) {
			return
		}
		if p.opts.OnError != nil {
			p.opts.OnError(err)
		}
	}
}

// PruneStateHistory removes the checkpoints in state's history recorded
// longer ago than retention, measured from now, and reports how many it
// removed. See PruneStateHistoryAt.
func PruneStateHistory(state *State, retention time.Duration) int {
	return PruneStateHistoryAt(state, retention, time.Now())
}

// PruneStateHistoryAt removes the checkpoints in state's history recorded
// before now-retention and reports how many it removed.
//
// Two things are never removed, whatever their age:
//
//   - the active resume cursor. That is the State's own Ledger, PagingToken
//     and LastID, which this function does not touch: history is an audit
//     trail, and pruning it can never move where the next run resumes;
//   - the most recent checkpoint, so a stream that has been idle for longer
//     than the retention window still carries a record of where it was.
//
// A nil state or a non-positive retention removes nothing. The surviving
// checkpoints keep their order.
func PruneStateHistoryAt(state *State, retention time.Duration, now time.Time) int {
	if state == nil || retention <= 0 || len(state.History) == 0 {
		return 0
	}
	cutoff := now.Add(-retention)

	// The newest checkpoint is found by time rather than position, so a
	// history whose entries were written out of order still keeps the one
	// that really is the latest.
	newest := 0
	for i, cp := range state.History {
		if !cp.RecordedAt.Before(state.History[newest].RecordedAt) {
			newest = i
		}
	}

	kept := state.History[:0]
	removed := 0
	for i, cp := range state.History {
		if i == newest || !cp.RecordedAt.Before(cutoff) {
			kept = append(kept, cp)
			continue
		}
		removed++
	}
	// Clear the tail so the dropped checkpoints can be collected.
	clear(state.History[len(kept):])
	state.History = kept
	return removed
}
