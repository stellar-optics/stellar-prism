package stream

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// SourceFactory builds an independent Source positioned at startLedger.
//
// A factory, rather than a single Source, is what lets the drainer fetch
// several ledger ranges concurrently: each worker owns a fresh cursor and
// therefore cannot race another worker's pagination.
type SourceFactory func(startLedger uint32) (Source, error)

// DrainerConfig configures a historical backfill drainer.
type DrainerConfig struct {
	// NewSource builds the source for one worker. Required.
	NewSource SourceFactory
	// Sink receives records in ascending ID order. Required.
	Sink Sink
	// From is the first ledger to drain. Required.
	From uint32
	// Until is the last ledger to drain. Zero means the server's latest
	// ledger, discovered during the run.
	Until uint32
	// Workers is the number of concurrent page fetchers. Values below one
	// become one.
	Workers int
	// Limit stops after this many records. Zero means unlimited.
	Limit int
	// GapStrategy selects what to do if From predates the retention window.
	// The empty value means GapFail.
	GapStrategy GapStrategy
	// MaxGapRecoveries bounds gap skips. Zero means no bound.
	MaxGapRecoveries int
	// Backoff governs retry delays after a failed fetch.
	Backoff Backoff
	// MaxRetries bounds consecutive failed fetches per worker. Zero means
	// retry indefinitely.
	MaxRetries int
	// Clock is injectable for tests. Defaults to RealClock.
	Clock Clock
	// OnRetry, when set, is called before each backoff wait.
	OnRetry func(attempt int, delay time.Duration, err error)
	// OnGap, when set, is called for every detected retention gap.
	OnGap func(GapEvent)
}

// Drainer backfills a ledger range by fetching pages concurrently.
//
// Each worker owns a contiguous ledger range and its own Source, so cursors
// never interfere. Records from all workers are merged by ID before reaching
// the Sink, preserving the ordering and de-duplication guarantees the tailer
// provides. Workers stop as soon as the sink fails or the caller cancels,
// and Run waits for every one of them before returning.
type Drainer struct {
	cfg       DrainerConfig
	until     uint32
	callbackM sync.Mutex
	gapM      sync.Mutex

	emitted    atomic.Int64
	duplicates atomic.Int64
	fetches    atomic.Int64
	retries    atomic.Int64
	gaps       atomic.Int64
	recovered  atomic.Int64
}

// NewDrainer validates cfg and returns a Drainer.
func NewDrainer(cfg DrainerConfig) (*Drainer, error) {
	if cfg.NewSource == nil {
		return nil, fmt.Errorf("drainer: NewSource is required")
	}
	if cfg.Sink == nil {
		return nil, fmt.Errorf("drainer: Sink is required")
	}
	if cfg.From == 0 {
		return nil, fmt.Errorf("drainer: From must be a concrete ledger")
	}
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.Clock == nil {
		cfg.Clock = RealClock{}
	}
	if cfg.Backoff.Initial == 0 && cfg.Backoff.Max == 0 {
		cfg.Backoff = DefaultBackoff()
	}
	strategy, err := ParseGapStrategy(string(cfg.GapStrategy))
	if err != nil {
		return nil, err
	}
	cfg.GapStrategy = strategy
	return &Drainer{cfg: cfg}, nil
}

// Until reports the last ledger the run will drain. It is zero until Run has
// discovered the server's latest ledger.
func (d *Drainer) Until() uint32 { return d.until }

// Run drains the configured range. Cancellation is a clean stop and returns
// nil; a sink failure, unrecoverable gap or fetch error is returned after all
// workers have exited.
func (d *Drainer) Run(ctx context.Context) (Stats, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	stats, err := d.run(ctx)
	if flushErr := d.cfg.Sink.Flush(); flushErr != nil && err == nil {
		err = fmt.Errorf("flushing output: %w", flushErr)
	}
	return stats, err
}

func (d *Drainer) run(ctx context.Context) (Stats, error) {
	probe, err := d.cfg.NewSource(d.cfg.From)
	if err != nil {
		return d.snapshot(), fmt.Errorf("constructing backfill source: %w", err)
	}
	batch, err := d.fetch(ctx, probe, "")
	if err != nil {
		if IsCancellation(err) && ctx.Err() != nil {
			return d.snapshot(), nil //nolint:nilerr // cancellation is a clean stop
		}
		return d.snapshot(), err
	}

	from := d.cfg.From
	if batch.Oldest != 0 && from < batch.Oldest {
		gap := &GapError{Source: probe.Name(), From: from, Oldest: batch.Oldest}
		d.gaps.Add(1)
		recovery := GapRecovery{
			Strategy:      d.cfg.GapStrategy,
			MaxRecoveries: d.cfg.MaxGapRecoveries,
			OnGap:         d.notifyGap,
		}
		recovered, recoverErr := recovery.Handle(probe, gap)
		if recoverErr != nil {
			return d.snapshot(), recoverErr
		}
		if recovered {
			d.recovered.Add(1)
			from = gap.Oldest
		}
	}

	d.until = d.cfg.Until
	if d.until == 0 {
		d.until = batch.Latest
	}
	if d.until == 0 {
		return d.snapshot(), fmt.Errorf("server did not report a latest ledger; pass an explicit Until bound")
	}
	if d.until < from {
		return d.snapshot(), nil
	}

	ranges := splitLedgerRange(from, d.until, d.cfg.Workers)
	if len(ranges) == 0 {
		return d.snapshot(), nil
	}

	group := NewTaskGroup(ctx)
	workCtx := group.Context()
	channels := make([]chan []Record, len(ranges))
	for i, ledgerRange := range ranges {
		ch := make(chan []Record, 1)
		channels[i] = ch
		r := ledgerRange
		group.Go(func(ctx context.Context) error {
			return d.worker(ctx, r, ch)
		})
	}

	streams := make([]mergeStream, len(channels))
	for i, ch := range channels {
		streams[i] = mergeStream{ch: ch}
	}

	var items mergeHeap
	for i := range streams {
		if !streams[i].refill(workCtx) {
			continue
		}
		if record, ok := streams[i].current(); ok {
			heap.Push(&items, mergeItem{record: record, worker: i})
		}
	}

	var lastID string
	for items.Len() > 0 {
		if workCtx.Err() != nil {
			return d.stop(group)
		}

		popped := heap.Pop(&items)
		item, ok := popped.(mergeItem)
		if !ok {
			return d.snapshot(), fmt.Errorf("drainer: unexpected merge item %T", popped)
		}
		record := item.record

		if lastID == "" || record.ID > lastID {
			if err := d.cfg.Sink.Emit(record); err != nil {
				group.Cancel()
				if waitErr := group.Wait(); waitErr != nil {
					err = errors.Join(err, waitErr)
				}
				return d.snapshot(), fmt.Errorf("writing record %s: %w", record.ID, err)
			}
			d.emitted.Add(1)
			lastID = record.ID

			if d.cfg.Limit > 0 && d.emitted.Load() >= int64(d.cfg.Limit) {
				return d.stop(group)
			}
		} else {
			d.duplicates.Add(1)
		}

		stream := &streams[item.worker]
		if record, ok := stream.advance(); ok {
			heap.Push(&items, mergeItem{record: record, worker: item.worker})
			continue
		}
		if streams[item.worker].refill(workCtx) {
			if record, ok := stream.current(); ok {
				heap.Push(&items, mergeItem{record: record, worker: item.worker})
			}
		}
	}

	if err := group.Wait(); err != nil {
		return d.snapshot(), err
	}
	if ctx.Err() != nil {
		return d.snapshot(), nil //nolint:nilerr // cancellation is a clean stop
	}
	return d.snapshot(), nil
}

func (d *Drainer) stop(group *TaskGroup) (Stats, error) {
	group.Cancel()
	if err := group.Wait(); err != nil {
		return d.snapshot(), err
	}
	return d.snapshot(), nil
}

func (d *Drainer) worker(ctx context.Context, ledgerRange ledgerRange, out chan<- []Record) error {
	defer close(out)

	src, err := d.cfg.NewSource(ledgerRange.start)
	if err != nil {
		return fmt.Errorf("constructing backfill source for ledger %d: %w", ledgerRange.start, err)
	}

	cursor := ""
	lower := ledgerRange.start
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		batch, err := d.fetch(ctx, src, cursor)
		if err != nil {
			return err
		}

		if gap := d.workerGap(src, cursor, lower, batch); gap != nil {
			recovered, recoverErr := d.recoverWorkerGap(src, gap)
			if recoverErr != nil {
				return recoverErr
			}
			if recovered {
				cursor = ""
				lower = gap.Oldest
				continue
			}
		}

		page := make([]Record, 0, len(batch.Records))
		reachedEnd := false
		for _, record := range batch.Records {
			if record.Sequence > ledgerRange.end {
				reachedEnd = true
				break
			}
			if record.Sequence < ledgerRange.start {
				continue
			}
			page = append(page, record)
		}

		if len(page) > 0 {
			select {
			case out <- page:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if reachedEnd || len(batch.Records) == 0 || batch.NextCursor == "" {
			return nil
		}
		cursor = batch.NextCursor
	}
}

func (d *Drainer) workerGap(src Source, cursor string, start uint32, batch Batch) *GapError {
	if batch.Oldest == 0 {
		return nil
	}
	if cursor == "" {
		if start < batch.Oldest {
			return &GapError{Source: src.Name(), From: start, Oldest: batch.Oldest}
		}
		return nil
	}
	at, ok := src.CursorLedger(cursor)
	if !ok || at >= batch.Oldest {
		return nil
	}
	return &GapError{Source: src.Name(), From: at, Oldest: batch.Oldest}
}

func (d *Drainer) recoverWorkerGap(src Source, gap *GapError) (bool, error) {
	d.gapM.Lock()
	defer d.gapM.Unlock()

	d.gaps.Add(1)
	if d.cfg.GapStrategy == GapFail {
		d.notifyGap(GapEvent{Gap: gap, Strategy: GapFail})
		return false, gap
	}
	if d.cfg.MaxGapRecoveries > 0 && d.recovered.Load() >= int64(d.cfg.MaxGapRecoveries) {
		d.notifyGap(GapEvent{Gap: gap, Strategy: d.cfg.GapStrategy})
		return false, fmt.Errorf(
			"giving up after %d gap recoveries: %w", d.recovered.Load(), gap)
	}

	rewinder, ok := src.(Rewinder)
	if !ok {
		d.notifyGap(GapEvent{Gap: gap, Strategy: d.cfg.GapStrategy})
		return false, fmt.Errorf(
			"source %q cannot resume after a gap because it does not implement Rewinder: %w",
			src.Name(), gap)
	}

	rewinder.Rewind(gap.Oldest)
	d.recovered.Add(1)
	d.notifyGap(GapEvent{Gap: gap, Strategy: d.cfg.GapStrategy, Recovered: true})
	return true, nil
}

func (d *Drainer) fetch(ctx context.Context, src Source, cursor string) (Batch, error) {
	var attempt int
	for {
		batch, err := src.Fetch(ctx, cursor)
		if err == nil {
			d.fetches.Add(1)
			return batch, nil
		}
		if IsCancellation(err) {
			return Batch{}, err
		}
		if !rpc.IsRetryable(err) {
			return Batch{}, fmt.Errorf("%s stream: %w", src.Name(), err)
		}

		attempt++
		d.retries.Add(1)
		if d.cfg.MaxRetries > 0 && attempt > d.cfg.MaxRetries {
			return Batch{}, fmt.Errorf(
				"%s stream: giving up after %d consecutive failures: %w",
				src.Name(), attempt-1, err)
		}

		delay := d.cfg.Backoff.Delay(attempt)
		d.notifyRetry(attempt, delay, err)
		if sleepErr := d.cfg.Clock.Sleep(ctx, delay); sleepErr != nil {
			return Batch{}, sleepErr
		}
	}
}

func (d *Drainer) notifyRetry(attempt int, delay time.Duration, err error) {
	if d.cfg.OnRetry == nil {
		return
	}
	d.callbackM.Lock()
	defer d.callbackM.Unlock()
	d.cfg.OnRetry(attempt, delay, err)
}

func (d *Drainer) notifyGap(event GapEvent) {
	if d.cfg.OnGap == nil {
		return
	}
	d.callbackM.Lock()
	defer d.callbackM.Unlock()
	d.cfg.OnGap(event)
}

func (d *Drainer) snapshot() Stats {
	return Stats{
		Emitted:    int(d.emitted.Load()),
		Duplicates: int(d.duplicates.Load()),
		Fetches:    int(d.fetches.Load()),
		Retries:    int(d.retries.Load()),
		Gaps:       int(d.gaps.Load()),
		Recovered:  int(d.recovered.Load()),
	}
}

type ledgerRange struct {
	start uint32
	end   uint32
}

func splitLedgerRange(from, until uint32, workers int) []ledgerRange {
	if from > until {
		return nil
	}
	if workers < 1 {
		workers = 1
	}

	total := uint64(until) - uint64(from) + 1
	count := uint64(workers)
	if count > total {
		count = total
	}
	size := (total + count - 1) / count

	ranges := make([]ledgerRange, 0, count)
	for i := uint64(0); i < count; i++ {
		start := uint64(from) + i*size
		if start > uint64(until) {
			break
		}
		end := start + size - 1
		if end > uint64(until) {
			end = uint64(until)
		}
		ranges = append(ranges, ledgerRange{
			start: uint32(start), //nolint:gosec // bounded by until, a uint32
			end:   uint32(end),   //nolint:gosec // bounded by until, a uint32
		})
	}
	return ranges
}

type mergeStream struct {
	ch   <-chan []Record
	page []Record
	idx  int
}

func (s *mergeStream) current() (Record, bool) {
	if s.idx < len(s.page) {
		return s.page[s.idx], true
	}
	return Record{}, false
}

func (s *mergeStream) advance() (Record, bool) {
	s.idx++
	return s.current()
}

func (s *mergeStream) refill(ctx context.Context) bool {
	s.page = nil
	s.idx = 0
	for {
		select {
		case page, ok := <-s.ch:
			if !ok {
				return false
			}
			if len(page) == 0 {
				continue
			}
			s.page = page
			return true
		case <-ctx.Done():
			return false
		}
	}
}

type mergeItem struct {
	record Record
	worker int
}

type mergeHeap []mergeItem

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool { return h[i].record.ID < h[j].record.ID }

func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x any) {
	item, ok := x.(mergeItem)
	if !ok {
		panic(fmt.Sprintf("mergeHeap.Push: unexpected type %T", x))
	}
	*h = append(*h, item)
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
