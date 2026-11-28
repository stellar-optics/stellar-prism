package sink

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// Fanout delivers every record to each of several sinks.
//
// It is how a stream writes to stdout while also posting to a webhook,
// archiving object blocks or sampling metrics without giving up the existing
// output. A failure in any sink is returned: silently dropping a delivery is
// worse than stopping a stream that has an operator watching it.
type Fanout struct {
	sinks []stream.Sink
}

// NewFanout returns a sink that forwards to sinks in order.
func NewFanout(sinks ...stream.Sink) *Fanout {
	return &Fanout{sinks: sinks}
}

// Emit implements stream.Sink.
func (f *Fanout) Emit(r stream.Record) error {
	for i, s := range f.sinks {
		if err := s.Emit(r); err != nil {
			return fmt.Errorf("fanout: sink %d: %w", i, err)
		}
	}
	return nil
}

// EmitBatch implements stream.BatchSink, forwarding the whole batch to sinks
// that can consume one and falling back to per-record emission otherwise.
func (f *Fanout) EmitBatch(records []stream.Record) error {
	for i, s := range f.sinks {
		if batch, ok := s.(stream.BatchSink); ok {
			if err := batch.EmitBatch(records); err != nil {
				return fmt.Errorf("fanout: sink %d: %w", i, err)
			}
			continue
		}
		for _, r := range records {
			if err := s.Emit(r); err != nil {
				return fmt.Errorf("fanout: sink %d: emitting record %s: %w", i, r.ID, err)
			}
		}
	}
	return nil
}

// Flush implements stream.Sink. Every sink is flushed even when an earlier one
// fails, so one broken destination cannot leave another with buffered data.
func (f *Fanout) Flush() error {
	errs := make([]error, 0, len(f.sinks))
	for i, s := range f.sinks {
		if err := s.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("fanout: sink %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

// Close flushes and closes every sink that owns a resource. It is safe to call
// more than once only if the children are; the sinks in this package are.
func (f *Fanout) Close() error {
	errs := make([]error, 0, len(f.sinks))
	for i, s := range f.sinks {
		closer, ok := s.(io.Closer)
		if !ok {
			continue
		}
		if err := closer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("fanout: sink %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

// OverflowPolicy decides what an AsyncFanoutSink does when a downstream sink
// cannot keep up and its buffer is full.
type OverflowPolicy string

const (
	// OverflowBlock applies backpressure: the producer waits for room. It is
	// the default, because losing a record should be a deliberate choice.
	OverflowBlock OverflowPolicy = "block"
	// OverflowDrop discards the record for that one sink and reports the drop
	// through OnError. The other sinks are unaffected.
	OverflowDrop OverflowPolicy = "drop"
	// OverflowDLQ hands the record to the dead letter sink instead of
	// discarding it, so overflow is recorded rather than lost.
	OverflowDLQ OverflowPolicy = "dlq"
)

// ErrAsyncFanoutClosed is returned by Emit once the sink has been closed.
var ErrAsyncFanoutClosed = errors.New("async fanout: sink is closed")

// AsyncFanoutOptions configures an AsyncFanoutSink.
type AsyncFanoutOptions struct {
	// Buffer is the per-sink channel capacity. Zero means
	// DefaultAsyncFanoutBuffer.
	Buffer int
	// Policy is what to do when a sink's buffer is full. Zero means
	// OverflowBlock.
	Policy OverflowPolicy
	// DLQ receives records that overflow when Policy is OverflowDLQ. Without
	// one, an overflowing record is dropped and reported, never silently
	// lost.
	DLQ stream.Sink
	// OnError observes asynchronous failures: a downstream Emit error, or a
	// record dropped by policy. It must not block.
	OnError func(error)
}

// DefaultAsyncFanoutBuffer is the per-sink buffer used when none is set.
const DefaultAsyncFanoutBuffer = 256

type asyncFanoutJob struct {
	record stream.Record
	flush  bool
	ack    chan error
}

type asyncFanoutWorker struct {
	sink stream.Sink
	jobs chan asyncFanoutJob
	done chan struct{}
}

// AsyncFanoutSink delivers every record to each downstream sink through its
// own goroutine and buffered channel.
//
// A network-bound sink therefore cannot stall a fast one: stdout keeps
// printing at its own pace while a slow webhook drains its buffer, or overflows
// according to the configured policy. The failure boundary is per sink too — an
// Emit error stops deliveries to that sink only, unless the policy is to keep
// going with a dead letter.
type AsyncFanoutSink struct {
	workers []*asyncFanoutWorker
	policy  OverflowPolicy
	dlq     stream.Sink
	onError func(error)
	dropped atomic.Int64

	mu     sync.Mutex
	closed bool
}

var _ stream.Sink = (*AsyncFanoutSink)(nil)

// NewAsyncFanout starts one worker per sink. Call Close to stop them; the
// records already buffered are delivered first.
func NewAsyncFanout(sinks []stream.Sink, opts AsyncFanoutOptions) *AsyncFanoutSink {
	buffer := opts.Buffer
	if buffer <= 0 {
		buffer = DefaultAsyncFanoutBuffer
	}
	policy := opts.Policy
	if policy == "" {
		policy = OverflowBlock
	}
	f := &AsyncFanoutSink{
		policy:  policy,
		dlq:     opts.DLQ,
		onError: opts.OnError,
	}
	for _, s := range sinks {
		w := &asyncFanoutWorker{
			sink: s,
			jobs: make(chan asyncFanoutJob, buffer),
			done: make(chan struct{}),
		}
		f.workers = append(f.workers, w)
		go w.run(f.observe)
	}
	return f
}

func (f *AsyncFanoutSink) observe(err error) {
	if err != nil && f.onError != nil {
		f.onError(err)
	}
}

// Dropped reports how many deliveries were discarded by the overflow policy.
func (f *AsyncFanoutSink) Dropped() int64 { return f.dropped.Load() }

// Emit implements stream.Sink by handing the record to every worker without
// waiting for any of them to write it.
func (f *AsyncFanoutSink) Emit(r stream.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return ErrAsyncFanoutClosed
	}
	var errs []error
	for _, w := range f.workers {
		if err := f.enqueue(w, r); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (f *AsyncFanoutSink) enqueue(w *asyncFanoutWorker, r stream.Record) error {
	job := asyncFanoutJob{record: r}
	select {
	case w.jobs <- job:
		return nil
	default:
	}

	switch f.policy {
	case OverflowDrop:
		f.dropped.Add(1)
		f.observe(fmt.Errorf("async fanout: dropped record %s: sink buffer full", r.ID))
		return nil
	case OverflowDLQ:
		f.dropped.Add(1)
		if f.dlq == nil {
			f.observe(fmt.Errorf("async fanout: dropped record %s: sink buffer full and no dead letter sink configured", r.ID))
			return nil
		}
		if err := f.dlq.Emit(r); err != nil {
			return fmt.Errorf("async fanout: dead lettering record %s: %w", r.ID, err)
		}
		return nil
	default:
		w.jobs <- job
		return nil
	}
}

// EmitBatch implements stream.BatchSink by emitting the records in order.
func (f *AsyncFanoutSink) EmitBatch(records []stream.Record) error {
	for _, r := range records {
		if err := f.Emit(r); err != nil {
			return err
		}
	}
	return nil
}

// Flush implements stream.Sink by waiting until every worker has drained its
// buffer and flushed its sink.
func (f *AsyncFanoutSink) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil
	}
	var errs []error
	for _, w := range f.workers {
		ack := make(chan error, 1)
		w.jobs <- asyncFanoutJob{flush: true, ack: ack}
		if err := <-ack; err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Close drains every worker, flushes and closes each downstream sink, and
// stops the goroutines. It is safe to call more than once.
func (f *AsyncFanoutSink) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	for _, w := range f.workers {
		close(w.jobs)
	}
	f.mu.Unlock()

	var errs []error
	for i, w := range f.workers {
		<-w.done
		errs = append(errs, w.close())
		if err := w.sink.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("async fanout: sink %d: %w", i, err))
		}
	}
	if f.dlq != nil {
		if err := f.dlq.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("async fanout: dead letter sink: %w", err))
		}
		if closer, ok := f.dlq.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, fmt.Errorf("async fanout: dead letter sink: %w", err))
			}
		}
	}
	return errors.Join(errs...)
}

// run delivers jobs until the channel is closed and drained, so a Close never
// discards records that were already accepted.
func (w *asyncFanoutWorker) run(report func(error)) {
	defer close(w.done)
	for job := range w.jobs {
		var err error
		if job.flush {
			err = w.sink.Flush()
		} else {
			err = w.sink.Emit(job.record)
			if err != nil {
				report(fmt.Errorf("async fanout: emitting record %s: %w", job.record.ID, err))
			}
		}
		if job.ack != nil {
			job.ack <- err
		}
	}
}

func (w *asyncFanoutWorker) close() error {
	closer, ok := w.sink.(io.Closer)
	if !ok {
		return nil
	}
	if err := closer.Close(); err != nil {
		return fmt.Errorf("async fanout: closing sink: %w", err)
	}
	return nil
}
