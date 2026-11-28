package stream

import (
	"fmt"
	"sync"
	"time"
)

// BatchSink is an optional extension for sinks that can write several records
// in one call. Batcher uses it when available, which lets a sink flush once
// per batch instead of once per record.
type BatchSink interface {
	Sink
	EmitBatch(records []Record) error
}

// BatcherOptions configures a Batcher.
type BatcherOptions struct {
	// MaxRecords flushes as soon as the buffer reaches this many records.
	// Zero disables the count threshold.
	MaxRecords int
	// MaxDelay flushes this long after the first buffered record arrived.
	// Zero disables the time threshold.
	MaxDelay time.Duration
	// OnError, when set, observes failures from a time-triggered flush. Emit
	// and Flush still return the error to their caller; OnError exists so a
	// background flush cannot fail silently.
	OnError func(error)
}

// Batcher buffers records in memory and forwards them in batches, either when
// MaxRecords is reached, when MaxDelay elapses, or on an explicit Flush.
//
// A Batcher is a Sink, so it can wrap an existing sink without changing the
// tailer. It uses a single timer rather than a goroutine: closing it stops the
// timer, and no goroutine is left behind on termination.
type Batcher struct {
	next Sink
	opts BatcherOptions

	mu       sync.Mutex
	buf      []Record
	timer    *time.Timer
	closed   bool
	asyncErr error
}

// NewBatcher wraps next in a batching buffer. Negative thresholds are treated
// as disabled.
func NewBatcher(next Sink, opts BatcherOptions) *Batcher {
	if opts.MaxRecords < 0 {
		opts.MaxRecords = 0
	}
	if opts.MaxDelay < 0 {
		opts.MaxDelay = 0
	}
	return &Batcher{next: next, opts: opts}
}

// Emit implements Sink by appending r to the buffer.
func (b *Batcher) Emit(r Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("batcher: emit after close")
	}
	if b.asyncErr != nil {
		return b.asyncErr
	}

	b.buf = append(b.buf, r)
	if b.opts.MaxRecords > 0 && len(b.buf) >= b.opts.MaxRecords {
		return b.flushLocked()
	}
	if b.opts.MaxDelay > 0 && b.timer == nil {
		b.timer = time.AfterFunc(b.opts.MaxDelay, b.flushOnTimer)
	}
	return nil
}

// Flush implements Sink by forwarding every buffered record immediately.
func (b *Batcher) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.flushLocked(); err != nil {
		return err
	}
	return b.asyncErr
}

// QueueDepth reports how many records are buffered but not yet delivered, so
// a metrics sink can publish the pipeline's buffered work.
func (b *Batcher) QueueDepth() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.buf)
}

// Close flushes any buffered records and stops the timer. It is safe to call
// more than once.
func (b *Batcher) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}
	b.closed = true
	if err := b.flushLocked(); err != nil {
		return err
	}
	return b.asyncErr
}

func (b *Batcher) flushOnTimer() {
	b.mu.Lock()
	if b.closed || b.timer == nil {
		b.mu.Unlock()
		return
	}
	err := b.flushLocked()
	if err != nil {
		b.asyncErr = err
	}
	onError := b.opts.OnError
	b.mu.Unlock()

	if err != nil && onError != nil {
		onError(err)
	}
}

func (b *Batcher) flushLocked() error {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	if len(b.buf) == 0 {
		return nil
	}

	batch := b.buf
	b.buf = nil

	if batchSink, ok := b.next.(BatchSink); ok {
		if err := batchSink.EmitBatch(batch); err != nil {
			err = fmt.Errorf("batcher: emitting %d records: %w", len(batch), err)
			b.asyncErr = err
			return err
		}
		return nil
	}

	for _, r := range batch {
		if err := b.next.Emit(r); err != nil {
			err = fmt.Errorf("batcher: emitting record %s: %w", r.ID, err)
			b.asyncErr = err
			return err
		}
	}
	return nil
}
