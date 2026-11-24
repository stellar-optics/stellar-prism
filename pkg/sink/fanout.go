package sink

import (
	"context"
	"errors"
	"fmt"
	"io"

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
	return f.EmitContext(context.Background(), r)
}

// EmitContext implements stream.ContextSink, so each destination's sink_write
// span becomes a child of the stream's poll_page span.
func (f *Fanout) EmitContext(ctx context.Context, r stream.Record) error {
	for i, s := range f.sinks {
		if err := emitOneSink(ctx, s, i, r); err != nil {
			return fmt.Errorf("fanout: sink %d: %w", i, err)
		}
	}
	return nil
}

// emitOneSink dispatches one record to one destination inside a sink_write
// span, recording the destination's error on the span when it fails.
func emitOneSink(ctx context.Context, s stream.Sink, index int, r stream.Record) error {
	_, span := startSinkWrite(ctx, r, index, s)
	defer span.End()
	err := s.Emit(r)
	if err != nil {
		stream.RecordSpanError(span, err)
	}
	return err
}

// EmitBatch implements stream.BatchSink, forwarding the whole batch to sinks
// that can consume one and falling back to per-record emission otherwise.
func (f *Fanout) EmitBatch(records []stream.Record) error {
	for i, s := range f.sinks {
		if batch, ok := s.(stream.BatchSink); ok {
			// A batch has no single record identity, so one span covers the
			// whole dispatch to this destination.
			_, span := startSinkBatchWrite(context.Background(), records, i, s)
			err := batch.EmitBatch(records)
			if err != nil {
				stream.RecordSpanError(span, err)
				span.End()
				return fmt.Errorf("fanout: sink %d: %w", i, err)
			}
			span.End()
			continue
		}
		for _, r := range records {
			if err := emitOneSink(context.Background(), s, i, r); err != nil {
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
