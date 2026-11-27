package sink

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// startSinkWrite starts a sink_write span for one dispatch to one destination.
//
// The span carries the record's identifying attributes plus the destination's
// position in the fanout and its concrete type, which is what tells a slow
// destination apart from the stream itself in a trace.
func startSinkWrite(ctx context.Context, r stream.Record, index int, destination stream.Sink) (context.Context, trace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, span := stream.Tracer().Start(ctx, "sink_write")
	// Building the attribute slice and reflecting on the destination type are
	// wasted work when nothing is recording, which is the default.
	if span.IsRecording() {
		span.SetAttributes(stream.RecordAttributes(r)...)
		span.SetAttributes(
			attribute.Int("sink.index", index),
			attribute.String("sink.type", fmt.Sprintf("%T", destination)),
		)
	}
	return ctx, span
}

// startSinkBatchWrite starts the single span that covers a whole batch
// dispatched to one destination. A batch has no single record identity, so the
// span takes the first record's attributes and records how large the batch
// was.
func startSinkBatchWrite(ctx context.Context, records []stream.Record, index int, destination stream.Sink) (context.Context, trace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, span := stream.Tracer().Start(ctx, "sink_write")
	if span.IsRecording() {
		span.SetAttributes(attribute.Int("sink.batch_size", len(records)))
		if len(records) > 0 {
			span.SetAttributes(stream.RecordAttributes(records[0])...)
		}
		span.SetAttributes(
			attribute.Int("sink.index", index),
			attribute.String("sink.type", fmt.Sprintf("%T", destination)),
		)
	}
	return ctx, span
}
