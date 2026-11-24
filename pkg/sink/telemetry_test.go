package sink

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

var errTraceSink = errors.New("destination rejected the record")

// traceTestSink records what it was given so the test can prove the fanout
// still delivers while tracing.
type traceTestSink struct {
	records []stream.Record
	err     error
}

func (s *traceTestSink) Emit(r stream.Record) error {
	if s.err != nil {
		return s.err
	}
	s.records = append(s.records, r)
	return nil
}

func (s *traceTestSink) Flush() error { return nil }

// traceTestBatchSink consumes whole batches, exercising the batch branch of
// Fanout.EmitBatch.
type traceTestBatchSink struct {
	batches [][]stream.Record
}

func (s *traceTestBatchSink) Emit(stream.Record) error { return nil }
func (s *traceTestBatchSink) Flush() error             { return nil }
func (s *traceTestBatchSink) EmitBatch(records []stream.Record) error {
	s.batches = append(s.batches, records)
	return nil
}

func traceTestRecord() stream.Record {
	return stream.Record{
		ID:       "00000000000000000001",
		Sequence: 42,
		Kind:     stream.KindEvent,
		Event: &stream.Event{
			Type:       "contract",
			ContractID: "CABC",
			TxHash:     "deadbeef",
		},
	}
}

// TestFanoutEmitsSinkWriteSpanPerDestination checks each destination gets its
// own sink_write span, nested under the caller's span and labelled with its
// position in the fanout.
func TestFanoutEmitsSinkWriteSpanPerDestination(t *testing.T) {
	exporter, shutdown := installSinkTestTracing(t)

	first, second := &traceTestSink{}, &traceTestSink{}
	fanout := NewFanout(first, second)

	parentCtx, parent := stream.Tracer().Start(context.Background(), "poll_page")
	if err := fanout.EmitContext(parentCtx, traceTestRecord()); err != nil {
		t.Fatalf("EmitContext() error = %v", err)
	}
	parent.End()
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("flushing spans: %v", err)
	}

	if len(first.records) != 1 || len(second.records) != 1 {
		t.Fatalf("fanout delivered %d and %d records, want 1 each", len(first.records), len(second.records))
	}

	writes := spansNamedIn(exporter.GetSpans().Snapshots(), "sink_write")
	if len(writes) != 2 {
		t.Fatalf("recorded %d sink_write spans, want 2", len(writes))
	}
	seen := map[int64]bool{}
	for _, span := range writes {
		if got, want := span.Parent().SpanID(), parent.SpanContext().SpanID(); got != want {
			t.Errorf("sink_write parent = %s, want %s", got, want)
		}
		index, ok := sinkSpanInt(span, "sink.index")
		if !ok {
			t.Fatal("sink_write span has no sink.index attribute")
		}
		seen[index] = true
		if got, want := sinkSpanString(span, "contract.id"), "CABC"; got != want {
			t.Errorf("sink_write contract.id = %q, want %q", got, want)
		}
		if got, want := sinkSpanInt64(span, "ledger.sequence"), int64(42); got != want {
			t.Errorf("sink_write ledger.sequence = %d, want %d", got, want)
		}
	}
	if !seen[0] || !seen[1] {
		t.Errorf("sink.index values = %v, want 0 and 1", seen)
	}
}

// TestFanoutRecordsSinkFailureOnSpan covers the diagnosis path for an
// intermittent destination.
func TestFanoutRecordsSinkFailureOnSpan(t *testing.T) {
	exporter, shutdown := installSinkTestTracing(t)

	fanout := NewFanout(&traceTestSink{err: errTraceSink})
	if err := fanout.Emit(traceTestRecord()); err == nil {
		t.Fatal("Emit() returned no error for a failing destination")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("flushing spans: %v", err)
	}

	writes := spansNamedIn(exporter.GetSpans().Snapshots(), "sink_write")
	if len(writes) != 1 {
		t.Fatalf("recorded %d sink_write spans, want 1", len(writes))
	}
	if got := writes[0].Status().Code.String(); got != "Error" {
		t.Errorf("sink_write status = %q, want Error", got)
	}
}

func TestFanoutBatchDispatchIsTraced(t *testing.T) {
	exporter, shutdown := installSinkTestTracing(t)

	batchSink := &traceTestBatchSink{}
	fanout := NewFanout(batchSink)
	records := []stream.Record{traceTestRecord(), traceTestRecord()}
	if err := fanout.EmitBatch(records); err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}
	if len(batchSink.batches) != 1 || len(batchSink.batches[0]) != 2 {
		t.Fatalf("batch sink received %v, want one batch of 2", batchSink.batches)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("flushing spans: %v", err)
	}

	writes := spansNamedIn(exporter.GetSpans().Snapshots(), "sink_write")
	if len(writes) != 1 {
		t.Fatalf("recorded %d sink_write spans, want 1", len(writes))
	}
	if got, want := sinkSpanInt64(writes[0], "sink.batch_size"), int64(2); got != want {
		t.Errorf("sink.batch_size = %d, want %d", got, want)
	}
}

// installSinkTestTracing installs an in-memory provider and returns its
// exporter plus the shutdown that flushes recorded spans.
// retainingExporter keeps exported spans available after the provider shuts
// down. The SDK's in-memory exporter clears its buffer on Shutdown, which
// would discard exactly the spans a test is asserting on.
type retainingExporter struct {
	*tracetest.InMemoryExporter
}

func (retainingExporter) Shutdown(context.Context) error { return nil }

func installSinkTestTracing(t *testing.T) (*retainingExporter, func(context.Context) error) {
	t.Helper()
	previous := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	exporter := &retainingExporter{InMemoryExporter: tracetest.NewInMemoryExporter()}
	shutdown, err := stream.InstallTelemetry(exporter, 1)
	if err != nil {
		t.Fatalf("InstallTelemetry() error = %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })
	return exporter, shutdown
}

func spansNamedIn(spans []sdktrace.ReadOnlySpan, name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, span := range spans {
		if span.Name() == name {
			out = append(out, span)
		}
	}
	return out
}

func sinkSpanAttr(span sdktrace.ReadOnlySpan, key string) (attribute.Value, bool) {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

func sinkSpanInt(span sdktrace.ReadOnlySpan, key string) (int64, bool) {
	value, ok := sinkSpanAttr(span, key)
	if !ok {
		return 0, false
	}
	return value.AsInt64(), true
}

func sinkSpanInt64(span sdktrace.ReadOnlySpan, key string) int64 {
	value, _ := sinkSpanAttr(span, key)
	return value.AsInt64()
}

func sinkSpanString(span sdktrace.ReadOnlySpan, key string) string {
	value, _ := sinkSpanAttr(span, key)
	return value.AsString()
}
