package stream

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var errTraceTestSink = errors.New("sink unavailable")

// traceStubSource serves a fixed list of pages so a tail run is deterministic
// and needs no RPC server.
type traceStubSource struct {
	pages []Batch
	calls int
}

func (s *traceStubSource) Name() string { return "trace-stub" }

func (s *traceStubSource) Fetch(context.Context, string) (Batch, error) {
	s.calls++
	if len(s.pages) == 0 {
		return Batch{}, nil
	}
	page := s.pages[0]
	s.pages = s.pages[1:]
	return page, nil
}

func (s *traceStubSource) CursorLedger(string) (uint32, bool) { return 0, false }

// matchEverythingFilter stands in for a real topic or contract filter; the
// trace only needs to show that filtering ran for each record.
type matchEverythingFilter struct{}

func (matchEverythingFilter) Match(Record) (bool, error) { return true, nil }

// TestTailerEmitsPollFilterAndSinkSpans is the acceptance test for traced
// ingestion: one poll_page span per fetch, with filter_event and sink_write
// spans recorded as its children carrying the record's identity.
func TestTailerEmitsPollFilterAndSinkSpans(t *testing.T) {
	exporter := &retainingExporter{InMemoryExporter: tracetest.NewInMemoryExporter()}
	restoreTracerProvider(t)
	shutdown, err := InstallTelemetry(exporter, 1)
	if err != nil {
		t.Fatalf("InstallTelemetry() error = %v", err)
	}

	src := &traceStubSource{pages: []Batch{{
		Records: []Record{
			{
				ID:       "00000000000000000001",
				Sequence: 100,
				Kind:     KindEvent,
				Event: &Event{
					Type:       "contract",
					ContractID: "CABC",
					TxHash:     "deadbeef",
				},
			},
			{ID: "00000000000000000002", Sequence: 100, Kind: KindEvent, Event: &Event{Type: "contract", ContractID: "CABC", TxHash: "cafebabe"}},
		},
		NextCursor: "cursor-100",
		Latest:     101,
		Oldest:     1,
	}}}

	var emitted []Record
	sink := SinkFunc(func(r Record) error {
		emitted = append(emitted, r)
		return nil
	})
	tailer := NewTailer(src, sink, Options{Limit: 2, Filter: matchEverythingFilter{}})
	if err := tailer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(emitted) != 2 {
		t.Fatalf("emitted %d records, want 2", len(emitted))
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("flushing spans: %v", err)
	}

	spans := exporter.GetSpans().Snapshots()
	polls := spansNamed(spans, "poll_page")
	if len(polls) != 1 {
		t.Fatalf("recorded %d poll_page spans, want 1", len(polls))
	}
	poll := polls[0]

	filters := spansNamed(spans, "filter_event")
	if len(filters) != 2 {
		t.Fatalf("recorded %d filter_event spans, want 2", len(filters))
	}
	writes := spansNamed(spans, "sink_write")
	if len(writes) != 2 {
		t.Fatalf("recorded %d sink_write spans, want 2", len(writes))
	}

	for _, child := range append(filters, writes...) {
		if got, want := child.Parent().SpanID(), poll.SpanContext().SpanID(); got != want {
			t.Errorf("%s span parent = %s, want poll_page span %s", child.Name(), got, want)
		}
		if got, want := child.SpanContext().TraceID(), poll.SpanContext().TraceID(); got != want {
			t.Errorf("%s span trace = %s, want %s", child.Name(), got, want)
		}
	}

	if got, want := spanString(t, poll, "stream.source"), "trace-stub"; got != want {
		t.Errorf("poll_page stream.source = %q, want %q", got, want)
	}
	if got, want := spanInt(t, poll, "stream.page_records"), int64(2); got != want {
		t.Errorf("poll_page stream.page_records = %d, want %d", got, want)
	}

	filter := filters[0]
	if got, want := spanInt(t, filter, "ledger.sequence"), int64(100); got != want {
		t.Errorf("filter_event ledger.sequence = %d, want %d", got, want)
	}
	if got, want := spanString(t, filter, "contract.id"), "CABC"; got != want {
		t.Errorf("filter_event contract.id = %q, want %q", got, want)
	}
	if got, want := spanString(t, filter, "event.type"), "contract"; got != want {
		t.Errorf("filter_event event.type = %q, want %q", got, want)
	}
	if got, want := spanString(t, filter, "transaction.hash"), "deadbeef"; got != want {
		t.Errorf("filter_event transaction.hash = %q, want %q", got, want)
	}

	write := writes[0]
	if got, want := spanString(t, write, "contract.id"), "CABC"; got != want {
		t.Errorf("sink_write contract.id = %q, want %q", got, want)
	}
	if got, want := spanString(t, write, "transaction.hash"), "deadbeef"; got != want {
		t.Errorf("sink_write transaction.hash = %q, want %q", got, want)
	}
	if got := spanString(t, write, "sink.type"); got == "" {
		t.Error("sink_write has no sink.type attribute")
	}
}

// TestTailerRecordsFailedSinkOnSpan checks a sink failure is visible on the
// trace rather than only in the returned error, which is what makes an
// intermittent destination diagnosable after the fact.
func TestTailerRecordsFailedSinkOnSpan(t *testing.T) {
	exporter := &retainingExporter{InMemoryExporter: tracetest.NewInMemoryExporter()}
	restoreTracerProvider(t)
	shutdown, err := InstallTelemetry(exporter, 1)
	if err != nil {
		t.Fatalf("InstallTelemetry() error = %v", err)
	}

	src := &traceStubSource{pages: []Batch{{Records: []Record{{ID: "00000000000000000001", Sequence: 5, Kind: KindEvent, Event: &Event{ContractID: "CABC"}}}}}}
	sink := SinkFunc(func(Record) error { return errTraceTestSink })
	if err := NewTailer(src, sink, Options{Limit: 1}).Run(context.Background()); err == nil {
		t.Fatal("Run() returned no error for a failing sink")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("flushing spans: %v", err)
	}

	writes := spansNamed(exporter.GetSpans().Snapshots(), "sink_write")
	if len(writes) != 1 {
		t.Fatalf("recorded %d sink_write spans, want 1", len(writes))
	}
	if status := writes[0].Status(); status.Code.String() != "Error" {
		t.Errorf("sink_write span status = %v, want Error", status.Code)
	}
	if len(writes[0].Events()) == 0 {
		t.Error("sink_write span recorded no exception event for the sink failure")
	}
}

// retainingExporter keeps exported spans available after the provider shuts
// down. The SDK's in-memory exporter clears its buffer on Shutdown, which
// would discard exactly the spans a test is asserting on.
type retainingExporter struct {
	*tracetest.InMemoryExporter
}

func (retainingExporter) Shutdown(context.Context) error { return nil }

func spansNamed(spans []sdktrace.ReadOnlySpan, name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, span := range spans {
		if span.Name() == name {
			out = append(out, span)
		}
	}
	return out
}

func spanAttr(span sdktrace.ReadOnlySpan, key string) (attribute.Value, bool) {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

func spanString(t *testing.T, span sdktrace.ReadOnlySpan, key string) string {
	t.Helper()
	value, ok := spanAttr(span, key)
	if !ok {
		t.Fatalf("%s span has no %q attribute", span.Name(), key)
	}
	return value.AsString()
}

func spanInt(t *testing.T, span sdktrace.ReadOnlySpan, key string) int64 {
	t.Helper()
	value, ok := spanAttr(span, key)
	if !ok {
		t.Fatalf("%s span has no %q attribute", span.Name(), key)
	}
	return value.AsInt64()
}
