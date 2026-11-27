package stream_test

import (
	"context"
	"testing"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestDedupEvictsOldestKeyAtCapacity(t *testing.T) {
	t.Parallel()

	dedup := stream.NewDedup(2)
	if dedup.Cap() != 2 {
		t.Fatalf("Cap() = %d, want 2", dedup.Cap())
	}

	if !dedup.Allow("a") || !dedup.Allow("b") {
		t.Fatal("first two keys were refused")
	}
	if dedup.Allow("a") {
		t.Error("Allow(a) = true for a retained key, want false")
	}
	if dedup.Dropped() != 1 {
		t.Errorf("Dropped() = %d, want 1", dedup.Dropped())
	}
	if dedup.Len() != 2 {
		t.Errorf("Len() = %d, want 2", dedup.Len())
	}

	// Inserting c evicts a, the oldest key.
	if !dedup.Allow("c") {
		t.Fatal("Allow(c) = false, want true after eviction")
	}
	if dedup.Contains("a") {
		t.Error("Contains(a) = true, want the oldest key evicted")
	}
	if !dedup.Contains("b") || !dedup.Contains("c") {
		t.Errorf("retained keys = b:%v c:%v, want both",
			dedup.Contains("b"), dedup.Contains("c"))
	}
	if !dedup.Allow("a") {
		t.Error("Allow(a) = false after eviction, want true")
	}
}

func TestDedupCapacityIncludesEmptyKeys(t *testing.T) {
	t.Parallel()

	dedup := stream.NewDedup(1)
	if !dedup.Allow("") || !dedup.Allow("next") {
		t.Fatal("Allow() refused a key after the previous key was evicted")
	}
	if got := dedup.Len(); got != 1 {
		t.Errorf("Len() = %d, want the cache capacity of 1", got)
	}
	if dedup.Contains("") || !dedup.Contains("next") {
		t.Errorf("retained keys = empty:%v next:%v, want only next",
			dedup.Contains(""), dedup.Contains("next"))
	}
}

func TestDedupDisabledAlwaysAllows(t *testing.T) {
	t.Parallel()

	dedup := stream.NewDedup(0)
	if dedup.Cap() != 0 || dedup.Len() != 0 {
		t.Fatalf("disabled dedup reports Cap=%d Len=%d, want 0/0", dedup.Cap(), dedup.Len())
	}
	for i := 0; i < 3; i++ {
		if !dedup.Allow("same") {
			t.Fatalf("Allow() = false on call %d, want true when disabled", i+1)
		}
	}
	if dedup.Dropped() != 0 {
		t.Errorf("Dropped() = %d, want 0", dedup.Dropped())
	}
}

func TestDedupResetClearsWindow(t *testing.T) {
	t.Parallel()

	dedup := stream.NewDedup(2)
	dedup.Allow("a")
	dedup.Reset()
	if dedup.Len() != 0 || dedup.Contains("a") {
		t.Errorf("Reset() left Len=%d Contains(a)=%v, want 0/false",
			dedup.Len(), dedup.Contains("a"))
	}
	if !dedup.Allow("a") {
		t.Error("Allow(a) = false after Reset(), want true")
	}
}

func TestEventDedupCacheDefaultWindowAndReset(t *testing.T) {
	t.Parallel()

	cache := stream.NewEventDedupCache(stream.DefaultDedupWindow)
	if cache.Cap() != 10_000 {
		t.Fatalf("Cap() = %d, want the default 10000-entry window", cache.Cap())
	}
	if !cache.Allow("event-1") || cache.Allow("event-1") {
		t.Fatal("event cache did not suppress a retained event ID")
	}
	cache.Reset()
	if !cache.Allow("event-1") {
		t.Fatal("event cache did not allow an event after reset")
	}
	if cache.Dropped() != 1 {
		t.Errorf("Dropped() = %d, want the historical duplicate count preserved", cache.Dropped())
	}
}

func TestEventKeyPrefersServerID(t *testing.T) {
	t.Parallel()

	first := stream.Record{ID: "0000000001-0001-0001", Kind: stream.KindEvent, Event: &stream.Event{
		ContractID: "C1", TxHash: "abc", TopicXDR: []string{"AAAA"}, ValueXDR: "BBBB",
	}}
	second := stream.Record{ID: "0000000001-0001-0001", Kind: stream.KindEvent, Event: &stream.Event{
		ContractID: "C2", TxHash: "def", TopicXDR: []string{"CCCC"}, ValueXDR: "DDDD",
	}}
	if stream.EventKey(first) != stream.EventKey(second) {
		t.Error("the same server ID produced different keys")
	}

	distinct := stream.Record{ID: "0000000001-0001-0002", Kind: stream.KindEvent, Event: first.Event}
	if stream.EventKey(first) == stream.EventKey(distinct) {
		t.Error("different server IDs produced the same key")
	}
}

func TestEventKeyFallsBackToContent(t *testing.T) {
	t.Parallel()

	record := stream.Record{Kind: stream.KindEvent, Event: &stream.Event{
		ContractID: "C1", TxHash: "abc", TxIndex: 1, OpIndex: 2,
		TopicXDR: []string{"AAAA"}, ValueXDR: "BBBB",
	}}
	other := stream.Record{Kind: stream.KindEvent, Event: &stream.Event{
		ContractID: "C1", TxHash: "abc", TxIndex: 1, OpIndex: 2,
		TopicXDR: []string{"AAAA"}, ValueXDR: "BBBB",
	}}
	if key := stream.EventKey(record); key == "" {
		t.Fatal("EventKey() = empty for an event with no ID")
	}
	if stream.EventKey(record) != stream.EventKey(other) {
		t.Error("identical content with no IDs produced different keys")
	}

	other.Event.ValueXDR = "CCCC"
	if stream.EventKey(record) == stream.EventKey(other) {
		t.Error("different content produced the same key")
	}
}

func TestEventKeyNonEventUsesID(t *testing.T) {
	t.Parallel()

	record := stream.Record{ID: "0000000001-0001", Kind: stream.KindTransaction}
	if got := stream.EventKey(record); got == "" {
		t.Fatal("EventKey() = empty for a non-event record with an ID")
	}
}

func TestDedupSinkSuppressesReplays(t *testing.T) {
	t.Parallel()

	var emitted []string
	inner := stream.SinkFunc(func(r stream.Record) error {
		emitted = append(emitted, r.ID)
		return nil
	})
	sink := stream.NewDedupSink(inner, 4)

	event := stream.Record{ID: "0000000001-0001-0001", Kind: stream.KindEvent, Event: &stream.Event{}}
	for i := 0; i < 2; i++ {
		if err := sink.Emit(event); err != nil {
			t.Fatalf("Emit() error = %v", err)
		}
	}
	if err := sink.Emit(stream.Record{ID: "0000000002-0001-0001", Kind: stream.KindEvent, Event: &stream.Event{}}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := sink.Emit(stream.Record{ID: "0000000003-0001", Kind: stream.KindTransaction}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	if len(emitted) != 3 {
		t.Fatalf("emitted %v, want the replay suppressed", emitted)
	}
	if sink.Dropped() != 1 {
		t.Errorf("Dropped() = %d, want 1", sink.Dropped())
	}
}

func TestDedupSinkBatchPreservesOrder(t *testing.T) {
	t.Parallel()

	inner := &recordingBatchSink{}
	sink := stream.NewDedupSink(inner, 4)

	records := []stream.Record{
		{ID: "1", Kind: stream.KindEvent, Event: &stream.Event{}},
		{ID: "2", Kind: stream.KindEvent, Event: &stream.Event{}},
		{ID: "1", Kind: stream.KindEvent, Event: &stream.Event{}},
		{ID: "3", Kind: stream.KindEvent, Event: &stream.Event{}},
	}
	if err := sink.EmitBatch(records); err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}
	if len(inner.batches) != 1 {
		t.Fatalf("batch calls = %d, want 1", len(inner.batches))
	}
	got := inner.batches[0]
	if len(got) != 3 || got[0].ID != "1" || got[1].ID != "2" || got[2].ID != "3" {
		t.Errorf("forwarded %v, want 1,2,3 in order", ids(got))
	}
}

func TestDedupSinkThroughTailerSuppressesReplayedPages(t *testing.T) {
	t.Parallel()

	event := func(value string) stream.Record {
		return stream.Record{
			Sequence: 1,
			Kind:     stream.KindEvent,
			Event: &stream.Event{
				ContractID: "C1",
				TxHash:     "abc",
				TopicXDR:   []string{"AAAA"},
				ValueXDR:   value,
			},
		}
	}

	// The source behaves like a server that replays a page after a reconnect.
	// Its records deliberately carry no IDs, so the tailer's high-water-mark
	// de-duplication cannot hide the replay: the sink's window is what has to
	// suppress it. Two records are distinct events; the repeated copies are
	// the same event delivered twice.
	src := &replaySource{batches: []stream.Batch{
		{Records: []stream.Record{event("first"), event("first"), event("second")}, Full: true},
		{Records: []stream.Record{event("first"), event("second")}},
	}}

	var emitted []string
	sink := stream.NewDedupSink(stream.SinkFunc(func(r stream.Record) error {
		emitted = append(emitted, r.Event.ValueXDR)
		return nil
	}), 4)

	if err := stream.NewTailer(src, sink, stream.Options{}).Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(emitted) != 2 || emitted[0] != "first" || emitted[1] != "second" {
		t.Errorf("emitted %v, want first,second exactly once", emitted)
	}
	if sink.Dropped() != 3 {
		t.Errorf("Dropped() = %d, want one in-page and two page-level replays", sink.Dropped())
	}
}

// replaySource serves a fixed script of batches, one per Fetch, then empty
// pages. It is the smallest Source that can replay a page.
type replaySource struct {
	batches []stream.Batch
	calls   int
}

func (s *replaySource) Name() string { return "replay" }

func (s *replaySource) Fetch(_ context.Context, _ string) (stream.Batch, error) {
	if s.calls >= len(s.batches) {
		return stream.Batch{}, nil
	}
	batch := s.batches[s.calls]
	s.calls++
	return batch, nil
}

func (s *replaySource) CursorLedger(string) (uint32, bool) { return 0, false }

func ids(records []stream.Record) []string {
	out := make([]string, len(records))
	for i, r := range records {
		out[i] = r.ID
	}
	return out
}

func TestDedupSinkFlushAndDescription(t *testing.T) {
	t.Parallel()

	inner := &recordingBatchSink{}
	sink := stream.NewDedupSink(inner, 2)
	if err := sink.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if !inner.flushed {
		t.Error("Flush() did not reach the wrapped sink")
	}
	if got := sink.String(); got != "dedup(window=2)" {
		t.Errorf("String() = %q, want dedup(window=2)", got)
	}
}

func TestDedupSinkBatchFallsBackToSingleEmits(t *testing.T) {
	t.Parallel()

	var emitted []string
	inner := stream.SinkFunc(func(r stream.Record) error {
		emitted = append(emitted, r.ID)
		return nil
	})
	sink := stream.NewDedupSink(inner, 2)
	records := []stream.Record{
		{ID: "1", Kind: stream.KindEvent, Event: &stream.Event{}},
		{ID: "1", Kind: stream.KindEvent, Event: &stream.Event{}},
		{ID: "2", Kind: stream.KindEvent, Event: &stream.Event{}},
	}
	if err := sink.EmitBatch(records); err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}
	if len(emitted) != 2 || emitted[0] != "1" || emitted[1] != "2" {
		t.Errorf("emitted %v, want 1,2 through the single-emit fallback", emitted)
	}
}

// recordingBatchSink captures batches so the wrapper's forwarding behaviour is
// observable without a real output.
type recordingBatchSink struct {
	batches [][]stream.Record
	flushed bool
}

func (s *recordingBatchSink) Emit(r stream.Record) error {
	s.batches = append(s.batches, []stream.Record{r})
	return nil
}

func (s *recordingBatchSink) EmitBatch(records []stream.Record) error {
	s.batches = append(s.batches, records)
	return nil
}

func (s *recordingBatchSink) Flush() error {
	s.flushed = true
	return nil
}
