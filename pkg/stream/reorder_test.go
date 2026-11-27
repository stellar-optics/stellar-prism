package stream

import (
	"context"
	"errors"
	"testing"
)

func makeEventRecord(id string, ledger uint32, txIdx, opIdx, evIdx uint32) Record {
	return Record{
		Kind:     KindEvent,
		ID:       id,
		Sequence: ledger,
		Event: &Event{
			TxIndex:    txIdx,
			OpIndex:    opIdx,
			EventIndex: evIdx,
		},
	}
}

func TestResequenceBufferOutOfOrderLedgers(t *testing.T) {
	buf := NewResequenceBuffer(10, 16)

	// Worker 2 and 3 arrive before worker 1.
	ready, err := buf.Push([]Record{
		makeEventRecord("12-1-0", 12, 1, 0, 0),
		makeEventRecord("11-0-1", 11, 0, 0, 1),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ready) != 0 {
		t.Fatalf("expected 0 ready records before ledger 10 arrives, got %d", len(ready))
	}
	if buf.Len() != 2 {
		t.Fatalf("expected 2 buffered records, got %d", buf.Len())
	}

	// Worker 1 delivers ledger 10 with out-of-order intra-ledger events.
	ready, err = buf.Push([]Record{
		makeEventRecord("10-1-0", 10, 1, 0, 0),
		makeEventRecord("10-0-1", 10, 0, 0, 1),
		makeEventRecord("10-0-0", 10, 0, 0, 0),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantIDs := []string{"10-0-0", "10-0-1", "10-1-0", "11-0-1", "12-1-0"}
	if len(ready) != len(wantIDs) {
		t.Fatalf("expected %d ready records, got %d", len(wantIDs), len(ready))
	}
	for i, want := range wantIDs {
		if ready[i].ID != want {
			t.Errorf("ready[%d].ID = %q, want %q", i, ready[i].ID, want)
		}
	}
	if buf.Len() != 0 {
		t.Fatalf("expected empty buffer after consecutive drain, got %d", buf.Len())
	}
}

func TestResequenceBufferDeduplicates(t *testing.T) {
	buf := NewResequenceBuffer(5, 8)
	ready, err := buf.Push([]Record{
		makeEventRecord("5-0-0", 5, 0, 0, 0),
		makeEventRecord("5-0-0", 5, 0, 0, 0),
		makeEventRecord("6-0-0", 6, 0, 0, 0),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ready) != 2 {
		t.Fatalf("expected 2 deduplicated records, got %d", len(ready))
	}
}

func TestResequenceBufferMissingLedgerWindow(t *testing.T) {
	buf := NewResequenceBuffer(100, 3)

	// Ledger 100 is missing; pushing ledger 104 exceeds window of 3 (104 - 100 = 4 > 3).
	_, err := buf.Push([]Record{
		makeEventRecord("104-0-0", 104, 0, 0, 0),
	})
	if !errors.Is(err, ErrMissingLedger) {
		t.Fatalf("expected ErrMissingLedger, got %v", err)
	}
}

func TestResequenceBufferFlush(t *testing.T) {
	buf := NewResequenceBuffer(10, 16)
	_, err := buf.Push([]Record{
		makeEventRecord("13-0-0", 13, 0, 0, 0),
		makeEventRecord("11-0-0", 11, 0, 0, 0),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	flushed := buf.Flush()
	if len(flushed) != 2 {
		t.Fatalf("expected 2 flushed records, got %d", len(flushed))
	}
	if flushed[0].Sequence != 11 || flushed[1].Sequence != 13 {
		t.Fatalf("unexpected flush order: %d, %d", flushed[0].Sequence, flushed[1].Sequence)
	}
}

type sliceSource struct {
	batches []Batch
	idx     int
}

func (s *sliceSource) Name() string { return "slice" }
func (s *sliceSource) Fetch(context.Context, string) (Batch, error) {
	if s.idx >= len(s.batches) {
		return Batch{}, nil
	}
	b := s.batches[s.idx]
	s.idx++
	return b, nil
}
func (s *sliceSource) CursorLedger(string) (uint32, bool) { return 0, false }

type collectSink struct {
	records []Record
}

func (c *collectSink) Emit(r Record) error {
	c.records = append(c.records, r)
	return nil
}
func (c *collectSink) Flush() error { return nil }
func (c *collectSink) Close() error { return nil }

func TestTailerUsesResequenceBufferWhenBackfillWorkersGt1(t *testing.T) {
	src := &sliceSource{
		batches: []Batch{
			{
				Records: []Record{
					makeEventRecord("20-0-0", 20, 0, 0, 0),
					makeEventRecord("22-0-0", 22, 0, 0, 0),
				},
				Full: true,
			},
			{
				Records: []Record{
					makeEventRecord("21-0-0", 21, 0, 0, 0),
					makeEventRecord("23-0-0", 23, 0, 0, 0),
				},
				Full: false,
			},
		},
	}
	sink := &collectSink{}
	tailer := NewTailer(src, sink, Options{
		BackfillWorkers:  4,
		ResequenceWindow: 16,
	})
	if err := tailer.Run(context.Background()); err != nil {
		t.Fatalf("tailer.Run failed: %v", err)
	}

	want := []string{"20-0-0", "21-0-0", "22-0-0", "23-0-0"}
	if len(sink.records) != len(want) {
		t.Fatalf("expected %d records, got %d", len(want), len(sink.records))
	}
	for i, id := range want {
		if sink.records[i].ID != id {
			t.Errorf("sink.records[%d].ID = %q, want %q", i, sink.records[i].ID, id)
		}
	}
}
