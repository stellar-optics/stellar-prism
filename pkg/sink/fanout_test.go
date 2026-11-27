package sink_test

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

type recordingSink struct {
	records []stream.Record
	batches [][]stream.Record
	flushes int
	closes  int
	err     error
}

func (r *recordingSink) Emit(record stream.Record) error {
	r.records = append(r.records, record)
	return r.err
}

func (r *recordingSink) Flush() error { r.flushes++; return r.err }

func (r *recordingSink) Close() error { r.closes++; return r.err }

type batchRecordingSink struct{ recordingSink }

func (b *batchRecordingSink) EmitBatch(records []stream.Record) error {
	b.batches = append(b.batches, records)
	return b.err
}

func TestFanoutEmitAndFlush(t *testing.T) {
	t.Parallel()

	first := &recordingSink{}
	second := &batchRecordingSink{}
	fanout := sink.NewFanout(first, second)

	r := eventRecord()
	if err := fanout.Emit(r); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if len(first.records) != 1 || len(second.records) != 1 {
		t.Errorf("Emit reached %d/%d sinks, want both", len(first.records), len(second.records))
	}

	if err := fanout.EmitBatch([]stream.Record{r, ledgerRecord()}); err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}
	if len(second.batches) != 1 || len(second.batches[0]) != 2 {
		t.Errorf("batch-capable sink did not receive the batch: %+v", second.batches)
	}
	if len(first.records) != 3 {
		t.Errorf("plain sink received %d records, want 3 via per-record fallback", len(first.records))
	}

	if err := fanout.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if first.flushes != 1 || second.flushes != 1 {
		t.Errorf("flushes = %d/%d, want 1 each", first.flushes, second.flushes)
	}
}

func TestFanoutStopsAtFirstError(t *testing.T) {
	t.Parallel()

	first := &recordingSink{err: errors.New("broken")}
	second := &recordingSink{}
	fanout := sink.NewFanout(first, second)

	err := fanout.Emit(eventRecord())
	if err == nil {
		t.Fatal("Emit() error = nil, want the sink failure")
	}
	if !strings.Contains(err.Error(), "fanout: sink 0") {
		t.Errorf("error = %q, want it to identify the failing sink", err)
	}
	if len(second.records) != 0 {
		t.Errorf("a sink after the failure still received %d records, want 0", len(second.records))
	}
}

func TestFanoutFlushesAndClosesEverySink(t *testing.T) {
	t.Parallel()

	broken := &recordingSink{err: errors.New("broken")}
	healthy := &recordingSink{}
	fanout := sink.NewFanout(broken, healthy)

	if err := fanout.Flush(); err == nil {
		t.Fatal("Flush() error = nil, want the failure to be reported")
	}
	if healthy.flushes != 1 {
		t.Errorf("healthy sink flushed %d times, want 1 despite the earlier failure", healthy.flushes)
	}

	if err := fanout.Close(); err == nil {
		t.Fatal("Close() error = nil, want the failure to be reported")
	}
	if broken.closes != 1 || healthy.closes != 1 {
		t.Errorf("closes = %d/%d, want both sinks closed", broken.closes, healthy.closes)
	}
}

func TestFanoutWithoutSinksIsANoOp(t *testing.T) {
	t.Parallel()

	fanout := sink.NewFanout()
	if err := fanout.Emit(eventRecord()); err != nil {
		t.Errorf("Emit() error = %v", err)
	}
	if err := fanout.EmitBatch([]stream.Record{eventRecord()}); err != nil {
		t.Errorf("EmitBatch() error = %v", err)
	}
	if err := fanout.Flush(); err != nil {
		t.Errorf("Flush() error = %v", err)
	}
	if err := fanout.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

// asyncProbeSink is a downstream sink whose Emit can be slowed down or
// blocked, so the fan-out's independence and overflow behaviour are
// observable. Unlike recordingSink it is safe to use from a worker goroutine.
type asyncProbeSink struct {
	mu      sync.Mutex
	records []stream.Record
	delay   time.Duration
	// release, when non-nil, holds every Emit until it is closed.
	release chan struct{}
	flushes int
	closes  int
}

func (s *asyncProbeSink) Emit(record stream.Record) error {
	if s.release != nil {
		<-s.release
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, record)
	return nil
}

func (s *asyncProbeSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushes++
	return nil
}

func (s *asyncProbeSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closes++
	return nil
}

func (s *asyncProbeSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}

func TestAsyncFanoutSlowSinkDoesNotBlockFastSink(t *testing.T) {
	t.Parallel()

	slow := &asyncProbeSink{delay: 40 * time.Millisecond}
	fast := &asyncProbeSink{}
	fanout := sink.NewAsyncFanout([]stream.Sink{slow, fast}, sink.AsyncFanoutOptions{Buffer: 8})
	defer func() {
		if err := fanout.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	start := time.Now()
	for i := 0; i < 5; i++ {
		if err := fanout.Emit(eventRecord()); err != nil {
			t.Fatalf("Emit() error = %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed >= slow.delay {
		t.Errorf("Emit blocked for %s: a slow sink delayed the producer", elapsed)
	}

	// The fast sink must receive everything while the slow one is still
	// working through its first record.
	deadline := time.Now().Add(2 * time.Second)
	for fast.count() < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("fast sink received %d records while slow sink was busy, want 5", fast.count())
		}
		time.Sleep(time.Millisecond)
	}
	if elapsed := time.Since(start); elapsed >= 5*slow.delay {
		t.Errorf("fast sink finished after %s, which is not faster than the slow sink alone", elapsed)
	}

	if err := fanout.Flush(); err != nil {
		t.Errorf("Flush() error = %v", err)
	}
	if slow.count() != 5 {
		t.Errorf("slow sink received %d records, want 5 after Flush", slow.count())
	}
	if fast.flushes != 1 || slow.flushes != 1 {
		t.Errorf("flushes = %d/%d, want 1 per sink", fast.flushes, slow.flushes)
	}
}

func TestAsyncFanoutOverflowPolicies(t *testing.T) {
	t.Parallel()

	t.Run("drop discards only the overflowing sink's record", func(t *testing.T) {
		t.Parallel()

		release := make(chan struct{})
		blocked := &asyncProbeSink{release: release}
		fanout := sink.NewAsyncFanout([]stream.Sink{blocked}, sink.AsyncFanoutOptions{
			Buffer: 1,
			Policy: sink.OverflowDrop,
		})

		const emitted = 4
		for i := 0; i < emitted; i++ {
			if err := fanout.Emit(eventRecord()); err != nil {
				t.Fatalf("Emit() error = %v", err)
			}
		}
		dropped := fanout.Dropped()
		if dropped == 0 {
			t.Error("Dropped() = 0, want the blocked sink's overflow to be counted")
		}
		close(release)
		if err := fanout.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		// Every record is either delivered or counted as dropped: a buffer
		// overflow must never lose a record silently.
		if got := int64(blocked.count()) + dropped; got != emitted {
			t.Errorf("delivered %d + dropped %d = %d, want %d", blocked.count(), dropped, got, emitted)
		}
		if blocked.closes != 1 {
			t.Errorf("sink closed %d times, want 1", blocked.closes)
		}
	})

	t.Run("dlq records the overflow instead of dropping it", func(t *testing.T) {
		t.Parallel()

		release := make(chan struct{})
		blocked := &asyncProbeSink{release: release}
		dlq := &asyncProbeSink{}
		fanout := sink.NewAsyncFanout([]stream.Sink{blocked}, sink.AsyncFanoutOptions{
			Buffer: 1,
			Policy: sink.OverflowDLQ,
			DLQ:    dlq,
		})

		for i := 0; i < 4; i++ {
			if err := fanout.Emit(eventRecord()); err != nil {
				t.Fatalf("Emit() error = %v", err)
			}
		}
		if dlq.count() == 0 {
			t.Error("dead letter sink received no records, want the overflow")
		}
		close(release)
		if err := fanout.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		// Nothing is lost: each record was either delivered or dead
		// lettered, and the total still matches what was emitted.
		if got := blocked.count() + dlq.count(); got != 4 {
			t.Errorf("delivered %d + dead lettered %d = %d, want 4", blocked.count(), dlq.count(), got)
		}
		if dlq.closes != 1 {
			t.Errorf("dead letter sink closed %d times, want 1", dlq.closes)
		}
	})
}
