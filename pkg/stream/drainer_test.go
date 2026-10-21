package stream_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

type noWaitClock struct{}

func (noWaitClock) Now() time.Time { return time.Unix(0, 0).UTC() }

func (noWaitClock) Sleep(ctx context.Context, _ time.Duration) error { return ctx.Err() }

func eventFactory(srv *fakerpc.Server, pageLimit uint) stream.SourceFactory {
	return func(start uint32) (stream.Source, error) {
		return stream.NewEventSource(srv, stream.EventQuery{
			StartLedger: start,
			PageLimit:   pageLimit,
		})
	}
}

func TestNewDrainerValidatesConfig(t *testing.T) {
	t.Parallel()

	factory := func(uint32) (stream.Source, error) { return plainSource{name: "events"}, nil }
	sink := stream.SinkFunc(func(stream.Record) error { return nil })

	tests := []struct {
		name string
		cfg  stream.DrainerConfig
	}{
		{name: "missing factory", cfg: stream.DrainerConfig{Sink: sink, From: 1}},
		{name: "missing sink", cfg: stream.DrainerConfig{NewSource: factory, From: 1}},
		{name: "missing from", cfg: stream.DrainerConfig{NewSource: factory, Sink: sink}},
		{
			name: "invalid strategy",
			cfg: stream.DrainerConfig{
				NewSource:   factory,
				Sink:        sink,
				From:        1,
				GapStrategy: stream.GapStrategy("skip"),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := stream.NewDrainer(tc.cfg); err == nil {
				t.Fatal("NewDrainer() error = nil, want a validation failure")
			}
		})
	}
}

func TestDrainerMergesConcurrentPagesInOrder(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(1000, 503)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(7))
	sink := &collector{}

	d, err := stream.NewDrainer(stream.DrainerConfig{
		NewSource: eventFactory(srv, 7),
		Sink:      sink,
		From:      1000,
		Workers:   4,
		Clock:     noWaitClock{},
	})
	if err != nil {
		t.Fatalf("NewDrainer() error = %v", err)
	}

	stats, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if d.Until() != 1502 {
		t.Errorf("Until() = %d, want 1502", d.Until())
	}
	assertStreamInvariants(t, sink.ids, idsOf(events))

	if stats.Emitted != len(events) {
		t.Errorf("Stats().Emitted = %d, want %d", stats.Emitted, len(events))
	}
	if stats.Fetches < 4 {
		t.Errorf("Stats().Fetches = %d, want at least one per worker", stats.Fetches)
	}
}

func TestDrainerFailsOnRetentionGapByDefault(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(1000, 100)
	srv := fakerpc.NewServer(events,
		fakerpc.WithPageLimit(10),
		fakerpc.WithRetention(1050),
	)
	sink := &collector{}

	d, err := stream.NewDrainer(stream.DrainerConfig{
		NewSource: eventFactory(srv, 10),
		Sink:      sink,
		From:      1000,
		Workers:   3,
		Clock:     noWaitClock{},
	})
	if err != nil {
		t.Fatalf("NewDrainer() error = %v", err)
	}

	stats, err := d.Run(context.Background())
	var gap *stream.GapError
	if !errors.As(err, &gap) {
		t.Fatalf("Run() error = %v, want a *stream.GapError", err)
	}
	if gap.From != 1000 || gap.Oldest != 1050 {
		t.Errorf("GapError = %+v, want From 1000 Oldest 1050", gap)
	}
	if stats.Gaps != 1 || stats.Recovered != 0 {
		t.Errorf("Stats() = %+v, want one unrecovered gap", stats)
	}
	if len(sink.ids) != 0 {
		t.Errorf("emitted %d records before the gap check, want 0", len(sink.ids))
	}
}

func TestDrainerResumesAtRetentionFloor(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(1000, 100)
	srv := fakerpc.NewServer(events,
		fakerpc.WithPageLimit(10),
		fakerpc.WithRetention(1050),
	)
	sink := &collector{}
	var gaps []stream.GapEvent

	d, err := stream.NewDrainer(stream.DrainerConfig{
		NewSource:   eventFactory(srv, 10),
		Sink:        sink,
		From:        1000,
		Workers:     3,
		GapStrategy: stream.GapResume,
		OnGap:       func(event stream.GapEvent) { gaps = append(gaps, event) },
		Clock:       noWaitClock{},
	})
	if err != nil {
		t.Fatalf("NewDrainer() error = %v", err)
	}

	stats, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	assertStreamInvariants(t, sink.ids, idsOf(events[50:]))
	if stats.Gaps != 1 || stats.Recovered != 1 {
		t.Errorf("Stats() = %+v, want one recovered gap", stats)
	}
	if len(gaps) != 1 || !gaps[0].Recovered || gaps[0].Gap.Oldest != 1050 {
		t.Errorf("OnGap events = %+v, want one recovery at ledger 1050", gaps)
	}
}

func TestDrainerHonoursLimit(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(1000, 100)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(5))
	sink := &collector{}

	d, err := stream.NewDrainer(stream.DrainerConfig{
		NewSource: eventFactory(srv, 5),
		Sink:      sink,
		From:      1000,
		Workers:   4,
		Limit:     9,
		Clock:     noWaitClock{},
	})
	if err != nil {
		t.Fatalf("NewDrainer() error = %v", err)
	}

	stats, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertStreamInvariants(t, sink.ids, idsOf(events[:9]))
	if stats.Emitted != 9 {
		t.Errorf("Stats().Emitted = %d, want 9", stats.Emitted)
	}
}

func TestDrainerStopsOnSinkError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("broken pipe downstream")
	events := fakerpc.MakeEvents(1000, 80)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(5))
	sink := &collector{failAt: 4, err: wantErr}

	d, err := stream.NewDrainer(stream.DrainerConfig{
		NewSource: eventFactory(srv, 5),
		Sink:      sink,
		From:      1000,
		Workers:   4,
		Clock:     noWaitClock{},
	})
	if err != nil {
		t.Fatalf("NewDrainer() error = %v", err)
	}

	stats, err := d.Run(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("Run() error = %v, want it to wrap %v", err, wantErr)
	}
	if stats.Emitted != 4 {
		t.Errorf("Stats().Emitted = %d, want 4", stats.Emitted)
	}
}

func TestDrainerRetriesTransientErrors(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(1000, 20)
	srv := fakerpc.NewServer(events,
		fakerpc.WithPageLimit(5),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			0: {Err: fakerpc.ErrDropped},
		}),
	)
	sink := &collector{}

	d, err := stream.NewDrainer(stream.DrainerConfig{
		NewSource: eventFactory(srv, 5),
		Sink:      sink,
		From:      1000,
		Workers:   1,
		Clock:     noWaitClock{},
	})
	if err != nil {
		t.Fatalf("NewDrainer() error = %v", err)
	}

	stats, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertStreamInvariants(t, sink.ids, idsOf(events))
	if stats.Retries != 1 {
		t.Errorf("Stats().Retries = %d, want 1", stats.Retries)
	}
}

type blockingSource struct {
	started chan struct{}
	once    sync.Once
}

func newBlockingSource() *blockingSource {
	return &blockingSource{started: make(chan struct{})}
}

func (s *blockingSource) Name() string { return "blocking" }

func (s *blockingSource) Fetch(ctx context.Context, _ string) (stream.Batch, error) {
	s.once.Do(func() { close(s.started) })
	<-ctx.Done()
	return stream.Batch{}, ctx.Err()
}

func (s *blockingSource) CursorLedger(string) (uint32, bool) { return 0, false }

func TestDrainerCancellationIsACleanStop(t *testing.T) {
	t.Parallel()

	src := newBlockingSource()
	d, err := stream.NewDrainer(stream.DrainerConfig{
		NewSource: func(uint32) (stream.Source, error) { return src, nil },
		Sink:      stream.SinkFunc(func(stream.Record) error { return nil }),
		From:      1000,
		Workers:   2,
		Clock:     noWaitClock{},
	})
	if err != nil {
		t.Fatalf("NewDrainer() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- func() error { _, err := d.Run(ctx); return err }() }()

	select {
	case <-src.started:
	case <-time.After(time.Second):
		t.Fatal("drainer never started fetching")
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() after cancellation = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}
}

func TestDrainerEmptyRange(t *testing.T) {
	t.Parallel()

	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 10), fakerpc.WithPageLimit(5))
	sink := &collector{}
	d, err := stream.NewDrainer(stream.DrainerConfig{
		NewSource: eventFactory(srv, 5),
		Sink:      sink,
		From:      1100,
		Until:     1000,
		Workers:   2,
		Clock:     noWaitClock{},
	})
	if err != nil {
		t.Fatalf("NewDrainer() error = %v", err)
	}

	stats, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if stats.Emitted != 0 || len(sink.ids) != 0 {
		t.Fatalf("emitted %d records for an empty range", stats.Emitted)
	}
}

func TestDrainerRecoversWorkerGap(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(1000, 20)
	srv := fakerpc.NewServer(events,
		fakerpc.WithPageLimit(5),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {OldestLedger: 1005},
		}),
	)
	sink := &collector{}
	var gaps []stream.GapEvent

	d, err := stream.NewDrainer(stream.DrainerConfig{
		NewSource:   eventFactory(srv, 5),
		Sink:        sink,
		From:        1000,
		Workers:     1,
		GapStrategy: stream.GapResume,
		OnGap:       func(event stream.GapEvent) { gaps = append(gaps, event) },
		Clock:       noWaitClock{},
	})
	if err != nil {
		t.Fatalf("NewDrainer() error = %v", err)
	}

	stats, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertStreamInvariants(t, sink.ids, idsOf(events[5:]))
	if stats.Gaps != 1 || stats.Recovered != 1 {
		t.Errorf("Stats() = %+v, want one recovered worker gap", stats)
	}
	if len(gaps) != 1 || !gaps[0].Recovered {
		t.Errorf("OnGap events = %+v, want one recovered notification", gaps)
	}
}

func TestDrainerFailsOnWorkerGap(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(1000, 20)
	srv := fakerpc.NewServer(events,
		fakerpc.WithPageLimit(5),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {OldestLedger: 1005},
		}),
	)
	sink := &collector{}

	d, err := stream.NewDrainer(stream.DrainerConfig{
		NewSource: eventFactory(srv, 5),
		Sink:      sink,
		From:      1000,
		Workers:   1,
		Clock:     noWaitClock{},
	})
	if err != nil {
		t.Fatalf("NewDrainer() error = %v", err)
	}

	stats, err := d.Run(context.Background())
	var gap *stream.GapError
	if !errors.As(err, &gap) {
		t.Fatalf("Run() error = %v, want a *stream.GapError", err)
	}
	if gap.Oldest != 1005 {
		t.Errorf("GapError.Oldest = %d, want 1005", gap.Oldest)
	}
	if stats.Gaps != 1 || stats.Recovered != 0 {
		t.Errorf("Stats() = %+v, want one unrecovered worker gap", stats)
	}
}

func TestDrainerHonoursMaxGapRecoveriesAcrossWorkers(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(1000, 20)
	srv := fakerpc.NewServer(events,
		fakerpc.WithPageLimit(5),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {OldestLedger: 1005},
			2: {OldestLedger: 1010},
		}),
	)
	sink := &collector{}

	d, err := stream.NewDrainer(stream.DrainerConfig{
		NewSource:        eventFactory(srv, 5),
		Sink:             sink,
		From:             1000,
		Workers:          1,
		GapStrategy:      stream.GapResume,
		MaxGapRecoveries: 1,
		Clock:            noWaitClock{},
	})
	if err != nil {
		t.Fatalf("NewDrainer() error = %v", err)
	}

	stats, err := d.Run(context.Background())
	var gap *stream.GapError
	if !errors.As(err, &gap) {
		t.Fatalf("Run() error = %v, want a *stream.GapError after the recovery limit", err)
	}
	if stats.Gaps != 2 || stats.Recovered != 1 {
		t.Errorf("Stats() = %+v, want two gaps with one recovery", stats)
	}
	if len(sink.ids) != 0 {
		t.Errorf("emitted %d records, want the run to stop before writing from the old range", len(sink.ids))
	}
}

func TestDrainerContinuesPastShortPages(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(1000, 20)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(3))
	sink := &collector{}

	d, err := stream.NewDrainer(stream.DrainerConfig{
		NewSource: eventFactory(srv, 10),
		Sink:      sink,
		From:      1000,
		Workers:   2,
		Clock:     noWaitClock{},
	})
	if err != nil {
		t.Fatalf("NewDrainer() error = %v", err)
	}

	stats, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertStreamInvariants(t, sink.ids, idsOf(events))
	if stats.Emitted != len(events) {
		t.Errorf("Stats().Emitted = %d, want %d", stats.Emitted, len(events))
	}
}
