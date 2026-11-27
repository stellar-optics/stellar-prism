package stream_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// memoryStore is an in-memory MetadataStore, used to observe what the pruner
// asks for without touching a file.
type memoryStore struct {
	mu       sync.Mutex
	entries  []stream.CursorMetadata
	policies []stream.PrunePolicy
	prunes   int
	err      error
}

func (s *memoryStore) Append(_ context.Context, meta stream.CursorMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.entries = append(s.entries, meta)
	return nil
}

func (s *memoryStore) Latest(_ context.Context, key string) (stream.CursorMetadata, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return stream.CursorMetadata{}, false, s.err
	}
	for i := len(s.entries) - 1; i >= 0; i-- {
		if s.entries[i].Key == key {
			return s.entries[i], true, nil
		}
	}
	return stream.CursorMetadata{}, false, nil
}

func (s *memoryStore) Prune(_ context.Context, policy stream.PrunePolicy) (stream.PruneStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prunes++
	s.policies = append(s.policies, policy)
	if s.err != nil {
		return stream.PruneStats{}, s.err
	}

	counts := map[string]int{}
	for _, entry := range s.entries {
		if !policy.Cutoff.IsZero() && entry.RecordedAt.Before(policy.Cutoff) {
			continue
		}
		counts[entry.Key]++
	}

	seen := map[string]int{}
	var kept []stream.CursorMetadata
	for _, entry := range s.entries {
		if !policy.Cutoff.IsZero() && entry.RecordedAt.Before(policy.Cutoff) {
			continue
		}
		index := seen[entry.Key]
		seen[entry.Key] = index + 1
		if policy.Keep > 0 && index < counts[entry.Key]-policy.Keep {
			continue
		}
		kept = append(kept, entry)
	}
	stats := stream.PruneStats{Kept: len(kept), Removed: len(s.entries) - len(kept)}
	s.entries = kept
	return stats, nil
}

func (s *memoryStore) pruneCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.prunes
}

func (s *memoryStore) lastPolicy() stream.PrunePolicy {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.policies) == 0 {
		return stream.PrunePolicy{}
	}
	return s.policies[len(s.policies)-1]
}

func (s *memoryStore) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func TestNewPrunerRequiresAStore(t *testing.T) {
	t.Parallel()

	pruner, err := stream.NewPruner(nil, stream.PruneOptions{})
	if err == nil {
		t.Fatal("NewPruner() error = nil, want a missing-store failure")
	}
	if pruner != nil {
		t.Error("NewPruner() returned a pruner alongside the error")
	}
}

// TestNewPrunerTreatsNegativeOptionsAsDisabled checks that a negative limit
// is read as "disabled" rather than as a limit nothing can satisfy.
//
// The CLI rejects a negative flag outright, so this only matters to a library
// caller: a negative Keep would otherwise prune every entry, which is the
// opposite of what the caller asked for.
func TestNewPrunerTreatsNegativeOptionsAsDisabled(t *testing.T) {
	t.Parallel()

	store := &memoryStore{}
	pruner, err := stream.NewPruner(store, stream.PruneOptions{
		Interval: -time.Hour,
		Keep:     -1,
		MaxAge:   -time.Hour,
	})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}

	// A negative Keep must not prune anything: zero means "keep every
	// entry".
	if _, err := pruner.PruneOnce(context.Background()); err != nil {
		t.Fatalf("PruneOnce() error = %v", err)
	}
	policy := store.lastPolicy()
	if policy.Keep != 0 {
		t.Errorf("policy.Keep = %d, want 0 (keep everything)", policy.Keep)
	}
	if !policy.Cutoff.IsZero() {
		t.Errorf("policy.Cutoff = %v, want the zero time (keep every age)", policy.Cutoff)
	}
}

// TestNewPrunerDefaultsToTheRealClock checks that a pruner built without a
// clock still knows what time it is.
func TestNewPrunerDefaultsToTheRealClock(t *testing.T) {
	t.Parallel()

	pruner, err := stream.NewPruner(&memoryStore{}, stream.PruneOptions{})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if _, err := pruner.PruneOnce(context.Background()); err != nil {
		t.Fatalf("PruneOnce() error = %v", err)
	}
}

// TestPrunerStartAcceptsANilContext checks that a caller with no context
// still gets a working pruner.
func TestPrunerStartAcceptsANilContext(t *testing.T) {
	t.Parallel()

	pruner, err := stream.NewPruner(&memoryStore{}, stream.PruneOptions{Interval: time.Hour})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	//nolint:staticcheck // SA1012: a nil context is exactly what is under test.
	if err := pruner.Start(nil); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := pruner.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestPrunerPrunesOnceWithTheConfiguredPolicy(t *testing.T) {
	t.Parallel()

	store := &memoryStore{}
	pruner, err := stream.NewPruner(store, stream.PruneOptions{
		Keep:   5,
		MaxAge: time.Hour,
		Clock:  fixedClock{now: time.Unix(1700000000, 0).UTC()},
	})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}

	stats, err := pruner.PruneOnce(context.Background())
	if err != nil {
		t.Fatalf("PruneOnce() error = %v", err)
	}
	if stats.Kept != 0 || stats.Removed != 0 {
		t.Errorf("PruneOnce() = %+v, want an empty journal to change nothing", stats)
	}

	policy := store.lastPolicy()
	if policy.Keep != 5 {
		t.Errorf("policy keep = %d, want 5", policy.Keep)
	}
	want := time.Unix(1700000000, 0).UTC().Add(-time.Hour)
	if !policy.Cutoff.Equal(want) {
		t.Errorf("policy cutoff = %v, want %v", policy.Cutoff, want)
	}
}

func TestPrunerRemovesExpiredEntries(t *testing.T) {
	t.Parallel()

	now := time.Unix(1700000000, 0).UTC()
	store := &memoryStore{entries: []stream.CursorMetadata{
		{Key: "events", Cursor: "old", RecordedAt: now.Add(-2 * time.Hour)},
		{Key: "events", Cursor: "new", RecordedAt: now.Add(-time.Minute)},
	}}
	pruner, err := stream.NewPruner(store, stream.PruneOptions{
		MaxAge: time.Hour,
		Clock:  fixedClock{now: now},
	})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}

	stats, err := pruner.PruneOnce(context.Background())
	if err != nil {
		t.Fatalf("PruneOnce() error = %v", err)
	}
	if stats.Removed != 1 || stats.Kept != 1 {
		t.Errorf("PruneOnce() = %+v, want 1 removed and 1 kept", stats)
	}
	if store.entries[0].Cursor != "new" {
		t.Errorf("kept %q, want the newer entry", store.entries[0].Cursor)
	}
}

func TestPrunerKeepsTheNewestEntriesPerKey(t *testing.T) {
	t.Parallel()

	store := &memoryStore{}
	for _, cursor := range []string{"a", "b", "c", "d"} {
		store.entries = append(store.entries, stream.CursorMetadata{Key: "events", Cursor: cursor})
	}
	store.entries = append(store.entries, stream.CursorMetadata{Key: "ledgers", Cursor: "other"})

	pruner, err := stream.NewPruner(store, stream.PruneOptions{Keep: 2})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if _, err := pruner.PruneOnce(context.Background()); err != nil {
		t.Fatalf("PruneOnce() error = %v", err)
	}

	var events, ledgers int
	for _, entry := range store.entries {
		switch entry.Key {
		case "events":
			events++
		case "ledgers":
			ledgers++
		}
	}
	if events != 2 {
		t.Errorf("kept %d event entries, want 2", events)
	}
	if ledgers != 1 {
		t.Errorf("kept %d ledger entries, want 1", ledgers)
	}
	if store.entries[0].Cursor != "c" {
		t.Errorf("first kept entry = %q, want the newest two", store.entries[0].Cursor)
	}
}

func TestPrunerStartPrunesInTheBackground(t *testing.T) {
	t.Parallel()

	store := &memoryStore{}
	pruner, err := stream.NewPruner(store, stream.PruneOptions{Interval: time.Millisecond})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := pruner.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	waitFor(t, "the background loop to prune", func() bool { return store.pruneCount() >= 2 })
}

func TestPrunerWithAZeroIntervalPrunesOnce(t *testing.T) {
	t.Parallel()

	store := &memoryStore{}
	pruner, err := stream.NewPruner(store, stream.PruneOptions{Interval: 0})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := pruner.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	waitFor(t, "the single pass to run", func() bool { return store.pruneCount() >= 1 })

	// The loop must stop after that pass rather than spinning.
	time.Sleep(50 * time.Millisecond)
	if got := store.pruneCount(); got != 1 {
		t.Errorf("pruned %d times, want exactly one", got)
	}
}

func TestPrunerStartTwiceIsRejected(t *testing.T) {
	t.Parallel()

	pruner, err := stream.NewPruner(&memoryStore{}, stream.PruneOptions{Interval: time.Hour})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := pruner.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	if err := pruner.Start(context.Background()); !errors.Is(err, stream.ErrPrunerStarted) {
		t.Errorf("Start() error = %v, want ErrPrunerStarted", err)
	}
}

func TestPrunerStartAfterCloseIsRejected(t *testing.T) {
	t.Parallel()

	pruner, err := stream.NewPruner(&memoryStore{}, stream.PruneOptions{Interval: time.Hour})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := pruner.Start(context.Background()); !errors.Is(err, stream.ErrPrunerClosed) {
		t.Errorf("Start() error = %v, want ErrPrunerClosed", err)
	}
}

func TestPrunerCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	pruner, err := stream.NewPruner(&memoryStore{}, stream.PruneOptions{Interval: time.Hour})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := pruner.Close(); err != nil {
		t.Errorf("second Close() error = %v, want nil", err)
	}
}

// TestPrunerConcurrentCloseWaitsForTheGoroutine checks that every Close
// waits for the goroutine, not only the first one.
//
// A second Close that returned while the loop was still running would report
// the pruner as stopped when it is not, and a caller that trusted that
// report could exit with the goroutine still alive.
func TestPrunerConcurrentCloseWaitsForTheGoroutine(t *testing.T) {
	before := runtime.NumGoroutine()

	store := &blockingStore{started: make(chan struct{}, 1), release: make(chan struct{})}
	pruner, err := stream.NewPruner(store, stream.PruneOptions{Interval: time.Hour})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the pruner never started a pass")
	}

	closed := make(chan error, 2)
	go func() { closed <- pruner.Close() }()
	// Give the first Close time to mark the pruner closed, so the second
	// Close takes the already-closed path.
	time.Sleep(50 * time.Millisecond)
	go func() { closed <- pruner.Close() }()

	// The pass is still running, so neither Close may return yet.
	select {
	case err := <-closed:
		close(store.release)
		t.Fatalf("Close() returned (%v) while the pruner was still running", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(store.release)
	for i := 0; i < 2; i++ {
		select {
		case err := <-closed:
			if err != nil {
				t.Errorf("Close() error = %v, want nil", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Close() did not return after the pruner finished")
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > before {
		t.Errorf("goroutines = %d, want at most %d: the pruner leaked one", got, before)
	}
}

// blockingStore holds a prune pass open until the test releases it, so the
// pruner's goroutine can be observed in the middle of its work.
type blockingStore struct {
	started chan struct{}
	release chan struct{}
}

func (s *blockingStore) Append(context.Context, stream.CursorMetadata) error { return nil }

func (s *blockingStore) Latest(context.Context, string) (stream.CursorMetadata, bool, error) {
	return stream.CursorMetadata{}, false, nil
}

func (s *blockingStore) Prune(context.Context, stream.PrunePolicy) (stream.PruneStats, error) {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-s.release
	return stream.PruneStats{}, nil
}

// TestPrunerCloseJoinsItsGoroutine checks the property that makes the
// pruner leak-free: Close waits for the goroutine it owns instead of merely
// asking it to stop.
func TestPrunerCloseJoinsItsGoroutine(t *testing.T) {
	before := runtime.NumGoroutine()

	store := &blockingStore{started: make(chan struct{}, 1), release: make(chan struct{})}
	pruner, err := stream.NewPruner(store, stream.PruneOptions{Interval: time.Hour})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the pruner never started a pass")
	}

	closed := make(chan error, 1)
	go func() { closed <- pruner.Close() }()

	// The pass is still running, so Close must still be waiting for it.
	select {
	case err := <-closed:
		close(store.release)
		t.Fatalf("Close() returned (%v) while the pruner was still running", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(store.release)
	select {
	case err := <-closed:
		if err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not return after the pruner finished")
	}

	// The goroutine is joined, so it is gone by the time Close returns.
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > before {
		t.Errorf("goroutines = %d, want at most %d: the pruner leaked one", got, before)
	}
}

func TestPrunerReportsFailuresAndKeepsRunning(t *testing.T) {
	t.Parallel()

	store := &memoryStore{}
	var (
		mu       sync.Mutex
		failures []error
	)
	pruner, err := stream.NewPruner(store, stream.PruneOptions{
		Interval: time.Millisecond,
		OnError: func(err error) {
			mu.Lock()
			defer mu.Unlock()
			failures = append(failures, err)
		},
	})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	t.Cleanup(func() {
		if err := pruner.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	store.setError(errors.New("disk on fire"))
	if err := pruner.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	waitFor(t, "the failure to be reported", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(failures) > 0
	})

	mu.Lock()
	first := failures[0]
	mu.Unlock()
	if first.Error() != "pruning cursor metadata: disk on fire" {
		t.Errorf("reported failure = %q, want the store error to be wrapped", first.Error())
	}

	// A failing pass must not stop the loop.
	waitFor(t, "the loop to keep going", func() bool { return store.pruneCount() >= 2 })
}

func TestPrunerCountsPassesAndRemovals(t *testing.T) {
	t.Parallel()

	now := time.Unix(1700000000, 0).UTC()
	store := &memoryStore{entries: []stream.CursorMetadata{
		{Key: "events", Cursor: "a", RecordedAt: now.Add(-2 * time.Hour)},
		{Key: "events", Cursor: "b", RecordedAt: now},
	}}
	pruner, err := stream.NewPruner(store, stream.PruneOptions{
		MaxAge: time.Hour,
		Clock:  fixedClock{now: now},
	})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if _, err := pruner.PruneOnce(context.Background()); err != nil {
		t.Fatalf("PruneOnce() error = %v", err)
	}

	stats := pruner.Stats()
	if stats.Passes != 1 {
		t.Errorf("passes = %d, want 1", stats.Passes)
	}
	if stats.Removed != 1 {
		t.Errorf("removed = %d, want 1", stats.Removed)
	}
	if stats.Errors != 0 {
		t.Errorf("errors = %d, want 0", stats.Errors)
	}
}

func TestPrunerReportsEachPass(t *testing.T) {
	t.Parallel()

	var (
		mu    sync.Mutex
		stats []stream.PruneStats
	)
	pruner, err := stream.NewPruner(&memoryStore{}, stream.PruneOptions{
		Interval: time.Millisecond,
		OnPrune: func(s stream.PruneStats) {
			mu.Lock()
			defer mu.Unlock()
			stats = append(stats, s)
		},
	})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := pruner.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	waitFor(t, "a pass to be reported", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(stats) > 0
	})
}

func TestPrunerPruneOnceHonoursCancellation(t *testing.T) {
	t.Parallel()

	pruner, err := stream.NewPruner(&memoryStore{}, stream.PruneOptions{})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pruner.PruneOnce(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("PruneOnce() error = %v, want context.Canceled", err)
	}
}

// TestPrunerBoundsTheMetadataFileDuringAStream is the end-to-end behaviour:
// a stream that records a checkpoint per page must not leave an unbounded
// file behind, even while it is still running.
func TestPrunerBoundsTheMetadataFileDuringAStream(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(1000, 40)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(5))

	src, err := stream.NewEventSource(srv, stream.EventQuery{
		StartLedger: 1000,
		PageLimit:   5,
	})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	store, err := stream.NewFileMetadataStore(path)
	if err != nil {
		t.Fatalf("NewFileMetadataStore() error = %v", err)
	}

	pruner, err := stream.NewPruner(store, stream.PruneOptions{
		Interval: time.Millisecond,
		Keep:     3,
	})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := pruner.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	tailer := stream.NewTailer(src, &collector{}, stream.Options{
		OnPage: func(page stream.Page) {
			if err := store.Append(context.Background(), stream.CursorMetadata{
				Key:        "events",
				Cursor:     page.Cursor,
				Ledger:     page.Ledger,
				RecordedAt: time.Now().UTC(),
			}); err != nil {
				t.Errorf("Append() error = %v", err)
			}
		},
	})
	if err := tailer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	// The pruner runs on an interval, so the file is compacted again once
	// the stream is done, exactly as the CLI does at shutdown.
	if _, err := pruner.PruneOnce(context.Background()); err != nil {
		t.Fatalf("PruneOnce() error = %v", err)
	}

	entries, err := store.Entries(context.Background(), "events")
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(entries) > 3 {
		t.Errorf("kept %d entries, want at most 3", len(entries))
	}
	if len(entries) == 0 {
		t.Error("the stream recorded no checkpoints at all")
	}

	// The newest checkpoint must be the position the stream reached, so a
	// later run can pick the stream up where this one stopped.
	latest, found, err := store.Latest(context.Background(), "events")
	if err != nil {
		t.Fatalf("Latest() error = %v", err)
	}
	if !found {
		t.Fatal("Latest() found no checkpoint")
	}
	if latest.Cursor != events[len(events)-1].ID {
		t.Errorf("latest cursor = %q, want %q", latest.Cursor, events[len(events)-1].ID)
	}
}

// fixedClock is a Clock whose time never moves.
type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time { return c.now }

func (c fixedClock) Sleep(ctx context.Context, _ time.Duration) error {
	return ctx.Err()
}

// waitFor waits for a condition, so a test about a background goroutine fails
// with a clear message instead of hanging.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// steppedClock is a Clock whose Now the test sets, and whose Sleep returns
// almost at once so a background loop can be driven quickly.
type steppedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *steppedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *steppedClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

func (c *steppedClock) Sleep(ctx context.Context, _ time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Millisecond):
		return nil
	}
}

// thirtyDayHistory builds a state whose history holds one checkpoint an hour
// for the 30 days before now, oldest first, and whose active cursor is the
// newest position.
func thirtyDayHistory(now time.Time) stream.State {
	const hours = 30 * 24
	st := stream.State{Version: stream.StateVersion, Source: "events"}
	for i := hours; i >= 1; i-- {
		ledger := uint32(1_000_000 + (hours-i)*720) //nolint:gosec // test data
		st.History = append(st.History, stream.Checkpoint{
			Ledger:      ledger,
			PagingToken: fmt.Sprintf("tok-%d", ledger),
			RecordedAt:  now.Add(-time.Duration(i) * time.Hour),
		})
	}
	last := st.History[len(st.History)-1]
	st.Ledger, st.PagingToken, st.Cursor, st.LastID = last.Ledger, last.PagingToken, last.PagingToken, last.PagingToken
	return st
}

// TestPruneStateHistoryThirtyDays prunes a simulated 30-day history down to a
// 7-day window and checks exactly what survives.
func TestPruneStateHistoryThirtyDays(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	st := thirtyDayHistory(now)
	before := len(st.History)
	ledger, token, lastID := st.Ledger, st.PagingToken, st.LastID

	removed := stream.PruneStateHistoryAt(&st, 7*24*time.Hour, now)

	// Hourly checkpoints from now-168h to now-1h are inside the window.
	const want = 7 * 24
	if len(st.History) != want {
		t.Fatalf("kept %d checkpoints, want %d (7 days of hourly checkpoints)", len(st.History), want)
	}
	if removed != before-want {
		t.Errorf("removed = %d, want %d", removed, before-want)
	}
	cutoff := now.Add(-7 * 24 * time.Hour)
	for i, cp := range st.History {
		if cp.RecordedAt.Before(cutoff) {
			t.Errorf("checkpoint %d at %v survived the 7-day cutoff %v", i, cp.RecordedAt, cutoff)
		}
		if i > 0 && !cp.RecordedAt.After(st.History[i-1].RecordedAt) {
			t.Errorf("checkpoint %d is out of order after pruning", i)
		}
	}
	if st.Ledger != ledger || st.PagingToken != token || st.LastID != lastID {
		t.Errorf("active cursor moved: %d/%q/%q, want %d/%q/%q", st.Ledger, st.PagingToken, st.LastID, ledger, token, lastID)
	}
}

// TestPruneStateHistoryKeepsLatestCheckpoint checks that a stream idle for
// longer than the retention window keeps its most recent checkpoint, and that
// the active resume cursor is never touched.
func TestPruneStateHistoryKeepsLatestCheckpoint(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	st := thirtyDayHistory(now.Add(-60 * 24 * time.Hour)) // all 60-90 days old
	newest := st.History[len(st.History)-1]
	token := st.PagingToken

	removed := stream.PruneStateHistoryAt(&st, 7*24*time.Hour, now)

	if len(st.History) != 1 || st.History[0] != newest {
		t.Fatalf("history = %+v, want only the newest checkpoint %+v", st.History, newest)
	}
	if removed != 30*24-1 {
		t.Errorf("removed = %d, want %d", removed, 30*24-1)
	}
	if st.PagingToken != token {
		t.Errorf("active cursor = %q, want %q", st.PagingToken, token)
	}
}

func TestPruneStateHistoryNoOps(t *testing.T) {
	t.Parallel()

	if got := stream.PruneStateHistory(nil, time.Hour); got != 0 {
		t.Errorf("nil state removed %d", got)
	}
	now := time.Now()
	st := thirtyDayHistory(now)
	if got := stream.PruneStateHistory(&st, 0); got != 0 || len(st.History) != 30*24 {
		t.Errorf("zero retention removed %d, kept %d; want nothing removed", got, len(st.History))
	}
	empty := stream.State{Source: "events", PagingToken: "x"}
	if got := stream.PruneStateHistory(&empty, time.Hour); got != 0 || empty.PagingToken != "x" {
		t.Errorf("empty history: removed %d, cursor %q", got, empty.PagingToken)
	}
}

// TestStateFileHistoryCompactsDuringSaves runs a simulated 30 days of a
// stream saving every hour through a StateFile with a 7-day retention,
// and checks that the file stays bounded and the resume cursor is intact.
func TestStateFileHistoryCompactsDuringSaves(t *testing.T) {
	t.Parallel()

	clock := &steppedClock{now: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}
	path := filepath.Join(t.TempDir(), "state.json")
	sf, err := stream.OpenStateFileWithOptions(path, stream.StateFileOptions{
		StreamID:  "events_amm",
		Retention: 7 * 24 * time.Hour,
		Clock:     clock,
	})
	if err != nil {
		t.Fatal(err)
	}

	start := clock.Now()
	var maxSize int64
	var last stream.State
	for step := 0; step <= 30*24; step++ { // hourly for 30 days
		at := start.Add(time.Duration(step) * time.Hour)
		clock.set(at)
		last = stream.State{
			Source:      "events",
			Ledger:      uint32(1_000 + step), //nolint:gosec // test data
			PagingToken: fmt.Sprintf("p%d", step),
			LastID:      fmt.Sprintf("p%d", step),
			UpdatedAt:   at,
		}
		if err := sf.Save(last); err != nil {
			t.Fatalf("Save() at step %d error = %v", step, err)
		}
		if info, err := os.Stat(path); err == nil && info.Size() > maxSize {
			maxSize = info.Size()
		}
	}

	got, err := sf.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Ledger != last.Ledger || got.PagingToken != last.PagingToken || got.LastID != last.LastID {
		t.Fatalf("resume cursor = %d/%q, want the last saved %d/%q", got.Ledger, got.PagingToken, last.Ledger, last.PagingToken)
	}
	// One checkpoint an hour, 7 days retained, plus at most one compaction
	// interval of growth between prunes.
	limit := 7*24 + int(stream.DefaultStateCompactInterval/time.Hour) + 1
	if n := len(got.History); n == 0 || n > limit {
		t.Fatalf("history holds %d checkpoints after 30 days, want 1..%d", n, limit)
	}
	cutoff := clock.Now().Add(-7*24*time.Hour - stream.DefaultStateCompactInterval)
	if oldest := got.History[0].RecordedAt; oldest.Before(cutoff) {
		t.Errorf("oldest checkpoint %v is older than retention plus one compaction interval (%v)", oldest, cutoff)
	}
	if newest := got.History[len(got.History)-1]; clock.Now().Sub(newest.RecordedAt) > stream.DefaultStateCheckpointInterval {
		t.Errorf("newest checkpoint %v is more than one checkpoint interval old", newest.RecordedAt)
	}
	// A compact file: well under the size an unpruned 30-day history would
	// reach (720 hourly checkpoints of ~90 bytes each).
	if maxSize > 32*1024 {
		t.Errorf("state file grew to %d bytes; pruning is not keeping it compact", maxSize)
	}
}

// TestStateFileCompactPrunesEveryStream checks the explicit compaction pass
// over a shared file: every stream's history is pruned, every resume cursor
// survives.
func TestStateFileCompactPrunesEveryStream(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	clock := &steppedClock{now: now}
	path := filepath.Join(t.TempDir(), "state.json")
	for _, id := range []string{"a", "b"} {
		// Retention off while seeding, so the save does not prune.
		sf, err := stream.OpenStateFileWithOptions(path, stream.StateFileOptions{StreamID: id, Clock: clock})
		if err != nil {
			t.Fatal(err)
		}
		if err := sf.Save(thirtyDayHistory(now)); err != nil {
			t.Fatal(err)
		}
	}

	sf, err := stream.OpenStateFileWithOptions(path, stream.StateFileOptions{
		StreamID: "a", Retention: 7 * 24 * time.Hour, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	removed, err := sf.Compact()
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if want := 2 * (30*24 - 7*24); removed != want {
		t.Errorf("Compact() removed %d, want %d", removed, want)
	}
	for _, id := range []string{"a", "b"} {
		other, err := stream.OpenStateFileWithOptions(path, stream.StateFileOptions{StreamID: id})
		if err != nil {
			t.Fatal(err)
		}
		st, err := other.Load()
		if err != nil {
			t.Fatal(err)
		}
		if len(st.History) != 7*24 {
			t.Errorf("%s history = %d checkpoints, want %d", id, len(st.History), 7*24)
		}
		if st.PagingToken == "" || st.PagingToken != st.History[len(st.History)-1].PagingToken {
			t.Errorf("%s resume cursor = %q, want the newest position", id, st.PagingToken)
		}
	}
}

// TestStateFileStartCompactionRunsInBackground checks the background
// compactor: it prunes without any save happening, and stop joins its
// goroutine.
func TestStateFileStartCompactionRunsInBackground(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	clock := &steppedClock{now: now}
	path := filepath.Join(t.TempDir(), "state.json")

	seed, err := stream.OpenStateFileWithOptions(path, stream.StateFileOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Save(thirtyDayHistory(now)); err != nil {
		t.Fatal(err)
	}

	sf, err := stream.OpenStateFileWithOptions(path, stream.StateFileOptions{Retention: 7 * 24 * time.Hour, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()
	stop := sf.StartCompaction(context.Background(), stream.DefaultStateCompactInterval, func(err error) {
		t.Errorf("compaction error: %v", err)
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := sf.Load()
		if err != nil {
			t.Fatal(err)
		}
		if len(st.History) == 7*24 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("background compaction never pruned the history (%d checkpoints)", len(st.History))
		}
		time.Sleep(5 * time.Millisecond)
	}
	stop()
	stop() // idempotent

	deadline = time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Errorf("goroutines after stop = %d, want <= %d", n, before)
	}
}

// TestStateFileHistoryRespectsCheckpointInterval checks that saves closer
// together than the checkpoint interval do not each add a checkpoint.
func TestStateFileHistoryRespectsCheckpointInterval(t *testing.T) {
	t.Parallel()

	clock := &steppedClock{now: time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)}
	sf, err := stream.OpenStateFileWithOptions(filepath.Join(t.TempDir(), "s.json"), stream.StateFileOptions{
		StreamID:           "s",
		CheckpointInterval: time.Hour,
		Clock:              clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	start := clock.Now()
	for minute := 0; minute < 180; minute += 5 { // three hours, one save every 5 minutes
		at := start.Add(time.Duration(minute) * time.Minute)
		if err := sf.Save(stream.State{Source: "events", Ledger: uint32(minute + 1), PagingToken: fmt.Sprint(minute), UpdatedAt: at}); err != nil { //nolint:gosec // test data
			t.Fatal(err)
		}
	}
	st, err := sf.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(st.History) != 3 {
		t.Fatalf("history = %d checkpoints over 3 hours at a 1h interval, want 3: %+v", len(st.History), st.History)
	}
}
