package stream

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/stellar/go-stellar-sdk/strkey"
)

// DefaultWatchlistDebounce coalesces the burst of file events a single save
// produces. Editors commonly truncate, write and chmod (or write a temporary
// file and rename it over the original) as separate steps; reloading on each
// one would read a half-written file and flap the filter. A short quiet
// period collapses the burst into one reload of the finished file.
const DefaultWatchlistDebounce = 100 * time.Millisecond

// ContractWatchlist provides a concurrency-safe snapshot of watched IDs.
type ContractWatchlist interface{ ContractIDs() []string }

// WatchlistChange describes what one reload changed in the active set.
type WatchlistChange struct {
	// Added lists contract IDs that were not watched before the reload, in
	// file order.
	Added []string
	// Removed lists contract IDs that are no longer watched, in their
	// previous file order.
	Removed []string
	// Total is the number of contracts watched after the reload.
	Total int
}

// Empty reports whether the reload left the set unchanged.
func (c WatchlistChange) Empty() bool { return len(c.Added) == 0 && len(c.Removed) == 0 }

// watchlistSnapshot is the immutable state a WatchlistFilter publishes. A
// reload builds a new one and swaps the pointer, so readers on the streaming
// path never take a lock and never observe a half-applied update.
type watchlistSnapshot struct {
	ids   []string
	index map[string]struct{}
}

func newWatchlistSnapshot(ids []string) *watchlistSnapshot {
	index := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		index[id] = struct{}{}
	}
	return &watchlistSnapshot{ids: ids, index: index}
}

// WatchlistFilter tracks a set of contract addresses read from a text file
// and keeps it current while a stream runs. Addresses are trimmed, "#"
// comments and blank lines are ignored, and duplicates collapse.
//
// The filter serves two roles. As a ContractWatchlist it supplies the
// contract set the event source sends to the RPC, so additions take effect on
// the next poll without restarting the stream. As a RecordFilter it drops
// events from contracts that were removed after a page was fetched, so a
// removal is honoured immediately rather than one page later.
//
// Reloads are atomic: the active set is replaced only after the new file has
// been read and validated in full. A file that fails to parse leaves the
// previous set in force and is reported on Errors.
type WatchlistFilter struct {
	path     string
	debounce time.Duration
	snap     atomic.Pointer[watchlistSnapshot]
	errs     chan error

	// reloadMu serialises reloads so diffs are computed against the set they
	// actually replace.
	reloadMu sync.Mutex
	onChange func(WatchlistChange)
}

// WatchlistManager is the earlier name of WatchlistFilter, kept so existing
// callers continue to compile.
type WatchlistManager = WatchlistFilter

// NewWatchlistFilter reads and validates the initial watchlist before the
// event stream starts, so a malformed file fails the command up front.
func NewWatchlistFilter(path string) (*WatchlistFilter, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("watchlist path is empty")
	}
	ids, err := readWatchlist(path)
	if err != nil {
		return nil, err
	}
	f := &WatchlistFilter{
		path:     path,
		debounce: DefaultWatchlistDebounce,
		errs:     make(chan error, 1),
	}
	f.snap.Store(newWatchlistSnapshot(ids))
	return f, nil
}

// NewWatchlistManager is the earlier name of NewWatchlistFilter.
func NewWatchlistManager(path string) (*WatchlistManager, error) {
	return NewWatchlistFilter(path)
}

// OnChange registers fn to be called after every reload that changes the
// active set. It must be called before Start; fn runs on the watcher
// goroutine and should not block for long.
func (f *WatchlistFilter) OnChange(fn func(WatchlistChange)) { f.onChange = fn }

// SetDebounce overrides DefaultWatchlistDebounce. It must be called before
// Start. A non-positive value reloads on every file event.
func (f *WatchlistFilter) SetDebounce(d time.Duration) { f.debounce = d }

// Path returns the file the filter reads.
func (f *WatchlistFilter) Path() string { return f.path }

// ContractIDs returns a copy of the active subscription set in file order.
func (f *WatchlistFilter) ContractIDs() []string {
	return append([]string(nil), f.snap.Load().ids...)
}

// Len returns the number of contracts currently watched.
func (f *WatchlistFilter) Len() int { return len(f.snap.Load().ids) }

// Contains reports whether contractID is currently watched.
func (f *WatchlistFilter) Contains(contractID string) bool {
	_, ok := f.snap.Load().index[contractID]
	return ok
}

// Match implements RecordFilter. Event records pass only when their contract
// is in the active set; every other kind of record passes untouched, because
// the watchlist says nothing about them.
func (f *WatchlistFilter) Match(record Record) (bool, error) {
	if record.Kind != KindEvent || record.Event == nil {
		return true, nil
	}
	return f.Contains(record.Event.ContractID), nil
}

// Errors receives file reload errors. The active set stays unchanged until a
// valid replacement has been read completely. The channel is closed when the
// watcher started by Start exits.
func (f *WatchlistFilter) Errors() <-chan error { return f.errs }

// Reload re-reads the file now and swaps in the new set if it is valid. It is
// what the file watcher calls, and is exported so callers can trigger a reload
// from another signal (SIGHUP, an admin endpoint) as well.
func (f *WatchlistFilter) Reload() (WatchlistChange, error) {
	ids, err := readWatchlist(f.path)
	if err != nil {
		return WatchlistChange{}, err
	}
	f.reloadMu.Lock()
	defer f.reloadMu.Unlock()
	next := newWatchlistSnapshot(ids)
	prev := f.snap.Swap(next)
	change := diffWatchlist(prev, next)
	if !change.Empty() && f.onChange != nil {
		f.onChange(change)
	}
	return change, nil
}

func diffWatchlist(prev, next *watchlistSnapshot) WatchlistChange {
	change := WatchlistChange{Total: len(next.ids)}
	for _, id := range next.ids {
		if _, ok := prev.index[id]; !ok {
			change.Added = append(change.Added, id)
		}
	}
	for _, id := range prev.ids {
		if _, ok := next.index[id]; !ok {
			change.Removed = append(change.Removed, id)
		}
	}
	return change
}

// Start watches the containing directory, so an editor's atomic
// write-and-rename replacement is observed as well as an in-place write. It
// returns once the watch is installed and reloads in the background until ctx
// is cancelled.
func (f *WatchlistFilter) Start(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("watching contract file: %w", err)
	}
	dir := filepath.Dir(f.path)
	if dir == "" {
		dir = "."
	}
	if err := watcher.Add(dir); err != nil {
		_ = watcher.Close()
		return fmt.Errorf("watching contract file directory: %w", err)
	}
	go f.watch(ctx, watcher)
	return nil
}

func (f *WatchlistFilter) watch(ctx context.Context, watcher *fsnotify.Watcher) {
	defer close(f.errs)
	defer watcher.Close()

	base := filepath.Base(f.path)
	// pending fires once the file has been quiet for the debounce period.
	// It is created stopped and only armed by a relevant event.
	pending := time.NewTimer(time.Hour)
	pending.Stop()
	defer pending.Stop()

	reload := func() {
		if _, err := f.Reload(); err != nil {
			f.report(err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if filepath.Base(event.Name) != base || event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Chmod) == 0 {
				continue
			}
			if f.debounce <= 0 {
				reload()
				continue
			}
			pending.Reset(f.debounce)
		case <-pending.C:
			reload()
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			f.report(fmt.Errorf("watching contract file: %w", err))
		}
	}
}

// report delivers err without ever blocking the watcher: a caller that is not
// draining Errors loses reports, not reloads.
func (f *WatchlistFilter) report(err error) {
	select {
	case f.errs <- err:
	default:
	}
}

func readWatchlist(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("reading contract watchlist %q: %w", path, err)
	}
	defer file.Close()
	seen := map[string]struct{}{}
	var ids []string
	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		value := strings.TrimSpace(scanner.Text())
		if comment := strings.IndexByte(value, '#'); comment >= 0 {
			value = strings.TrimSpace(value[:comment])
		}
		if value == "" {
			continue
		}
		if _, err := strkey.Decode(strkey.VersionByteContract, value); err != nil {
			return nil, fmt.Errorf("contract watchlist %q line %d has an invalid contract ID %q: %w", path, lineNumber, value, err)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		ids = append(ids, value)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading contract watchlist %q: %w", path, err)
	}
	return ids, nil
}
