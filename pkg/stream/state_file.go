package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"
	"unicode"
)

// StateVersion is the on-disk schema version of a single-stream state file.
// A reader refuses any version it does not know rather than guessing at
// unknown fields, so a file written by a newer prism fails loudly instead of
// resuming from a misinterpreted position.
const StateVersion = 1

// PartitionedStateVersion is the on-disk schema version of a state file
// shared by several streams: one State per stream ID, all in one document.
const PartitionedStateVersion = 2

// LegacyStreamID is the partition a single-stream (version 1) state file is
// kept under when a partitioned writer for a different stream upgrades the
// file, so the original stream's position is preserved rather than dropped.
const LegacyStreamID = "default"

// maxStreamIDLength bounds a stream ID. IDs are written into the file as map
// keys and printed in diagnostics, so they stay short and printable.
const maxStreamIDLength = 256

// DefaultStateRetention is how long checkpoint history is kept when no other
// retention is configured.
const DefaultStateRetention = 7 * 24 * time.Hour

// DefaultStateCheckpointInterval is the minimum spacing between two history
// checkpoints of one stream.
//
// The tailer saves after every record, so recording each save would grow the
// history by one entry per record. One checkpoint an hour keeps a week of
// audit trail under two hundred entries, which keeps every save cheap.
const DefaultStateCheckpointInterval = time.Hour

// DefaultStateCompactInterval is how often history older than the retention
// window is pruned, both during saves and by the background compactor.
const DefaultStateCompactInterval = 6 * time.Hour

// Checkpoint is one entry in a stream's position history: where the stream
// had reached at RecordedAt. The history is an audit trail; the position a
// run resumes from is always the State's own fields, never a checkpoint.
type Checkpoint struct {
	// Ledger is the sequence the stream had reached.
	Ledger uint32 `json:"ledger"`
	// PagingToken is the resume cursor at that point.
	PagingToken string `json:"paging_token,omitempty"`
	// LastID is the de-duplication high-water mark at that point.
	LastID string `json:"last_id,omitempty"`
	// RecordedAt is when the checkpoint was taken. Retention is measured
	// against it.
	RecordedAt time.Time `json:"recorded_at"`
}

// State is the durable position of a stream: everything needed to resume
// where a previous run stopped, plus a bounded audit trail of where it has
// been. It is stored as a small JSON document so that a human can read it and
// a diff can explain it.
type State struct {
	// Version is the schema version; written as StateVersion.
	Version int `json:"version"`
	// Source names the stream that wrote the state ("events", "ledgers",
	// ...). A position from one stream is meaningless to another, so it is
	// recorded and checked rather than assumed.
	Source string `json:"source"`
	// Ledger is the sequence of the last successfully emitted record.
	Ledger uint32 `json:"ledger"`
	// PagingToken resumes the stream immediately after the last emitted
	// record. It is the server's own cursor format: the emitted record's ID
	// for events, refined to the batch cursor once a page has drained.
	PagingToken string `json:"paging_token"`
	// Cursor is an alias for PagingToken.
	Cursor string `json:"cursor,omitempty"`
	// LastID is the highest Record.ID emitted, so the tailer's
	// de-duplication high-water mark survives a restart and any replayed
	// page is still suppressed.
	LastID string `json:"last_id"`
	// UpdatedAt is when the position was last persisted.
	UpdatedAt time.Time `json:"updated_at"`
	// History is the checkpoint journal, oldest first. StateFile.Save
	// maintains it: a caller that leaves it nil keeps the persisted history
	// and has a checkpoint appended at most once per checkpoint interval.
	// PruneStateHistory bounds it.
	History []Checkpoint `json:"history,omitempty"`
}

// partitionedState is the on-disk form of a shared state file.
type partitionedState struct {
	Version int              `json:"version"`
	Streams map[string]State `json:"streams"`
}

// StateFileOptions configures a StateFile.
type StateFileOptions struct {
	// StreamID selects this stream's partition in a shared state file.
	// Empty keeps the single-stream (version 1) layout, which is what a
	// state file written by an older prism uses.
	//
	// With a stream ID the file is a map of stream ID to State, and every
	// read-modify-write is serialised across processes by an exclusive
	// lock on <path>.lock, so several prism processes can share one file
	// without any of them overwriting another's cursor.
	StreamID string
	// Retention prunes history checkpoints older than this; the newest
	// checkpoint is always kept. Zero keeps the whole history.
	Retention time.Duration
	// CheckpointInterval is the minimum spacing between two history
	// checkpoints. Zero uses DefaultStateCheckpointInterval.
	CheckpointInterval time.Duration
	// CompactInterval is how often a save also prunes the history of every
	// stream in the file. Zero uses DefaultStateCompactInterval.
	CompactInterval time.Duration
	// Clock is injectable for tests. Defaults to RealClock.
	Clock Clock
}

// StateFile persists stream positions as a JSON file, replacing it
// atomically.
//
// Each Save writes a complete snapshot to a temporary file in the
// destination directory, fsyncs it, and renames it over the destination, so
// a concurrent reader only ever observes a complete snapshot — either the
// previous state or the new one, never a torn file. If a crash lands between
// the data sync and the rename, the destination reverts to the previous
// position; that can only re-emit records on the next run, never skip them,
// so the at-least-once delivery guarantee holds unconditionally.
//
// A partitioned StateFile (one opened with a stream ID) additionally holds an
// exclusive cross-process lock on <path>.lock for the duration of each
// read-modify-write, so that two processes saving different streams cannot
// both read the old document and have the second rename discard the first
// one's update.
//
// A StateFile holds no open file descriptors between operations and spawns
// no goroutines unless StartCompaction is called, so there is nothing to
// clean up when a run terminates.
type StateFile struct {
	path string
	opts StateFileOptions

	// mu serialises writes within this process; the file lock serialises
	// them across processes.
	mu          sync.Mutex
	lastCompact time.Time
}

// OpenStateFile prepares path for use as a single-stream position file,
// creating the parent directory if needed. History is kept for
// DefaultStateRetention.
//
// An existing file is read and validated immediately, so a corrupt or
// version-incompatible state file fails here — before any streaming or RPC
// work starts — rather than in the middle of a run. A missing file is not an
// error: it simply means nothing has been persisted yet.
func OpenStateFile(path string) (*StateFile, error) {
	return OpenStateFileWithOptions(path, StateFileOptions{Retention: DefaultStateRetention})
}

// OpenStateFileWithOptions prepares path as a state file configured by opts.
// See OpenStateFile.
func OpenStateFileWithOptions(path string, opts StateFileOptions) (*StateFile, error) {
	if path == "" {
		return nil, errors.New("state file path is empty")
	}
	if opts.StreamID != "" {
		if err := ValidateStreamID(opts.StreamID); err != nil {
			return nil, err
		}
	}
	if opts.Retention < 0 {
		opts.Retention = 0
	}
	if opts.CheckpointInterval <= 0 {
		opts.CheckpointInterval = DefaultStateCheckpointInterval
	}
	if opts.CompactInterval <= 0 {
		opts.CompactInterval = DefaultStateCompactInterval
	}
	if opts.Clock == nil {
		opts.Clock = RealClock{}
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating state file directory %q: %w", dir, err)
	}
	s := &StateFile{path: path, opts: opts}
	if s.partitioned() {
		// Taking the lock once up front proves the platform and the file
		// system support it, so an unusable setup fails before streaming.
		if err := s.withFileLock(func() error { return nil }); err != nil {
			return nil, err
		}
	}
	if _, err := s.Load(); err != nil {
		return nil, err
	}
	return s, nil
}

// ValidateStreamID reports whether id can name a partition of a shared
// state file.
func ValidateStreamID(id string) error {
	if id == "" {
		return errors.New("stream ID is empty")
	}
	if len(id) > maxStreamIDLength {
		return fmt.Errorf("stream ID is %d bytes long; the limit is %d", len(id), maxStreamIDLength)
	}
	for _, r := range id {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return fmt.Errorf("stream ID %q contains whitespace or a control character", id)
		}
	}
	return nil
}

// Path reports the file this StateFile persists to.
func (s *StateFile) Path() string { return s.path }

// StreamID reports the partition this StateFile reads and writes, or "" for
// a single-stream file.
func (s *StateFile) StreamID() string { return s.opts.StreamID }

func (s *StateFile) partitioned() bool { return s.opts.StreamID != "" }

func (s *StateFile) lockPath() string { return s.path + ".lock" }

// withFileLock runs fn holding the cross-process lock of a partitioned file.
// A single-stream file is only ever written by one process, so it takes no
// file lock and leaves nothing beside the state file.
func (s *StateFile) withFileLock(fn func() error) (err error) {
	if !s.partitioned() {
		return fn()
	}
	f, err := os.OpenFile(s.lockPath(), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("opening state lock %q: %w", s.lockPath(), err)
	}
	defer func() {
		err = errors.Join(err, unlockFile(f), f.Close())
	}()
	if err := lockFile(f, true, true); err != nil {
		return fmt.Errorf("locking shared state file %q: %w", s.path, err)
	}
	return fn()
}

// stateDocument is a decoded state file of either layout.
type stateDocument struct {
	// partitioned is true for a version 2 file.
	partitioned bool
	// single is the content of a version 1 file.
	single State
	// streams is the content of a version 2 file.
	streams map[string]State
}

// readDocument reads and decodes the file. A missing or empty file decodes
// as an empty single-stream document.
func (s *StateFile) readDocument() (stateDocument, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return stateDocument{}, nil
	}
	if err != nil {
		return stateDocument{}, fmt.Errorf("reading state file %q: %w", s.path, err)
	}
	return decodeStateDocument(s.path, data)
}

func decodeStateDocument(path string, data []byte) (stateDocument, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return stateDocument{}, nil
	}
	var head struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return stateDocument{}, fmt.Errorf("state file %q is not valid JSON: %w", path, err)
	}
	switch head.Version {
	case StateVersion:
		var st State
		if err := json.Unmarshal(data, &st); err != nil {
			return stateDocument{}, fmt.Errorf("state file %q is not valid JSON: %w", path, err)
		}
		return stateDocument{single: st}, nil
	case PartitionedStateVersion:
		var ps partitionedState
		if err := json.Unmarshal(data, &ps); err != nil {
			return stateDocument{}, fmt.Errorf("state file %q is not valid JSON: %w", path, err)
		}
		if ps.Streams == nil {
			ps.Streams = map[string]State{}
		}
		return stateDocument{partitioned: true, streams: ps.Streams}, nil
	default:
		return stateDocument{}, fmt.Errorf(
			"state file %q has schema version %d, but this build reads versions %d and %d",
			path, head.Version, StateVersion, PartitionedStateVersion)
	}
}

// errPartitionedNeedsStreamID is returned when a single-stream reader meets
// a shared file.
func errPartitionedNeedsStreamID(path string) error {
	return fmt.Errorf(
		"state file %q is shared by several streams (schema version %d); select a stream with --stream-id",
		path, PartitionedStateVersion)
}

// entry returns this StateFile's stream from doc.
//
// A partitioned reader that meets a version 1 file adopts its single state:
// that is how an existing state file keeps its position when prism starts
// using stream IDs. The tailer still checks the State's Source, so a
// position recorded by a different kind of stream is refused, not misapplied.
func (s *StateFile) entry(doc stateDocument) (State, error) {
	if !s.partitioned() {
		if doc.partitioned {
			return State{}, errPartitionedNeedsStreamID(s.path)
		}
		return doc.single, nil
	}
	if doc.partitioned {
		return doc.streams[s.opts.StreamID], nil
	}
	return doc.single, nil
}

// normalize fills the PagingToken/Cursor aliases from each other and stamps
// the version a reader expects.
func normalize(st State) State {
	if st.PagingToken == "" && st.Cursor != "" {
		st.PagingToken = st.Cursor
	}
	if st.Cursor == "" && st.PagingToken != "" {
		st.Cursor = st.PagingToken
	}
	if st.Source != "" || st.Ledger != 0 || st.PagingToken != "" {
		st.Version = StateVersion
	}
	return st
}

// Load reads the persisted state of this stream.
//
// A missing or empty file, or a shared file with no partition for this
// stream, yields the zero State, which the tailer reads as "nothing
// persisted yet": the stream starts from wherever it was configured to
// start. A file that exists but cannot be read, parsed or understood is
// reported as an error wrapping the underlying cause — silently starting
// over would re-emit the whole stream, which is exactly what durable cursors
// exist to prevent.
func (s *StateFile) Load() (State, error) {
	doc, err := s.readDocument()
	if err != nil {
		return State{}, err
	}
	st, err := s.entry(doc)
	if err != nil {
		return State{}, err
	}
	return normalize(st), nil
}

// StreamIDs lists the partitions of the file, sorted. A single-stream file
// reports none.
func (s *StateFile) StreamIDs() ([]string, error) {
	ids, _, err := StateFileStreams(s.path)
	return ids, err
}

// StateFileStreams reports whether the state file at path is shared by
// several streams and, if so, which stream IDs it holds, sorted. A missing
// file is neither shared nor an error.
func StateFileStreams(path string) (ids []string, partitioned bool, err error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("reading state file %q: %w", path, err)
	}
	doc, err := decodeStateDocument(path, data)
	if err != nil {
		return nil, false, err
	}
	if !doc.partitioned {
		return nil, false, nil
	}
	for id := range doc.streams {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, true, nil
}

// Save atomically replaces this stream's persisted state.
//
// It is called by the tailer after each successfully emitted record, which
// is what narrows the restart window from "the whole run" to "one record":
// the file can never run ahead of the stream, because it is only written
// once the record it describes has been handed to the sink.
//
// In a shared file only this stream's partition changes; every other
// stream's state is carried over exactly as it was read under the lock.
func (s *StateFile) Save(st State) error {
	if st.Source == "" {
		return errors.New("stream state must record which stream it belongs to")
	}
	st.Version = StateVersion

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.withFileLock(func() error {
		doc, err := s.readDocument()
		if err != nil {
			if s.partitioned() {
				// Rewriting a shared file that cannot be read would
				// discard every other stream's cursor.
				return err
			}
			// A single-stream file is this stream's alone: the only thing
			// lost by not reading it is the history, which is better than
			// failing to record the position.
			doc = stateDocument{}
		}
		prev, err := s.entry(doc)
		if err != nil {
			return err
		}
		now := s.opts.Clock.Now()
		st.History = s.mergeHistory(prev, st, now)
		compact := s.compactDue(now)
		if compact {
			s.pruneState(&st, now)
		}
		data, err := s.encode(doc, st, compact, now)
		if err != nil {
			return err
		}
		if err := s.writeAtomic(data, ".tmp*"); err != nil {
			return err
		}
		if compact {
			s.lastCompact = now
		}
		return nil
	})
}

// encode renders the document that results from replacing this stream's
// partition with st.
func (s *StateFile) encode(doc stateDocument, st State, compactAll bool, now time.Time) ([]byte, error) {
	if !s.partitioned() {
		data, err := json.Marshal(st)
		if err != nil {
			return nil, fmt.Errorf("encoding stream state: %w", err)
		}
		return data, nil
	}
	streams := make(map[string]State, len(doc.streams)+1)
	if doc.partitioned {
		for id, other := range doc.streams {
			streams[id] = other
		}
	} else if doc.single.Source != "" && doc.single.Source != st.Source {
		// Upgrading a single-stream file that belongs to a different
		// stream: keep its position under its own partition.
		streams[LegacyStreamID] = doc.single
	}
	streams[s.opts.StreamID] = st
	if compactAll {
		for id, other := range streams {
			s.pruneState(&other, now)
			streams[id] = other
		}
	}
	data, err := json.Marshal(partitionedState{Version: PartitionedStateVersion, Streams: streams})
	if err != nil {
		return nil, fmt.Errorf("encoding stream state: %w", err)
	}
	return data, nil
}

// mergeHistory returns the history to persist with st: the caller's own if
// it set one, otherwise the persisted history plus a checkpoint for st when
// the last checkpoint is at least one checkpoint interval old.
func (s *StateFile) mergeHistory(prev, st State, now time.Time) []Checkpoint {
	if st.History != nil {
		return st.History
	}
	history := slices.Clone(prev.History)
	token := st.PagingToken
	if token == "" {
		token = st.Cursor
	}
	if token == "" && st.Ledger == 0 {
		return history
	}
	at := st.UpdatedAt
	if at.IsZero() {
		at = now
	}
	if n := len(history); n > 0 {
		last := history[n-1]
		if last.PagingToken == token && last.Ledger == st.Ledger {
			return history
		}
		if at.Sub(last.RecordedAt) < s.opts.CheckpointInterval {
			return history
		}
	}
	return append(history, Checkpoint{
		Ledger:      st.Ledger,
		PagingToken: token,
		LastID:      st.LastID,
		RecordedAt:  at.UTC(),
	})
}

// compactDue reports whether a save at now should also prune history.
func (s *StateFile) compactDue(now time.Time) bool {
	if s.opts.Retention <= 0 {
		return false
	}
	return s.lastCompact.IsZero() || now.Sub(s.lastCompact) >= s.opts.CompactInterval
}

// pruneState prunes st's history by the configured retention.
func (s *StateFile) pruneState(st *State, now time.Time) int {
	if s.opts.Retention <= 0 {
		return 0
	}
	return PruneStateHistoryAt(st, s.opts.Retention, now)
}

// Compact prunes the history of every stream in the file now, whatever the
// compaction schedule, and reports how many checkpoints it removed. Nothing
// is written when nothing was removed.
func (s *StateFile) Compact() (int, error) {
	if s.opts.Retention <= 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	err := s.withFileLock(func() error {
		doc, err := s.readDocument()
		if err != nil {
			return err
		}
		now := s.opts.Clock.Now()
		var data []byte
		switch {
		case doc.partitioned:
			for id, st := range doc.streams {
				removed += s.pruneState(&st, now)
				doc.streams[id] = st
			}
			if removed > 0 {
				data, err = json.Marshal(partitionedState{Version: PartitionedStateVersion, Streams: doc.streams})
			}
		case doc.single.Source != "":
			if s.partitioned() {
				// The file has not been upgraded yet; the next Save will
				// do that, under the same rules.
				return nil
			}
			removed = s.pruneState(&doc.single, now)
			if removed > 0 {
				data, err = json.Marshal(doc.single)
			}
		}
		if err != nil {
			return fmt.Errorf("encoding stream state: %w", err)
		}
		s.lastCompact = now
		if data == nil {
			return nil
		}
		return s.writeAtomic(data, ".compact-*")
	})
	return removed, err
}

// StartCompaction prunes the file's history in a background goroutine every
// interval (DefaultStateCompactInterval when zero) until ctx is cancelled or
// the returned stop function is called. stop waits for the goroutine, so a
// terminated stream leaves nothing running behind it. onError, when set, is
// told about a pass that failed; the loop keeps going.
func (s *StateFile) StartCompaction(ctx context.Context, interval time.Duration, onError func(error)) (stop func()) {
	if interval <= 0 {
		interval = DefaultStateCompactInterval
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if err := s.opts.Clock.Sleep(ctx, interval); err != nil {
				return
			}
			if _, err := s.Compact(); err != nil && onError != nil {
				onError(err)
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
}

// Rewind atomically replaces this stream's saved position with target and
// keeps a timestamped backup of the previous complete file. Cursor and
// high-water identifiers are cleared so replayed records are not
// de-duplicated away. Other streams in a shared file are left untouched.
func (s *StateFile) Rewind(target uint32) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var backup string
	err := s.withFileLock(func() error {
		data, err := os.ReadFile(s.path)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("reading state backup: %w", err)
		}
		doc, err := decodeStateDocument(s.path, data)
		if err != nil {
			return err
		}
		current, err := s.entry(doc)
		if err != nil {
			return err
		}
		if current.Source == "" {
			return errors.New("cannot rewind an empty state file")
		}
		backup = fmt.Sprintf("%s.backup-%d", s.path, time.Now().UTC().UnixNano())
		if err := os.WriteFile(backup, data, 0o600); err != nil {
			return fmt.Errorf("writing state backup: %w", err)
		}
		current.Ledger, current.PagingToken, current.Cursor, current.LastID = target, "", "", ""
		current.UpdatedAt = time.Now().UTC()
		current.Version = StateVersion
		if current.History == nil {
			current.History = []Checkpoint{}
		}
		encoded, err := s.encode(doc, current, false, time.Time{})
		if err != nil {
			return err
		}
		return s.writeAtomic(encoded, ".rewind-*")
	})
	if err != nil {
		return "", err
	}
	return backup, nil
}

// writeAtomic replaces the file with data through a synced temporary file
// and a rename. The caller holds mu and, for a shared file, the file lock.
func (s *StateFile) writeAtomic(data []byte, pattern string) error {
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, filepath.Base(s.path)+pattern)
	if err != nil {
		return fmt.Errorf("creating temporary file next to %q: %w", s.path, err)
	}
	tmpName := tmp.Name()
	// Runs only when the rename below did not consume the temporary file,
	// i.e. when the write is already failing. A leftover temp file would be
	// harmless, but removing it keeps the directory honest.
	defer func() {
		if tmpName != "" {
			os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("writing state to %q: %w", tmpName, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("syncing state to %q: %w", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing state file %q: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("replacing state file %q: %w", s.path, err)
	}
	tmpName = ""
	return nil
}
