package stream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// maxMetadataLine bounds a single line of the metadata file. A checkpoint is
// a few hundred bytes, so this leaves generous headroom while keeping a
// corrupt file from exhausting memory.
const maxMetadataLine = 1 << 20

// FileMetadataStore keeps cursor metadata in a file, one JSON object per
// line.
//
// The file is a journal: checkpoints are appended as the stream advances and
// the pruner rewrites it, keeping only what is still useful. A rewrite goes
// through a temporary file and a rename, so a crash in the middle of one
// leaves the previous file intact rather than a half-written one.
//
// The store is safe for concurrent use. The stream appends checkpoints while
// the background pruner rewrites the file, and the two must not interleave.
type FileMetadataStore struct {
	path string
	mu   sync.Mutex
	// shared makes every write also hold an exclusive lock on
	// <path>.lock, so several processes can append to and prune one
	// journal without a prune's rewrite discarding another process's
	// appends.
	shared bool
}

// NewFileMetadataStore returns a store backed by the file at path.
//
// The file itself is created by the first append. The parent directory must
// already exist: creating a directory tree behind the operator's back would
// hide a typo in the path until the first write, which may be hours into a
// run.
func NewFileMetadataStore(path string) (*FileMetadataStore, error) {
	if path == "" {
		return nil, errors.New("cursor metadata: a file path is required")
	}
	dir := filepath.Dir(path)
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("cursor metadata directory %s: %w", dir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("cursor metadata directory %s is not a directory", dir)
	}
	return &FileMetadataStore{path: path}, nil
}

// NewSharedFileMetadataStore returns a store like NewFileMetadataStore whose
// writes are also serialised across processes by an exclusive lock on
// <path>.lock, for a journal several prism processes write under different
// stream keys.
func NewSharedFileMetadataStore(path string) (*FileMetadataStore, error) {
	s, err := NewFileMetadataStore(path)
	if err != nil {
		return nil, err
	}
	s.shared = true
	return s, nil
}

// Path returns the file the store writes to.
func (s *FileMetadataStore) Path() string { return s.path }

// withProcessLock runs fn holding the cross-process lock of a shared store.
// The caller holds mu.
func (s *FileMetadataStore) withProcessLock(fn func() error) (err error) {
	if !s.shared {
		return fn()
	}
	lockPath := s.path + ".lock"
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("opening cursor metadata lock %s: %w", lockPath, err)
	}
	defer func() {
		err = errors.Join(err, unlockFile(f), f.Close())
	}()
	if err := lockFile(f, true, true); err != nil {
		return fmt.Errorf("locking cursor metadata file %s: %w", s.path, err)
	}
	return fn()
}

// Append records one checkpoint.
//
// The entry is appended as a single line, so a crash can lose at most the
// line being written and never corrupts the entries before it.
func (s *FileMetadataStore) Append(ctx context.Context, meta CursorMetadata) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if meta.RecordedAt.IsZero() {
		meta.RecordedAt = time.Now().UTC()
	}

	line, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("encoding cursor metadata: %w", err)
	}
	line = append(line, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.withProcessLock(func() error {
		f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return fmt.Errorf("opening cursor metadata file %s: %w", s.path, err)
		}
		if _, err := f.Write(line); err != nil {
			closeErr := f.Close()
			return errors.Join(
				fmt.Errorf("writing cursor metadata to %s: %w", s.path, err),
				closeErr)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("closing cursor metadata file %s: %w", s.path, err)
		}
		return nil
	})
}

// Latest returns the most recent checkpoint recorded for key.
//
// A missing file is not an error: it simply holds no checkpoints.
func (s *FileMetadataStore) Latest(ctx context.Context, key string) (CursorMetadata, bool, error) {
	if err := ctx.Err(); err != nil {
		return CursorMetadata{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		latest CursorMetadata
		found  bool
	)
	err := s.forEachLine(func(_ int, line []byte) error {
		meta, ok := decodeMetadata(line)
		if !ok || meta.Key != key {
			return nil
		}
		latest = meta
		found = true
		return nil
	})
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return CursorMetadata{}, false, nil
		}
		return CursorMetadata{}, false, err
	}
	return latest, found, nil
}

// Entries returns every checkpoint recorded for key, oldest first.
//
// It reads the whole journal, so it is meant for inspection and for tests
// rather than for the streaming path.
func (s *FileMetadataStore) Entries(ctx context.Context, key string) ([]CursorMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var out []CursorMetadata
	err := s.forEachLine(func(_ int, line []byte) error {
		meta, ok := decodeMetadata(line)
		if !ok || meta.Key != key {
			return nil
		}
		out = append(out, meta)
		return nil
	})
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return out, nil
}

// Prune rewrites the file, keeping the entries the policy calls for.
//
// The newest entries of every key are kept, up to policy.Keep per key, and
// entries recorded before policy.Cutoff are dropped whatever their age limit.
// Lines that cannot be decoded are dropped too: they are unusable, and a
// rewrite is the only chance to get rid of them.
//
// The rewrite is atomic. The new content is written to a temporary file in
// the same directory and renamed over the old one, so a reader either sees
// the whole old file or the whole new one.
func (s *FileMetadataStore) Prune(ctx context.Context, policy PrunePolicy) (PruneStats, error) {
	if err := ctx.Err(); err != nil {
		return PruneStats{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var stats PruneStats
	err := s.withProcessLock(func() error {
		var err error
		stats, err = s.pruneLocked(policy)
		return err
	})
	return stats, err
}

// pruneLocked is Prune with mu (and, for a shared store, the process lock)
// held.
func (s *FileMetadataStore) pruneLocked(policy PrunePolicy) (PruneStats, error) {
	counts, err := s.countRetained(policy)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Nothing has been written yet, so there is nothing to prune.
			return PruneStats{}, nil
		}
		return PruneStats{}, err
	}

	stats := PruneStats{}
	seen := make(map[string]int, len(counts))

	err = s.rewrite(func(_ int, line []byte) (bool, error) {
		meta, ok := decodeMetadata(line)
		if !ok {
			// An unreadable line is dropped by the rewrite.
			return false, nil
		}
		if expired(meta, policy.Cutoff) {
			return false, nil
		}
		index := seen[meta.Key]
		seen[meta.Key] = index + 1
		if policy.Keep > 0 && index < counts[meta.Key]-policy.Keep {
			return false, nil
		}
		return true, nil
	}, &stats)
	if err != nil {
		return PruneStats{}, err
	}
	return stats, nil
}

// countRetained counts the entries each key keeps, ignoring the ones the
// policy drops anyway.
//
// Counting first is what lets the rewrite keep the *newest* entries without
// holding the whole journal in memory: the second pass knows how many
// entries each key has and can drop everything before the last few.
func (s *FileMetadataStore) countRetained(policy PrunePolicy) (map[string]int, error) {
	counts := make(map[string]int)
	err := s.forEachLine(func(_ int, line []byte) error {
		meta, ok := decodeMetadata(line)
		if !ok || expired(meta, policy.Cutoff) {
			return nil
		}
		counts[meta.Key]++
		return nil
	})
	if err != nil {
		return nil, err
	}
	return counts, nil
}

// rewrite replaces the file with the lines keep accepts.
//
// The counters in stats are updated as the new content is written, so a
// failure part-way through leaves the old file untouched and reports nothing
// as removed.
func (s *FileMetadataStore) rewrite(keep func(index int, line []byte) (bool, error), stats *PruneStats) error {
	mode := fs.FileMode(0o600)
	if info, err := os.Stat(s.path); err == nil {
		mode = info.Mode().Perm()
	}

	tmp, err := os.CreateTemp(filepath.Dir(s.path), filepath.Base(s.path)+".prune-*")
	if err != nil {
		return fmt.Errorf("creating temporary cursor metadata file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		// A no-op once the rename succeeded; the point is to clean up
		// after a failure.
		_ = os.Remove(tmpName)
	}()

	buf := bufio.NewWriter(tmp)
	kept, removed := 0, 0
	err = s.forEachLine(func(index int, line []byte) error {
		ok, keepErr := keep(index, line)
		if keepErr != nil {
			return keepErr
		}
		if !ok {
			removed++
			return nil
		}
		if _, writeErr := buf.Write(line); writeErr != nil {
			return writeErr
		}
		if writeErr := buf.WriteByte('\n'); writeErr != nil {
			return writeErr
		}
		kept++
		return nil
	})
	if err != nil {
		closeErr := tmp.Close()
		return errors.Join(err, closeErr)
	}

	if err := buf.Flush(); err != nil {
		closeErr := tmp.Close()
		return errors.Join(fmt.Errorf("writing temporary cursor metadata file: %w", err), closeErr)
	}
	if err := tmp.Sync(); err != nil {
		closeErr := tmp.Close()
		return errors.Join(fmt.Errorf("syncing temporary cursor metadata file: %w", err), closeErr)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temporary cursor metadata file: %w", err)
	}
	if err := os.Chmod(tmpName, mode); err != nil {
		return fmt.Errorf("setting permissions on temporary cursor metadata file: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("replacing cursor metadata file %s: %w", s.path, err)
	}

	stats.Kept = kept
	stats.Removed = removed
	return nil
}

// forEachLine calls fn for every non-empty line of the file.
//
// A missing file is reported as fs.ErrNotExist so callers can tell "nothing
// has been written yet" from "the file cannot be read".
func (s *FileMetadataStore) forEachLine(fn func(index int, line []byte) error) error {
	f, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("cursor metadata file %s: %w", s.path, err)
		}
		return fmt.Errorf("opening cursor metadata file %s: %w", s.path, err)
	}
	defer func() {
		_ = f.Close()
	}()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, bufio.MaxScanTokenSize), maxMetadataLine)
	index := 0
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		if err := fn(index, line); err != nil {
			return fmt.Errorf("reading cursor metadata file %s: %w", s.path, err)
		}
		index++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading cursor metadata file %s: %w", s.path, err)
	}
	return nil
}

// decodeMetadata parses one line of the journal.
//
// A line that does not parse is reported as unusable rather than as an
// error: a half-written line from a crashed process must not stop the
// pruner from compacting the rest of the file.
func decodeMetadata(line []byte) (CursorMetadata, bool) {
	var meta CursorMetadata
	if err := json.Unmarshal(line, &meta); err != nil {
		return CursorMetadata{}, false
	}
	return meta, true
}

// expired reports whether a checkpoint is older than the cutoff.
//
// A checkpoint with no timestamp is never expired: its age is unknown, and
// deleting data whose age cannot be established is the wrong direction for
// an error.
func expired(meta CursorMetadata, cutoff time.Time) bool {
	if cutoff.IsZero() || meta.RecordedAt.IsZero() {
		return false
	}
	return meta.RecordedAt.Before(cutoff)
}
