package sink

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// RotatorOptions configures a RotatingWriter. The zero value rotates only on
// an explicit Rotate call and keeps every backup.
type RotatorOptions struct {
	// MaxSize rotates the active file before a write would push it past this
	// many bytes. Zero disables size-based rotation. A single write larger
	// than MaxSize is still written whole rather than split.
	MaxSize int64
	// MaxAge rotates the active file once it has been open (or, for a file
	// left by a previous run, since it was last modified) for this long. Zero
	// disables time-based rotation.
	MaxAge time.Duration
	// Daily rotates when the UTC date changes, so one file holds one day of
	// events. Archives are named <path>.<YYYY-MM-DD>.log instead of with a
	// timestamp, which is what a daily log directory expects.
	Daily bool
	// MaxBackups is how many rotated files to keep, oldest first. Zero keeps
	// all of them; it does not mean "delete everything".
	MaxBackups int
	// Compress gzips each rotated file. By default compression happens
	// synchronously as part of rotation, so a crash cannot leave a
	// half-written archive next to a file that looks complete.
	Compress bool
	// AsyncCompress moves gzip work to a background goroutine, keeping the
	// write path free when a rotation happens during a burst. Close waits for
	// pending compressions, so no archive is lost on shutdown.
	AsyncCompress bool
	// Now is the clock used for timestamps and age checks. Tests set it; nil
	// means time.Now.
	Now func() time.Time
}

// RotatingWriter is an append-only file writer that rotates at a size or age
// boundary. Rotated files are named "<path>.<UTC timestamp>" and, when
// compression is enabled, end in ".gz".
//
// Writes are atomic with respect to rotation but not to record boundaries: a
// rotation may occur between two writes while a sink is emitting one record.
// Keeping whole records in one file would require buffering each record,
// which for a large ledger payload would trade the very memory bound rotation
// exists to provide.
//
// See docs/sinks/file_rotation_and_syslog.md for CLI rotation flags and
// external logrotate configurations.
type RotatingWriter struct {
	path string
	opts RotatorOptions
	now  func() time.Time

	mu       sync.Mutex
	file     *os.File
	size     int64
	openedAt time.Time
	closed   bool

	// compressMu serialises background compression and pruning, so a prune
	// can never delete an archive that is still being written.
	compressMu sync.Mutex
	wg         sync.WaitGroup
	asyncErr   error
}

// NewRotatingWriter opens or creates path for appending.
//
// An existing file's size and modification time seed the rotation state, so
// restarting a stream continues the size policy and an overdue time rotation
// happens on the first write rather than being lost.
func NewRotatingWriter(path string, opts RotatorOptions) (*RotatingWriter, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("rotating writer: path is required")
	}
	if opts.MaxSize < 0 {
		return nil, fmt.Errorf("rotating writer: MaxSize must not be negative")
	}
	if opts.MaxBackups < 0 {
		return nil, fmt.Errorf("rotating writer: MaxBackups must not be negative")
	}

	now := opts.Now
	if now == nil {
		now = time.Now
	}

	w := &RotatingWriter{
		path: path,
		opts: opts,
		now:  now,
	}
	if err := w.open(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *RotatingWriter) open() error {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("rotating writer: open %s: %w", w.path, err)
	}

	info, err := f.Stat()
	if err != nil {
		closeErr := f.Close()
		return errors.Join(fmt.Errorf("rotating writer: stat %s: %w", w.path, err), closeErr)
	}

	w.file = f
	w.size = info.Size()
	w.openedAt = info.ModTime()
	// A file this writer just created starts its first period now. A file left
	// by a previous run keeps its modification time, so an overdue rotation
	// still happens on the first write.
	if w.size == 0 || w.openedAt.IsZero() {
		w.openedAt = w.now()
	}
	return nil
}

// Write implements io.Writer. It rotates first if this write would cross the
// size boundary or the time boundary has passed.
func (w *RotatingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, fmt.Errorf("rotating writer: write after close: %w", os.ErrClosed)
	}
	if err := w.rotateIfNeededLocked(int64(len(p))); err != nil {
		return 0, err
	}

	n, err := w.file.Write(p)
	w.size += int64(n)
	if err != nil {
		return n, fmt.Errorf("rotating writer: write %s: %w", w.path, err)
	}
	return n, nil
}

// RotateIfNeeded rotates when the age boundary has passed or when next more
// bytes would exceed the size bound.
func (w *RotatingWriter) RotateIfNeeded(next int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("rotating writer: rotate after close: %w", os.ErrClosed)
	}
	return w.rotateIfNeededLocked(int64(next))
}

func (w *RotatingWriter) rotateIfNeededLocked(next int64) error {
	// An empty file has nothing worth archiving; wait for its first write so
	// a short-lived process cannot litter the directory with empty backups.
	if w.size == 0 {
		return nil
	}
	if w.opts.MaxSize > 0 && w.size+next > w.opts.MaxSize {
		return w.rotateLocked()
	}
	if w.opts.Daily && !sameUTCDate(w.openedAt, w.now()) {
		return w.rotateLocked()
	}
	if w.opts.MaxAge > 0 && w.now().Sub(w.openedAt) >= w.opts.MaxAge {
		return w.rotateLocked()
	}
	return nil
}

// sameUTCDate reports whether two instants fall on the same UTC day.
func sameUTCDate(a, b time.Time) bool {
	aYear, aMonth, aDay := a.UTC().Date()
	bYear, bMonth, bDay := b.UTC().Date()
	return aYear == bYear && aMonth == bMonth && aDay == bDay
}

// Rotate closes the active file, archives it and opens a fresh one. It is a
// no-op for an empty file.
func (w *RotatingWriter) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("rotating writer: rotate after close: %w", os.ErrClosed)
	}
	if w.size == 0 {
		return nil
	}
	return w.rotateLocked()
}

func (w *RotatingWriter) rotateLocked() error {
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("rotating writer: sync %s: %w", w.path, err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("rotating writer: close %s: %w", w.path, err)
	}
	w.file = nil

	archivedAt := w.now().UTC()
	if w.opts.Daily {
		// A daily archive is named for the period it covers, not for the
		// instant the next day's first write happened to rotate it.
		archivedAt = w.openedAt.UTC()
	}
	backup := w.backupPath(archivedAt)
	if err := os.Rename(w.path, backup); err != nil {
		return fmt.Errorf("rotating writer: rename %s: %w", w.path, err)
	}

	// Reopen immediately: even if archiving below fails, the stream can
	// continue writing to the original path.
	if err := w.open(); err != nil {
		return err
	}

	if w.opts.Compress {
		if w.opts.AsyncCompress {
			w.wg.Add(1)
			go w.compressAsync(backup)
		} else {
			if _, err := compressFile(backup); err != nil {
				return err
			}
			if err := w.pruneLocked(); err != nil {
				return err
			}
		}
	} else if err := w.pruneLocked(); err != nil {
		return err
	}
	return nil
}

// compressAsync gzips one archive and then prunes, off the write path. Its
// errors are kept for Close, because a background failure has no caller to
// return to at the moment it happens.
func (w *RotatingWriter) compressAsync(backup string) {
	defer w.wg.Done()

	w.compressMu.Lock()
	defer w.compressMu.Unlock()

	_, err := compressFile(backup)
	if err == nil {
		err = w.pruneLocked()
	}
	if err != nil {
		w.mu.Lock()
		if w.asyncErr == nil {
			w.asyncErr = err
		}
		w.mu.Unlock()
	}
}

func (w *RotatingWriter) backupPath(t time.Time) string {
	var base string
	if w.opts.Daily {
		base = fmt.Sprintf("%s.%s.log", w.path, t.Format("2006-01-02"))
	} else {
		base = fmt.Sprintf("%s.%s", w.path, t.Format("20060102-150405.000000000"))
	}

	candidate := base
	for i := 1; ; i++ {
		if _, err := os.Stat(candidate); errors.Is(err, os.ErrNotExist) {
			return candidate
		}
		candidate = fmt.Sprintf("%s.%d", base, i)
	}
}

// pruneLocked deletes the oldest backups beyond MaxBackups.
func (w *RotatingWriter) pruneLocked() error {
	if w.opts.MaxBackups <= 0 {
		return nil
	}

	backups, err := w.backups()
	if err != nil {
		return err
	}
	if len(backups) <= w.opts.MaxBackups {
		return nil
	}

	var errs []error
	for _, backup := range backups[:len(backups)-w.opts.MaxBackups] {
		if err := os.Remove(backup); err != nil {
			errs = append(errs, fmt.Errorf("rotating writer: remove %s: %w", backup, err))
		}
	}
	return errors.Join(errs...)
}

// backups returns rotated files for this writer, oldest first.
func (w *RotatingWriter) backups() ([]string, error) {
	matches, err := filepath.Glob(w.path + ".*")
	if err != nil {
		return nil, fmt.Errorf("rotating writer: list backups: %w", err)
	}

	type backup struct {
		path    string
		modTime time.Time
	}
	found := make([]backup, 0, len(matches))
	for _, match := range matches {
		info, err := os.Stat(match)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		found = append(found, backup{path: match, modTime: info.ModTime()})
	}
	sort.Slice(found, func(i, j int) bool {
		if found[i].modTime.Equal(found[j].modTime) {
			return found[i].path < found[j].path
		}
		return found[i].modTime.Before(found[j].modTime)
	})

	paths := make([]string, len(found))
	for i, b := range found {
		paths[i] = b.path
	}
	return paths, nil
}

// Sync flushes the active file to stable storage.
func (w *RotatingWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed || w.file == nil {
		return nil
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("rotating writer: sync %s: %w", w.path, err)
	}
	return nil
}

// Close syncs and closes the active file and waits for any background
// compression to finish. It does not rotate, and is safe to call more than
// once.
func (w *RotatingWriter) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	file := w.file
	w.file = nil
	w.mu.Unlock()

	var errs []error
	if file != nil {
		errs = append(errs,
			wrapError("sync", w.path, file.Sync()),
			wrapError("close", w.path, file.Close()))
	}

	// The wait happens unlocked: a pending compressor takes w.mu to record
	// an error, and would deadlock against a Close holding it.
	w.wg.Wait()

	w.mu.Lock()
	errs = append(errs, w.asyncErr)
	w.mu.Unlock()
	return errors.Join(errs...)
}

func wrapError(action, path string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("rotating writer: %s %s: %w", action, path, err)
}

func compressFile(path string) (string, error) {
	src, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("rotating writer: open backup %s: %w", path, err)
	}

	compressed := path + ".gz"
	dst, err := os.OpenFile(compressed, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return "", fmt.Errorf("rotating writer: create %s: %w", compressed, err)
	}

	gz := gzip.NewWriter(dst)
	_, copyErr := io.Copy(gz, src)
	gzCloseErr := gz.Close()
	dstCloseErr := dst.Close()
	srcCloseErr := src.Close()

	if copyErr != nil || gzCloseErr != nil || dstCloseErr != nil || srcCloseErr != nil {
		_ = os.Remove(compressed)
		return "", errors.Join(
			wrapError("compress", path, copyErr),
			wrapError("compress", path, gzCloseErr),
			wrapError("compress", path, dstCloseErr),
			wrapError("close backup", path, srcCloseErr),
		)
	}
	if err := os.Remove(path); err != nil {
		return "", fmt.Errorf("rotating writer: remove uncompressed backup %s: %w", path, err)
	}
	return compressed, nil
}

// FileRotator is a stream.Sink that writes through a RotatingWriter.
//
// The inner sink is built on top of the rotating writer, so NDJSON stays
// NDJSON and the human view stays the human view; the rotator only decides
// which file the bytes land in.
type FileRotator struct {
	writer *RotatingWriter
	sink   stream.Sink
}

// NewFileRotator opens path for appending and builds a sink that writes to
// it.
//
// A build function is required because the rotator must not assume NDJSON:
// every existing sink should be usable with rotation by construction, not by
// reimplementing its formatting here.
func NewFileRotator(path string, build func(io.Writer) stream.Sink, opts RotatorOptions) (*FileRotator, error) {
	if build == nil {
		return nil, fmt.Errorf("file rotator: sink builder is required")
	}

	writer, err := NewRotatingWriter(path, opts)
	if err != nil {
		return nil, err
	}

	inner := build(writer)
	if inner == nil {
		_ = writer.Close()
		return nil, fmt.Errorf("file rotator: sink builder returned nil")
	}

	return &FileRotator{writer: writer, sink: inner}, nil
}

// Emit implements stream.Sink.
func (f *FileRotator) Emit(r stream.Record) error {
	return f.sink.Emit(r)
}

// Flush implements stream.Sink. It flushes the inner formatting buffer and
// syncs the active file, so records survive a crash as soon as the tailer has
// emitted them.
func (f *FileRotator) Flush() error {
	return errors.Join(f.sink.Flush(), f.writer.Sync())
}

// Close flushes and closes the active file. It is safe to call more than
// once.
func (f *FileRotator) Close() error {
	return errors.Join(f.Flush(), f.writer.Close())
}

// Path returns the active file path.
func (f *FileRotator) Path() string {
	return f.writer.path
}

// RollingFileSinkOptions configures a RollingFileSink. The zero value rotates
// only on explicit calls and keeps every archive.
type RollingFileSinkOptions struct {
	// MaxSizeMB rotates before a write would push the active file past this
	// many megabytes. Zero disables size-based rotation.
	MaxSizeMB int64
	// MaxBackups keeps the newest this many archives. Zero keeps all.
	MaxBackups int
	// Compress gzips archives in a background goroutine.
	Compress bool
	// Now is the clock used for daily rotation. Tests set it; nil means
	// time.Now.
	Now func() time.Time
}

// RollingFileSink is the file output described by the sink rotation flags: it
// rotates on a size threshold or at the UTC date transition, compresses closed
// files in the background and prunes the oldest archives.
//
// It embeds FileRotator, so the inner sink still decides the format; the only
// thing this type adds is the rotation policy an operator configures.
type RollingFileSink struct {
	*FileRotator
}

// NewRollingFileSink opens path for appending and builds a sink that rotates
// on size or date boundaries. build must return the sink that formats records
// into the writer it is given.
func NewRollingFileSink(path string, build func(io.Writer) stream.Sink, opts RollingFileSinkOptions) (*RollingFileSink, error) {
	if opts.MaxSizeMB < 0 {
		return nil, fmt.Errorf("rolling file sink: MaxSizeMB must not be negative")
	}
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("rolling file sink: path is required")
	}

	rotator, err := NewFileRotator(path, build, RotatorOptions{
		MaxSize:       opts.MaxSizeMB << 20,
		MaxBackups:    opts.MaxBackups,
		Compress:      opts.Compress,
		AsyncCompress: opts.Compress,
		Daily:         true,
		Now:           opts.Now,
	})
	if err != nil {
		return nil, err
	}
	return &RollingFileSink{FileRotator: rotator}, nil
}
