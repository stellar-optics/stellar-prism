package sink_test

import (
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return string(b)
}

func backups(t *testing.T, path string) []string {
	t.Helper()
	matches, err := filepath.Glob(path + ".*")
	if err != nil {
		t.Fatalf("globbing backups: %v", err)
	}
	return matches
}

func TestRotatingWriterRotatesAtSize(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "stream.ndjson")
	w, err := sink.NewRotatingWriter(path, sink.RotatorOptions{MaxSize: 10})
	if err != nil {
		t.Fatalf("NewRotatingWriter() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	for _, chunk := range []string{"12345", "67890", "abcde", "fghij"} {
		if _, err := w.Write([]byte(chunk)); err != nil {
			t.Fatalf("Write(%q) error = %v", chunk, err)
		}
	}

	if got := readFile(t, path); got != "abcdefghij" {
		t.Errorf("active file = %q, want the current generation", got)
	}
	found := backups(t, path)
	if len(found) != 1 {
		t.Fatalf("got %d backups, want 1", len(found))
	}
	if got := readFile(t, found[0]); got != "1234567890" {
		t.Errorf("first backup = %q, want the rotated generation", got)
	}
}

func TestRotatingWriterKeepsSingleOversizedWriteWhole(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "stream.ndjson")
	w, err := sink.NewRotatingWriter(path, sink.RotatorOptions{MaxSize: 4})
	if err != nil {
		t.Fatalf("NewRotatingWriter() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	if _, err := w.Write([]byte("a-very-long-record")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got := readFile(t, path); got != "a-very-long-record" {
		t.Errorf("active file = %q, want the oversized write intact", got)
	}
	if got := len(backups(t, path)); got != 0 {
		t.Errorf("got %d backups after the first write, want 0", got)
	}
}

func TestRotatingWriterRotatesOnAge(t *testing.T) {
	t.Parallel()

	now := time.Now()
	path := filepath.Join(t.TempDir(), "stream.ndjson")
	w, err := sink.NewRotatingWriter(path, sink.RotatorOptions{
		MaxAge: time.Hour,
		Now:    func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewRotatingWriter() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	if _, err := w.Write([]byte("before")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	now = now.Add(2 * time.Hour)
	if err := w.RotateIfNeeded(0); err != nil {
		t.Fatalf("RotateIfNeeded() error = %v", err)
	}
	if got := readFile(t, path); got != "" {
		t.Errorf("active file = %q after time rotation, want empty", got)
	}
	if got := len(backups(t, path)); got != 1 {
		t.Errorf("got %d backups, want 1", got)
	}

	if _, err := w.Write([]byte("after")); err != nil {
		t.Fatalf("Write() after rotation error = %v", err)
	}
	if got := readFile(t, path); got != "after" {
		t.Errorf("active file = %q, want the post-rotation write", got)
	}
}

func TestRotatingWriterCompresses(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "stream.ndjson")
	w, err := sink.NewRotatingWriter(path, sink.RotatorOptions{
		MaxSize:  5,
		Compress: true,
	})
	if err != nil {
		t.Fatalf("NewRotatingWriter() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	if _, err := w.Write([]byte("first")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := w.Write([]byte("second")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	var compressed string
	for _, backup := range backups(t, path) {
		if strings.HasSuffix(backup, ".gz") {
			compressed = backup
		}
	}
	if compressed == "" {
		t.Fatalf("no .gz backup found among %v", backups(t, path))
	}

	f, err := os.Open(compressed)
	if err != nil {
		t.Fatalf("open compressed backup: %v", err)
	}
	defer func() { _ = f.Close() }()

	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip.NewReader() error = %v", err)
	}
	defer func() { _ = zr.Close() }()

	content, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("reading gzip: %v", err)
	}
	if got := string(content); got != "first" {
		t.Errorf("compressed backup = %q, want %q", got, "first")
	}
}

func TestRotatingWriterKeepsMaxBackups(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "stream.ndjson")
	w, err := sink.NewRotatingWriter(path, sink.RotatorOptions{
		MaxSize:    1,
		MaxBackups: 2,
		Now: func() time.Time {
			// Distinct timestamps keep filenames unique even when rotation
			// happens faster than the clock's resolution.
			return time.Now().Add(time.Duration(len(backupsFor(path))) * time.Second)
		},
	})
	if err != nil {
		t.Fatalf("NewRotatingWriter() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	for _, chunk := range []string{"a", "b", "c", "d", "e"} {
		if _, err := w.Write([]byte(chunk)); err != nil {
			t.Fatalf("Write(%q) error = %v", chunk, err)
		}
	}

	if got := len(backups(t, path)); got != 2 {
		t.Fatalf("got %d backups, want 2", got)
	}
}

func backupsFor(path string) []string {
	matches, _ := filepath.Glob(path + ".*")
	return matches
}

func TestRotatingWriterRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	if _, err := sink.NewRotatingWriter("", sink.RotatorOptions{}); err == nil {
		t.Error("empty path accepted")
	}
	if _, err := sink.NewRotatingWriter(filepath.Join(t.TempDir(), "x"), sink.RotatorOptions{MaxSize: -1}); err == nil {
		t.Error("negative MaxSize accepted")
	}
	if _, err := sink.NewRotatingWriter(filepath.Join(t.TempDir(), "x"), sink.RotatorOptions{MaxBackups: -1}); err == nil {
		t.Error("negative MaxBackups accepted")
	}
}

func TestFileRotatorWritesNDJSONAcrossRotations(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "events.ndjson")
	rotator, err := sink.NewFileRotator(path, func(w io.Writer) stream.Sink {
		return sink.NewNDJSON(w, false)
	}, sink.RotatorOptions{MaxSize: 1})
	if err != nil {
		t.Fatalf("NewFileRotator() error = %v", err)
	}

	for i := range 3 {
		rec := stream.Record{
			ID:       "0000000001-0001",
			Sequence: 1,
			Kind:     stream.KindEvent,
			Event:    &stream.Event{Type: "contract", ValueXDR: "AAAAAwAAAAc="},
		}
		if err := rotator.Emit(rec); err != nil {
			t.Fatalf("Emit(%d) error = %v", i, err)
		}
	}
	if err := rotator.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := rotator.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want idempotence", err)
	}

	files := append([]string{path}, backups(t, path)...)
	lines := 0
	for _, file := range files {
		content := readFile(t, file)
		lines += len(strings.Split(strings.TrimSpace(content), "\n"))
		if strings.Contains(content, "\n\n") {
			t.Errorf("%s contains a blank line", file)
		}
	}
	if lines != 3 {
		t.Errorf("found %d records across %d files, want 3", lines, len(files))
	}
}

func TestFileRotatorRequiresBuilder(t *testing.T) {
	t.Parallel()

	if _, err := sink.NewFileRotator(filepath.Join(t.TempDir(), "x"), nil, sink.RotatorOptions{}); err == nil {
		t.Error("nil builder accepted")
	}
}

func TestFileRotatorAppendsCompressedRecords(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "events.ndjson")
	rotator, err := sink.NewFileRotator(path, func(w io.Writer) stream.Sink {
		return sink.NewNDJSON(w, false)
	}, sink.RotatorOptions{MaxSize: 10, Compress: true})
	if err != nil {
		t.Fatalf("NewFileRotator() error = %v", err)
	}
	defer func() { _ = rotator.Close() }()

	for range 3 {
		if err := rotator.Emit(stream.Record{
			ID:       "0000000001-0001",
			Sequence: 1,
			Kind:     stream.KindEvent,
			Event:    &stream.Event{Type: "contract", ValueXDR: "AAAAAwAAAAc="},
		}); err != nil {
			t.Fatalf("Emit() error = %v", err)
		}
	}
	if got := len(backups(t, path)); got == 0 {
		t.Fatal("expected at least one compressed backup")
	}
}

func TestRotatingWriterManualRotate(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "stream.ndjson")
	w, err := sink.NewRotatingWriter(path, sink.RotatorOptions{})
	if err != nil {
		t.Fatalf("NewRotatingWriter() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	if err := w.Rotate(); err != nil {
		t.Fatalf("Rotate() on an empty file error = %v", err)
	}
	if got := len(backups(t, path)); got != 0 {
		t.Errorf("got %d backups for an empty file, want 0", got)
	}

	if _, err := w.Write([]byte("payload")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := w.Rotate(); err != nil {
		t.Fatalf("Rotate() error = %v", err)
	}
	found := backups(t, path)
	if len(found) != 1 {
		t.Fatalf("got %d backups, want 1", len(found))
	}
	if got := readFile(t, found[0]); got != "payload" {
		t.Errorf("backup = %q, want %q", got, "payload")
	}
	if got := readFile(t, path); got != "" {
		t.Errorf("active file = %q after Rotate(), want empty", got)
	}
}

func TestRotatingWriterRejectsUseAfterClose(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "stream.ndjson")
	w, err := sink.NewRotatingWriter(path, sink.RotatorOptions{})
	if err != nil {
		t.Fatalf("NewRotatingWriter() error = %v", err)
	}
	if _, err := w.Write([]byte("x")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := w.Write([]byte("y")); err == nil {
		t.Error("Write() after close accepted")
	}
	if err := w.Rotate(); err == nil {
		t.Error("Rotate() after close accepted")
	}
	if err := w.RotateIfNeeded(1); err == nil {
		t.Error("RotateIfNeeded() after close accepted")
	}
	if err := w.Sync(); err != nil {
		t.Errorf("Sync() after close error = %v, want a no-op", err)
	}
}

func TestRotatingWriterOpenFailure(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "missing", "nested", "stream.ndjson")
	if _, err := sink.NewRotatingWriter(path, sink.RotatorOptions{}); err == nil {
		t.Error("NewRotatingWriter() into a missing directory accepted")
	}
}

func TestRotatingWriterDisambiguatesCollidingBackupNames(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "stream.ndjson")
	fixed := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	w, err := sink.NewRotatingWriter(path, sink.RotatorOptions{
		MaxSize: 1,
		Now:     func() time.Time { return fixed },
	})
	if err != nil {
		t.Fatalf("NewRotatingWriter() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	if _, err := w.Write([]byte("a")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := w.Write([]byte("b")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := w.Write([]byte("c")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	found := backups(t, path)
	if len(found) != 2 {
		t.Fatalf("got %d backups, want 2", len(found))
	}
	if found[0] == found[1] {
		t.Errorf("both backups are named %q, want a .1 disambiguator", found[0])
	}
}

func TestFileRotatorPathAndBuilderNil(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "events.ndjson")
	rotator, err := sink.NewFileRotator(path, func(w io.Writer) stream.Sink {
		return sink.NewNDJSON(w, false)
	}, sink.RotatorOptions{})
	if err != nil {
		t.Fatalf("NewFileRotator() error = %v", err)
	}
	if got := rotator.Path(); got != path {
		t.Errorf("Path() = %q, want %q", got, path)
	}
	if err := rotator.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := sink.NewFileRotator(path, func(io.Writer) stream.Sink { return nil }, sink.RotatorOptions{}); err == nil {
		t.Error("nil-returning builder accepted")
	}
}

func TestRotatingWriterRotatesDailyAndCompressesInBackground(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "events.log")
	now := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	w, err := sink.NewRotatingWriter(path, sink.RotatorOptions{
		Daily:         true,
		Compress:      true,
		AsyncCompress: true,
		Now:           func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewRotatingWriter() error = %v", err)
	}

	if _, err := w.Write([]byte("first day\n")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	now = now.Add(24 * time.Hour)
	if _, err := w.Write([]byte("second day\n")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	// Close waits for the background compressor, so the archive is complete.
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	archives := backups(t, path)
	if len(archives) != 1 {
		t.Fatalf("archives = %v, want one daily archive", archives)
	}
	if !strings.HasSuffix(archives[0], "2026-11-28.log.gz") {
		t.Errorf("archive = %q, want a YYYY-MM-DD.log.gz name for the day it covers", archives[0])
	}

	file, err := os.Open(archives[0])
	if err != nil {
		t.Fatalf("opening archive: %v", err)
	}
	reader, err := gzip.NewReader(file)
	if err != nil {
		_ = file.Close()
		t.Fatalf("reading gzip archive: %v", err)
	}
	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("decompressing archive: %v", err)
	}
	if err := errors.Join(reader.Close(), file.Close()); err != nil {
		t.Fatalf("closing archive: %v", err)
	}
	if string(content) != "first day\n" {
		t.Errorf("archive content = %q, want %q", content, "first day\n")
	}
	if got := readFile(t, path); got != "second day\n" {
		t.Errorf("active file = %q, want %q", got, "second day\n")
	}
}

func TestRotatingWriterPrunesOldestArchives(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "events.log")
	w, err := sink.NewRotatingWriter(path, sink.RotatorOptions{MaxSize: 40, MaxBackups: 1})
	if err != nil {
		t.Fatalf("NewRotatingWriter() error = %v", err)
	}
	for i := range 3 {
		if _, err := w.Write([]byte(strings.Repeat("x", 30))); err != nil {
			t.Fatalf("Write(%d) error = %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if got := backups(t, path); len(got) != 1 {
		t.Errorf("archives = %v, want MaxBackups=1 to keep one", got)
	}
}

func TestRollingFileSinkRotatesDailyAndCompresses(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "events.ndjson")
	now := time.Date(2026, 11, 28, 8, 0, 0, 0, time.UTC)
	rotator, err := sink.NewRollingFileSink(path, func(w io.Writer) stream.Sink {
		return sink.NewNDJSON(w, false)
	}, sink.RollingFileSinkOptions{MaxSizeMB: 1, MaxBackups: 2, Compress: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("NewRollingFileSink() error = %v", err)
	}
	if got := rotator.Path(); got != path {
		t.Errorf("Path() = %q, want %q", got, path)
	}

	if err := rotator.Emit(ledgerRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	now = now.Add(24 * time.Hour)
	if err := rotator.Emit(ledgerRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := rotator.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	archives := backups(t, path)
	if len(archives) != 1 {
		t.Fatalf("archives = %v, want one daily archive", archives)
	}
	if !strings.HasSuffix(archives[0], "2026-11-28.log.gz") {
		t.Errorf("archive = %q, want a compressed daily archive", archives[0])
	}
}
