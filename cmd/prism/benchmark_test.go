package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/internal/cli"
)

func TestBenchmarkProfilerIsInstalled(t *testing.T) {
	if cli.NewBenchmarkProfiler == nil {
		t.Fatal("the binary did not install the benchmark profiler")
	}
}

func TestBenchmarkProfilerWritesProfiles(t *testing.T) {
	dir := t.TempDir()
	cpu := filepath.Join(dir, "cpu.pprof")
	heap := filepath.Join(dir, "heap.pprof")

	profiler, err := newBenchmarkProfiler(cpu, heap)
	if err != nil {
		t.Fatalf("newBenchmarkProfiler() error = %v", err)
	}

	// Give the CPU profiler a window with work in it.
	deadline := time.Now().Add(50 * time.Millisecond)
	for work := 0; time.Now().Before(deadline); work++ {
	}

	if err := profiler.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	for _, path := range []string{cpu, heap} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("profile %s: %v", path, err)
		}
		if info.Size() == 0 {
			t.Fatalf("profile %s is empty", path)
		}
	}

	// A second Finish must not panic or reopen anything.
	if err := profiler.Finish(); err != nil {
		t.Fatalf("second Finish() error = %v", err)
	}
}

func TestBenchmarkProfilerWithoutPathsWritesNothing(t *testing.T) {
	profiler, err := newBenchmarkProfiler("", "")
	if err != nil {
		t.Fatalf("newBenchmarkProfiler() error = %v", err)
	}
	if err := profiler.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
}
