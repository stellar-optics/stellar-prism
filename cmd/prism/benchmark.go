package main

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/stellar-optics/stellar-prism/internal/cli"
)

// The benchmark command itself lives in internal/cli, beside the sink flags and
// the engine it measures. Runtime profiling is the part this binary owns:
// writing pprof files is why a benchmark exists for sizing CPU and memory
// limits, and runtime/pprof has no place in the streaming library's dependency
// graph. It is installed the way pprof.go installs the profiling server.
func init() {
	cli.NewBenchmarkProfiler = newBenchmarkProfiler
}

// benchmarkProfiler writes a CPU profile across the measured run and a heap
// profile when the run ends.
type benchmarkProfiler struct {
	cpu      *os.File
	heapPath string
}

var _ cli.BenchmarkProfiler = (*benchmarkProfiler)(nil)

// newBenchmarkProfiler starts profiling into the given files. An empty path
// leaves that profile off, so --cpu-profile and --mem-profile are independent.
func newBenchmarkProfiler(cpuPath, heapPath string) (cli.BenchmarkProfiler, error) {
	p := &benchmarkProfiler{heapPath: heapPath}
	if cpuPath == "" {
		return p, nil
	}

	file, err := os.Create(cpuPath)
	if err != nil {
		return nil, fmt.Errorf("creating CPU profile %s: %w", cpuPath, err)
	}
	if err := pprof.StartCPUProfile(file); err != nil {
		return nil, errors.Join(fmt.Errorf("starting the CPU profile: %w", err), file.Close())
	}
	p.cpu = file
	return p, nil
}

// Finish stops the CPU profile, waits for its samples to be written and then
// writes the heap profile. Both halves report their own failure so a broken
// heap profile does not hide a CPU profile that was written.
func (p *benchmarkProfiler) Finish() error {
	var errs []error

	if p.cpu != nil {
		// StopCPUProfile flushes the samples collected so far before it
		// returns, so the file is complete once it is closed.
		pprof.StopCPUProfile()
		if err := p.cpu.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing the CPU profile: %w", err))
		}
		p.cpu = nil
	}

	if p.heapPath != "" {
		path := p.heapPath
		p.heapPath = ""

		file, err := os.Create(path)
		if err != nil {
			errs = append(errs, fmt.Errorf("creating heap profile %s: %w", path, err))
		} else {
			// A heap profile describes live objects, so collect first or it
			// reports the run's garbage as if it were still held.
			runtime.GC()
			writeErr := pprof.WriteHeapProfile(file)
			if closeErr := file.Close(); writeErr == nil {
				writeErr = closeErr
			}
			if writeErr != nil {
				errs = append(errs, fmt.Errorf("writing the heap profile: %w", writeErr))
			}
		}
	}

	return errors.Join(errs...)
}
