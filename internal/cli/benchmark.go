package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

const (
	// benchmarkSourceSynthetic builds records in process instead of reading a
	// fixture directory.
	benchmarkSourceSynthetic = "synthetic"
	// benchmarkReplayPrefix selects the fixture loader; the rest of the value
	// is the directory to replay from.
	benchmarkReplayPrefix = "replay:"

	// benchmarkPoolSize is how many synthetic records are built before the
	// run. Building them up front keeps record construction out of the timed
	// section, so the numbers describe the sinks rather than fmt.Sprintf.
	benchmarkPoolSize = 1024

	// benchmarkLatencySamples bounds the latency reservoir. Percentiles come
	// from a fixed-size ring, so a long run does not grow without bound.
	benchmarkLatencySamples = 8192

	// maxBenchmarkRate caps --rate where one record per nanosecond still has a
	// representable ticker interval.
	maxBenchmarkRate = int(time.Second)
)

// BenchmarkProfiler records runtime profiles across one benchmark run.
//
// The CLI owns the flags and the lifecycle; cmd/prism supplies a
// runtime/pprof-backed implementation, for the same reason it supplies the
// profiling server: the streaming library has no reason to depend on
// runtime/pprof. It is nil in builds and tests that do not install one.
type BenchmarkProfiler interface {
	// Finish stops CPU profiling and writes the heap profile requested when
	// the profiler was created.
	Finish() error
}

// NewBenchmarkProfiler starts profiling a benchmark run into the given files.
// An empty path disables that profile. It is installed by cmd/prism.
var NewBenchmarkProfiler func(cpuPath, heapPath string) (BenchmarkProfiler, error)

type benchmarkOptions struct {
	rate        int
	duration    time.Duration
	source      string
	cpuProfile  string
	heapProfile string
	sinks       sinkFlags
}

// benchmarkReport is the measurement one run produced. Durations are kept
// unrounded so the report can choose its own precision.
type benchmarkReport struct {
	source       string
	records      uint64
	bytes        uint64
	elapsed      time.Duration
	totalLatency time.Duration
	latencies    []time.Duration
	allocBytes   uint64
	allocObjects uint64
}

// newBenchmarkCmd builds the benchmark command.
//
// The load is generated locally on purpose: pulling records from a remote RPC
// would mostly measure the network between prism and that RPC, while the
// question the command answers is how fast the configured sinks can absorb
// records. Enabling a sink flag therefore measures that sink; with no sink the
// run writes nothing and reports prism's own encoding cost.
func newBenchmarkCmd(_ *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var opts benchmarkOptions

	cmd := &cobra.Command{
		Use:   "benchmark",
		Short: "Measure end-to-end ingestion and sink throughput",
		Long: `Generate records at a fixed rate, or as fast as the sinks accept them, and
report throughput, latency and memory allocation for the configured sinks.

A rate of zero runs at maximum speed. Every sink flag the streaming commands
accept works here too, so the same flags that configure ingestion in production
configure the benchmark: a webhook, Elasticsearch, NATS, S3 or GCS archive,
Parquet or SQS/SNS/PubSub destination is measured exactly as it would be
written to.

Use --source replay:DIR to replay captured records instead of generating them.
The directory is walked for .json, .ndjson and .jsonl files; a JSON file may
hold one record or an array of records, an NDJSON file one record per line. The
fixtures repeat until the duration elapses. Files are ordered by path, so a
replay is reproducible.

--cpu-profile and --mem-profile record a pprof profile of the measured run, which
is how a deployment's CPU and memory limits are sized against real throughput.`,
		Example: `  # Measure encoding throughput against the in-process sink
  prism benchmark --duration 30s

  # Drive a webhook at 5,000 records per second for one minute
  prism benchmark --rate 5000 --duration 1m --webhook-url https://worker.example/events

  # Replay captured ledger batches into a local Elasticsearch
  prism benchmark --source replay:./testdata/ledgers --duration 5m --sink-es-url http://localhost:9200`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if opts.rate < 0 {
				return fmt.Errorf("--rate must not be negative")
			}
			if opts.rate > maxBenchmarkRate {
				return fmt.Errorf("--rate must be at most %d", maxBenchmarkRate)
			}
			if opts.duration <= 0 {
				return fmt.Errorf("--duration must be greater than zero")
			}
			if err := opts.sinks.validate(cmd); err != nil {
				return err
			}

			records, source, err := loadBenchmarkSource(opts.source)
			if err != nil {
				return err
			}

			profiler, err := startBenchmarkProfiler(opts.cpuProfile, opts.heapProfile)
			if err != nil {
				return err
			}

			measure := &benchmarkSink{}
			out, err := opts.sinks.addTo(measure, false, func(err error) {
				fmt.Fprintf(stderr, "prism: benchmark sink error: %v\n", err)
			})
			if err != nil {
				return errors.Join(err, finishBenchmarkProfiler(profiler))
			}
			defer closeBenchmarkSinks(&opts.sinks, stderr)

			// Ctrl-C ends the run and still prints what it measured, which is
			// what an operator lowering --duration wants.
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			runtime.GC() // measure the run's allocations, not the setup's garbage
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)

			started := time.Now()
			report, runErr := runBenchmark(ctx, out, opts.rate, opts.duration, records)
			if err := out.Flush(); runErr == nil {
				runErr = err
			}
			report.elapsed = time.Since(started)

			runtime.ReadMemStats(&after)
			report.source = source
			report.bytes = measure.bytes
			report.allocBytes = after.TotalAlloc - before.TotalAlloc
			report.allocObjects = after.Mallocs - before.Mallocs
			printBenchmarkReport(stdout, report)

			if err := finishBenchmarkProfiler(profiler); runErr == nil {
				runErr = err
			}
			return runErr
		},
	}

	cmd.Flags().IntVar(&opts.rate, "rate", 0,
		"records per second to generate (0 runs at maximum speed)")
	cmd.Flags().DurationVar(&opts.duration, "duration", 30*time.Second,
		"run the benchmark for this long")
	cmd.Flags().StringVar(&opts.source, "source", benchmarkSourceSynthetic,
		"load source: \"synthetic\" or \"replay:DIR\" to replay captured fixture records")
	cmd.Flags().StringVar(&opts.cpuProfile, "cpu-profile", "",
		"write a pprof CPU profile covering the measured run to this file")
	cmd.Flags().StringVar(&opts.heapProfile, "mem-profile", "",
		"write a pprof heap profile at the end of the run to this file")
	opts.sinks.register(cmd)
	return cmd
}

// benchmarkSink is the default destination: it serialises every record the way
// the JSON sinks do and discards it, so a run without a sink still measures
// prism's encoding cost and can report bytes per second. When another sink is
// enabled the fanout places this one first, so each measured Emit covers the
// encoding plus that sink's write.
type benchmarkSink struct{ bytes uint64 }

func (s *benchmarkSink) Emit(record stream.Record) error {
	body, err := sink.MarshalRecord(record, false)
	if err != nil {
		return err
	}
	s.bytes += uint64(len(body)) + 1 // the newline an NDJSON sink appends
	return nil
}

func (*benchmarkSink) Flush() error { return nil }

// closeBenchmarkSinks releases the sinks the flags opened. The fanout holds the
// same values, so closing here also stops whatever they were writing to.
func closeBenchmarkSinks(s *sinkFlags, stderr io.Writer) {
	for i := len(s.constructedSinks) - 1; i >= 0; i-- {
		if err := s.constructedSinks[i].Close(); err != nil {
			fmt.Fprintf(stderr, "prism: closing benchmark sink: %v\n", err)
		}
	}
}

// runBenchmark emits records until the duration elapses or ctx is cancelled.
//
// A rate-limited run waits for the ticker before each record, so --rate is a
// ceiling on load rather than a burst followed by a pause: a slow sink simply
// falls behind the requested rate, which the report shows as fewer records.
func runBenchmark(ctx context.Context, output stream.Sink, rate int, duration time.Duration, replay []stream.Record) (benchmarkReport, error) {
	report := benchmarkReport{latencies: make([]time.Duration, 0, benchmarkLatencySamples)}

	pool := replay
	if len(pool) == 0 {
		pool = syntheticBenchmarkRecords(benchmarkPoolSize)
	}

	deadline := time.NewTimer(duration)
	defer deadline.Stop()

	var ticks <-chan time.Time
	if rate > 0 {
		interval := time.Second / time.Duration(rate)
		if interval < time.Nanosecond {
			interval = time.Nanosecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		ticks = ticker.C
	}

	for sent := uint64(0); ; sent++ {
		if ticks != nil {
			select {
			case <-ctx.Done():
				return report, nil
			case <-deadline.C:
				return report, nil
			case <-ticks:
			}
		} else {
			select {
			case <-ctx.Done():
				return report, nil
			case <-deadline.C:
				return report, nil
			default:
			}
		}

		started := time.Now()
		if err := output.Emit(pool[sent%uint64(len(pool))]); err != nil {
			return report, err
		}
		latency := time.Since(started)

		report.records++
		report.totalLatency += latency
		if len(report.latencies) < benchmarkLatencySamples {
			report.latencies = append(report.latencies, latency)
		} else {
			report.latencies[(report.records-1)%benchmarkLatencySamples] = latency
		}
	}
}

// syntheticBenchmarkRecords builds ledger-close records, the largest shape
// prism writes, so the reported throughput is not flattered by undersized
// records. Sequence and hash vary per record to keep the encoding realistic.
func syntheticBenchmarkRecords(n int) []stream.Record {
	records := make([]stream.Record, n)
	now := time.Now().UTC()
	for i := range records {
		sequence := uint32(1_000_000 + i)
		hash := fmt.Sprintf("%08x%056x", sequence, i)
		records[i] = stream.Record{
			ID:       fmt.Sprintf("%010d-0000", sequence),
			Sequence: sequence,
			Time:     now,
			Kind:     stream.KindLedger,
			Close: &stream.LedgerClose{
				Hash:        hash,
				HeaderXDR:   hash,
				MetadataXDR: hash,
			},
		}
	}
	return records
}

// loadBenchmarkSource resolves --source into records and the label the report
// prints. A synthetic run returns no records: the generator builds its own pool
// and the label is all the caller needs.
func loadBenchmarkSource(source string) ([]stream.Record, string, error) {
	source = strings.TrimSpace(source)
	if source == "" || source == benchmarkSourceSynthetic {
		return nil, benchmarkSourceSynthetic, nil
	}
	if !strings.HasPrefix(source, benchmarkReplayPrefix) {
		return nil, "", fmt.Errorf("--source must be %q or %q", benchmarkSourceSynthetic, benchmarkReplayPrefix+"DIR")
	}
	dir := strings.TrimPrefix(source, benchmarkReplayPrefix)
	records, err := loadBenchmarkFixtures(dir)
	if err != nil {
		return nil, "", err
	}
	return records, benchmarkReplayPrefix + dir, nil
}

// benchmarkFixtureExtensions are the file types a replay directory may hold.
var benchmarkFixtureExtensions = map[string]bool{".json": true, ".ndjson": true, ".jsonl": true}

// loadBenchmarkFixtures walks dir and decodes every fixture record it finds.
//
// filepath.WalkDir visits files in lexical order, so replaying the same
// directory twice produces the same load.
func loadBenchmarkFixtures(dir string) ([]stream.Record, error) {
	if dir == "" {
		return nil, fmt.Errorf("%s needs a fixture directory", benchmarkReplayPrefix)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("reading replay fixture directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("replay fixture path %q is not a directory", dir)
	}

	var records []stream.Record
	err = filepath.WalkDir(dir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !benchmarkFixtureExtensions[strings.ToLower(filepath.Ext(path))] {
			return nil
		}
		loaded, err := readBenchmarkFixture(path)
		if err != nil {
			return err
		}
		records = append(records, loaded...)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("loading replay fixtures: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("replay fixture directory %q contains no records", dir)
	}
	return records, nil
}

// readBenchmarkFixture decodes one fixture file as NDJSON, a JSON array or a
// single JSON record, in that order.
func readBenchmarkFixture(path string) (records []stream.Record, err error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := file.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".ndjson" || ext == ".jsonl" {
		return readBenchmarkNDJSON(file, path)
	}

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	var batch []stream.Record
	if err := json.Unmarshal(data, &batch); err == nil {
		return batch, nil
	}
	var record stream.Record
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("decoding replay fixture %s: %w", path, err)
	}
	return []stream.Record{record}, nil
}

// readBenchmarkNDJSON decodes one record per line. The buffer is large because
// a captured ledger carries several base64 XDR blobs and outstrips
// bufio.Scanner's default token size.
func readBenchmarkNDJSON(r io.Reader, path string) ([]stream.Record, error) {
	var records []stream.Record
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for line := 1; scanner.Scan(); line++ {
		if len(strings.TrimSpace(scanner.Text())) == 0 {
			continue
		}
		var record stream.Record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, fmt.Errorf("decoding replay record at %s:%d: %w", path, line, err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading replay fixture %s: %w", path, err)
	}
	return records, nil
}

// startBenchmarkProfiler starts the run profiler when a profile was requested.
func startBenchmarkProfiler(cpuPath, heapPath string) (BenchmarkProfiler, error) {
	if cpuPath == "" && heapPath == "" {
		return nil, nil
	}
	if NewBenchmarkProfiler == nil {
		return nil, fmt.Errorf("profiling a benchmark run is not supported by this build")
	}
	profiler, err := NewBenchmarkProfiler(cpuPath, heapPath)
	if err != nil {
		return nil, fmt.Errorf("starting the benchmark profiler: %w", err)
	}
	return profiler, nil
}

// finishBenchmarkProfiler writes the profiles a run collected. It is safe to
// call with a nil profiler.
func finishBenchmarkProfiler(profiler BenchmarkProfiler) error {
	if profiler == nil {
		return nil
	}
	if err := profiler.Finish(); err != nil {
		return fmt.Errorf("finishing the benchmark profiler: %w", err)
	}
	return nil
}

// printBenchmarkReport writes the summary table. Records per second and bytes
// per second both use the same elapsed window, which includes the final flush,
// so an asynchronous sink is measured through its last write.
func printBenchmarkReport(w io.Writer, report benchmarkReport) {
	elapsed := report.elapsed
	if elapsed <= 0 {
		elapsed = time.Nanosecond
	}
	seconds := elapsed.Seconds()

	average := time.Duration(0)
	if report.records > 0 {
		average = report.totalLatency / time.Duration(report.records)
	}

	rows := [][2]string{
		{"Source", report.source},
		{"Total records", strconv.FormatUint(report.records, 10)},
		{"Elapsed", elapsed.Round(time.Millisecond).String()},
		{"Throughput", fmt.Sprintf("%.0f records/s", float64(report.records)/seconds)},
		{"Average latency", average.Round(time.Nanosecond).String()},
		{"p95 latency", benchmarkPercentile(report.latencies, 0.95).Round(time.Nanosecond).String()},
		{"p99 latency", benchmarkPercentile(report.latencies, 0.99).Round(time.Nanosecond).String()},
		{"Sink write speed", fmt.Sprintf("%.2f MB/s", float64(report.bytes)/seconds/(1024*1024))},
		{"Memory allocated", fmt.Sprintf("%s (%s/record)",
			humanBytes(report.allocBytes), humanBytes(perRecord(report.allocBytes, report.records)))},
		{"Allocations", fmt.Sprintf("%d (%d/record)",
			report.allocObjects, perRecord(report.allocObjects, report.records))},
	}

	fmt.Fprintln(w, "Benchmark summary")
	fmt.Fprintln(w)
	for _, row := range rows {
		fmt.Fprintf(w, "  %-20s %s\n", row[0], row[1])
	}
}

// perRecord reports total divided by records, rounded to the nearest integer.
func perRecord(total, records uint64) uint64 {
	if records == 0 {
		return 0
	}
	return (total + records/2) / records
}

// humanBytes renders a byte count with a binary unit, matching the units sinks
// and metrics endpoints use.
func humanBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	value := float64(n)
	for _, suffix := range []string{"KiB", "MiB", "GiB", "TiB"} {
		value /= unit
		if value < unit {
			return fmt.Sprintf("%.1f %s", value, suffix)
		}
	}
	return fmt.Sprintf("%.1f PiB", value/unit)
}

// benchmarkPercentile returns the nearest-rank percentile of the sampled
// latencies, so p99 is an observed latency rather than an interpolation.
func benchmarkPercentile(values []time.Duration, percentile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	index := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}
