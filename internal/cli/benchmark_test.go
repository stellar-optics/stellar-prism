package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestBenchmarkCommandPrintsReport(t *testing.T) {
	stdout, _, err := run(t, "benchmark", "--duration", "20ms", "--rate", "1000")
	if err != nil {
		t.Fatalf("benchmark command error: %v", err)
	}
	for _, want := range []string{
		"Benchmark summary",
		"Source",
		"Total records",
		"Throughput",
		"Average latency",
		"p95 latency",
		"p99 latency",
		"Sink write speed",
		"Memory allocated",
		"Allocations",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("benchmark report missing %q:\n%s", want, stdout)
		}
	}
}

func TestBenchmarkCommandRejectsInvalidFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"negative rate", []string{"--rate", "-1"}, "--rate must not be negative"},
		{"zero duration", []string{"--duration", "0s"}, "--duration must be greater than zero"},
		{"unknown source", []string{"--source", "captured"}, "--source must be"},
		{"replay without directory", []string{"--source", "replay:"}, "needs a fixture directory"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := run(t, append([]string{"benchmark", "--duration", "5ms"}, tc.args...)...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestBenchmarkProfileRequiresBuildSupport(t *testing.T) {
	// The cli test binary does not install a profiler, which is what a library
	// build without cmd/prism looks like.
	previous := NewBenchmarkProfiler
	NewBenchmarkProfiler = nil
	t.Cleanup(func() { NewBenchmarkProfiler = previous })

	for _, flag := range []string{"--cpu-profile", "--mem-profile"} {
		_, _, err := run(t, "benchmark", "--duration", "5ms", flag, filepath.Join(t.TempDir(), "profile.pprof"))
		if err == nil || !strings.Contains(err.Error(), "not supported by this build") {
			t.Fatalf("%s error = %v, want a build-support error", flag, err)
		}
	}
}

func TestBenchmarkRateLimitsLoad(t *testing.T) {
	report, err := runBenchmark(context.Background(), &benchmarkSink{}, 100, 80*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	if report.records == 0 {
		t.Fatal("rate-limited benchmark emitted no records")
	}
	// 100 records/s over 80ms is at most 8 ticks; allow generous slack for a
	// slow or overloaded machine but still catch an ignored --rate.
	if report.records > 20 {
		t.Fatalf("rate-limited benchmark emitted %d records, want at most 20", report.records)
	}
}

func TestRunBenchmarkStopsAtDuration(t *testing.T) {
	output := &benchmarkSink{}
	started := time.Now()
	report, err := runBenchmark(context.Background(), output, 0, 20*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	if report.records == 0 {
		t.Fatal("unlimited benchmark emitted no records")
	}
	if output.bytes == 0 {
		t.Fatal("benchmark did not serialise the generated records")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("benchmark ran for %s after a 20ms window", elapsed)
	}
}

func TestRunBenchmarkRepeatsReplayRecords(t *testing.T) {
	output := &benchmarkSink{}
	report, err := runBenchmark(context.Background(), output, 1000, 30*time.Millisecond, []stream.Record{
		{ID: "fixture", Kind: stream.KindLedger},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.records == 0 {
		t.Fatal("benchmark emitted no replay records")
	}
	if output.bytes == 0 {
		t.Fatal("benchmark did not serialise replay records")
	}
}

func TestRunBenchmarkStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	report, err := runBenchmark(ctx, &benchmarkSink{}, 0, time.Minute, nil)
	if err != nil {
		t.Fatalf("cancelled benchmark error = %v, want nil", err)
	}
	if report.records != 0 {
		t.Fatalf("cancelled benchmark emitted %d records", report.records)
	}
}

func TestLoadBenchmarkFixturesReadsEveryFormat(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("batch.json", `[{"ID":"one","Kind":"ledger"},{"ID":"two","Kind":"ledger"}]`)
	write("records.ndjson", `{"ID":"three","Kind":"ledger"}`+"\n")
	write("single.json", `{"ID":"four","Kind":"ledger"}`)
	write("ignored.txt", "not a fixture")

	records, err := loadBenchmarkFixtures(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, len(records))
	for i, record := range records {
		got[i] = record.ID
	}
	want := "one,two,three,four"
	if strings.Join(got, ",") != want {
		t.Fatalf("fixture IDs = %s, want %s (files in path order)", strings.Join(got, ","), want)
	}
}

func TestLoadBenchmarkFixturesRejectsUnusableDirectories(t *testing.T) {
	empty := t.TempDir()
	if _, err := loadBenchmarkFixtures(empty); err == nil || !strings.Contains(err.Error(), "contains no records") {
		t.Fatalf("empty directory error = %v", err)
	}

	file := filepath.Join(t.TempDir(), "fixture.json")
	if err := os.WriteFile(file, []byte(`{"ID":"one"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadBenchmarkFixtures(file); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("file path error = %v", err)
	}

	if _, err := loadBenchmarkFixtures(filepath.Join(empty, "missing")); err == nil {
		t.Fatal("missing directory did not fail")
	}
}

func TestBenchmarkPercentileIsNearestRank(t *testing.T) {
	values := make([]time.Duration, 100)
	for i := range values {
		values[i] = time.Duration(i+1) * time.Millisecond
	}

	tests := []struct {
		percentile float64
		want       time.Duration
	}{
		{0.50, 50 * time.Millisecond},
		{0.95, 95 * time.Millisecond},
		{0.99, 99 * time.Millisecond},
		{1, 100 * time.Millisecond},
	}
	for _, tc := range tests {
		if got := benchmarkPercentile(values, tc.percentile); got != tc.want {
			t.Errorf("p%.2f = %s, want %s", tc.percentile, got, tc.want)
		}
	}
	if got := benchmarkPercentile(nil, 0.95); got != 0 {
		t.Errorf("percentile of no samples = %s, want 0", got)
	}
}

func TestPrintBenchmarkReportComputesRates(t *testing.T) {
	var out strings.Builder
	printBenchmarkReport(&out, benchmarkReport{
		source:       benchmarkReplayPrefix + "fixtures",
		records:      1000,
		bytes:        1024 * 1024,
		elapsed:      time.Second,
		totalLatency: 100 * time.Millisecond,
		latencies:    []time.Duration{time.Millisecond},
		allocBytes:   2 << 20,
		allocObjects: 10_000,
	})

	report := out.String()
	for _, want := range []string{
		"replay:fixtures",
		"1000 records/s",
		"100µs", // average latency of 1000 records over 100ms
		"1.00 MB/s",
		"2.0 MiB (2.0 KiB/record)",
		"10000 (10/record)",
	} {
		if !strings.Contains(report, want) {
			t.Errorf("report missing %q:\n%s", want, report)
		}
	}
}
