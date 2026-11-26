package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// staticSource serves one fixed page and ignores the cursor, which is all an
// end-to-end wiring test needs.
type staticSource struct {
	records []stream.Record
	served  bool
}

func (s *staticSource) Name() string { return "static" }

func (s *staticSource) Fetch(context.Context, string) (stream.Batch, error) {
	if s.served {
		return stream.Batch{}, nil
	}
	s.served = true
	return stream.Batch{
		Records:    s.records,
		NextCursor: "0000000002-0000000000",
	}, nil
}

func (s *staticSource) CursorLedger(string) (uint32, bool) { return 100, true }

func staticEvent(id string) stream.Record {
	return stream.Record{
		ID:       id,
		Sequence: 100,
		Time:     time.Unix(1_700_000_000, 0).UTC(),
		Kind:     stream.KindEvent,
		Event: &stream.Event{
			Type:       "contract",
			ContractID: "CBGSBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKKY3",
			TxHash:     "abc123",
			TopicXDR:   []string{"AAAAAwAAAAc="},
			ValueXDR:   "AAAAAwAAAAc=",
		},
	}
}

// runStreamWithDLQ drives the same path a cobra command uses, with a source
// that deliberately emits a malformed record between two valid ones.
func runStreamWithDLQ(t *testing.T, sf *streamFlags) (stdout, stderr string, err error) {
	t.Helper()

	bad := stream.Record{
		ID:       "0000000002-0000000000",
		Sequence: 100,
		Kind:     stream.Kind("nonsense"),
	}
	src := &staticSource{records: []stream.Record{
		staticEvent("0000000001-0000000000"),
		bad,
	}}

	cmd := &cobra.Command{Use: "events"}
	cmd.SetContext(context.Background())

	var outBuf, errBuf bytes.Buffer
	g := &globalFlags{color: "never"}
	err = runStream(cmd, fakerpc.NewServer(nil),
		func(uint32) (stream.Source, error) { return src, nil },
		sf.from, sf, g, &outBuf, &errBuf)
	return outBuf.String(), errBuf.String(), err
}

func TestDLQCapturesMalformedRecordEndToEnd(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "dead.ndjson")
	sf := &streamFlags{from: 100, asJSON: true, dlqFile: path, pageSize: 10, backfillWorkers: 1,
		sinks: sinkFlags{fanoutOverflow: string(sink.OverflowBlock)},
	}

	stdout, stderr, err := runStreamWithDLQ(t, sf)
	if err != nil {
		t.Fatalf("runStream() error = %v, want the malformed record to be captured", err)
	}

	if got := len(nonEmptyLines(stdout)); got != 1 {
		t.Fatalf("stdout has %d records, want only the valid one\n%s", got, stdout)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading dead letter queue: %v", err)
	}
	lines := nonEmptyLines(string(data))
	if len(lines) != 1 {
		t.Fatalf("queue has %d entries, want 1\n%s", len(lines), data)
	}

	var entry sink.DLQEntry
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("dead letter is not valid JSON: %v\n%s", err, lines[0])
	}
	if entry.Kind != sink.DLQKind || entry.Stage != sink.DLQStageValidation {
		t.Errorf("entry = %+v, want a validation dead letter", entry)
	}
	if !strings.Contains(entry.Error, "unknown kind") {
		t.Errorf("error = %q, want the validation cause", entry.Error)
	}
	if entry.Record == nil || entry.Record.ID != "0000000002-0000000000" {
		t.Errorf("record = %+v, want the malformed record preserved", entry.Record)
	}
	if !strings.Contains(stderr, "dead-lettered record 0000000002-0000000000") {
		t.Errorf("stderr does not report the dead letter\n%s", stderr)
	}
}

func TestDLQClosesItsFileWithBatching(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "dead.ndjson")
	sf := &streamFlags{
		from:            100,
		asJSON:          true,
		dlqFile:         path,
		pageSize:        10,
		backfillWorkers: 1,
		batch:           true,
		batchSize:       1,
		batchTimeout:    0,
		sinks:           sinkFlags{fanoutOverflow: string(sink.OverflowBlock)},
	}

	if _, _, err := runStreamWithDLQ(t, sf); err != nil {
		t.Fatalf("runStream() error = %v", err)
	}

	// Reopening the same file is the observable proof that the batched chain
	// closed the dead letter writer instead of leaving it for process exit.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading dead letter queue: %v", err)
	}
	if got := len(nonEmptyLines(string(data))); got != 1 {
		t.Fatalf("queue has %d entries, want 1\n%s", got, data)
	}
}

// TestDLQComposesWithTopicPatterns pins how the client-side filter stacks
// with the dead letter queue: the PatternSink wraps the DLQ chain, and it
// passes non-event records through untouched, so a malformed record still
// lands in the queue whether or not the patterns would have matched it.
func TestDLQComposesWithTopicPatterns(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		pattern string
		wantOut int
	}{
		// The fixture event's topic decodes to u32 7, so this pattern keeps
		// it on stdout.
		{name: "matching pattern", pattern: "7", wantOut: 1},
		{name: "filtering pattern", pattern: "nope", wantOut: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "dead.ndjson")
			sf := &streamFlags{
				from:            100,
				asJSON:          true,
				dlqFile:         path,
				pageSize:        10,
				backfillWorkers: 1,
				topicPatterns:   []string{tc.pattern},
				sinks:           sinkFlags{fanoutOverflow: string(sink.OverflowBlock)},
			}

			stdout, _, err := runStreamWithDLQ(t, sf)
			if err != nil {
				t.Fatalf("runStream() error = %v, want the malformed record captured", err)
			}
			if got := len(nonEmptyLines(stdout)); got != tc.wantOut {
				t.Errorf("stdout has %d records, want %d\n%s", got, tc.wantOut, stdout)
			}

			// Filtering happens before the DLQ chain, so a filtered-out valid
			// record never dead-letters; the malformed one is not an event, so
			// the pattern sink passes it through and it still does.
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading dead letter queue: %v", err)
			}
			if got := len(nonEmptyLines(string(data))); got != 1 {
				t.Errorf("queue has %d entries, want 1\n%s", got, data)
			}
		})
	}
}

func TestDLQFileMustDifferFromOutputFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "records.ndjson")
	if _, _, err := run(t, "events", "--from", "100",
		"--output-file", path, "--dlq-file", filepath.Join(dir, ".", "records.ndjson")); err == nil {
		t.Fatal("Execute() error = nil, want the shared path to be rejected")
	}
}
