package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// streamFlags are the options shared by every streaming command.
type streamFlags struct {
	follow           bool
	from             uint32
	since            time.Duration
	limit            int
	asJSON           bool
	format           string
	csvFields        string
	tsv              bool
	raw              bool
	compact          bool
	poll             time.Duration
	pageSize         uint
	retries          int
	adaptive         bool
	backfillWorkers  int
	batch            bool
	batchSize        int
	batchTimeout     time.Duration
	gapStrategy      string
	maxGapRecoveries int
	quiet            bool
	verbosity        int
	process          func([]stream.Record) ([]stream.Record, error)
	filter           stream.RecordFilter

	sinks sinkFlags

	output         string
	rotateSize     int64
	rotateInterval time.Duration
	rotateCompress bool
	rotateKeep     int

	stateFile     string
	resume        bool
	pruneInterval time.Duration
	pruneKeep     int
	pruneMaxAge   time.Duration

	// statePassphraseEnv names the environment variable holding the
	// passphrase that encrypts the state file.
	statePassphraseEnv string
	// statePassphraseFile names the file holding the passphrase that
	// encrypts the state file.
	statePassphraseFile string
	// stateKMSProvider is "aws" or "gcp": the key management service that
	// holds the key of the state file.
	stateKMSProvider string
	// stateKMSKeyID identifies the key in the key management service.
	stateKMSKeyID string
	// stateKMSRegion is the region of the AWS key.
	stateKMSRegion string
	// stateKMSTokenEnv names the environment variable holding the token
	// used to reach the GCP key service.
	stateKMSTokenEnv string

	// haLeaseFile is the lease file of the active-passive election. When
	// it is set, only the instance holding the lease streams.
	haLeaseFile string
	// haHeartbeat is how often the leader renews the lease.
	haHeartbeat time.Duration
	// haTTL is how long the lease stays valid without a renewal.
	haTTL time.Duration

	// healthPort is the port the liveness and readiness probes are served
	// on. Zero leaves them off.
	healthPort int
	// maxLag is how many ledgers the stream may be behind before it stops
	// being ready.
	maxLag uint32

	// drainTimeout bounds how long a shutdown may take to flush the
	// records in memory and commit the final cursor.
	drainTimeout time.Duration

	// dlqFile is the optional dead letter queue. When set, records that fail
	// validation or delivery are written there instead of stopping the run.
	dlqFile string

	// stateKey identifies this stream inside the cursor metadata file. Two
	// runs share a key only when they watch the same thing, so a recorded
	// cursor is never applied to a different set of filters.
	stateKey string

	// topicPatterns and fanoutDir are events-only; the other commands leave
	// them empty, which is what keeps runStream generic.
	topicPatterns []string
	fanoutDir     string

	// dedupWindow is the events-only re-poll de-duplicator window. Zero
	// disables it.
	dedupWindow int
}

func (s *streamFlags) register(cmd *cobra.Command, defaultPage uint) {
	cmd.Flags().BoolVarP(&s.follow, "follow", "f", false,
		"keep streaming as new ledgers close, like tail -f")
	cmd.Flags().Uint32Var(&s.from, "from", 0,
		"start from this ledger and continue forward (default: the latest ledger)")
	cmd.Flags().DurationVar(&s.since, "since", 0,
		"start about this long ago, e.g. --since 10m; converted to an approximate start ledger (mutually exclusive with --from)")
	cmd.Flags().IntVarP(&s.limit, "limit", "n", 0,
		"stop after this many records (0 means unlimited)")
	cmd.Flags().BoolVar(&s.asJSON, "json", false,
		"emit NDJSON, one record per line")
	cmd.Flags().StringVar(&s.format, "format", "", "output format: csv or tsv (default is human-readable; --json remains supported)")
	cmd.Flags().StringVar(&s.csvFields, "csv-fields", "", "comma-separated CSV/TSV columns: id,ledger,time,kind,contract,tx,topic0,value")
	cmd.Flags().BoolVar(&s.tsv, "tsv", false, "write tab-separated output (equivalent to --format tsv)")
	cmd.Flags().BoolVar(&s.raw, "raw", false,
		"leave XDR base64-encoded instead of decoding it")
	cmd.Flags().BoolVar(&s.compact, "compact", false,
		"print only the header line for each record, omitting decoded payloads")
	cmd.Flags().DurationVar(&s.poll, "poll", stream.DefaultPollInterval,
		"how long to wait between polls once caught up")
	cmd.Flags().UintVar(&s.pageSize, "page-size", defaultPage,
		"how many records to request per RPC call")
	cmd.Flags().IntVar(&s.retries, "max-retries", 0,
		"give up after this many consecutive RPC failures (0 means never give up)")
	cmd.Flags().BoolVar(&s.adaptive, "adaptive", false,
		"tune the wait between polls from how many new ledgers close each interval (with --follow)")
	cmd.Flags().IntVar(&s.backfillWorkers, "backfill-workers", 1,
		"fetch historical pages concurrently with this many workers when --from is set (1 means sequential)")
	cmd.Flags().BoolVar(&s.batch, "batch", false,
		"buffer records and flush them in batches")
	cmd.Flags().IntVar(&s.batchSize, "batch-size", 100,
		"flush a batch after this many records (0 means only the timeout triggers)")
	cmd.Flags().DurationVar(&s.batchTimeout, "batch-timeout", 250*time.Millisecond,
		"flush a batch after this long (0 means only the record count triggers)")
	cmd.Flags().StringVar(&s.gapStrategy, "gap-strategy", string(stream.GapFail),
		`what to do when retention pruned records: "fail" (exit 2) or "resume" from the oldest retained ledger`)
	cmd.Flags().IntVar(&s.maxGapRecoveries, "max-gap-recoveries", 0,
		"limit how many times --gap-strategy resume may skip ahead (0 means no limit)")
	cmd.Flags().BoolVar(&s.quiet, "quiet", false,
		"suppress diagnostic output on stderr; only emit final exit code")
	cmd.Flags().StringVar(&s.output, "output-file", "",
		"append records to this file instead of stdout")
	cmd.Flags().Int64Var(&s.rotateSize, "rotate-size", 0,
		"rotate the output file before a write would exceed this many bytes (0 disables)")
	cmd.Flags().DurationVar(&s.rotateInterval, "rotate-interval", 0,
		"rotate the output file after this long (0 disables)")
	cmd.Flags().BoolVar(&s.rotateCompress, "rotate-compress", false,
		"gzip rotated output files")
	cmd.Flags().IntVar(&s.rotateKeep, "rotate-keep", 0,
		"keep at most this many rotated files (0 keeps all)")
	cmd.Flags().StringVar(&s.stateFile, "state-file", "",
		"record cursor metadata in this file, so a later run can resume from it")
	cmd.Flags().BoolVar(&s.resume, "resume", false,
		"resume from the last cursor recorded in --state-file")
	cmd.Flags().DurationVar(&s.pruneInterval, "prune-interval", stream.DefaultPruneInterval,
		"how often to compact the cursor metadata file in the background (0 compacts once)")
	cmd.Flags().IntVar(&s.pruneKeep, "prune-keep", stream.DefaultPruneKeep,
		"keep at most this many cursor metadata entries per stream (0 keeps all)")
	cmd.Flags().DurationVar(&s.pruneMaxAge, "prune-max-age", 0,
		"drop cursor metadata entries older than this (0 keeps entries regardless of age)")
	cmd.Flags().StringVar(&s.statePassphraseEnv, "state-passphrase-env", "",
		"encrypt the state file with a passphrase read from this environment variable")
	cmd.Flags().StringVar(&s.statePassphraseFile, "state-passphrase-file", "",
		"encrypt the state file with a passphrase read from this file")
	cmd.Flags().StringVar(&s.stateKMSProvider, "state-kms-provider", "",
		`encrypt the state file with a key management service: "aws" or "gcp"`)
	cmd.Flags().StringVar(&s.stateKMSKeyID, "state-kms-key-id", "",
		"the key to use in the key management service (an ARN for AWS, a resource name for GCP)")
	cmd.Flags().StringVar(&s.stateKMSRegion, "state-kms-region", "",
		"region of the AWS key (default us-east-1, else $AWS_REGION)")
	cmd.Flags().StringVar(&s.stateKMSTokenEnv, "state-kms-token-env", gcsTokenEnvVar,
		"environment variable holding the OAuth token used to reach the GCP key service")
	cmd.Flags().StringVar(&s.haLeaseFile, "ha-lease-file", "",
		"run as an active-passive pair, taking this lease file as the lock; only the leader streams")
	cmd.Flags().DurationVar(&s.haHeartbeat, "ha-heartbeat", stream.DefaultHAHeartbeat,
		"how often the leader renews the lease")
	cmd.Flags().DurationVar(&s.haTTL, "ha-ttl", stream.DefaultHATTL,
		"how long the lease stays valid without a renewal")
	cmd.Flags().IntVar(&s.healthPort, "health-port", 0,
		"serve /healthz, /readyz and /metrics on this port (0 disables the health server)")
	cmd.Flags().Uint32Var(&s.maxLag, "max-lag", 0,
		"ledgers the stream may be behind before /readyz reports a failure (0 means no limit)")
	cmd.Flags().DurationVar(&s.drainTimeout, "drain-timeout", DefaultDrainTimeout,
		"how long a shutdown may take to flush records and commit the final cursor")
	cmd.Flags().StringVar(&s.dlqFile, "dlq-file", "",
		"append malformed or undeliverable records to this file as NDJSON instead of stopping the stream")
	s.sinks.register(cmd)
}

// validate checks the stream options before anything is fetched.
//
// cmd is required: it is what tells an explicitly-set flag apart from one
// that is merely carrying its default.
func (s *streamFlags) validate(cmd *cobra.Command) error {
	if err := s.sinks.validate(cmd); err != nil {
		return err
	}
	format := strings.ToLower(s.format)
	if format != "" && format != "csv" && format != "tsv" {
		return fmt.Errorf(`invalid --format %q: want "csv" or "tsv"`, s.format)
	}
	if s.tsv && format != "" && format != "tsv" {
		return fmt.Errorf("--tsv conflicts with --format csv")
	}
	if (format != "" || s.tsv || s.csvFields != "") && s.asJSON {
		return fmt.Errorf("CSV/TSV output conflicts with --json")
	}
	if s.csvFields != "" && format == "" && !s.tsv {
		return fmt.Errorf("--csv-fields requires --format csv or --format tsv")
	}
	if cmd.Flags().Changed("since") {
		if s.from != 0 {
			return fmt.Errorf("--from and --since are mutually exclusive; pass only one")
		}
		if s.since < 0 {
			return fmt.Errorf("--since must not be negative")
		}
	}
	if s.backfillWorkers < 1 {
		return fmt.Errorf("--backfill-workers must be at least 1")
	}
	if _, err := stream.ParseGapStrategy(s.gapStrategy); err != nil {
		return err
	}
	if s.batch {
		if s.batchSize < 0 {
			return fmt.Errorf("--batch-size must not be negative")
		}
		if s.batchTimeout < 0 {
			return fmt.Errorf("--batch-timeout must not be negative")
		}
		if s.batchSize == 0 && s.batchTimeout == 0 {
			return fmt.Errorf("batching needs --batch-size or --batch-timeout above zero")
		}
	}
	if s.output == "" {
		if s.rotateSize != 0 || s.rotateInterval != 0 || s.rotateCompress || s.rotateKeep != 0 {
			return fmt.Errorf("--rotate-* flags require --output-file")
		}
	}
	if s.dlqFile != "" && s.output != "" && samePath(s.dlqFile, s.output) {
		return fmt.Errorf("--dlq-file must differ from --output-file")
	}
	if s.sinks.auditFile != "" {
		if s.output != "" && samePath(s.sinks.auditFile, s.output) {
			return fmt.Errorf("--sink-audit must differ from --output-file")
		}
		if s.dlqFile != "" && samePath(s.sinks.auditFile, s.dlqFile) {
			return fmt.Errorf("--sink-audit must differ from --dlq-file")
		}
	}
	if s.dedupWindow < 0 {
		return fmt.Errorf("--dedup-window must not be negative")
	}
	if err := s.validateState(cmd); err != nil {
		return err
	}
	if err := s.validateRuntime(); err != nil {
		return err
	}
	return nil
}

// validateRuntime checks the high availability, health and drain options.
//
// A zero heartbeat or lease lifetime means "use the default", so it is
// accepted rather than rejected: the election fills the documented defaults
// in, and a caller that never set the flags gets the same behaviour as one
// that set them to their default.
func (s *streamFlags) validateRuntime() error {
	if s.haHeartbeat < 0 {
		return fmt.Errorf("--ha-heartbeat must not be negative")
	}
	if s.haTTL < 0 {
		return fmt.Errorf("--ha-ttl must not be negative")
	}
	if s.haHeartbeat > 0 && s.haTTL > 0 && s.haTTL < 2*s.haHeartbeat {
		// A lease that expires before the leader could renew it would be
		// handed away while the leader was still working on it.
		return fmt.Errorf("--ha-ttl must be at least twice --ha-heartbeat")
	}
	if s.healthPort < 0 || s.healthPort > 65535 {
		return fmt.Errorf("--health-port must be between 0 and 65535")
	}
	if s.drainTimeout < 0 {
		return fmt.Errorf("--drain-timeout must not be negative")
	}
	return nil
}

// samePath reports whether two paths name the same file, even when one is
// relative and the other absolute.
func samePath(a, b string) bool {
	absA, errA := filepath.Abs(a)
	absB, errB := filepath.Abs(b)
	if errA == nil && errB == nil {
		return absA == absB
	}
	return filepath.Clean(a) == filepath.Clean(b)
}

// validateState checks the cursor metadata options.
//
// The pruning flags only mean something when there is a file to prune, so
// they are rejected on their own rather than being silently ignored.
func (s *streamFlags) validateState(cmd *cobra.Command) error {
	if s.pruneInterval < 0 {
		return fmt.Errorf("--prune-interval must not be negative")
	}
	if s.pruneKeep < 0 {
		return fmt.Errorf("--prune-keep must not be negative")
	}
	if s.pruneMaxAge < 0 {
		return fmt.Errorf("--prune-max-age must not be negative")
	}
	if err := s.validateStateEncryption(cmd); err != nil {
		return err
	}
	if s.stateFile != "" {
		return nil
	}
	if s.resume {
		return fmt.Errorf("--resume requires --state-file")
	}
	for _, name := range []string{"prune-interval", "prune-keep", "prune-max-age"} {
		if cmd.Flags().Changed(name) {
			return fmt.Errorf("--%s requires --state-file", name)
		}
	}
	return nil
}

// validateStateEncryption checks the state file encryption options.
//
// The key comes from exactly one place, so asking for two of them is an
// error rather than a silently ignored flag: an operator who believes the
// file is encrypted with a passphrase must never end up with a file that is
// encrypted with something else.
func (s *streamFlags) validateStateEncryption(cmd *cobra.Command) error {
	sources := 0
	if s.statePassphraseEnv != "" {
		sources++
	}
	if s.statePassphraseFile != "" {
		sources++
	}
	if s.stateKMSProvider != "" {
		sources++
	}
	if sources > 1 {
		return fmt.Errorf("only one of --state-passphrase-env, --state-passphrase-file and --state-kms-provider may be given")
	}
	if s.stateKMSProvider != "" {
		provider := strings.ToLower(s.stateKMSProvider)
		if provider != "aws" && provider != "gcp" {
			return fmt.Errorf(`invalid --state-kms-provider %q: want "aws" or "gcp"`, s.stateKMSProvider)
		}
		if s.stateKMSKeyID == "" {
			return fmt.Errorf("--state-kms-provider requires --state-kms-key-id")
		}
	} else if s.stateKMSKeyID != "" {
		return fmt.Errorf("--state-kms-key-id requires --state-kms-provider")
	}
	if s.stateKMSRegion != "" && s.stateKMSProvider == "" {
		return fmt.Errorf("--state-kms-region requires --state-kms-provider")
	}
	if s.stateKMSTokenEnv != "" && cmd.Flags().Changed("state-kms-token-env") && s.stateKMSProvider == "" {
		return fmt.Errorf("--state-kms-token-env requires --state-kms-provider")
	}

	if s.stateFile != "" {
		return nil
	}
	for _, name := range []string{
		"state-passphrase-env",
		"state-passphrase-file",
		"state-kms-provider",
		"state-kms-key-id",
		"state-kms-region",
		"state-kms-token-env",
	} {
		if cmd.Flags().Changed(name) {
			return fmt.Errorf("--%s requires --state-file", name)
		}
	}
	return nil
}

// newSink builds the output sink implied by the flags.
//
// With --output-file the chosen format is built on top of a rotating writer,
// so every streaming command supports rotation without its own plumbing. When
// --dlq-file is set the result is wrapped in a dead letter queue, so malformed
// records and delivery failures are recorded instead of ending the stream.
// When --batch is set the result is wrapped in a batcher that flushes into the
// underlying sink, so rotation still sees whole batches.
func (s *streamFlags) newSink(
	w io.Writer,
	colorMode string,
	onBatchError func(error),
	onDeadLetter func(sink.DLQEntry),
) (stream.Sink, error) {
	build := func(out io.Writer) stream.Sink {
		format := strings.ToLower(s.format)
		if s.tsv {
			format = "tsv"
		}
		if format == "csv" || format == "tsv" {
			fields := []string(nil)
			if s.csvFields != "" {
				fields = strings.Split(s.csvFields, ",")
			}
			csv, err := sink.NewCSVSink(out, sink.CSVOptions{Fields: fields, Delimiter: map[bool]rune{true: '\t', false: ','}[format == "tsv"]})
			if err != nil {
				return errorSink{err: err}
			}
			return csv
		}
		if s.asJSON {
			return sink.NewNDJSON(out, !s.raw)
		}
		return sink.NewHuman(out, lensformat.PaletteFor(out, colorMode), s.compact || s.raw)
	}

	var out stream.Sink
	if s.output == "" {
		out = build(w)
	} else {
		rotator, err := sink.NewFileRotator(s.output, build, sink.RotatorOptions{
			MaxSize:    s.rotateSize,
			MaxAge:     s.rotateInterval,
			MaxBackups: s.rotateKeep,
			Compress:   s.rotateCompress,
		})
		if err != nil {
			return nil, err
		}
		out = rotator
	}

	base := out
	out, err := s.sinks.addTo(base, !s.raw, onBatchError)
	if err != nil {
		closeSink(base)
		return nil, err
	}

	var dlq *sink.DLQ
	if s.dlqFile != "" {
		file, err := os.OpenFile(s.dlqFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			closeSink(out)
			return nil, fmt.Errorf("opening dead letter queue: %w", err)
		}

		dlq, err = sink.NewDLQ(out, file, sink.DLQOptions{
			CloseWriter:  true,
			OnDeadLetter: onDeadLetter,
		})
		if err != nil {
			_ = file.Close()
			closeSink(out)
			return nil, err
		}
		out = dlq
	}

	if !s.batch {
		return out, nil
	}

	batcher := stream.NewBatcher(out, stream.BatcherOptions{
		MaxRecords: s.batchSize,
		MaxDelay:   s.batchTimeout,
		OnError: func(err error) {
			if s.sinks.metrics != nil {
				s.sinks.metrics.RecordError("sink")
			}
			onBatchError(err)
		},
	})
	if dlq == nil {
		return batcher, nil
	}
	// Batcher.Close stops its timer and flushes, but it does not close what it
	// wraps. Without this pairing, a batched run would leave the dead letter
	// file open and its last buffer unflushed.
	return &closingSink{
		Sink: batcher,
		closeFn: func() error {
			return errors.Join(batcher.Close(), dlq.Close())
		},
	}, nil
}

type errorSink struct{ err error }

func (s errorSink) Emit(stream.Record) error { return s.err }
func (s errorSink) Flush() error             { return s.err }

// closingSink gives a wrapper without its own Close a teardown path. It is how
// a sink chain tells runStream that there is more to release than the outermost
// Batcher knows about.
type closingSink struct {
	stream.Sink
	closeFn func() error
}

// Close runs the teardown registered by the code that built the chain.
func (c *closingSink) Close() error { return c.closeFn() }

// closeSink releases a sink that was built before a later construction step
// failed, so a validation error cannot leak an open file.
func closeSink(s stream.Sink) {
	if closer, ok := s.(io.Closer); ok {
		_ = closer.Close()
	}
}

// options builds the tailer options implied by the flags.
func (s *streamFlags) options(stderr io.Writer, quiet bool) (stream.Options, error) {
	strategy, err := stream.ParseGapStrategy(s.gapStrategy)
	if err != nil {
		return stream.Options{}, err
	}

	// Determine effective quiet mode: flag overrides the quiet parameter.
	effectiveQuiet := s.quiet || quiet

	// verbosity check helper: returns true if the given level should be shown
	// given the effective verbosity level (higher numeric = more verbose).
	showAtLeast := func(level int, minimum int) bool {
		if effectiveQuiet {
			return false
		}
		return level >= minimum
	}

	return stream.Options{
		Follow:       s.follow,
		Limit:        s.limit,
		Filter:       s.filter,
		PollInterval: s.poll,
		MaxRetries:   s.retries,
		// DrainTimeout bounds the final flush, so a sink that cannot be
		// written does not hold the process past the grace period an
		// orchestrator gives it.
		DrainTimeout: s.drainTimeout,
		// Adaptive switches the follow loop to the adaptive poller. Its bounds
		// stay zero so the tailer fills the documented defaults in; the CLI
		// keeps its flag surface to one switch.
		Adaptive:         s.adaptive,
		GapStrategy:      strategy,
		MaxGapRecoveries: s.maxGapRecoveries,
		OnRetry: func(attempt int, delay time.Duration, err error) {
			if s.sinks.metrics != nil {
				s.sinks.metrics.RecordError("rpc")
			}
			if showAtLeast(attempt, s.verbosity) {
				// Reconnection is reported on stderr so it never contaminates a
				// piped NDJSON stream, and so a stalled tail explains itself
				// rather than appearing to hang.
				fmt.Fprintf(stderr, "prism: RPC call failed (%v); retrying in %s (attempt %d)\n",
					err, delay.Round(time.Millisecond), attempt)
			}
		},
		OnGap: func(event stream.GapEvent) {
			if s.sinks.metrics != nil {
				s.sinks.metrics.RecordError("gap")
			}
			if showAtLeast(1, s.verbosity) && event.Recovered {
				fmt.Fprintf(stderr,
					"prism: %v; recovering by resuming at ledger %d\n",
					event.Gap, event.Gap.Oldest)
			}
		},
	}, nil
}

// resolveSince converts the --since window into an approximate start ledger
// against the server's reported head. It returns zero when --since was not
// passed, leaving the caller to apply its own default: --from, a recorded
// cursor, or the server's head.
//
// The window is resolved against the head at the moment of the call, so the
// ledger it names is genuinely "that long ago" rather than that long ago
// whenever the stream happened to start.
func (s *streamFlags) resolveSince(cmd *cobra.Command, client rpc.Client, stderr io.Writer) (uint32, error) {
	if !cmd.Flags().Changed("since") {
		return 0, nil
	}
	latest, err := latestLedger(cmd.Context(), client)
	if err != nil {
		return 0, err
	}
	start := stream.StartLedgerForWindow(latest, s.since)
	// The resolution is informational and approximate by design, so say
	// so on stderr, where it never contaminates a piped --json stream.
	fmt.Fprintf(stderr, "prism: --since %s resolves to start ledger %d (head %d, assuming a %s ledger cadence)\n",
		s.since, start, latest, stream.RoughLedgerCloseInterval)
	return start, nil
}

func newEventsCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var (
		sf            streamFlags
		contractIDs   []string
		topics        []string
		eventTypes    []string
		filterExpr    string
		topicRegex    []string
		contractsFile string
	)

	cmd := &cobra.Command{
		Use:   "events",
		Short: "Stream Soroban contract events",
		Long: `Stream Soroban contract events, decoded as they arrive.

Without --follow this drains what the server already has and exits, which is
useful in a pipeline. With --follow it keeps going, reconnecting through
dropped connections without losing or repeating events.

Topic filters are evaluated by the server and use its wildcard syntax: "*"
matches exactly one segment, and "**" matches zero or more and must come
last. The semantics are identical to ` + "`stellar events --topic`" + `.

Use --filter-expr for client-side predicates over decoded values and topics,
or --topic-regex for regular expressions over decoded topic symbols and
strings. --contracts-file reads a hot-reloaded watchlist; addresses beyond
the RPC limit of five are queried in coordinated groups.

Events are retained by RPC for roughly 24 hours, so --from cannot reach
further back than that. If the stream ever falls behind that window, prism
reports the missed ledger range and exits 2 rather than skipping silently.

With --state-file the position reached is recorded after every page, and a
background pruner keeps that file from growing without bound. Add --resume to
start the next run where the last one stopped.`,
		Example: `  # Follow one contract
  prism events --contract CBGSB... --follow

  # Filter by topic, taking the last 20 and exiting
  prism events --contract CBGSB... --topic 'AAAAAQ==,*' --limit 20

  # NDJSON into jq
  prism events --contract CBGSB... --follow --json | jq -r '.contract'

  # Filter decoded payload values locally
  prism events --filter-expr 'value.amount > 1000000000 && topics[0].symbol == "transfer"'

  # Match related topic names and hot-reload a larger contract set
  prism events --contracts-file /etc/prism/watchlist.txt --topic-regex '0:^transfer.*,1:.*' --follow

  # Backfill from a ledger then keep following
  prism events --from 1234567 --follow`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := sf.validate(cmd); err != nil {
				return err
			}
			filters := stream.FilterSet{}
			if filterExpr != "" {
				compiled, err := stream.CompileExpression(filterExpr)
				if err != nil {
					return err
				}
				filters = append(filters, compiled)
			}
			if len(topicRegex) > 0 {
				compiled, err := stream.CompileTopicRegexFilter(topicRegex)
				if err != nil {
					return err
				}
				filters = append(filters, compiled)
			}
			if len(filters) > 0 {
				sf.filter = filters
			}
			var watchlist *stream.WatchlistManager
			var cancelWatch context.CancelFunc
			if contractsFile != "" {
				if len(contractIDs) != 0 {
					return fmt.Errorf("--contract cannot be combined with --contracts-file")
				}
				var err error
				watchlist, err = stream.NewWatchlistManager(contractsFile)
				if err != nil {
					return err
				}
				watchCtx, cancel := context.WithCancel(cmd.Context())
				cancelWatch = cancel
				defer cancelWatch()
				if err := watchlist.Start(watchCtx); err != nil {
					return err
				}
				go func() {
					for err := range watchlist.Errors() {
						fmt.Fprintf(stderr, "prism: %v\n", err)
					}
				}()
			}

			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()

			sf.stateKey = eventsStateKey(contractIDs, topics, eventTypes)

			newSource := func(startLedger uint32) (stream.Source, error) {
				query := stream.EventQuery{
					ContractIDs: contractIDs,
					Topics:      topics,
					EventTypes:  eventTypes,
					StartLedger: startLedger,
					PageLimit:   sf.pageSize,
				}
				if watchlist != nil {
					query.Watchlist = watchlist
				}
				return stream.NewEventSource(client, query)
			}

			return runStream(cmd, client, newSource, sf.from, &sf, g, stdout, stderr)
		},
	}

	cmd.Flags().StringSliceVar(&contractIDs, "contract", nil,
		"contract ID to watch; repeatable, up to 5")
	// StringArray, not StringSlice: a comma separates segments *within* one
	// topic filter, so comma-splitting the flag would turn a single
	// two-segment filter into two unrelated single-segment ones.
	cmd.Flags().StringArrayVar(&topics, "topic", nil,
		`topic filter as comma-separated base64 ScVals, with "*" and "**" wildcards; repeatable, up to 5`)
	cmd.Flags().StringSliceVar(&eventTypes, "type", nil,
		"event type to include: contract, system or diagnostic (default: all)")
	cmd.Flags().StringVar(&filterExpr, "filter-expr", "", "client-side boolean expression evaluated against decoded event payloads")
	cmd.Flags().StringArrayVar(&topicRegex, "topic-regex", nil, "client-side topic rule as segment-index:regexp; repeatable and all rules must match")
	cmd.Flags().StringVar(&contractsFile, "contracts-file", "", "read and hot-reload contract IDs from this file (one per line)")
	// StringArray, not StringSlice: a comma separates segment *patterns*
	// within one filter, which StringSlice's comma-splitting would destroy.
	cmd.Flags().StringArrayVar(&sf.topicPatterns, "topic-pattern", nil,
		`client-side topic pattern as comma-separated segments over decoded topics: "*", "**", "prefix:TEXT", "re:REGEX", or plain text with "*" and "?" globs; repeatable, up to 5`)
	cmd.Flags().StringVar(&sf.fanoutDir, "fanout-dir", "",
		"write each watched contract's events to its own NDJSON file in this directory, in addition to the main output")
	cmd.Flags().IntVar(&sf.dedupWindow, "dedup-window", 0,
		"drop re-polled duplicate events, remembering this many recent fingerprints (0 disables; 4096 covers a page with margin)")
	sf.register(cmd, stream.DefaultPageLimit)
	registerFlagCompletion(cmd, "type", "contract", "system", "diagnostic")
	registerFlagCompletion(cmd, "gap-strategy", string(stream.GapFail), string(stream.GapResume))

	return cmd
}

func newLedgersCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var sf streamFlags
	var consensus bool
	var expected []string

	cmd := &cobra.Command{
		Use:   "ledgers",
		Short: "Stream ledger close metadata",
		Long: `Stream ledger close metadata as ledgers close.

Ledger metadata is large, so --compact is often what you want when watching
for liveness rather than reading contents.

With --state-file the position reached is recorded after every page, and a
background pruner keeps that file from growing without bound. Add --resume to
start the next run where the last one stopped.`,
		Example: `  prism ledgers --follow --compact
  prism ledgers --from 1234567 --limit 5 --json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(expected) > 0 && !consensus {
				return fmt.Errorf("--expected-validator requires --consensus")
			}
			if consensus && sf.backfillWorkers > 1 {
				return fmt.Errorf("--consensus requires --backfill-workers 1 for consecutive close deltas")
			}
			if err := sf.validate(cmd); err != nil {
				return err
			}

			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()

			sf.stateKey = "ledgers"

			newSource := func(startLedger uint32) (stream.Source, error) {
				return stream.NewLedgerSource(client, stream.LedgerQuery{
					StartLedger:        startLedger,
					PageLimit:          sf.pageSize,
					Consensus:          consensus,
					ExpectedValidators: expected,
				}), nil
			}
			return runStream(cmd, client, newSource, sf.from, &sf, g, stdout, stderr)
		},
	}

	cmd.Flags().BoolVar(&consensus, "consensus", false, "extract signed consensus value and close delta diagnostics (headers do not prove quorum health)")
	cmd.Flags().StringSliceVar(&expected, "expected-validator", nil, "expected value signer address; repeatable, requires --consensus")
	sf.register(cmd, stream.DefaultLedgerPageLimit)
	registerFlagCompletion(cmd, "gap-strategy", string(stream.GapFail), string(stream.GapResume))
	return cmd
}

// sourceFactory builds an independent source positioned at a ledger.
type sourceFactory = stream.SourceFactory

// runStream wires a source to a sink and runs the tailer until it finishes or
// the operator interrupts it. When backfill workers are configured it drains
// the historical range concurrently before following live records.
//
// from is the ledger the operator asked for, or zero when they did not ask
// for one. A zero means "start at the server's head", unless a recorded
// cursor says otherwise.
func runStream(
	cmd *cobra.Command,
	client rpc.Client,
	newSource sourceFactory,
	from uint32,
	sf *streamFlags,
	g *globalFlags,
	stdout, stderr io.Writer,
) (err error) {
	if err := sf.validate(cmd); err != nil {
		return err
	}
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	defer func() {
		if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
			err = cause
		}
	}()

	// Diagnostics come from more than one goroutine: the stream reports
	// retries and gaps, and the background cursor pruner reports what it
	// compacted. The writer is serialised so those lines cannot interleave
	// and so a test's stderr buffer cannot race.
	stderr = newSyncWriter(stderr)

	// Ctrl-C ends the stream cleanly rather than killing it mid-record, so a
	// partially written line never reaches a downstream consumer. The signal
	// stops the fetching; the drain at the end of the run flushes what is
	// left and commits the final cursor before the process goes away.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	go func() {
		select {
		case <-signals:
			fmt.Fprintf(stderr, "prism: interrupted; draining for up to %s\n", sf.drainTimeout)
			cancel(nil)
		case <-ctx.Done():
		}
	}()

	out, err := sf.newSink(stdout, g.color, func(err error) {
		fmt.Fprintf(stderr, "prism: batch flush failed: %v\n", err)
	}, func(entry sink.DLQEntry) {
		record := "-"
		if entry.Record != nil {
			record = entry.Record.ID
		}
		fmt.Fprintf(stderr, "prism: dead-lettered record %s (%s): %s\n",
			record, entry.Stage, entry.Error)
	})
	if err != nil {
		return err
	}
	if sf.sinks.metrics != nil {
		if addr := sf.sinks.metrics.Addr(); addr != "" {
			fmt.Fprintf(stderr, "prism: serving metrics on http://%s/metrics\n", addr)
		}
	}
	localOut := out
	// The output is closed exactly once, whether the run ends normally, is
	// interrupted or fails: the drain closes it, and the deferred teardown
	// closes it when the drain never ran. A second close would report a
	// file that is already closed as an error.
	var closeOutOnce once
	closeOut := func() error {
		if !closeOutOnce.claim() {
			return nil
		}
		closer, ok := localOut.(io.Closer)
		if !ok {
			return nil
		}
		return closer.Close()
	}
	var shutdown func()
	out, shutdown, err = startBroadcasts(ctx, g, out, stderr, cancel)
	if err != nil {
		if closer, ok := localOut.(io.Closer); ok {
			_ = closer.Close()
		}
		return err
	}
	defer shutdown()
	defer func() {
		// A failure to close the output is joined with whatever else
		// went wrong rather than replacing it: two causes are more
		// useful than one, and neither is worth losing.
		err = errors.Join(err, closeOut())
	}()

	// The de-duplicator sits between the tailer and the output shaping, so
	// a re-polled event is suppressed for every sink downstream — including
	// a fanout directory — without any of them knowing about it.
	if sf.dedupWindow > 0 {
		out = stream.NewDedupSink(out, sf.dedupWindow)
	}

	if len(sf.topicPatterns) > 0 {
		patterns, err := stream.CompileTopicPatterns(sf.topicPatterns)
		if err != nil {
			return err
		}
		out = stream.NewPatternSink(out, patterns)
	}

	// The fanout shares this stream's records: the demux inside writes one
	// NDJSON file per contract while stdout keeps the combined stream.
	var fan *fanoutSink
	if sf.fanoutDir != "" {
		f, err := newFanoutSink(sf.fanoutDir)
		if err != nil {
			return err
		}
		fan = f
		out = &teeSink{main: out, second: fan}
		defer func() {
			// Wind the demux down whatever happened above: it drains, flushes
			// and closes every per-contract file, and joins any sink error
			// with err so a fanout failure still fails the command.
			err = errors.Join(err, fan.finish())
		}()
	}

	// The cursor metadata file is opened before the stream starts, so a
	// recorded cursor is known before the first fetch and the pruner is
	// running for the whole of the run.
	state, err := openCursorState(ctx, sf, sf.stateKey, stderr)
	if err != nil {
		return err
	}
	var closeStateOnce once
	closeState := func() error {
		if !closeStateOnce.claim() {
			return nil
		}
		// The context may already be cancelled by an interrupt, but the
		// final compaction is local work that should still happen.
		return state.close(context.WithoutCancel(ctx))
	}
	defer func() {
		err = errors.Join(err, closeState())
	}()

	opts, err := sf.options(stderr, sf.quiet)
	if err != nil {
		return err
	}
	opts.Process = sf.process

	// The health server is started before the stream so the probes answer
	// from the first moment, rather than only once the stream is running.
	healthServer, health, err := startHealthServer(ctx, sf, stderr)
	if err != nil {
		return err
	}
	if healthServer != nil {
		defer func() {
			err = errors.Join(err, healthServer.Close())
		}()
	}
	if health != nil {
		onRetry := opts.OnRetry
		opts.OnRetry = func(attempt int, delay time.Duration, retryErr error) {
			// A failed call is proof that the endpoint did not answer.
			health.SetRPCReachable(false)
			if onRetry != nil {
				onRetry(attempt, delay, retryErr)
			}
		}
		onGap := opts.OnGap
		opts.OnGap = func(event stream.GapEvent) {
			health.SetGapped(true)
			if onGap != nil {
				onGap(event)
			}
		}
		opts.OnProgress = func(progress stream.Progress) {
			health.RecordProgress(progress)
		}
	}

	// The lease makes this instance the leader or a standby. Only the leader
	// fetches, so two instances sharing a state file never emit the same
	// records twice.
	election, err := leaderElection(ctx, sf, health, stderr)
	if err != nil {
		return err
	}
	if election != nil {
		defer func() {
			// Releasing the lease lets a standby take over at once
			// instead of waiting out the TTL.
			err = errors.Join(err, election.Close())
		}()
		opts.Leader = election
		if state != nil {
			// The cursor the previous leader committed is where this
			// one picks the stream up.
			opts.LeaderResume = func(resumeCtx context.Context) (string, error) {
				meta, found, err := state.store.Latest(resumeCtx, state.key)
				if err != nil {
					return "", err
				}
				if !found {
					return "", nil
				}
				return meta.Cursor, nil
			}
		}
	}

	// --since is an explicit position like --from, so it is resolved here and
	// overrides whatever the caller passed.
	since, err := sf.resolveSince(cmd, client, stderr)
	if err != nil {
		return err
	}
	if since != 0 {
		from = since
	}

	if state != nil {
		// An explicit --from or --since wins over a recorded cursor: the
		// operator asked for a position, so it is the position that counts.
		if from == 0 {
			opts.StartCursor = state.cursor
		}
		opts.OnPage = func(page stream.Page) {
			if recordErr := state.record(ctx, page); recordErr != nil {
				fmt.Fprintf(stderr, "prism: recording cursor metadata: %v\n", recordErr)
			}
		}
	}

	startFrom := from
	if startFrom == 0 && opts.StartCursor == "" {
		// Starting "now" means asking the server where now is; the
		// RPC requires a concrete start ledger.
		latest, err := latestLedger(ctx, client)
		if err != nil {
			return err
		}
		startFrom = latest
	}

	if from > 0 && sf.backfillWorkers > 1 {
		drainer, err := stream.NewDrainer(stream.DrainerConfig{
			NewSource:        newSource,
			Sink:             out,
			Filter:           sf.filter,
			From:             from,
			Workers:          sf.backfillWorkers,
			Limit:            sf.limit,
			GapStrategy:      opts.GapStrategy,
			MaxGapRecoveries: opts.MaxGapRecoveries,
			Backoff:          opts.Backoff,
			MaxRetries:       opts.MaxRetries,
			Clock:            opts.Clock,
			OnRetry:          opts.OnRetry,
			OnGap:            opts.OnGap,
			OnPage:           opts.OnPage,
		})
		if err != nil {
			return err
		}

		stats, err := drainer.Run(ctx)
		if err != nil {
			return reportStreamError(err, stderr)
		}
		if sf.limit > 0 {
			remaining := sf.limit - stats.Emitted
			if remaining <= 0 {
				return nil
			}
			opts.Limit = remaining
		}
		if !sf.follow {
			return nil
		}
		if drainer.Until() >= from {
			if drainer.Until() == ^uint32(0) {
				return fmt.Errorf("cannot follow beyond the maximum ledger sequence")
			}
			startFrom = drainer.Until() + 1
		}
	}

	src, err := newSource(startFrom)
	if err != nil {
		return err
	}
	tailer := stream.NewTailer(src, out, opts)
	runErr := tailer.Run(ctx)

	// The stream has stopped fetching, either because it finished or
	// because it was interrupted. Everything it is still holding is
	// flushed now, within the drain timeout, so the final cursor is
	// committed before the process goes away.
	drainErr := runWithTimeout(sf.drainTimeout, func() error {
		var errs []error
		if err := tailer.Drain(context.WithoutCancel(ctx), sf.drainTimeout); err != nil {
			errs = append(errs, err)
		}
		if err := closeOut(); err != nil {
			errs = append(errs, err)
		}
		if err := closeState(); err != nil {
			errs = append(errs, err)
		}
		return errors.Join(errs...)
	})
	if drainErr != nil {
		fmt.Fprintf(stderr, "prism: draining the stream: %v\n", drainErr)
		if runErr == nil {
			runErr = drainErr
		}
	}
	return reportStreamError(runErr, stderr)
}

func reportStreamError(err error, stderr io.Writer) error {
	var gap *stream.GapError
	if errors.As(err, &gap) {
		fmt.Fprintf(stderr, "prism: %v\n", gap)
		fmt.Fprintf(stderr,
			"prism: restart with --from %d to resume from the oldest data the server still has\n",
			gap.Oldest)
		return &ExitCodeError{Code: 2, Message: gap.Error()}
	}
	return err
}

// latestLedger asks the server where the head is, so a stream with no --from
// can start at "now".
func latestLedger(ctx context.Context, client rpc.Client) (uint32, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	resp, err := client.GetLatestLedger(ctx)
	if err != nil {
		return 0, fmt.Errorf("determining the latest ledger: %w", err)
	}
	if resp.Sequence == 0 {
		return 0, fmt.Errorf("server did not report a latest ledger; pass --from to choose a start point")
	}
	return resp.Sequence, nil
}
