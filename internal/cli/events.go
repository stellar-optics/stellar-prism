package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
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
	limit            int
	asJSON           bool
	raw              bool
	compact          bool
	poll             time.Duration
	pageSize         uint
	retries          int
	backfillWorkers  int
	batch            bool
	batchSize        int
	batchTimeout     time.Duration
	gapStrategy      string
	maxGapRecoveries int
	quiet            bool
	verbosity        int

	output         string
	rotateSize     int64
	rotateInterval time.Duration
	rotateCompress bool
	rotateKeep     int
}

func (s *streamFlags) register(cmd *cobra.Command, defaultPage uint) {
	cmd.Flags().BoolVarP(&s.follow, "follow", "f", false,
		"keep streaming as new ledgers close, like tail -f")
	cmd.Flags().Uint32Var(&s.from, "from", 0,
		"start from this ledger and continue forward (default: the latest ledger)")
	cmd.Flags().IntVarP(&s.limit, "limit", "n", 0,
		"stop after this many records (0 means unlimited)")
	cmd.Flags().BoolVar(&s.asJSON, "json", false,
		"emit NDJSON, one record per line")
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
}

func (s *streamFlags) validate() error {
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
	return nil
}

// newSink builds the output sink implied by the flags.
//
// With --output-file the chosen format is built on top of a rotating writer,
// so every streaming command supports rotation without its own plumbing. When
// --batch is set the result is wrapped in a batcher that flushes into the
// underlying sink, so rotation still sees whole batches.
func (s *streamFlags) newSink(w io.Writer, colorMode string, onBatchError func(error)) (stream.Sink, error) {
	build := func(out io.Writer) stream.Sink {
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

	if !s.batch {
		return out, nil
	}
	return stream.NewBatcher(out, stream.BatcherOptions{
		MaxRecords: s.batchSize,
		MaxDelay:   s.batchTimeout,
		OnError:    onBatchError,
	}), nil
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
		Follow:           s.follow,
		Limit:            s.limit,
		PollInterval:     s.poll,
		MaxRetries:       s.retries,
		GapStrategy:      strategy,
		MaxGapRecoveries: s.maxGapRecoveries,
		OnRetry: func(attempt int, delay time.Duration, err error) {
			if showAtLeast(attempt, s.verbosity) {
				// Reconnection is reported on stderr so it never contaminates a
				// piped NDJSON stream, and so a stalled tail explains itself
				// rather than appearing to hang.
				fmt.Fprintf(stderr, "prism: RPC call failed (%v); retrying in %s (attempt %d)\n",
					err, delay.Round(time.Millisecond), attempt)
			}
		},
		OnGap: func(event stream.GapEvent) {
			if showAtLeast(1, s.verbosity) && event.Recovered {
				fmt.Fprintf(stderr,
					"prism: %v; recovering by resuming at ledger %d\n",
					event.Gap, event.Gap.Oldest)
			}
		},
	}, nil
}

func newEventsCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var (
		sf          streamFlags
		contractIDs []string
		topics      []string
		eventTypes  []string
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

Events are retained by RPC for roughly 24 hours, so --from cannot reach
further back than that. If the stream ever falls behind that window, prism
reports the missed ledger range and exits 2 rather than skipping silently.`,
		Example: `  # Follow one contract
  prism events --contract CBGSB... --follow

  # Filter by topic, taking the last 20 and exiting
  prism events --contract CBGSB... --topic 'AAAAAQ==,*' --limit 20

  # NDJSON into jq
  prism events --contract CBGSB... --follow --json | jq -r '.contract'

  # Backfill from a ledger then keep following
  prism events --from 1234567 --follow`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := sf.validate(); err != nil {
				return err
			}

			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()

			from := sf.from
			if from == 0 {
				// Starting "now" means asking the server where now is; the
				// RPC requires a concrete start ledger.
				latest, err := latestLedger(cmd.Context(), client)
				if err != nil {
					return err
				}
				from = latest
			}

			newSource := func(startLedger uint32) (stream.Source, error) {
				return stream.NewEventSource(client, stream.EventQuery{
					ContractIDs: contractIDs,
					Topics:      topics,
					EventTypes:  eventTypes,
					StartLedger: startLedger,
					PageLimit:   sf.pageSize,
				})
			}

			return runStream(cmd.Context(), newSource, from, &sf, g, stdout, stderr)
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
	sf.register(cmd, stream.DefaultPageLimit)
	registerFlagCompletion(cmd, "type", "contract", "system", "diagnostic")
	registerFlagCompletion(cmd, "gap-strategy", string(stream.GapFail), string(stream.GapResume))

	return cmd
}

func newLedgersCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var sf streamFlags

	cmd := &cobra.Command{
		Use:   "ledgers",
		Short: "Stream ledger close metadata",
		Long: `Stream ledger close metadata as ledgers close.

Ledger metadata is large, so --compact is often what you want when watching
for liveness rather than reading contents.`,
		Example: `  prism ledgers --follow --compact
  prism ledgers --from 1234567 --limit 5 --json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := sf.validate(); err != nil {
				return err
			}

			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()

			from := sf.from
			if from == 0 {
				latest, err := latestLedger(cmd.Context(), client)
				if err != nil {
					return err
				}
				from = latest
			}

			newSource := func(startLedger uint32) (stream.Source, error) {
				return stream.NewLedgerSource(client, stream.LedgerQuery{
					StartLedger: startLedger,
					PageLimit:   sf.pageSize,
				}), nil
			}
			return runStream(cmd.Context(), newSource, from, &sf, g, stdout, stderr)
		},
	}

	sf.register(cmd, stream.DefaultLedgerPageLimit)
	registerFlagCompletion(cmd, "gap-strategy", string(stream.GapFail), string(stream.GapResume))
	return cmd
}

// sourceFactory builds an independent source positioned at a ledger.
type sourceFactory = stream.SourceFactory

// runStream wires a source to a sink and runs the tailer until it finishes or
// the operator interrupts it. When backfill workers are configured it drains
// the historical range concurrently before following live records.
func runStream(
	ctx context.Context,
	newSource sourceFactory,
	from uint32,
	sf *streamFlags,
	g *globalFlags,
	stdout, stderr io.Writer,
) (err error) {
	if err := sf.validate(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Ctrl-C ends the stream cleanly rather than killing it mid-record, so a
	// partially written line never reaches a downstream consumer.
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	out, err := sf.newSink(stdout, g.color, func(err error) {
		fmt.Fprintf(stderr, "prism: batch flush failed: %v\n", err)
	})
	if err != nil {
		return err
	}
	if closer, ok := out.(io.Closer); ok {
		defer func() {
			closeErr := closer.Close()
			if closeErr == nil {
				return
			}
			if err == nil {
				err = closeErr
				return
			}
		}()
	}

	opts, err := sf.options(stderr, sf.quiet)
	if err != nil {
		return err
	}

	if sf.from > 0 && sf.backfillWorkers > 1 {
		drainer, err := stream.NewDrainer(stream.DrainerConfig{
			NewSource:        newSource,
			Sink:             out,
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
			from = drainer.Until() + 1
		}
	}

	src, err := newSource(from)
	if err != nil {
		return err
	}
	tailer := stream.NewTailer(src, out, opts)
	return reportStreamError(tailer.Run(ctx), stderr)
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
