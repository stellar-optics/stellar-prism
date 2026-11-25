package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// newAggregateCmd constructs the "prism aggregate" command.
//
// See docs/guides/multi_stream_aggregation.md for tumbling vs. sliding
// window semantics, supported metrics, and throughput analytics pipelines.
func newAggregateCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var (
		sf          streamFlags
		contractIDs []string
		topics      []string
		eventTypes  []string
		windowStr   string
		metrics     []string
	)

	cmd := &cobra.Command{
		Use:   "aggregate",
		Short: "Aggregate events over time windows",
		Long: `Aggregate Soroban contract events into time-windowed metrics.

Supported window durations: 10s, 30s, 1m, 5m, 15m, 1h (e.g., --window 1m)

Supported metrics:
  count                    - number of events in window
  sum:<field>              - sum of numeric field (e.g., sum:value.amount)
  avg:<field>              - average of numeric field
  min:<field>              - minimum of numeric field
  max:<field>              - maximum of numeric field
  unique_count:<field>     - count of unique values for field (e.g., unique_count:topics[0].symbol)

Multiple --metric flags can be specified. Each window emits one aggregate record per contract.
`,
		Example: `  # Count events per contract per minute
  prism aggregate --contract CBGSB... --window 1m --metric count

  # Sum and avg of transfer amounts over 5-minute windows
  prism aggregate --contract CBGSB... --window 5m --metric "sum:value.amount" --metric "avg:value.amount"

  # Unique senders per contract per hour
  prism aggregate --contract CBGSB... --window 1h --metric "unique_count:value.from"`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := sf.validate(cmd); err != nil {
				return err
			}
			if len(contractIDs) == 0 {
				return fmt.Errorf("--contract is required")
			}
			if windowStr == "" {
				return fmt.Errorf("--window is required")
			}
			if len(metrics) == 0 {
				return fmt.Errorf("at least one --metric is required")
			}

			window, err := parseWindow(windowStr)
			if err != nil {
				return fmt.Errorf("invalid --window: %w", err)
			}

			aggMetrics, err := parseMetrics(metrics)
			if err != nil {
				return fmt.Errorf("invalid --metric: %w", err)
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
				return stream.NewEventSource(client, query)
			}

			parts := []string{fromKeyPart(sf.from)}
			for _, c := range contractIDs {
				parts = append(parts, "contract="+c)
			}
			for _, topic := range topics {
				parts = append(parts, "topic="+topic)
			}
			for _, et := range eventTypes {
				parts = append(parts, "type="+et)
			}
			sf.derivedKey = cursorKey(cmd.Name(), parts...)

			return runAggregate(cmd, client, newSource, sf.from, &sf, g, stdout, stderr, window, aggMetrics)
		},
	}

	cmd.Flags().StringSliceVar(&contractIDs, "contract", nil,
		"contract ID to watch; repeatable, up to 5")
	cmd.Flags().StringArrayVar(&topics, "topic", nil,
		`topic filter as comma-separated base64 ScVals, with "*" and "**" wildcards; repeatable, up to 5`)
	cmd.Flags().StringSliceVar(&eventTypes, "type", nil,
		"event type to include: contract, system or diagnostic (default: all)")
	cmd.Flags().StringVar(&windowStr, "window", "", "time window for aggregation (e.g., 10s, 1m, 5m, 1h)")
	cmd.Flags().StringArrayVar(&metrics, "metric", nil,
		"metric to compute: count, sum:<field>, avg:<field>, min:<field>, max:<field>, unique_count:<field>; repeatable")

	sf.register(cmd, stream.DefaultPageLimit)
	registerFlagCompletion(cmd, "type", "contract", "system", "diagnostic")
	registerFlagCompletion(cmd, "gap-strategy", string(stream.GapFail), string(stream.GapResume))

	return cmd
}

type aggMetric = stream.AggMetric

func parseMetrics(specs []string) ([]aggMetric, error) {
	var out []aggMetric
	for _, s := range specs {
		parts := strings.SplitN(s, ":", 2)
		name := parts[0]
		field := ""
		if len(parts) == 2 {
			field = parts[1]
		}
		switch name {
		case "count":
			if field != "" {
				return nil, fmt.Errorf("count metric does not take a field")
			}
		case "sum", "avg", "min", "max", "unique_count":
			if field == "" {
				return nil, fmt.Errorf("%s metric requires a field (e.g., %s:value.amount)", name, name)
			}
		default:
			return nil, fmt.Errorf("unknown metric %q", name)
		}
		out = append(out, aggMetric{Name: name, Field: field})
	}
	return out, nil
}

func parseWindow(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if len(s) < 2 {
		return 0, fmt.Errorf("window must be like 10s, 1m, 5m, 1h")
	}
	unit := s[len(s)-1]
	valStr := s[:len(s)-1]
	val, err := strconv.Atoi(valStr)
	if err != nil {
		return 0, fmt.Errorf("invalid window value: %w", err)
	}
	switch unit {
	case 's':
		return time.Duration(val) * time.Second, nil
	case 'm':
		return time.Duration(val) * time.Minute, nil
	case 'h':
		return time.Duration(val) * time.Hour, nil
	default:
		return 0, fmt.Errorf("window unit must be s, m, or h")
	}
}

func runAggregate(
	cmd *cobra.Command,
	client rpc.Client,
	newSource stream.SourceFactory,
	from uint32,
	sf *streamFlags,
	g *globalFlags,
	stdout, stderr io.Writer,
	window time.Duration,
	metrics []aggMetric,
) error {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	var err error
	defer func() {
		if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
			err = cause
		}
	}()

	if !g.skipNetworkCheck {
		networkClient, ok := client.(rpc.NetworkClient)
		if ok {
			if err := rpc.ValidateNetwork(ctx, networkClient, g.network); err != nil {
				return err
			}
		}
	}

	stderr = newSyncWriter(stderr)

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

	// Build output sink
	var out stream.Sink
	format := strings.ToLower(sf.format)
	if format == "csv" || format == "tsv" {
		delim := ','
		if format == "tsv" {
			delim = '\t'
		}
		csvSink, err := sink.NewCSVSink(stdout, sink.CSVOptions{
			Fields:    []string{"window_start", "window_end", "contract", "metric", "value"},
			Delimiter: delim,
		})
		if err != nil {
			return err
		}
		out = csvSink
	} else if sf.asJSON || format == "ndjson" {
		out = sink.NewNDJSON(stdout, true)
	} else {
		out = sink.NewHuman(stdout, lensformat.PaletteFor(stdout, g.color), false)
	}

	// The cursor metadata file
	state, err := openCursorState(ctx, sf, sf.stateKey, stderr)
	if err != nil {
		return err
	}
	var closeStateOnce once
	closeState := func() error {
		if !closeStateOnce.claim() {
			return nil
		}
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

	// Health server
	healthServer, health, err := startHealthServer(ctx, sf, stderr)
	if err != nil {
		return err
	}
	if healthServer != nil {
		defer func() {
			err = errors.Join(err, healthServer.Close())
		}()
	}

	// Leader election
	election, err := leaderElection(ctx, sf, health, stderr)
	if err != nil {
		return err
	}
	if election != nil {
		defer func() {
			err = errors.Join(err, election.Close())
		}()
		opts.Leader = election
	}

	// Cursor store
	key := sf.derivedKey
	if sf.pgCursorKey != "" {
		key = sf.pgCursorKey
	}
	dsn := sf.pgDSN
	if dsn == "" {
		dsn = os.Getenv(envPGDSN)
	}
	store, closeStore, err := openCursorStore(ctx, dsn)
	if err != nil {
		return err
	}
	if closeStore != nil {
		defer func() {
			if closeErr := closeStore(); closeErr != nil {
				fmt.Fprintf(stderr, "prism: closing the PostgreSQL cursor store: %v\n", closeErr)
			}
		}()
	}
	opts.Store = store
	opts.StoreKey = key

	// Start position
	startFrom := from
	if startFrom == 0 && opts.StartCursor == "" {
		if opts.State != nil {
			saved, err := opts.State.Load()
			if err == nil && saved.PagingToken != "" && saved.Ledger != 0 {
				startFrom = saved.Ledger
			}
		}
		if startFrom == 0 {
			latest, err := latestLedger(ctx, client)
			if err != nil {
				return err
			}
			startFrom = latest
		}
	}

	src, err := newSource(startFrom)
	if err != nil {
		return err
	}

	// Wrap with aggregator
	aggSrc := stream.NewAggregateSource(src, window, metrics)

	tailer := stream.NewTailer(aggSrc, out, opts)
	summary := newRunStats(sf.stats)
	summary.start()
	runErr := tailer.Run(ctx)

	drainErr := runWithTimeout(sf.drainTimeout, func() error {
		var errs []error
		if err := tailer.Drain(context.WithoutCancel(ctx), sf.drainTimeout); err != nil {
			errs = append(errs, err)
		}
		if err := closeOut(out); err != nil {
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
	summary.add(tailer.Stats())
	return reportStreamError(runErr, stderr, store, key)
}

func closeOut(out stream.Sink) error {
	if closer, ok := out.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return out.Flush()
}
