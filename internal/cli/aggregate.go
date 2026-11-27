package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
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
		dsn = sf.statePG
	}
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

// validateAggregate checks the events-only --aggregate flags.
func (s *streamFlags) validateAggregate(cmd *cobra.Command) error {
	if !s.aggregate {
		// Only commands that define --aggregate (events, serve) own these
		// flags; prism aggregate has an unrelated --window of its own.
		if cmd.Flags().Lookup("aggregate") == nil {
			return nil
		}
		for _, name := range []string{"window", "aggregate-interval"} {
			if f := cmd.Flags().Lookup(name); f != nil && f.Changed {
				return fmt.Errorf("--%s requires --aggregate", name)
			}
		}
		return nil
	}
	if s.aggregateWindow <= 0 {
		return fmt.Errorf("--window must be positive")
	}
	if s.aggregateEvery <= 0 {
		return fmt.Errorf("--aggregate-interval must be positive")
	}
	if s.format != "" || s.tsv || s.csvFields != "" {
		return fmt.Errorf("--aggregate prints window summaries and cannot be combined with CSV/TSV output")
	}
	return nil
}

// windowAggregateSink feeds every event into a sliding time window and
// prints a summary of the window on a timer, in place of the events
// themselves: prism events --aggregate --window 1m.
//
// The window runs on the stream's event clock: the close time of the newest
// event seen, advanced by the wall time elapsed since it arrived. On a live
// --follow stream that is the wall clock, so throughput decays to zero when
// traffic stops; while backfilling history it tracks the ledgers being
// replayed, so the summary describes them rather than an empty present.
type windowAggregateSink struct {
	out    io.Writer
	json   bool
	now    func() time.Time
	window time.Duration

	mu          sync.Mutex
	buf         *stream.SlidingWindowBuffer
	newestEvent time.Time
	newestAt    time.Time
	err         error

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// newWindowAggregateSink starts a sink that prints every interval. now is
// injectable for tests; nil uses the wall clock.
func newWindowAggregateSink(out io.Writer, window, every time.Duration, asJSON bool, now func() time.Time) *windowAggregateSink {
	if now == nil {
		now = time.Now
	}
	buf, err := stream.NewSlidingWindowBuffer(window)
	if err != nil {
		// validateAggregate rejects a non-positive window before this
		// point; reaching here is a programming error.
		panic(err)
	}
	s := &windowAggregateSink{
		out:    out,
		json:   asJSON,
		now:    now,
		window: window,
		buf:    buf,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go s.loop(every)
	return s
}

func (s *windowAggregateSink) loop(every time.Duration) {
	defer close(s.done)
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.print()
		}
	}
}

// Emit implements stream.Sink.
func (s *windowAggregateSink) Emit(r stream.Record) error {
	if r.Kind != stream.KindEvent || r.Event == nil {
		return nil
	}
	now := s.now()
	at := r.Time
	if at.IsZero() {
		at = now
	}
	sender := stream.EventSender(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	if !at.Before(s.newestEvent) {
		s.newestEvent, s.newestAt = at, now
	}
	s.buf.Add(at, sender)
	return s.err
}

// Flush implements stream.Sink. Summaries are written as they are printed,
// so there is nothing buffered; a failed write is reported here.
func (s *windowAggregateSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close stops the timer and prints a final summary, so a bounded run (or an
// interrupted one) always ends with the window it last saw.
func (s *windowAggregateSink) Close() error {
	s.closeOnce.Do(func() {
		close(s.stop)
		<-s.done
		s.print()
	})
	return s.Flush()
}

// clock returns the event-clock reference for a snapshot.
func (s *windowAggregateSink) clock(now time.Time) time.Time {
	if s.newestEvent.IsZero() {
		return now
	}
	return s.newestEvent.Add(now.Sub(s.newestAt))
}

func (s *windowAggregateSink) print() {
	s.mu.Lock()
	defer s.mu.Unlock()
	summary := s.buf.Snapshot(s.clock(s.now()))
	if err := writeWindowSummary(s.out, summary, s.json); err != nil && s.err == nil {
		s.err = fmt.Errorf("writing window summary: %w", err)
	}
}

// windowSummaryJSON is the NDJSON shape of one --aggregate summary line.
type windowSummaryJSON struct {
	Type          string  `json:"type"`
	WindowStart   string  `json:"window_start"`
	WindowEnd     string  `json:"window_end"`
	Window        string  `json:"window"`
	Count         int     `json:"count"`
	EventsPerSec  float64 `json:"events_per_sec"`
	UniqueSenders int     `json:"unique_senders"`
}

// writeWindowSummary prints one summary as a human line or an NDJSON object.
func writeWindowSummary(w io.Writer, s stream.WindowSummary, asJSON bool) error {
	if asJSON {
		line, err := json.Marshal(windowSummaryJSON{
			Type:          "window",
			WindowStart:   s.WindowStart.UTC().Format(time.RFC3339),
			WindowEnd:     s.WindowEnd.UTC().Format(time.RFC3339),
			Window:        s.Window.String(),
			Count:         s.Count,
			EventsPerSec:  math.Round(s.EventsPerSec*1000) / 1000,
			UniqueSenders: s.UniqueSenders,
		})
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(w, "%s\n", line)
		return err
	}
	_, err := fmt.Fprintf(w, "%s  window=%s  events=%d  events/sec=%.2f  unique_senders=%d\n",
		s.WindowEnd.UTC().Format(time.RFC3339), s.Window, s.Count, s.EventsPerSec, s.UniqueSenders)
	return err
}
