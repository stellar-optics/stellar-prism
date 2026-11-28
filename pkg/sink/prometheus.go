package sink

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

const defaultMetricsNamespace = "prism"

// defaultLatencyBuckets match Prometheus's own default histogram buckets,
// measured in seconds. Ledger latency is the delay between a ledger closing
// and prism emitting it.
var defaultLatencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// PrometheusOptions configures a Prometheus sink.
type PrometheusOptions struct {
	// Namespace prefixes every metric name. Empty uses "prism".
	Namespace string
	// Buckets are the ledger-latency histogram boundaries, in seconds. Empty
	// uses Prometheus's default buckets. The sink sorts them.
	Buckets []float64
	// ListenAddr, when set, starts an HTTP server exposing /metrics on this
	// address (":9090", "127.0.0.1:9090"). Empty leaves serving to the caller,
	// who can mount Handler instead.
	ListenAddr string
	// Now is the clock used for ledger latency. Tests set it; nil means
	// time.Now.
	Now func() time.Time
}

// Prometheus is a stream.Sink that keeps counters and a latency histogram in
// the Prometheus exposition format.
//
// It is dependency-free on purpose: the only metric families prism needs are
// three counters, one histogram and one gauge, and rendering the text format
// directly avoids pulling the Prometheus client into a CLI. Buckets are
// cumulative like any histograms, and output is deterministic so tests can
// assert on it.
type Prometheus struct {
	namespace string
	buckets   []float64
	now       func() time.Time

	mu           sync.Mutex
	records      map[string]uint64
	events       map[string]uint64
	errors       map[string]uint64
	bucketCounts []uint64
	latencySum   float64
	latencyCount uint64
	lastLedger   uint32

	// Events processed counts the records that reached this sink, which is
	// the throughput an operator watches during ingestion.
	eventsProcessed uint64
	// ledgerLag is how many ledgers the stream is behind the chain head, as
	// reported by the tailer after each poll.
	ledgerLag int64
	// queueDepth is how many records are buffered downstream and not yet
	// delivered.
	queueDepth int64
	// rpcLatency is the histogram of upstream RPC call durations.
	rpcLatencySum   float64
	rpcLatencyCount uint64
	rpcBuckets      []uint64

	server *metricsServer
}

// NewPrometheus validates opts and builds the sink, starting the /metrics
// server when ListenAddr is set.
func NewPrometheus(opts PrometheusOptions) (*Prometheus, error) {
	namespace := opts.Namespace
	if namespace == "" {
		namespace = defaultMetricsNamespace
	}
	if !validMetricName(namespace) {
		return nil, fmt.Errorf("prometheus: %q is not a valid metric namespace", opts.Namespace)
	}

	buckets := append([]float64(nil), opts.Buckets...)
	if len(buckets) == 0 {
		buckets = append([]float64(nil), defaultLatencyBuckets...)
	}
	for _, b := range buckets {
		if math.IsNaN(b) || math.IsInf(b, 0) || b <= 0 {
			return nil, fmt.Errorf("prometheus: bucket %v must be a positive number", b)
		}
	}
	sort.Float64s(buckets)

	now := opts.Now
	if now == nil {
		now = time.Now
	}

	p := &Prometheus{
		namespace:    namespace,
		buckets:      buckets,
		now:          now,
		records:      make(map[string]uint64),
		events:       make(map[string]uint64),
		errors:       make(map[string]uint64),
		bucketCounts: make([]uint64, len(buckets)+1), // last bucket is +Inf
		rpcBuckets:   make([]uint64, len(buckets)+1),
	}

	if opts.ListenAddr != "" {
		server, err := startMetricsServer(opts.ListenAddr, p.Handler())
		if err != nil {
			return nil, err
		}
		p.server = server
	}
	return p, nil
}

// Emit implements stream.Sink.
func (p *Prometheus) Emit(r stream.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.observeLocked(r)
	p.eventsProcessed++
	return nil
}

// EmitBatch implements stream.BatchSink, taking the lock once per batch.
func (p *Prometheus) EmitBatch(records []stream.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, r := range records {
		p.observeLocked(r)
	}
	p.eventsProcessed += uint64(len(records))
	return nil
}

// SetLedgerLag records how many ledgers the stream is behind the chain head.
// It is called from the tailer's progress hook, so a scrape always shows the
// lag of the most recent poll even when no record was emitted.
func (p *Prometheus) SetLedgerLag(ledgers int64) {
	if ledgers < 0 {
		ledgers = 0
	}
	p.mu.Lock()
	p.ledgerLag = ledgers
	p.mu.Unlock()
}

// SetQueueDepth records how many records are buffered but not yet delivered.
func (p *Prometheus) SetQueueDepth(depth int64) {
	if depth < 0 {
		depth = 0
	}
	p.mu.Lock()
	p.queueDepth = depth
	p.mu.Unlock()
}

// ObserveRPCLatency records the duration of one upstream RPC call in the
// prism_rpc_latency_seconds histogram.
func (p *Prometheus) ObserveRPCLatency(d time.Duration) {
	seconds := d.Seconds()
	if seconds < 0 {
		seconds = 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.rpcLatencySum += seconds
	p.rpcLatencyCount++
	start := sort.SearchFloat64s(p.buckets, seconds)
	for i := start; i < len(p.rpcBuckets); i++ {
		p.rpcBuckets[i]++
	}
}

// Flush implements stream.Sink. Metrics are kept in memory, so there is
// nothing to flush.
func (p *Prometheus) Flush() error { return nil }

// RecordError counts an error by stage ("rpc", "sink", ...).
//
// The sink cannot see failures in the tailer or in another fanout branch, so
// the CLI calls this from its retry and batch-error hooks.
func (p *Prometheus) RecordError(stage string) {
	if stage == "" {
		stage = "unknown"
	}
	p.mu.Lock()
	p.errors[stage]++
	p.mu.Unlock()
}

// Handler returns an http.Handler serving the metrics in the Prometheus text
// exposition format at any path.
func (p *Prometheus) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		if _, err := w.Write([]byte(p.gather())); err != nil {
			return
		}
	})
}

// Addr returns the bound /metrics address, or "" when no server is running.
func (p *Prometheus) Addr() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.server == nil {
		return ""
	}
	return p.server.Addr()
}

// Close stops the metrics server. It is safe to call more than once.
func (p *Prometheus) Close() error {
	p.mu.Lock()
	server := p.server
	p.server = nil
	p.mu.Unlock()

	if server == nil {
		return nil
	}
	return server.Close()
}

func (p *Prometheus) observeLocked(r stream.Record) {
	p.records[string(r.Kind)]++
	if r.Event != nil {
		eventType := r.Event.Type
		if eventType == "" {
			eventType = "unknown"
		}
		p.events[eventType]++
	}

	if r.Sequence > p.lastLedger {
		p.lastLedger = r.Sequence
	}

	if r.Time.IsZero() {
		return
	}
	seconds := p.now().Sub(r.Time).Seconds()
	if seconds < 0 {
		seconds = 0
	}
	p.latencySum += seconds
	p.latencyCount++
	// Every bucket whose upper bound is >= the observation counts it, and the
	// +Inf bucket counts every observation.
	start := sort.SearchFloat64s(p.buckets, seconds)
	for i := start; i < len(p.bucketCounts); i++ {
		p.bucketCounts[i]++
	}
}

func (p *Prometheus) gather() string {
	p.mu.Lock()
	defer p.mu.Unlock()

	var b strings.Builder

	writeCounter := func(name, help string, values map[string]uint64) {
		if len(values) == 0 {
			return
		}
		fmt.Fprintf(&b, "# HELP %s %s\n", name, help)
		fmt.Fprintf(&b, "# TYPE %s counter\n", name)
		for _, label := range sortedKeys(values) {
			fmt.Fprintf(&b, "%s{kind=%s} %d\n", name, promQuote(label), values[label])
		}
	}

	writeCounter(p.namespace+"_records_total", "Records emitted by prism.", p.records)

	// The events counter labels by event type rather than "kind", so it needs
	// its own label name.
	if len(p.events) > 0 {
		name := p.namespace + "_events_total"
		fmt.Fprintf(&b, "# HELP %s Soroban events emitted, by event type.\n", name)
		fmt.Fprintf(&b, "# TYPE %s counter\n", name)
		for _, eventType := range sortedKeys(p.events) {
			fmt.Fprintf(&b, "%s{type=%s} %d\n", name, promQuote(eventType), p.events[eventType])
		}
	}

	if len(p.errors) > 0 {
		name := p.namespace + "_errors_total"
		fmt.Fprintf(&b, "# HELP %s Errors observed while streaming, by stage.\n", name)
		fmt.Fprintf(&b, "# TYPE %s counter\n", name)
		for _, stage := range sortedKeys(p.errors) {
			fmt.Fprintf(&b, "%s{stage=%s} %d\n", name, promQuote(stage), p.errors[stage])
		}
	}

	latency := p.namespace + "_ledger_latency_seconds"
	fmt.Fprintf(&b, "# HELP %s Delay between a ledger closing and prism emitting it.\n", latency)
	fmt.Fprintf(&b, "# TYPE %s histogram\n", latency)
	for i, boundary := range p.buckets {
		fmt.Fprintf(&b, "%s_bucket{le=%q} %d\n", latency, formatFloat(boundary), p.bucketCounts[i])
	}
	fmt.Fprintf(&b, "%s_bucket{le=\"+Inf\"} %d\n", latency, p.bucketCounts[len(p.buckets)])
	fmt.Fprintf(&b, "%s_sum %s\n", latency, formatFloat(p.latencySum))
	fmt.Fprintf(&b, "%s_count %d\n", latency, p.latencyCount)

	sequence := p.namespace + "_ledger_sequence"
	fmt.Fprintf(&b, "# HELP %s Highest ledger sequence emitted.\n", sequence)
	fmt.Fprintf(&b, "# TYPE %s gauge\n", sequence)
	fmt.Fprintf(&b, "%s %d\n", sequence, p.lastLedger)

	// The families below are the operational health surface documented for
	// scraping: throughput, ledger lag, RPC latency and buffered work. They
	// are always present, including at zero, so a dashboard does not have to
	// special-case a stream that has not emitted yet.
	processed := p.namespace + "_events_processed_total"
	fmt.Fprintf(&b, "# HELP %s Records processed by the event pipeline.\n", processed)
	fmt.Fprintf(&b, "# TYPE %s counter\n", processed)
	fmt.Fprintf(&b, "%s %d\n", processed, p.eventsProcessed)

	lag := p.namespace + "_ledger_lag_ledgers"
	fmt.Fprintf(&b, "# HELP %s Ledgers between the stream position and the chain head.\n", lag)
	fmt.Fprintf(&b, "# TYPE %s gauge\n", lag)
	fmt.Fprintf(&b, "%s %d\n", lag, p.ledgerLag)

	rpcLatency := p.namespace + "_rpc_latency_seconds"
	fmt.Fprintf(&b, "# HELP %s Duration of upstream RPC calls.\n", rpcLatency)
	fmt.Fprintf(&b, "# TYPE %s histogram\n", rpcLatency)
	for i, boundary := range p.buckets {
		fmt.Fprintf(&b, "%s_bucket{le=%q} %d\n", rpcLatency, formatFloat(boundary), p.rpcBuckets[i])
	}
	fmt.Fprintf(&b, "%s_bucket{le=\"+Inf\"} %d\n", rpcLatency, p.rpcBuckets[len(p.buckets)])
	fmt.Fprintf(&b, "%s_sum %s\n", rpcLatency, formatFloat(p.rpcLatencySum))
	fmt.Fprintf(&b, "%s_count %d\n", rpcLatency, p.rpcLatencyCount)

	queue := p.namespace + "_queue_depth"
	fmt.Fprintf(&b, "# HELP %s Records buffered but not yet delivered.\n", queue)
	fmt.Fprintf(&b, "# TYPE %s gauge\n", queue)
	fmt.Fprintf(&b, "%s %d\n", queue, p.queueDepth)

	return b.String()
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// promQuote renders a label value with the escaping the exposition format
// defines: backslash, double quote and newline. Unlike fmt's %q it leaves
// ordinary UTF-8 alone and never emits \x escapes, which Prometheus would
// reject.
func promQuote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// validMetricName accepts the Prometheus name grammar. A namespace is used as
// a prefix, so it must not contain a label or brace.
func validMetricName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_', r == ':':
		case i > 0 && r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

// metricsServer owns a listener and the goroutine serving it, so Close leaves
// nothing running.
type metricsServer struct {
	listener net.Listener
	server   *http.Server
	done     chan struct{}

	mu       sync.Mutex
	serveErr error
}

func startMetricsServer(addr string, handler http.Handler) (*metricsServer, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("prometheus: listening on %s: %w", addr, err)
	}

	s := &metricsServer{
		listener: listener,
		server: &http.Server{
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
		},
		done: make(chan struct{}),
	}
	go func() {
		defer close(s.done)
		if err := s.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.mu.Lock()
			s.serveErr = err
			s.mu.Unlock()
		}
	}()
	return s, nil
}

func (s *metricsServer) Addr() string {
	return s.listener.Addr().String()
}

func (s *metricsServer) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	shutdownErr := s.server.Shutdown(ctx)
	<-s.done

	s.mu.Lock()
	serveErr := s.serveErr
	s.mu.Unlock()
	return errors.Join(shutdownErr, serveErr)
}
