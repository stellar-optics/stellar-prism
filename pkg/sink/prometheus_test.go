package sink_test

import (
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func gatherMetrics(t *testing.T, p *sink.Prometheus) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	p.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/plain") {
		t.Errorf("Content-Type = %q, want the Prometheus text format", got)
	}
	return recorder.Body.String()
}

func TestPrometheusTracksRecordsEventsLatencyAndErrors(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_102, 0).UTC()
	p, err := sink.NewPrometheus(sink.PrometheusOptions{
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewPrometheus() error = %v", err)
	}

	for range 4 {
		if err := p.Emit(eventRecord()); err != nil {
			t.Fatalf("Emit() error = %v", err)
		}
	}
	if err := p.EmitBatch([]stream.Record{ledgerRecord()}); err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}
	p.RecordError("rpc")
	p.RecordError("rpc")
	if err := p.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	body := gatherMetrics(t, p)

	for _, want := range []string{
		`prism_records_total{kind="event"} 4`,
		`prism_records_total{kind="ledger"} 1`,
		`prism_events_total{type="contract"} 4`,
		`prism_errors_total{stage="rpc"} 2`,
		"# TYPE prism_ledger_latency_seconds histogram",
		`prism_ledger_latency_seconds_bucket{le="2.5"} 5`,
		`prism_ledger_latency_seconds_bucket{le="+Inf"} 5`,
		"prism_ledger_latency_seconds_sum 10",
		"prism_ledger_latency_seconds_count 5",
		"prism_ledger_sequence 100",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics do not contain %q\n%s", want, body)
		}
	}
}

func TestPrometheusCustomNamespace(t *testing.T) {
	t.Parallel()

	p, err := sink.NewPrometheus(sink.PrometheusOptions{Namespace: "myapp"})
	if err != nil {
		t.Fatalf("NewPrometheus() error = %v", err)
	}
	if err := p.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	body := gatherMetrics(t, p)
	if !strings.Contains(body, `myapp_records_total{kind="event"} 1`) {
		t.Errorf("custom namespace was not applied\n%s", body)
	}
	if strings.Contains(body, "prism_records_total") {
		t.Errorf("default namespace leaked into custom output\n%s", body)
	}
}

func TestPrometheusEscapesLabelValues(t *testing.T) {
	t.Parallel()

	p, err := sink.NewPrometheus(sink.PrometheusOptions{})
	if err != nil {
		t.Fatalf("NewPrometheus() error = %v", err)
	}
	r := eventRecord()
	r.Event.Type = "contract\"quoted\nnext"
	if err := p.Emit(r); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	body := gatherMetrics(t, p)
	if !strings.Contains(body, `prism_events_total{type="contract\"quoted\nnext"} 1`) {
		t.Errorf("label value was not escaped in the exposition format\n%s", body)
	}
}

func TestPrometheusClampsFutureLedgerTime(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0).UTC()
	p, err := sink.NewPrometheus(sink.PrometheusOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("NewPrometheus() error = %v", err)
	}
	r := eventRecord()
	r.Time = now.Add(time.Minute)
	if err := p.Emit(r); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	body := gatherMetrics(t, p)
	if !strings.Contains(body, "prism_ledger_latency_seconds_sum 0") {
		t.Errorf("a future timestamp should clamp to zero latency, not go negative\n%s", body)
	}
}

func TestPrometheusServerServesMetricsAndCloses(t *testing.T) {
	t.Parallel()

	p, err := sink.NewPrometheus(sink.PrometheusOptions{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewPrometheus() error = %v", err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	addr := p.Addr()
	if addr == "" {
		t.Fatal("Addr() = \"\", want the bound address")
	}

	if err := p.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading metrics response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(string(body), `prism_records_total{kind="event"} 1`) {
		t.Errorf("served metrics do not contain the record count\n%s", body)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
	if got := p.Addr(); got != "" {
		t.Errorf("Addr() after Close = %q, want empty", got)
	}
}

func TestPrometheusHandlerRejectsNonRead(t *testing.T) {
	t.Parallel()

	p, err := sink.NewPrometheus(sink.PrometheusOptions{})
	if err != nil {
		t.Fatalf("NewPrometheus() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	p.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", recorder.Code)
	}
	if got := recorder.Header().Get("Allow"); got != "GET, HEAD" {
		t.Errorf("Allow = %q, want GET, HEAD", got)
	}
}

func TestPrometheusConcurrentEmit(t *testing.T) {
	t.Parallel()

	p, err := sink.NewPrometheus(sink.PrometheusOptions{})
	if err != nil {
		t.Fatalf("NewPrometheus() error = %v", err)
	}

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Emit(eventRecord()); err != nil {
				t.Errorf("Emit() error = %v", err)
			}
		}()
	}
	wg.Wait()

	body := gatherMetrics(t, p)
	if !strings.Contains(body, `prism_records_total{kind="event"} 50`) {
		t.Errorf("concurrent emits were lost\n%s", body)
	}
}

func TestPrometheusValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts sink.PrometheusOptions
	}{
		{name: "invalid namespace", opts: sink.PrometheusOptions{Namespace: "bad name"}},
		{name: "zero bucket", opts: sink.PrometheusOptions{Buckets: []float64{1, 0}}},
		{name: "negative bucket", opts: sink.PrometheusOptions{Buckets: []float64{-1}}},
		{name: "NaN bucket", opts: sink.PrometheusOptions{Buckets: []float64{math.NaN()}}},
		{name: "bad listen address", opts: sink.PrometheusOptions{ListenAddr: "definitely-not-an-address"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := sink.NewPrometheus(tc.opts)
			if err == nil {
				_ = p.Close()
				t.Fatal("NewPrometheus() error = nil, want a validation failure")
			}
		})
	}

	p, err := sink.NewPrometheus(sink.PrometheusOptions{Buckets: []float64{10, 0.5, 1}})
	if err != nil {
		t.Fatalf("NewPrometheus(unsorted buckets) error = %v", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestPrometheusOperationalMetricFamilies(t *testing.T) {
	t.Parallel()

	p, err := sink.NewPrometheus(sink.PrometheusOptions{})
	if err != nil {
		t.Fatalf("NewPrometheus() error = %v", err)
	}

	if err := p.Emit(eventRecord()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := p.EmitBatch([]stream.Record{ledgerRecord()}); err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}
	p.SetLedgerLag(7)
	p.ObserveRPCLatency(150 * time.Millisecond)
	p.SetQueueDepth(3)

	body := gatherMetrics(t, p)
	for _, want := range []string{
		"prism_events_processed_total 2",
		"prism_ledger_lag_ledgers 7",
		`prism_rpc_latency_seconds_bucket{le="0.1"} 0`,
		`prism_rpc_latency_seconds_bucket{le="0.25"} 1`,
		`prism_rpc_latency_seconds_bucket{le="+Inf"} 1`,
		"prism_rpc_latency_seconds_sum 0.15",
		"prism_rpc_latency_seconds_count 1",
		"prism_queue_depth 3",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics body does not contain %q:\n%s", want, body)
		}
	}

	// Gauges clamp at zero rather than exporting an impossible negative lag
	// or queue depth.
	p.SetLedgerLag(-4)
	p.SetQueueDepth(-1)
	body = gatherMetrics(t, p)
	for _, want := range []string{"prism_ledger_lag_ledgers 0", "prism_queue_depth 0"} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics body does not contain %q:\n%s", want, body)
		}
	}
}
