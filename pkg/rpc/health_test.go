package rpc_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// healthFunc adapts a function to rpc.HealthClient, which keeps each test's
// failure mode in one line.
type healthFunc func(context.Context) (protocol.GetHealthResponse, error)

func (f healthFunc) GetHealth(ctx context.Context) (protocol.GetHealthResponse, error) {
	return f(ctx)
}

func healthyResponse(now time.Time, ledgerAge time.Duration) protocol.GetHealthResponse {
	return protocol.GetHealthResponse{
		Status:                rpc.HealthStatusHealthy,
		LatestLedger:          100,
		LatestLedgerCloseTime: now.Add(-ledgerAge).Unix(),
		OldestLedger:          90,
		OldestLedgerCloseTime: now.Add(-time.Hour).Unix(),
		LedgerRetentionWindow: 10,
	}
}

// advancingClock returns a fixed base on the first call and base+step on every
// later call, so probe latency is deterministic without a real sleep.
func advancingClock(base time.Time, step time.Duration) func() time.Time {
	var calls atomic.Int64
	return func() time.Time {
		if calls.Add(1) == 1 {
			return base
		}
		return base.Add(step)
	}
}

func TestNewHealthCheckerValidation(t *testing.T) {
	t.Parallel()

	client := healthFunc(func(context.Context) (protocol.GetHealthResponse, error) {
		return protocol.GetHealthResponse{}, nil
	})

	tests := []struct {
		name    string
		client  rpc.HealthClient
		options []rpc.HealthOption
		wantErr bool
	}{
		{name: "nil client", client: nil, wantErr: true},
		{name: "zero values use defaults", client: client},
		{
			name:    "negative interval",
			client:  client,
			options: []rpc.HealthOption{rpc.WithHealthInterval(-time.Second)},
			wantErr: true,
		},
		{
			name:    "negative timeout",
			client:  client,
			options: []rpc.HealthOption{rpc.WithHealthTimeout(-time.Second)},
			wantErr: true,
		},
		{
			name:    "negative max ledger age",
			client:  client,
			options: []rpc.HealthOption{rpc.WithMaxLedgerAge(-time.Second)},
			wantErr: true,
		},
		{
			name:   "nil clock and observer options are ignored",
			client: client,
			options: []rpc.HealthOption{
				rpc.WithHealthClock(nil),
				rpc.WithHealthObserver(nil),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			checker, err := rpc.NewHealthChecker(tc.client, tc.options...)
			if tc.wantErr {
				if err == nil {
					t.Fatal("NewHealthChecker() error = nil, want a configuration error")
				}
				if !errors.Is(err, rpc.ErrInvalidHealthConfig) {
					t.Errorf("error %v does not wrap ErrInvalidHealthConfig", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewHealthChecker() error = %v", err)
			}
			if checker == nil {
				t.Fatal("NewHealthChecker() returned a nil checker")
			}
		})
	}
}

func TestHealthCheckerReportsHealthyEndpoint(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	client := healthFunc(func(context.Context) (protocol.GetHealthResponse, error) {
		return healthyResponse(base, 2*time.Second), nil
	})
	checker, err := rpc.NewHealthChecker(client,
		rpc.WithHealthClock(advancingClock(base, 150*time.Millisecond)),
		rpc.WithMaxLedgerAge(30*time.Second),
	)
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}

	report := checker.Check(context.Background())
	if !report.Healthy || report.Err != nil {
		t.Fatalf("Check() = healthy=%v err=%v, want healthy", report.Healthy, report.Err)
	}
	if report.Status != rpc.HealthStatusHealthy {
		t.Errorf("Status = %q, want %q", report.Status, rpc.HealthStatusHealthy)
	}
	if report.LatestLedger != 100 || report.OldestLedger != 90 {
		t.Errorf("ledgers = %d/%d, want 100/90", report.LatestLedger, report.OldestLedger)
	}
	if report.LedgerLatency != 2*time.Second+150*time.Millisecond {
		t.Errorf("LedgerLatency = %s, want 2.15s", report.LedgerLatency)
	}
	if report.ProbeLatency != 150*time.Millisecond {
		t.Errorf("ProbeLatency = %s, want 150ms", report.ProbeLatency)
	}
	if report.LedgerRetentionWindow != 10 {
		t.Errorf("LedgerRetentionWindow = %d, want 10", report.LedgerRetentionWindow)
	}
	if report.CheckedAt != base.Add(150*time.Millisecond) {
		t.Errorf("CheckedAt = %s, want %s", report.CheckedAt, base.Add(150*time.Millisecond))
	}
	if report.LatestLedgerCloseTime.IsZero() || report.OldestLedgerCloseTime.IsZero() {
		t.Error("close times were not populated")
	}
	if report.ConsecutiveFailures != 0 {
		t.Errorf("ConsecutiveFailures = %d, want 0", report.ConsecutiveFailures)
	}
	if _, ok := checker.Latest(); !ok {
		t.Error("Latest() reports no report after Check")
	}
}

func TestHealthCheckerClassifiesFailures(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	probeErr := errors.New("connection reset by peer")

	tests := []struct {
		name       string
		response   protocol.GetHealthResponse
		err        error
		want       error
		wantStatus string
	}{
		{
			name:       "transport failure",
			err:        probeErr,
			want:       rpc.ErrHealthCheckFailed,
			wantStatus: "",
		},
		{
			name: "server reports unhealthy",
			response: protocol.GetHealthResponse{
				Status:                rpc.HealthStatusUnhealthy,
				LatestLedger:          100,
				LatestLedgerCloseTime: base.Unix(),
			},
			want:       rpc.ErrUnhealthyStatus,
			wantStatus: rpc.HealthStatusUnhealthy,
		},
		{
			name: "empty status is unknown",
			response: protocol.GetHealthResponse{
				LatestLedger:          100,
				LatestLedgerCloseTime: base.Unix(),
			},
			want:       rpc.ErrUnhealthyStatus,
			wantStatus: rpc.HealthStatusUnknown,
		},
		{
			name: "missing latest ledger",
			response: protocol.GetHealthResponse{
				Status:                rpc.HealthStatusHealthy,
				LatestLedgerCloseTime: base.Unix(),
			},
			want:       rpc.ErrInvalidHealthResponse,
			wantStatus: rpc.HealthStatusHealthy,
		},
		{
			name: "missing close time",
			response: protocol.GetHealthResponse{
				Status:       rpc.HealthStatusHealthy,
				LatestLedger: 100,
			},
			want:       rpc.ErrInvalidHealthResponse,
			wantStatus: rpc.HealthStatusHealthy,
		},
		{
			name: "oldest ahead of latest",
			response: protocol.GetHealthResponse{
				Status:                rpc.HealthStatusHealthy,
				LatestLedger:          100,
				LatestLedgerCloseTime: base.Unix(),
				OldestLedger:          101,
			},
			want:       rpc.ErrInvalidHealthResponse,
			wantStatus: rpc.HealthStatusHealthy,
		},
		{
			name:       "stale ledger",
			response:   healthyResponse(base, 2*time.Minute),
			want:       rpc.ErrStaleLedger,
			wantStatus: rpc.HealthStatusHealthy,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := healthFunc(func(context.Context) (protocol.GetHealthResponse, error) {
				return tc.response, tc.err
			})
			checker, err := rpc.NewHealthChecker(client,
				rpc.WithHealthClock(func() time.Time { return base }),
				rpc.WithMaxLedgerAge(30*time.Second),
			)
			if err != nil {
				t.Fatalf("NewHealthChecker() error = %v", err)
			}

			report := checker.Check(context.Background())
			if report.Healthy {
				t.Fatal("Check() = healthy, want a failure")
			}
			if !errors.Is(report.Err, tc.want) {
				t.Errorf("error = %v, want it to wrap %v", report.Err, tc.want)
			}
			if tc.wantStatus != "" && report.Status != tc.wantStatus {
				t.Errorf("Status = %q, want %q", report.Status, tc.wantStatus)
			}
			if tc.name == "transport failure" && !errors.Is(report.Err, probeErr) {
				t.Errorf("error = %v, want it to preserve the transport error", report.Err)
			}
			if report.ConsecutiveFailures != 1 {
				t.Errorf("ConsecutiveFailures = %d, want 1", report.ConsecutiveFailures)
			}
		})
	}
}

func TestHealthCheckerResetsFailuresAfterSuccess(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	var fail atomic.Bool
	fail.Store(true)
	client := healthFunc(func(context.Context) (protocol.GetHealthResponse, error) {
		if fail.Load() {
			return protocol.GetHealthResponse{}, errors.New("temporary failure")
		}
		return healthyResponse(base, time.Second), nil
	})
	checker, err := rpc.NewHealthChecker(client,
		rpc.WithHealthClock(func() time.Time { return base }),
	)
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}

	first := checker.Check(context.Background())
	second := checker.Check(context.Background())
	if first.ConsecutiveFailures != 1 || second.ConsecutiveFailures != 2 {
		t.Fatalf("failure counts = %d,%d, want 1,2",
			first.ConsecutiveFailures, second.ConsecutiveFailures)
	}

	fail.Store(false)
	recovered := checker.Check(context.Background())
	if !recovered.Healthy || recovered.ConsecutiveFailures != 0 {
		t.Errorf("recovery = healthy=%v failures=%d, want healthy and 0",
			recovered.Healthy, recovered.ConsecutiveFailures)
	}
}

func TestHealthCheckerHonoursContextCancellation(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	client := healthFunc(func(context.Context) (protocol.GetHealthResponse, error) {
		calls.Add(1)
		return protocol.GetHealthResponse{}, nil
	})
	checker, err := rpc.NewHealthChecker(client)
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report := checker.Check(ctx)
	if report.Healthy {
		t.Fatal("Check() = healthy for a cancelled context")
	}
	if !errors.Is(report.Err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", report.Err)
	}
	if calls.Load() != 0 {
		t.Errorf("GetHealth called %d times, want 0 for an already-cancelled context", calls.Load())
	}

	var nilContext context.Context
	nilReport := checker.Check(nilContext)
	if nilReport.Healthy || !errors.Is(nilReport.Err, rpc.ErrHealthCheckFailed) {
		t.Errorf("Check(nil) = healthy=%v err=%v, want a health failure",
			nilReport.Healthy, nilReport.Err)
	}
}

func TestHealthCheckerCallsObserver(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	var reports []rpc.HealthReport
	client := healthFunc(func(context.Context) (protocol.GetHealthResponse, error) {
		return healthyResponse(base, time.Second), nil
	})
	checker, err := rpc.NewHealthChecker(client,
		rpc.WithHealthClock(func() time.Time { return base }),
		rpc.WithHealthObserver(func(report rpc.HealthReport) {
			reports = append(reports, report)
		}),
	)
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}

	checker.Check(context.Background())
	if len(reports) != 1 {
		t.Fatalf("observer calls = %d, want 1", len(reports))
	}
	if !reports[0].Healthy {
		t.Error("observer received an unhealthy report")
	}
}

func TestHealthCheckerPeriodicStopJoins(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	var calls atomic.Int64
	reports := make(chan rpc.HealthReport, 8)
	client := healthFunc(func(context.Context) (protocol.GetHealthResponse, error) {
		calls.Add(1)
		return healthyResponse(base, time.Second), nil
	})
	checker, err := rpc.NewHealthChecker(client,
		rpc.WithHealthInterval(2*time.Millisecond),
		rpc.WithHealthTimeout(time.Second),
		rpc.WithHealthClock(func() time.Time { return base }),
		rpc.WithHealthObserver(func(report rpc.HealthReport) {
			reports <- report
		}),
	)
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := checker.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	for range 2 {
		select {
		case report := <-reports:
			if !report.Healthy {
				t.Fatalf("periodic report is unhealthy: %v", report.Err)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for a periodic report")
		}
	}

	checker.Stop()
	afterStop := calls.Load()
	if afterStop < 2 {
		t.Errorf("GetHealth calls = %d, want at least 2", afterStop)
	}
	if _, ok := checker.Latest(); !ok {
		t.Error("Latest() reports no report after periodic checks")
	}

	// Stop is idempotent and a stopped checker cannot be restarted.
	checker.Stop()
	if err := checker.Start(context.Background()); !errors.Is(err, rpc.ErrHealthCheckerStarted) {
		t.Errorf("second Start() error = %v, want ErrHealthCheckerStarted", err)
	}

	cancel()
}

func TestHealthCheckerStopWaitsForInFlightProbe(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	started := make(chan struct{})
	release := make(chan struct{})
	client := healthFunc(func(context.Context) (protocol.GetHealthResponse, error) {
		close(started)
		<-release
		return healthyResponse(base, time.Second), nil
	})
	checker, err := rpc.NewHealthChecker(client,
		rpc.WithHealthInterval(time.Hour),
		rpc.WithHealthTimeout(time.Hour),
		rpc.WithHealthClock(func() time.Time { return base }),
	)
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}

	if err := checker.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("probe did not start")
	}

	stopped := make(chan struct{})
	go func() {
		checker.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatal("Stop() returned while a probe was still in flight")
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop() did not return after the probe finished")
	}
}

func TestHealthCheckerStopBeforeStart(t *testing.T) {
	t.Parallel()

	client := healthFunc(func(context.Context) (protocol.GetHealthResponse, error) {
		return protocol.GetHealthResponse{}, nil
	})
	checker, err := rpc.NewHealthChecker(client)
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}

	checker.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	if err := checker.Start(ctx); err != nil {
		t.Fatalf("Start() after Stop() error = %v", err)
	}
	cancel()
	checker.Stop()
}

func TestHealthCheckerStartValidation(t *testing.T) {
	t.Parallel()

	client := healthFunc(func(context.Context) (protocol.GetHealthResponse, error) {
		return protocol.GetHealthResponse{}, nil
	})
	checker, err := rpc.NewHealthChecker(client)
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}

	//nolint:staticcheck // deliberately checking the nil-context guard.
	if err := checker.Start(nil); !errors.Is(err, rpc.ErrInvalidHealthConfig) {
		t.Errorf("Start(nil) error = %v, want ErrInvalidHealthConfig", err)
	}
}

func TestHealthCheckerRunStopsOnCancellation(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	var calls atomic.Int64
	client := healthFunc(func(context.Context) (protocol.GetHealthResponse, error) {
		calls.Add(1)
		return healthyResponse(base, time.Second), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	checker, err := rpc.NewHealthChecker(client,
		rpc.WithHealthInterval(2*time.Millisecond),
		rpc.WithHealthClock(func() time.Time { return base }),
		rpc.WithHealthObserver(func(rpc.HealthReport) { cancel() }),
	)
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}

	if err := checker.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if calls.Load() != 1 {
		t.Errorf("GetHealth calls = %d, want 1", calls.Load())
	}
}

func TestHealthCheckerFutureCloseTimeClampsLatency(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	client := healthFunc(func(context.Context) (protocol.GetHealthResponse, error) {
		return protocol.GetHealthResponse{
			Status:                rpc.HealthStatusHealthy,
			LatestLedger:          100,
			LatestLedgerCloseTime: base.Add(time.Hour).Unix(),
		}, nil
	})
	checker, err := rpc.NewHealthChecker(client,
		rpc.WithHealthClock(func() time.Time { return base }),
	)
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}

	report := checker.Check(context.Background())
	if !report.Healthy {
		t.Fatalf("Check() = unhealthy: %v", report.Err)
	}
	if report.LedgerLatency != 0 {
		t.Errorf("LedgerLatency = %s, want 0 for a future close time", report.LedgerLatency)
	}
}

func TestCheckHealth(t *testing.T) {
	t.Parallel()

	healthyClient := healthFunc(func(context.Context) (protocol.GetHealthResponse, error) {
		return healthyResponse(time.Now(), 2*time.Second), nil
	})

	status, err := rpc.CheckHealth(context.Background(), healthyClient)
	if err != nil {
		t.Fatalf("CheckHealth() unexpected error = %v", err)
	}
	if !status.Healthy {
		t.Error("status.Healthy = false, want true")
	}
	if status.Status != rpc.HealthStatusHealthy {
		t.Errorf("status.Status = %q, want %q", status.Status, rpc.HealthStatusHealthy)
	}

	unhealthyClient := healthFunc(func(context.Context) (protocol.GetHealthResponse, error) {
		return protocol.GetHealthResponse{
			Status: rpc.HealthStatusUnhealthy,
		}, nil
	})

	status, err = rpc.CheckHealth(context.Background(), unhealthyClient)
	if err == nil {
		t.Fatal("CheckHealth() on unhealthy expected error, got nil")
	}
	if status == nil || status.Healthy {
		t.Errorf("CheckHealth() status = %v, want unhealthy status", status)
	}
}
