package rpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

// Health checking defaults. Soroban ledgers close roughly every five seconds,
// so a minute of staleness means the endpoint has missed many closes and is no
// longer a useful source of live data.
const (
	// DefaultHealthCheckInterval is how often Start probes the endpoint.
	DefaultHealthCheckInterval = 30 * time.Second
	// DefaultHealthCheckTimeout bounds a single getHealth request.
	DefaultHealthCheckTimeout = 5 * time.Second
	// DefaultMaxLedgerAge is the oldest a latest-ledger close time may be
	// before the endpoint is considered stale.
	DefaultMaxLedgerAge = 60 * time.Second
)

// Health statuses reported by Soroban RPC. The raw server value is preserved
// in HealthReport.Status; HealthStatusHealthy is the only value that means the
// endpoint is serving a chain head normally.
const (
	// HealthStatusHealthy is the status a healthy RPC reports.
	HealthStatusHealthy = "healthy"
	// HealthStatusUnhealthy is the status an RPC reports while it is not
	// serving requests normally.
	HealthStatusUnhealthy = "unhealthy"
	// HealthStatusUnknown is used for a response that carried no status at
	// all, which is not healthy enough to trust.
	HealthStatusUnknown = "unknown"
)

// Health-check errors. Every failure is wrapped with one of these, so callers
// can classify a report with errors.Is without matching error strings.
var (
	// ErrHealthUnsupported means an RPC client does not implement HealthClient.
	ErrHealthUnsupported = errors.New("rpc: endpoint does not support getHealth")
	// ErrHealthCheckFailed means the getHealth call itself failed.
	ErrHealthCheckFailed = errors.New("rpc: health check failed")
	// ErrUnhealthyStatus means getHealth succeeded but reported a status other
	// than "healthy".
	ErrUnhealthyStatus = errors.New("rpc: reported an unhealthy status")
	// ErrStaleLedger means the newest ledger known to the endpoint closed
	// longer ago than the configured limit.
	ErrStaleLedger = errors.New("rpc: latest ledger is stale")
	// ErrInvalidHealthResponse means getHealth returned a response missing the
	// fields needed to evaluate ledger latency.
	ErrInvalidHealthResponse = errors.New("rpc: invalid getHealth response")
	// ErrInvalidHealthConfig means a checker option was out of range.
	ErrInvalidHealthConfig = errors.New("rpc: invalid health checker configuration")
	// ErrHealthCheckerStarted is returned when Start is called twice.
	ErrHealthCheckerStarted = errors.New("rpc: health checker already started")
)

// HealthClient is the optional capability used to evaluate an endpoint's
// health. It is deliberately separate from Client: adding getHealth to the
// streaming interface would force every existing Client implementation and
// test double to grow a method the streaming path never calls.
//
// SDKClient and stream.Pool both implement HealthClient.
type HealthClient interface {
	// GetHealth reports the server's status, ledger retention bounds and the
	// close time of its latest ledger.
	GetHealth(ctx context.Context) (protocol.GetHealthResponse, error)
}

var _ HealthClient = (*SDKClient)(nil)

// HealthStatus represents the health evaluation of an RPC endpoint.
type HealthStatus struct {
	Status                string        `json:"status"`
	LatestLedger          uint32        `json:"latestLedger"`
	OldestLedger          uint32        `json:"oldestLedger"`
	LedgerRetentionWindow uint32        `json:"ledgerRetentionWindow"`
	LedgerLatency         time.Duration `json:"ledgerLatency"`
	ProbeLatency          time.Duration `json:"probeLatency"`
	Healthy               bool          `json:"healthy"`
}

// CheckHealth evaluates the health of the endpoint, verifying that
// status is healthy and the ledger sequence is progressing.
func (c *SDKClient) CheckHealth(ctx context.Context, opts ...HealthOption) (*HealthStatus, error) {
	return CheckHealth(ctx, c, opts...)
}

// CheckHealth evaluates the health of client, verifying that status == "healthy"
// and the ledger sequence is progressing within the configured max ledger age.
func CheckHealth(ctx context.Context, client HealthClient, opts ...HealthOption) (*HealthStatus, error) {
	checker, err := NewHealthChecker(client, opts...)
	if err != nil {
		return nil, err
	}
	report := checker.Check(ctx)
	status := &HealthStatus{
		Status:                report.Status,
		LatestLedger:          report.LatestLedger,
		OldestLedger:          report.OldestLedger,
		LedgerRetentionWindow: report.LedgerRetentionWindow,
		LedgerLatency:         report.LedgerLatency,
		ProbeLatency:          report.ProbeLatency,
		Healthy:               report.Healthy,
	}
	if !report.Healthy {
		if report.Err != nil {
			return status, report.Err
		}
		return status, ErrUnhealthyStatus
	}
	return status, nil
}

// CheckHealth evaluates health for HealthChecker and returns a HealthStatus.
func (c *HealthChecker) CheckHealth(ctx context.Context) (*HealthStatus, error) {
	report := c.Check(ctx)
	status := &HealthStatus{
		Status:                report.Status,
		LatestLedger:          report.LatestLedger,
		OldestLedger:          report.OldestLedger,
		LedgerRetentionWindow: report.LedgerRetentionWindow,
		LedgerLatency:         report.LedgerLatency,
		ProbeLatency:          report.ProbeLatency,
		Healthy:               report.Healthy,
	}
	if !report.Healthy {
		if report.Err != nil {
			return status, report.Err
		}
		return status, ErrUnhealthyStatus
	}
	return status, nil
}

// HealthReport is the outcome of one health check.
//
// Healthy is true only when the endpoint answered, reported "healthy", and its
// latest ledger is no older than the configured limit. Err explains the first
// condition that failed and wraps one of the package's health errors.
type HealthReport struct {
	// CheckedAt is when the check finished.
	CheckedAt time.Time
	// Healthy reports whether this individual check passed.
	Healthy bool
	// Status is the raw status string returned by getHealth.
	Status string
	// LatestLedger is the newest ledger the endpoint reports.
	LatestLedger uint32
	// OldestLedger is the oldest ledger still retained by the endpoint.
	OldestLedger uint32
	// LatestLedgerCloseTime is when LatestLedger closed.
	LatestLedgerCloseTime time.Time
	// OldestLedgerCloseTime is when OldestLedger closed, when reported.
	OldestLedgerCloseTime time.Time
	// LedgerRetentionWindow is the number of ledgers the endpoint retains.
	LedgerRetentionWindow uint32
	// LedgerLatency is how long ago the latest ledger closed. It is the
	// staleness metric the checker compares with MaxLedgerAge.
	LedgerLatency time.Duration
	// ProbeLatency is how long the getHealth request took.
	ProbeLatency time.Duration
	// ConsecutiveFailures counts consecutive failed checks, including this one.
	ConsecutiveFailures int
	// Err explains why the check failed; nil when Healthy is true.
	Err error
}

// HealthOption configures a HealthChecker.
type HealthOption func(*HealthChecker)

// WithHealthInterval sets how often Start probes. Zero keeps the default; a
// negative value is rejected by NewHealthChecker.
func WithHealthInterval(d time.Duration) HealthOption {
	return func(c *HealthChecker) { c.interval = d }
}

// WithHealthTimeout bounds a single getHealth request. Zero keeps the default;
// a negative value is rejected by NewHealthChecker.
func WithHealthTimeout(d time.Duration) HealthOption {
	return func(c *HealthChecker) { c.timeout = d }
}

// WithMaxLedgerAge sets how old the latest ledger may be before the endpoint
// is considered stale. Zero keeps the default; a negative value is rejected by
// NewHealthChecker.
func WithMaxLedgerAge(d time.Duration) HealthOption {
	return func(c *HealthChecker) { c.maxLedgerAge = d }
}

// WithHealthClock overrides the clock used for ledger latency. It exists so
// tests are deterministic; production code leaves it unset.
func WithHealthClock(now func() time.Time) HealthOption {
	return func(c *HealthChecker) {
		if now != nil {
			c.now = now
		}
	}
}

// WithHealthObserver installs a callback invoked after every periodic check,
// and after a direct Check. It runs synchronously on the checking goroutine, so
// it must not block indefinitely or call Stop.
func WithHealthObserver(fn func(HealthReport)) HealthOption {
	return func(c *HealthChecker) {
		if fn != nil {
			c.observer = fn
		}
	}
}

// HealthChecker evaluates getHealth and ledger latency periodically.
//
// It owns at most one goroutine. Start launches it, and Stop cancels it and
// waits for it to return, so a terminated caller cannot leave a probe running
// behind. A one-shot Check uses no goroutine at all.
//
// The checker never creates an RPC client or contacts an endpoint of its own:
// all network access goes through the injected HealthClient.
type HealthChecker struct {
	client       HealthClient
	interval     time.Duration
	timeout      time.Duration
	maxLedgerAge time.Duration
	now          func() time.Time
	observer     func(HealthReport)

	mu       sync.Mutex
	last     HealthReport
	hasLast  bool
	failures int

	runMu   sync.Mutex
	started bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewHealthChecker returns a checker that probes client.
//
// A nil client is a configuration error: silently doing nothing would make a
// broken health command look healthy. Zero-valued durations take their
// defaults, while negative values are rejected rather than normalised, so a
// mistyped flag fails before any probe runs.
func NewHealthChecker(client HealthClient, opts ...HealthOption) (*HealthChecker, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: client is required", ErrInvalidHealthConfig)
	}

	c := &HealthChecker{
		client:       client,
		interval:     DefaultHealthCheckInterval,
		timeout:      DefaultHealthCheckTimeout,
		maxLedgerAge: DefaultMaxLedgerAge,
		now:          time.Now,
	}
	for _, opt := range opts {
		opt(c)
	}

	if c.interval < 0 {
		return nil, fmt.Errorf("%w: interval must not be negative", ErrInvalidHealthConfig)
	}
	if c.timeout < 0 {
		return nil, fmt.Errorf("%w: timeout must not be negative", ErrInvalidHealthConfig)
	}
	if c.maxLedgerAge < 0 {
		return nil, fmt.Errorf("%w: max ledger age must not be negative", ErrInvalidHealthConfig)
	}
	if c.interval == 0 {
		c.interval = DefaultHealthCheckInterval
	}
	if c.timeout == 0 {
		c.timeout = DefaultHealthCheckTimeout
	}
	if c.maxLedgerAge == 0 {
		c.maxLedgerAge = DefaultMaxLedgerAge
	}
	if c.now == nil {
		c.now = time.Now
	}

	return c, nil
}

// Check performs one probe and records its result. It is safe to call while
// the periodic loop is running and from several goroutines.
//
// Check returns a report rather than an error so a caller can print the
// metrics for an unhealthy endpoint. report.Err is nil exactly when the probe
// passed.
func (c *HealthChecker) Check(ctx context.Context) HealthReport {
	if ctx == nil {
		// A nil context has no cancellation signal and no observer to notify,
		// so record the failure directly.
		return c.finish(HealthReport{
			CheckedAt: c.now(),
			Err:       fmt.Errorf("%w: nil context", ErrHealthCheckFailed),
		})
	}
	if err := ctx.Err(); err != nil {
		return c.complete(ctx, HealthReport{
			CheckedAt: c.now(),
			Err:       fmt.Errorf("%w: %w", ErrHealthCheckFailed, err),
		})
	}

	started := c.now()
	probeCtx, cancel := context.WithTimeout(ctx, c.timeout)
	resp, err := c.client.GetHealth(probeCtx)
	cancel()
	finished := c.now()

	report := HealthReport{
		CheckedAt:             finished,
		Status:                resp.Status,
		LatestLedger:          resp.LatestLedger,
		OldestLedger:          resp.OldestLedger,
		LedgerRetentionWindow: resp.LedgerRetentionWindow,
		ProbeLatency:          nonNegative(finished.Sub(started)),
	}

	switch {
	case err != nil:
		report.Err = fmt.Errorf("%w: %w", ErrHealthCheckFailed, err)
	case !strings.EqualFold(strings.TrimSpace(resp.Status), HealthStatusHealthy):
		if report.Status == "" {
			report.Status = HealthStatusUnknown
		}
		report.Err = fmt.Errorf("%w: server reported %q", ErrUnhealthyStatus, report.Status)
	case resp.LatestLedger == 0:
		report.Err = fmt.Errorf("%w: server reported no latest ledger", ErrInvalidHealthResponse)
	case resp.LatestLedgerCloseTime <= 0:
		report.Err = fmt.Errorf("%w: server reported no latest ledger close time", ErrInvalidHealthResponse)
	case resp.OldestLedger > resp.LatestLedger:
		report.Err = fmt.Errorf("%w: oldest ledger %d is ahead of latest ledger %d",
			ErrInvalidHealthResponse, resp.OldestLedger, resp.LatestLedger)
	default:
		report.LatestLedgerCloseTime = time.Unix(resp.LatestLedgerCloseTime, 0).UTC()
		report.LedgerLatency = nonNegative(finished.Sub(report.LatestLedgerCloseTime))
		if report.LedgerLatency > c.maxLedgerAge {
			report.Err = fmt.Errorf("%w: latest ledger %d closed %s ago (limit %s)",
				ErrStaleLedger, resp.LatestLedger, report.LedgerLatency.Round(time.Millisecond), c.maxLedgerAge)
		}
	}

	if resp.OldestLedgerCloseTime > 0 {
		report.OldestLedgerCloseTime = time.Unix(resp.OldestLedgerCloseTime, 0).UTC()
	}

	return c.complete(ctx, report)
}

// Start begins probing immediately and then every configured interval.
//
// Start returns ErrHealthCheckerStarted if the checker is already running or
// has already been stopped. The supplied context bounds the checker's
// lifetime; cancelling it stops the loop just as Stop does.
func (c *HealthChecker) Start(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidHealthConfig)
	}

	c.runMu.Lock()
	defer c.runMu.Unlock()

	if c.started {
		return ErrHealthCheckerStarted
	}
	c.started = true

	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.wg.Add(1)
	go c.loop(runCtx)
	return nil
}

// Run starts the checker and blocks until ctx is cancelled or the checker is
// stopped. It returns nil for an ordinary cancellation, which is how the CLI
// distinguishes Ctrl-C from a health failure.
func (c *HealthChecker) Run(ctx context.Context) error {
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()

	<-ctx.Done()
	return nil
}

// Stop cancels the periodic loop and waits for it to return.
//
// It is safe to call before Start, more than once, and concurrently: every
// call waits for the goroutine rather than only the first, so a caller that
// trusts Stop cannot exit with a probe still running.
func (c *HealthChecker) Stop() {
	c.runMu.Lock()
	cancel := c.cancel
	c.runMu.Unlock()

	if cancel != nil {
		cancel()
	}
	c.wg.Wait()
}

// Latest returns the most recent report and whether one exists.
func (c *HealthChecker) Latest() (HealthReport, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last, c.hasLast
}

// loop probes once immediately, then on each tick, until ctx is cancelled.
func (c *HealthChecker) loop(ctx context.Context) {
	defer c.wg.Done()

	if ctx.Err() != nil {
		return
	}
	c.probe(ctx)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.probe(ctx)
		}
	}
}

// probe runs one check. Reporting a cancellation as endpoint unhealthiness
// would make Ctrl-C look like a failure, so complete suppresses the observer
// once the checker's context is cancelled.
func (c *HealthChecker) probe(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	c.Check(ctx)
}

// complete records a report and notifies an observer when the check was not
// interrupted. State is updated under a mutex so a direct Check and the
// periodic loop can coexist.
func (c *HealthChecker) complete(ctx context.Context, report HealthReport) HealthReport {
	report = c.finish(report)
	if c.observer != nil && ctx != nil && ctx.Err() == nil {
		c.observer(report)
	}
	return report
}

// finish records a report and returns it.
func (c *HealthChecker) finish(report HealthReport) HealthReport {
	c.mu.Lock()
	if report.Err != nil {
		c.failures++
	} else {
		c.failures = 0
	}
	report.ConsecutiveFailures = c.failures
	report.Healthy = report.Err == nil
	c.last = report
	c.hasLast = true
	c.mu.Unlock()

	return report
}

// nonNegative clamps a measured duration to zero. A clock that moves backwards
// (NTP correction, or a test clock) must not produce a negative latency that
// makes a stale endpoint look fresh.
func nonNegative(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
}
