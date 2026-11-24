package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// Defaults for a failover pool. The health probe interval is deliberately
// slower than a ledger close: probing a recovered endpoint is maintenance,
// not part of the streaming hot path.
const (
	DefaultHealthInterval  = 30 * time.Second
	DefaultHealthTimeout   = 5 * time.Second
	DefaultFailureCooldown = 30 * time.Second
)

// DefaultBackoff tunes the retry delays used when probing pool endpoints
// that have fallen out of rotation.
const (
	DefaultBackoffInitial = 500 * time.Millisecond
	DefaultBackoffMax     = 30 * time.Second
	DefaultBackoffFactor  = 2.0
	DefaultBackoffJitter  = true
)

// PoolOption configures an RPC Pool.
type PoolOption func(*Pool)

// WithClientFactory overrides how a client is constructed for each URL. Tests
// use it to inject in-memory servers; production code normally leaves it
// unset and gets the SDK-backed client.
func WithClientFactory(f func(url string) rpc.Client) PoolOption {
	return func(p *Pool) {
		if f != nil {
			p.factory = f
		}
	}
}

// WithHealthInterval sets how often unhealthy endpoints are probed. A value
// of zero disables the background probe entirely, which is useful for a
// short-lived CLI invocation and for deterministic tests.
func WithHealthInterval(d time.Duration) PoolOption {
	return func(p *Pool) { p.healthInterval = d }
}

// WithHealthTimeout bounds a single health probe.
func WithHealthTimeout(d time.Duration) PoolOption {
	return func(p *Pool) {
		if d > 0 {
			p.healthTimeout = d
		}
	}
}

// WithFailureCooldown sets how long a failed endpoint is skipped before it is
// eligible to serve traffic again. During the cooldown it is still probed in
// the background.
func WithFailureCooldown(d time.Duration) PoolOption {
	return func(p *Pool) {
		if d > 0 {
			p.failureCooldown = d
		}
	}
}

// WithBackoff sets the retry backoff configuration for pool endpoint recovery.
// This controls exponential backoff with jitter used when probing unhealthy
// endpoints, helping prevent thundering herd problems during network partitions.
func WithBackoff(initial, maxDelay time.Duration, factor float64, jitter bool) PoolOption {
	return func(p *Pool) {
		p.backoffInitial = initial
		p.backoffMax = maxDelay
		p.backoffFactor = factor
		p.backoffJitter = jitter
	}
}

// endpoint is one member of a Pool and its mutable health state.
type endpoint struct {
	url    string
	client rpc.Client

	healthy       bool
	failures      int
	cooldownUntil time.Time
}

// Pool is an rpc.Client that spreads requests over several RPC endpoints with
// automatic failover.
//
// Calls are distributed round-robin. A failed endpoint is put into a cooldown
// and the same call is retried on the next endpoint, so a single unhealthy
// backend does not fail a stream. Permanent errors are returned immediately:
// retrying a malformed cursor or filter on another endpoint cannot help.
//
// When every endpoint is cooling down, one is still tried rather than failing
// without a request. A stale health mark is far more likely than a complete
// outage, and the call itself is the authoritative check.
//
// A background goroutine probes unhealthy endpoints and restores them as they
// recover. Close stops it and waits, so a Pool never leaks a goroutine.
type Pool struct {
	mu        sync.Mutex
	endpoints []*endpoint
	next      int
	probeNext int

	factory         func(url string) rpc.Client
	healthInterval  time.Duration
	healthTimeout   time.Duration
	failureCooldown time.Duration
	backoffInitial  time.Duration
	backoffMax      time.Duration
	backoffFactor   float64
	backoffJitter   bool
	clock           Clock

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closeOnce sync.Once
	closeErr  error
}

// NewPool returns a Pool over the given URLs.
//
// At least one URL is required. Duplicate and blank URLs are ignored so a
// value like "a,,b" does not create an endpoint that can never work.
func NewPool(urls []string, opts ...PoolOption) (*Pool, error) {
	p := &Pool{
		factory: func(url string) rpc.Client {
			return rpc.New(url, 30*time.Second)
		},
		healthInterval:  DefaultHealthInterval,
		healthTimeout:   DefaultHealthTimeout,
		failureCooldown: DefaultFailureCooldown,
		backoffInitial:  DefaultBackoffInitial,
		backoffMax:      DefaultBackoffMax,
		backoffFactor:   DefaultBackoffFactor,
		backoffJitter:   DefaultBackoffJitter,
		clock:           RealClock{},
	}
	for _, opt := range opts {
		opt(p)
	}

	seen := make(map[string]bool, len(urls))
	for _, raw := range urls {
		url := strings.TrimSpace(raw)
		if url == "" || seen[url] {
			continue
		}
		seen[url] = true
		p.endpoints = append(p.endpoints, &endpoint{
			url:     url,
			client:  p.factory(url),
			healthy: true,
		})
	}
	if len(p.endpoints) == 0 {
		return nil, fmt.Errorf("rpc pool: at least one endpoint URL is required")
	}

	p.ctx, p.cancel = context.WithCancel(context.Background())
	if p.healthInterval > 0 {
		p.wg.Add(1)
		go p.healthLoop()
	}
	return p, nil
}

// Close stops the health probe and releases every underlying client.
// It is safe to call more than once.
func (p *Pool) Close() error {
	p.closeOnce.Do(func() {
		p.cancel()
		p.wg.Wait()

		var errs []error
		for _, ep := range p.endpoints {
			if closer, ok := ep.client.(io.Closer); ok {
				if err := closer.Close(); err != nil {
					errs = append(errs, fmt.Errorf("%s: %w", ep.url, err))
				}
			}
		}
		p.closeErr = errors.Join(errs...)
	})
	return p.closeErr
}

// Healthy reports how many endpoints are currently considered healthy.
func (p *Pool) Healthy() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	n := 0
	for _, ep := range p.endpoints {
		if ep.healthy {
			n++
		}
	}
	return n
}

// URLs returns the endpoints in pool order.
func (p *Pool) URLs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]string, 0, len(p.endpoints))
	for _, ep := range p.endpoints {
		out = append(out, ep.url)
	}
	return out
}

// HealthCheck probes every unhealthy endpoint once, in round-robin order.
// Healthy endpoints are left alone because they are already carrying traffic.
func (p *Pool) HealthCheck(ctx context.Context) {
	p.mu.Lock()
	start := p.probeNext
	p.probeNext++
	eps := make([]*endpoint, len(p.endpoints))
	copy(eps, p.endpoints)
	p.mu.Unlock()

	for i := range eps {
		ep := eps[(start+i)%len(eps)]
		if p.isHealthy(ep) {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, p.healthTimeout)
		_, err := ep.client.GetLatestLedger(probeCtx)
		cancel()
		if err == nil {
			p.markSuccess(ep)
		} else {
			p.markFailure(ep, err)
		}
	}
	// Apply exponential backoff with jitter before the next probe cycle, but
	// only while endpoints are still down: there is nothing to spread out once
	// the pool has recovered, and sleeping then would only delay live traffic.
	if p.backoffInitial > 0 {
		if failures, down := p.failureState(); down {
			if sleepErr := p.clock.Sleep(ctx, p.backoffDelay(failures)); sleepErr != nil {
				return
			}
		}
	}
}

// failureState reports the highest failure count across the pool and whether
// any endpoint is currently marked unhealthy, both read under the lock that
// guards the endpoints: HealthCheck runs concurrently with the streaming
// calls that keep adjusting them.
func (p *Pool) failureState() (failures int, down bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, ep := range p.endpoints {
		if !ep.healthy {
			down = true
		}
		if ep.failures > failures {
			failures = ep.failures
		}
	}
	return failures, down
}

// backoffDelay computes the delay before the next probe of a failing
// endpoint: exponential in the failure count, capped at backoffMax, and
// jittered so that pools which lost the same partition do not reconnect in
// lockstep.
func (p *Pool) backoffDelay(failureCount int) time.Duration {
	attempt := failureCount + 1
	initial := p.backoffInitial
	maxDelay := p.backoffMax
	factor := p.backoffFactor

	if initial <= 0 {
		initial = DefaultBackoffInitial
	}
	if maxDelay <= 0 {
		maxDelay = DefaultBackoffMax
	}
	if factor < 1 {
		factor = 2
	}

	d := float64(initial)
	for range attempt - 1 {
		d *= factor
		if d >= float64(maxDelay) {
			d = float64(maxDelay)
			break
		}
	}
	if d > float64(maxDelay) {
		d = float64(maxDelay)
	}

	if p.backoffJitter {
		// Full jitter: pick a random value in [0, d], then double it to get
		// a range of [0, 2d], effectively giving [d/2, d] after normalization.
		d = d/2 + d/2*rand.Float64()
	}
	return time.Duration(d)
}

func (p *Pool) healthLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.healthInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.HealthCheck(p.ctx)
		}
	}
}

// GetEvents implements rpc.Client.
func (p *Pool) GetEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	return poolCall(p, ctx, "getEvents", func(c rpc.Client) (protocol.GetEventsResponse, error) {
		return c.GetEvents(ctx, req)
	})
}

// GetEventsBatch implements rpc.Client. A batch that fails on one endpoint is
// retried whole on the next: the endpoints share no cursor state, so a
// half-completed batch cannot be resumed elsewhere.
func (p *Pool) GetEventsBatch(ctx context.Context, reqs []protocol.GetEventsRequest) ([]protocol.GetEventsResponse, error) {
	return poolCall(p, ctx, "getEventsBatch", func(c rpc.Client) ([]protocol.GetEventsResponse, error) {
		return c.GetEventsBatch(ctx, reqs)
	})
}

// GetLedgers implements rpc.Client.
func (p *Pool) GetLedgers(ctx context.Context, req protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error) {
	return poolCall(p, ctx, "getLedgers", func(c rpc.Client) (protocol.GetLedgersResponse, error) {
		return c.GetLedgers(ctx, req)
	})
}

// GetTransaction implements rpc.Client.
func (p *Pool) GetTransaction(ctx context.Context, req protocol.GetTransactionRequest) (protocol.GetTransactionResponse, error) {
	return poolCall(p, ctx, "getTransaction", func(c rpc.Client) (protocol.GetTransactionResponse, error) {
		return c.GetTransaction(ctx, req)
	})
}

// GetTransactions implements rpc.Client.
func (p *Pool) GetTransactions(ctx context.Context, req protocol.GetTransactionsRequest) (protocol.GetTransactionsResponse, error) {
	return poolCall(p, ctx, "getTransactions", func(c rpc.Client) (protocol.GetTransactionsResponse, error) {
		return c.GetTransactions(ctx, req)
	})
}

// SimulateTransaction implements rpc.Client.
func (p *Pool) SimulateTransaction(ctx context.Context, req protocol.SimulateTransactionRequest) (protocol.SimulateTransactionResponse, error) {
	return poolCall(p, ctx, "simulateTransaction", func(c rpc.Client) (protocol.SimulateTransactionResponse, error) {
		return c.SimulateTransaction(ctx, req)
	})
}

// GetLatestLedger implements rpc.Client.
func (p *Pool) GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error) {
	return poolCall(p, ctx, "getLatestLedger", func(c rpc.Client) (protocol.GetLatestLedgerResponse, error) {
		return c.GetLatestLedger(ctx)
	})
}

// GetLedgerEntries implements rpc.LedgerEntriesClient for a failover pool.
func (p *Pool) GetLedgerEntries(ctx context.Context, req protocol.GetLedgerEntriesRequest) (protocol.GetLedgerEntriesResponse, error) {
	return poolCall(p, ctx, "getLedgerEntries", func(c rpc.Client) (protocol.GetLedgerEntriesResponse, error) {
		entries, ok := c.(rpc.LedgerEntriesClient)
		if !ok {
			return protocol.GetLedgerEntriesResponse{}, rpc.Permanent(fmt.Errorf(
				"pooled endpoint does not implement LedgerEntriesClient"))
		}
		return entries.GetLedgerEntries(ctx, req)
	})
}

// GetNetwork implements rpc.NetworkClient for a failover pool.
func (p *Pool) GetNetwork(ctx context.Context) (protocol.GetNetworkResponse, error) {
	return poolCall(p, ctx, "getNetwork", func(c rpc.Client) (protocol.GetNetworkResponse, error) {
		network, ok := c.(rpc.NetworkClient)
		if !ok {
			return protocol.GetNetworkResponse{}, rpc.Permanent(fmt.Errorf("pooled endpoint does not implement NetworkClient"))
		}
		return network.GetNetwork(ctx)
	})
}

// GetHealth implements rpc.HealthClient, so the health command works when
// --rpc-url is repeated. Failover, cooldowns and permanent-error handling are
// the same as for every other pooled call.
func (p *Pool) GetHealth(ctx context.Context) (protocol.GetHealthResponse, error) {
	return poolCall(p, ctx, "getHealth", func(c rpc.Client) (protocol.GetHealthResponse, error) {
		health, ok := c.(rpc.HealthClient)
		if !ok {
			return protocol.GetHealthResponse{}, rpc.Permanent(fmt.Errorf(
				"%w: pooled endpoint does not implement HealthClient", rpc.ErrHealthUnsupported))
		}
		return health.GetHealth(ctx)
	})
}

// isHealthy reports the current health mark for ep.
func (p *Pool) isHealthy(ep *endpoint) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return ep.healthy
}

// markSuccess records a working endpoint and clears any cooldown.
func (p *Pool) markSuccess(ep *endpoint) {
	p.mu.Lock()
	defer p.mu.Unlock()

	ep.healthy = true
	ep.failures = 0
	ep.cooldownUntil = time.Time{}
}

// markFailure records a failed call. Only retryable failures take an endpoint
// out of rotation; an endpoint that returned a generic application error is
// still reachable, and the next call should be free to use it.
func (p *Pool) markFailure(ep *endpoint, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	ep.failures++
	if rpc.IsRetryable(err) {
		ep.healthy = false
		// The cooldown doubles with each consecutive failure, so a dead
		// endpoint is probed less and less often until it recovers.
		ep.cooldownUntil = time.Now().Add(p.backoffDelay(ep.failures))
	}
}

// pick returns the next endpoint not already tried by this call.
//
// Healthy endpoints win. If none are healthy, an endpoint whose cooldown has
// elapsed is used next. If every endpoint is still cooling down, the one
// closest to recovery is tried anyway — see the Pool doc comment.
func (p *Pool) pick(tried map[*endpoint]bool) *endpoint {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	n := len(p.endpoints)

	for i := 0; i < n; i++ {
		idx := (p.next + i) % n
		ep := p.endpoints[idx]
		if tried[ep] {
			continue
		}
		if ep.healthy || !now.Before(ep.cooldownUntil) {
			p.next = (idx + 1) % n
			return ep
		}
	}

	var best *endpoint
	for _, ep := range p.endpoints {
		if tried[ep] {
			continue
		}
		if best == nil || ep.cooldownUntil.Before(best.cooldownUntil) {
			best = ep
		}
	}
	if best != nil {
		for i, ep := range p.endpoints {
			if ep == best {
				p.next = (i + 1) % n
				break
			}
		}
	}
	return best
}

// poolCall runs fn against successive endpoints until one succeeds or every
// endpoint has been tried.
func poolCall[T any](p *Pool, ctx context.Context, op string, fn func(rpc.Client) (T, error)) (T, error) {
	var zero T

	tried := make(map[*endpoint]bool, len(p.endpoints))
	var errs []error

	for range p.endpoints {
		if err := ctx.Err(); err != nil {
			return zero, err
		}

		ep := p.pick(tried)
		if ep == nil {
			break
		}
		tried[ep] = true

		result, err := fn(ep.client)
		if err == nil {
			p.markSuccess(ep)
			return result, nil
		}
		if rpc.IsPermanent(err) || IsCancellation(err) {
			return zero, err
		}

		p.markFailure(ep, err)
		errs = append(errs, fmt.Errorf("%s: %w", ep.url, err))
	}

	if len(errs) == 0 {
		return zero, fmt.Errorf("rpc pool: no endpoint available for %s", op)
	}
	return zero, fmt.Errorf("rpc pool: %s failed on %d endpoint(s): %w", op, len(errs), errors.Join(errs...))
}
