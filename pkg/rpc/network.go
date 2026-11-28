package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

// NetworkClient is the optional RPC capability used to identify a chain.
type NetworkClient interface {
	GetNetwork(context.Context) (protocol.GetNetworkResponse, error)
}

// ExpectedPassphrase returns the canonical passphrase for a named preset.
func ExpectedPassphrase(preset string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(preset)) {
	case "testnet":
		return network.TestNetworkPassphrase, nil
	case "mainnet":
		return network.PublicNetworkPassphrase, nil
	case "futurenet":
		return network.FutureNetworkPassphrase, nil
	case "standalone":
		return "Standalone Network ; February 2017", nil
	default:
		return "", fmt.Errorf("unknown network preset %q: want testnet, mainnet, futurenet, or standalone", preset)
	}
}

// ValidateNetwork verifies that the endpoint reports the passphrase selected
// by preset. It fails before a stream starts, preventing accidental writes to
// the wrong Stellar network.
func ValidateNetwork(ctx context.Context, client NetworkClient, preset string) error {
	expected, err := ExpectedPassphrase(preset)
	if err != nil {
		return err
	}
	actual, err := client.GetNetwork(ctx)
	if err != nil {
		return fmt.Errorf("get network: %w", err)
	}
	if actual.Passphrase != expected {
		return fmt.Errorf("network mismatch: --network %s expects %q, but RPC reports %q", strings.ToLower(preset), expected, actual.Passphrase)
	}
	return nil
}

// FailoverOption configures a FailoverRpcClient.
type FailoverOption func(*FailoverRpcClient)

// WithFailoverHealthInterval sets how often unhealthy endpoints are probed.
func WithFailoverHealthInterval(d time.Duration) FailoverOption {
	return func(f *FailoverRpcClient) { f.healthInterval = d }
}

// WithFailoverTimeout sets the request timeout for calls.
func WithFailoverTimeout(d time.Duration) FailoverOption {
	return func(f *FailoverRpcClient) { f.timeout = d }
}

// WithFailoverTransport overrides the HTTP round-tripper for endpoint clients.
func WithFailoverTransport(rt http.RoundTripper) FailoverOption {
	return func(f *FailoverRpcClient) { f.transport = rt }
}

// WithFailoverClientFactory overrides client creation for endpoints.
func WithFailoverClientFactory(fn func(url string) Client) FailoverOption {
	return func(f *FailoverRpcClient) { f.factory = fn }
}

// FailoverEndpoint represents one endpoint and its health state in FailoverRpcClient.
type FailoverEndpoint struct {
	URL      string
	Client   Client
	Healthy  bool
	Failures int
}

// FailoverRpcClient manages a round-robin / fallback pool of endpoints with automated health checks.
type FailoverRpcClient struct {
	mu             sync.Mutex
	endpoints      []*FailoverEndpoint
	current        int
	healthInterval time.Duration
	timeout        time.Duration
	transport      http.RoundTripper
	factory        func(url string) Client

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closed bool
}

// NewFailoverRpcClient returns a FailoverRpcClient managing a pool over the given URLs.
func NewFailoverRpcClient(urls []string, timeout time.Duration, opts ...FailoverOption) (*FailoverRpcClient, error) {
	cleaned := cleanURLs(urls)
	if len(cleaned) == 0 {
		return nil, errors.New("failover rpc client: at least one endpoint URL is required")
	}

	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	f := &FailoverRpcClient{
		healthInterval: 10 * time.Second,
		timeout:        timeout,
	}
	for _, opt := range opts {
		opt(f)
	}

	if f.factory == nil {
		f.factory = func(u string) Client {
			return NewWithTransport(u, f.timeout, f.transport)
		}
	}

	seen := make(map[string]bool, len(cleaned))
	for _, u := range cleaned {
		if seen[u] {
			continue
		}
		seen[u] = true
		f.endpoints = append(f.endpoints, &FailoverEndpoint{
			URL:     u,
			Client:  f.factory(u),
			Healthy: true,
		})
	}

	f.ctx, f.cancel = context.WithCancel(context.Background())
	if f.healthInterval > 0 {
		f.wg.Add(1)
		go f.healthLoop()
	}

	return f, nil
}

// Endpoints returns the current endpoints and their health status.
func (f *FailoverRpcClient) Endpoints() []*FailoverEndpoint {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*FailoverEndpoint, len(f.endpoints))
	for i, ep := range f.endpoints {
		clone := *ep
		out[i] = &clone
	}
	return out
}

func (f *FailoverRpcClient) nextEndpoint() (*FailoverEndpoint, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := len(f.endpoints)
	if n == 0 {
		return nil, -1
	}
	for i := 0; i < n; i++ {
		idx := (f.current + i) % n
		if f.endpoints[idx].Healthy {
			f.current = (idx + 1) % n
			return f.endpoints[idx], idx
		}
	}
	idx := f.current % n
	f.current = (idx + 1) % n
	return f.endpoints[idx], idx
}

func (f *FailoverRpcClient) markFailure(idx int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if idx >= 0 && idx < len(f.endpoints) {
		f.endpoints[idx].Failures++
		if isFailoverError(err) || f.endpoints[idx].Failures >= 1 {
			f.endpoints[idx].Healthy = false
		}
	}
}

func (f *FailoverRpcClient) markSuccess(idx int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if idx >= 0 && idx < len(f.endpoints) {
		f.endpoints[idx].Failures = 0
		f.endpoints[idx].Healthy = true
	}
}

func isFailoverError(err error) bool {
	if err == nil {
		return false
	}
	if IsRetryable(err) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "500") || strings.Contains(msg, "502") ||
		strings.Contains(msg, "503") || strings.Contains(msg, "504") ||
		strings.Contains(msg, "429") || strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "connection refused") || strings.Contains(msg, "eof")
}

func executeFailover[T any](f *FailoverRpcClient, op func(c Client) (T, error)) (T, error) {
	var zero T
	f.mu.Lock()
	n := len(f.endpoints)
	f.mu.Unlock()
	if n == 0 {
		return zero, errors.New("failover rpc client: no endpoints configured")
	}

	var lastErr error
	for attempt := 0; attempt < n; attempt++ {
		ep, idx := f.nextEndpoint()
		if ep == nil {
			break
		}
		res, err := op(ep.Client)
		if err == nil {
			f.markSuccess(idx)
			return res, nil
		}
		lastErr = err
		if isFailoverError(err) {
			f.markFailure(idx, err)
			continue
		}
		return zero, err
	}
	return zero, lastErr
}

// GetEvents implements Client.
func (f *FailoverRpcClient) GetEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	return executeFailover(f, func(c Client) (protocol.GetEventsResponse, error) {
		return c.GetEvents(ctx, req)
	})
}

// GetEventsBatch implements Client.
func (f *FailoverRpcClient) GetEventsBatch(ctx context.Context, reqs []protocol.GetEventsRequest) ([]protocol.GetEventsResponse, error) {
	return executeFailover(f, func(c Client) ([]protocol.GetEventsResponse, error) {
		return c.GetEventsBatch(ctx, reqs)
	})
}

// GetLedgers implements Client.
func (f *FailoverRpcClient) GetLedgers(ctx context.Context, req protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error) {
	return executeFailover(f, func(c Client) (protocol.GetLedgersResponse, error) {
		return c.GetLedgers(ctx, req)
	})
}

// GetTransaction implements Client.
func (f *FailoverRpcClient) GetTransaction(ctx context.Context, req protocol.GetTransactionRequest) (protocol.GetTransactionResponse, error) {
	return executeFailover(f, func(c Client) (protocol.GetTransactionResponse, error) {
		return c.GetTransaction(ctx, req)
	})
}

// GetTransactions implements Client.
func (f *FailoverRpcClient) GetTransactions(ctx context.Context, req protocol.GetTransactionsRequest) (protocol.GetTransactionsResponse, error) {
	return executeFailover(f, func(c Client) (protocol.GetTransactionsResponse, error) {
		return c.GetTransactions(ctx, req)
	})
}

// SimulateTransaction implements Client.
func (f *FailoverRpcClient) SimulateTransaction(ctx context.Context, req protocol.SimulateTransactionRequest) (protocol.SimulateTransactionResponse, error) {
	return executeFailover(f, func(c Client) (protocol.SimulateTransactionResponse, error) {
		return c.SimulateTransaction(ctx, req)
	})
}

// GetLatestLedger implements Client.
func (f *FailoverRpcClient) GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error) {
	return executeFailover(f, func(c Client) (protocol.GetLatestLedgerResponse, error) {
		return c.GetLatestLedger(ctx)
	})
}

// GetHealth implements HealthClient.
func (f *FailoverRpcClient) GetHealth(ctx context.Context) (protocol.GetHealthResponse, error) {
	return executeFailover(f, func(c Client) (protocol.GetHealthResponse, error) {
		if hc, ok := c.(HealthClient); ok {
			return hc.GetHealth(ctx)
		}
		return protocol.GetHealthResponse{}, ErrHealthUnsupported
	})
}

// GetNetwork implements NetworkClient.
func (f *FailoverRpcClient) GetNetwork(ctx context.Context) (protocol.GetNetworkResponse, error) {
	return executeFailover(f, func(c Client) (protocol.GetNetworkResponse, error) {
		if nc, ok := c.(NetworkClient); ok {
			return nc.GetNetwork(ctx)
		}
		return protocol.GetNetworkResponse{}, errors.New("network client not supported")
	})
}

// ProbeEndpoints triggers an immediate health check of unhealthy endpoints.
func (f *FailoverRpcClient) ProbeEndpoints() {
	f.probeEndpoints()
}

func (f *FailoverRpcClient) healthLoop() {
	defer f.wg.Done()
	ticker := time.NewTicker(f.healthInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.ctx.Done():
			return
		case <-ticker.C:
			f.probeEndpoints()
		}
	}
}

func (f *FailoverRpcClient) probeEndpoints() {
	f.mu.Lock()
	type probeItem struct {
		idx int
		c   Client
	}
	var unhealthy []probeItem
	for i, ep := range f.endpoints {
		if !ep.Healthy {
			unhealthy = append(unhealthy, probeItem{idx: i, c: ep.Client})
		}
	}
	f.mu.Unlock()

	for _, item := range unhealthy {
		ctx, cancel := context.WithTimeout(f.ctx, 3*time.Second)
		var healthy bool
		if hc, ok := item.c.(HealthClient); ok {
			resp, err := hc.GetHealth(ctx)
			if err == nil && strings.EqualFold(strings.TrimSpace(resp.Status), HealthStatusHealthy) {
				healthy = true
			}
		} else {
			resp, err := item.c.GetLatestLedger(ctx)
			if err == nil && resp.Sequence > 0 {
				healthy = true
			}
		}
		cancel()
		if healthy {
			f.markSuccess(item.idx)
		}
	}
}

// Close releases resources and cancels background health checks.
func (f *FailoverRpcClient) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	f.cancel()
	f.mu.Unlock()

	f.wg.Wait()

	var errs []error
	f.mu.Lock()
	for _, ep := range f.endpoints {
		if closer, ok := ep.Client.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	f.mu.Unlock()
	return errors.Join(errs...)
}
