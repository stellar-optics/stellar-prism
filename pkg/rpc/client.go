// Package rpc defines the narrow view of Soroban RPC that prism depends on,
// and an adapter over the Stellar SDK client that implements it.
//
// The Client interface exists so the streaming state machine can be tested
// without a network. Everything the streaming core does against RPC goes
// through this interface, which keeps the fake in internal/fakerpc small
// enough to reason about.
package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/clients/rpcclient"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

// DefaultURL is the public testnet endpoint, so prism works with no
// configuration. Mainnet and local networks are selected with --rpc-url or
// SOROBAN_RPC_URL.
const DefaultURL = "https://soroban-testnet.stellar.org"

// URLEnvVar is the environment variable consulted when --rpc-url is not given.
const URLEnvVar = "SOROBAN_RPC_URL"

// Client is the subset of Soroban RPC that prism uses.
//
// It is deliberately tiny: read methods, all taking a context. New
// stream sources should prefer composing these over widening the interface,
// because every method added here must also be faked in tests.
type Client interface {
	// GetEvents returns contract events matching the request. Either
	// StartLedger or Pagination.Cursor must be set, never both — the RPC
	// rejects requests carrying both.
	GetEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error)

	// GetEventsBatch returns contract events matching each request, combining
	// them into as few HTTP roundtrips as the server allows. Each entry's
	// StartLedger or Pagination.Cursor must be mutually exclusive; at least
	// one must be set.
	//
	// A failed entry aborts the batch: the responses gathered before the
	// failure are returned alongside the error, so a caller never has to
	// guess how far the batch got.
	GetEventsBatch(ctx context.Context, reqs []protocol.GetEventsRequest) ([]protocol.GetEventsResponse, error)

	// GetLedgers returns ledger metadata starting at the requested point.
	GetLedgers(ctx context.Context, req protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error)

	// GetTransaction returns a single transaction by hash.
	GetTransaction(ctx context.Context, req protocol.GetTransactionRequest) (protocol.GetTransactionResponse, error)

	// GetTransactions returns transactions matching the request.
	GetTransactions(ctx context.Context, req protocol.GetTransactionsRequest) (protocol.GetTransactionsResponse, error)

	// SimulateTransaction estimates resource usage and authorization for a
	// transaction envelope without submitting it.
	SimulateTransaction(ctx context.Context, req protocol.SimulateTransactionRequest) (protocol.SimulateTransactionResponse, error)

	// GetLatestLedger reports the head of the chain, which is how a stream
	// with no explicit start ledger works out where "now" is.
	GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error)
}

// ResolveURL picks a single RPC endpoint from, in order of precedence: an
// explicit flag value, the SOROBAN_RPC_URL environment variable, then
// DefaultURL.
//
// It is kept for callers that only support one endpoint; ResolveURLs is the
// form used by the CLI, where a failover pool may be configured.
func ResolveURL(flagValue string) string {
	return ResolveURLs([]string{flagValue})[0]
}

// ResolveURLs picks the RPC endpoints from, in order of precedence: explicit
// flag values, the SOROBAN_RPC_URL environment variable, then DefaultURL.
//
// Multiple endpoints may be configured by repeating --rpc-url, or by
// separating them with commas in SOROBAN_RPC_URL. Blank entries are ignored.
func ResolveURLs(flagValues []string) []string {
	urls := cleanURLs(flagValues)
	if len(urls) > 0 {
		return urls
	}
	if env := strings.TrimSpace(os.Getenv(URLEnvVar)); env != "" {
		if urls := cleanURLs(strings.Split(env, ",")); len(urls) > 0 {
			return urls
		}
	}
	return []string{DefaultURL}
}

func cleanURLs(values []string) []string {
	urls := make([]string, 0, len(values))
	for _, value := range values {
		if url := strings.TrimSpace(value); url != "" {
			urls = append(urls, url)
		}
	}
	return urls
}

// ResolveSRV resolves the DNS SRV records for domain (the _rpc._tcp.<domain>
// records published by a clustered RPC deployment) and returns one https URL
// per advertised node. It is how a fleet of prism instances can share a
// single hostname and still spread across the cluster behind it.
func ResolveSRV(domain string) ([]string, error) {
	_, records, err := net.LookupSRV("rpc", "tcp", domain)
	if err != nil {
		return nil, fmt.Errorf("srv lookup for %s: %w", domain, err)
	}

	urls := make([]string, 0, len(records))
	for _, srv := range records {
		// SRV targets are absolute names, so they carry a trailing dot that
		// must be trimmed before the host is used in a URL.
		target := strings.TrimSuffix(srv.Target, ".")
		urls = append(urls, fmt.Sprintf("https://%s:%d", target, srv.Port))
	}
	return urls, nil
}

// SDKClient adapts the Stellar SDK's RPC client to the Client interface.
type SDKClient struct {
	inner *rpcclient.Client
}

// New returns a Client backed by the Stellar SDK, talking to url.
//
// The HTTP client carries a per-request timeout so a hung connection surfaces
// as a retryable error rather than stalling the stream indefinitely.
func New(url string, timeout time.Duration) *SDKClient {
	return NewWithTransport(url, timeout, nil)
}

// NewWithTransport returns a Client backed by the Stellar SDK that sends every
// HTTP request through transport.
//
// A nil transport means http.DefaultTransport, so New and NewWithTransport
// behave identically when no transport is supplied. A *WireLogger is the
// intended use: it observes the exchange while remaining transparent to the
// streaming code and the SDK.
func NewWithTransport(url string, timeout time.Duration, transport http.RoundTripper) *SDKClient {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	httpClient := &http.Client{Timeout: timeout}
	if transport != nil {
		httpClient.Transport = transport
	}
	return &SDKClient{
		inner: rpcclient.NewClient(url, httpClient),
	}
}

// GetEvents implements Client.
func (c *SDKClient) GetEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	resp, err := c.inner.GetEvents(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc getEvents: %w", err)
	}
	return resp, nil
}

// GetEventsBatch implements Client.
func (c *SDKClient) GetEventsBatch(ctx context.Context, reqs []protocol.GetEventsRequest) ([]protocol.GetEventsResponse, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	responses := make([]protocol.GetEventsResponse, 0, len(reqs))
	for _, req := range reqs {
		resp, err := c.inner.GetEvents(ctx, req)
		if err != nil {
			// Hand back the responses gathered so far with the error. Dropping
			// them would force the caller to re-issue requests that already
			// succeeded; swallowing the error would report a truncated batch as
			// complete.
			return responses, fmt.Errorf("rpc getEventsBatch: %w", err)
		}
		responses = append(responses, resp)
	}
	return responses, nil
}

// GetLedgers implements Client.
func (c *SDKClient) GetLedgers(ctx context.Context, req protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error) {
	resp, err := c.inner.GetLedgers(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc getLedgers: %w", err)
	}
	return resp, nil
}

// GetLedgerEntries returns the requested ledger entries.
//
// It is deliberately not part of Client: the streaming core only needs the
// core methods above, and keeping the interface small avoids forcing every
// test double to implement a method it never uses. Callers that need it, such
// as contract ABI resolution, assert for LedgerEntriesClient instead.
func (c *SDKClient) GetLedgerEntries(ctx context.Context, req protocol.GetLedgerEntriesRequest) (protocol.GetLedgerEntriesResponse, error) {
	resp, err := c.inner.GetLedgerEntries(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc getLedgerEntries: %w", err)
	}
	return resp, nil
}

// LedgerEntriesClient is the optional capability used to resolve contract
// specs from on-chain WASM. SDKClient implements it.
type LedgerEntriesClient interface {
	GetLedgerEntries(ctx context.Context, req protocol.GetLedgerEntriesRequest) (protocol.GetLedgerEntriesResponse, error)
}

// GetTransaction implements Client.
func (c *SDKClient) GetTransaction(ctx context.Context, req protocol.GetTransactionRequest) (protocol.GetTransactionResponse, error) {
	resp, err := c.inner.GetTransaction(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc getTransaction: %w", err)
	}
	return resp, nil
}

// GetTransactions implements Client.
func (c *SDKClient) GetTransactions(ctx context.Context, req protocol.GetTransactionsRequest) (protocol.GetTransactionsResponse, error) {
	resp, err := c.inner.GetTransactions(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc getTransactions: %w", err)
	}
	return resp, nil
}

// SimulateTransaction implements Client.
func (c *SDKClient) SimulateTransaction(ctx context.Context, req protocol.SimulateTransactionRequest) (protocol.SimulateTransactionResponse, error) {
	resp, err := c.inner.SimulateTransaction(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc simulateTransaction: %w", err)
	}
	return resp, nil
}

// GetLatestLedger implements Client.
func (c *SDKClient) GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error) {
	resp, err := c.inner.GetLatestLedger(ctx)
	if err != nil {
		return resp, fmt.Errorf("rpc getLatestLedger: %w", err)
	}
	return resp, nil
}

// GetHealth implements HealthClient. It is intentionally not part of the
// streaming Client interface; only the health command needs it.
func (c *SDKClient) GetHealth(ctx context.Context) (protocol.GetHealthResponse, error) {
	resp, err := c.inner.GetHealth(ctx)
	if err != nil {
		return resp, fmt.Errorf("rpc getHealth: %w", err)
	}
	return resp, nil
}

// Close releases the underlying HTTP resources.
func (c *SDKClient) Close() error {
	if err := c.inner.Close(); err != nil {
		return fmt.Errorf("rpc close: %w", err)
	}
	return nil
}

// NewClientOption configures optional behaviour of the SDK-backed client.
type NewClientOption struct {
	// ProxyURL, when set, routes RPC traffic through an HTTP, HTTPS or SOCKS5
	// proxy instead of dialling the endpoint directly.
	ProxyURL string
}

// newClientOptions collects options for SDK client creation.
type newClientOptions struct {
	transport http.RoundTripper
}

// applyNewClientOptions resolves opts into the concrete transport settings
// used to build the client. An unusable option is reported and ignored rather
// than fatal: a mistyped proxy should degrade to a direct connection, not
// leave prism unable to start.
func applyNewClientOptions(opts ...NewClientOption) newClientOptions {
	applied := newClientOptions{}
	for _, opt := range opts {
		if opt.ProxyURL == "" {
			continue
		}
		transport, err := proxyURLToTransport(opt.ProxyURL)
		if err != nil {
			fmt.Fprintf(os.Stderr, "rpc: ignoring proxy URL %q: %v\n", opt.ProxyURL, err)
			continue
		}
		applied.transport = transport
	}
	return applied
}

// proxyURLToTransport converts a proxy URL into the transport that uses it.
// net/http speaks HTTP, HTTPS and SOCKS5 proxies natively, so the proxy only
// has to be handed to the transport rather than reimplemented.
func proxyURLToTransport(raw string) (http.RoundTripper, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid proxy URL: %w", err)
	}
	switch u.Scheme {
	case "http", "https", "socks5":
		if u.Host == "" {
			return nil, fmt.Errorf("proxy URL %q has no host", raw)
		}
		return &http.Transport{Proxy: http.ProxyURL(u)}, nil
	default:
		return nil, fmt.Errorf("unsupported proxy scheme %q, use http, https, or socks5", u.Scheme)
	}
}

// PermanentError marks a failure that retrying cannot fix, such as a
// malformed filter or a cursor outside the server's retention window.
//
// The streaming loop retries anything not marked permanent, so misclassifying
// a permanent error merely wastes backoff attempts, while misclassifying a
// transient one aborts a stream that would have recovered. When in doubt,
// leave it transient.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err so the streaming loop stops rather than retrying.
func Permanent(err error) error { return &PermanentError{Err: err} }

// IsPermanent reports whether err should abort the stream instead of being
// retried.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// IsRetryable reports whether err looks like a transient condition worth
// retrying: a dropped connection, a timeout, or a server-side hiccup.
//
// A context cancellation is never retryable — that is the operator asking us
// to stop.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if IsPermanent(err) {
		return false
	}
	// Network-level failures are the common case and are always worth a retry.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	// The SDK surfaces JSON-RPC and transport errors as plain strings, so fall
	// back to matching the shapes that indicate a server or connection problem
	// rather than a malformed request.
	msg := strings.ToLower(err.Error())
	for _, frag := range []string{
		"connection refused",
		"connection reset",
		"broken pipe",
		"eof",
		"timeout",
		"timed out",
		"temporary failure",
		"no such host",
		"server error",
		"bad gateway",
		"service unavailable",
		"gateway timeout",
		"too many requests",
		"502", "503", "504", "429",
	} {
		if strings.Contains(msg, frag) {
			return true
		}
	}
	return false
}
