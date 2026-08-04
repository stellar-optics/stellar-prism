// Package rpc defines the narrow view of Soroban RPC that prism depends on,
// and an adapter over the Stellar SDK client that implements it.
//
// The Client interface exists so the streaming state machine can be tested
// without a network. Everything prism does against RPC goes through these
// three methods, which keeps the fake in internal/fakerpc small enough to
// reason about.
package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
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
// It is deliberately tiny: four read methods, all taking a context. New
// stream sources should prefer composing these over widening the interface,
// because every method added here must also be faked in tests.
type Client interface {
	// GetEvents returns contract events matching the request. Either
	// StartLedger or Pagination.Cursor must be set, never both — the RPC
	// rejects requests carrying both.
	GetEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error)

	// GetLedgers returns ledger metadata starting at the requested point.
	GetLedgers(ctx context.Context, req protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error)

	// GetTransaction returns a single transaction by hash.
	GetTransaction(ctx context.Context, req protocol.GetTransactionRequest) (protocol.GetTransactionResponse, error)

	// GetLatestLedger reports the head of the chain, which is how a stream
	// with no explicit start ledger works out where "now" is.
	GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error)
}

// ResolveURL picks the RPC endpoint from, in order of precedence: an explicit
// flag value, the SOROBAN_RPC_URL environment variable, then DefaultURL.
func ResolveURL(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	if env := strings.TrimSpace(os.Getenv(URLEnvVar)); env != "" {
		return env
	}
	return DefaultURL
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
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &SDKClient{
		inner: rpcclient.NewClient(url, &http.Client{Timeout: timeout}),
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

// GetLedgers implements Client.
func (c *SDKClient) GetLedgers(ctx context.Context, req protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error) {
	resp, err := c.inner.GetLedgers(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc getLedgers: %w", err)
	}
	return resp, nil
}

// GetTransaction implements Client.
func (c *SDKClient) GetTransaction(ctx context.Context, req protocol.GetTransactionRequest) (protocol.GetTransactionResponse, error) {
	resp, err := c.inner.GetTransaction(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc getTransaction: %w", err)
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

// Close releases the underlying HTTP resources.
func (c *SDKClient) Close() error {
	if err := c.inner.Close(); err != nil {
		return fmt.Errorf("rpc close: %w", err)
	}
	return nil
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
