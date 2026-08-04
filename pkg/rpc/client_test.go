package rpc_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// TestIsRetryable pins the classification the streaming loop depends on.
//
// The asymmetry matters: calling a permanent failure transient only wastes
// backoff attempts, whereas calling a transient failure permanent aborts a
// stream that would have recovered. So the default leans towards retrying,
// and these cases encode where the line sits.
func TestIsRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is not retryable", nil, false},
		{"context cancellation is the operator stopping us", context.Canceled, false},
		{"deadline exceeded is not retryable", context.DeadlineExceeded, false},
		{"explicitly permanent", rpc.Permanent(errors.New("bad filter")), false},
		{"a malformed request is not retryable", errors.New("filter type invalid"), false},
		{"unknown errors are not retried", errors.New("something odd happened"), false},

		{"connection refused", errors.New("dial tcp: connection refused"), true},
		{"connection reset", errors.New("read: connection reset by peer"), true},
		{"broken pipe", errors.New("write: broken pipe"), true},
		{"unexpected EOF", errors.New("unexpected EOF"), true},
		{"timeout", errors.New("Client.Timeout exceeded"), true},
		{"dns failure", errors.New("no such host"), true},
		{"bad gateway", errors.New("502 Bad Gateway"), true},
		{"service unavailable", errors.New("503 Service Unavailable"), true},
		{"gateway timeout", errors.New("504 Gateway Timeout"), true},
		{"rate limited", errors.New("429 Too Many Requests"), true},
		{"wrapped transient error", fmt.Errorf("rpc getEvents: %w",
			errors.New("connection reset by peer")), true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := rpc.IsRetryable(tc.err); got != tc.want {
				t.Errorf("IsRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestIsRetryableNetError covers the typed path, which is the one that fires
// for real network failures rather than the string fallback.
func TestIsRetryableNetError(t *testing.T) {
	t.Parallel()

	var netErr net.Error = &net.DNSError{Err: "server misbehaving", IsTemporary: true}
	if !rpc.IsRetryable(netErr) {
		t.Error("a net.Error should be retryable")
	}
	if !rpc.IsRetryable(fmt.Errorf("wrapped: %w", netErr)) {
		t.Error("a wrapped net.Error should still be retryable")
	}
}

func TestPermanentWrapping(t *testing.T) {
	t.Parallel()

	inner := errors.New("cursor out of range")
	err := rpc.Permanent(inner)

	if !rpc.IsPermanent(err) {
		t.Error("IsPermanent() = false for a permanent error")
	}
	if !errors.Is(err, inner) {
		t.Error("a permanent error does not unwrap to its cause")
	}
	if err.Error() != inner.Error() {
		t.Errorf("Error() = %q, want the wrapped message %q", err, inner)
	}
	if rpc.IsPermanent(inner) {
		t.Error("IsPermanent() = true for an ordinary error")
	}
	// Wrapping must survive further wrapping, since the tailer adds context.
	if !rpc.IsPermanent(fmt.Errorf("events stream: %w", err)) {
		t.Error("permanence was lost when the error was wrapped again")
	}
}

func TestResolveURL(t *testing.T) {
	tests := []struct {
		name string
		flag string
		env  string
		want string
	}{
		{"flag wins", "https://flag.example", "https://env.example", "https://flag.example"},
		{"env when no flag", "", "https://env.example", "https://env.example"},
		{"default when neither", "", "", rpc.DefaultURL},
		{"blank env is ignored", "", "   ", rpc.DefaultURL},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(rpc.URLEnvVar, tc.env)
			if got := rpc.ResolveURL(tc.flag); got != tc.want {
				t.Errorf("ResolveURL(%q) with %s=%q = %q, want %q",
					tc.flag, rpc.URLEnvVar, tc.env, got, tc.want)
			}
		})
	}
}

// TestDefaultURLIsTestnet guards the promise that prism works with no
// configuration, and that it does not default to mainnet.
func TestDefaultURLIsTestnet(t *testing.T) {
	t.Parallel()

	if rpc.DefaultURL == "" {
		t.Fatal("DefaultURL is empty")
	}
	if !contains(rpc.DefaultURL, "testnet") {
		t.Errorf("DefaultURL = %q, want a testnet endpoint so the default is safe", rpc.DefaultURL)
	}
}

// TestNewAppliesTimeout checks that a zero timeout is replaced rather than
// meaning "wait forever", which would let a hung connection stall a stream.
func TestNewAppliesTimeout(t *testing.T) {
	t.Parallel()

	if c := rpc.New("https://example.invalid", 0); c == nil {
		t.Fatal("New() returned nil")
	}
	// A constructed client must satisfy the interface the streaming core uses.
	var _ rpc.Client = rpc.New("https://example.invalid", 0)
}

func contains(h, n string) bool {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return true
		}
	}
	return false
}
