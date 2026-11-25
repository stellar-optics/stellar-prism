package rpc

import (
	"net/http"
	"net/url"
	"slices"
	"testing"
)

// TestProxyURLToTransport covers the schemes net/http speaks natively. The
// transport must hand the proxy back to the dialer rather than rewriting the
// request, which is what makes HTTPS endpoints keep their TLS handshake.
func TestProxyURLToTransport(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"http://proxy.example:8080",
		"https://proxy.example:8443",
		"socks5://proxy.example:1080",
	} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()

			roundTripper, err := proxyURLToTransport(raw)
			if err != nil {
				t.Fatalf("proxyURLToTransport(%q) error = %v", raw, err)
			}
			transport, ok := roundTripper.(*http.Transport)
			if !ok {
				t.Fatalf("proxyURLToTransport(%q) returned %T, want *http.Transport", raw, roundTripper)
			}
			if transport.Proxy == nil {
				t.Fatal("transport has no Proxy function, so the proxy would never be used")
			}
			got, err := transport.Proxy(&http.Request{})
			if err != nil {
				t.Fatalf("Proxy() error = %v", err)
			}
			if got.String() != raw {
				t.Errorf("Proxy() = %q, want %q", got, raw)
			}
		})
	}
}

// TestProxyURLToTransportRejectsBadInput proves an unusable proxy fails loudly
// at construction time, so the caller can fall back to a direct connection
// instead of silently sending traffic to the wrong host.
func TestProxyURLToTransportRejectsBadInput(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"ftp://proxy.example:21", // unsupported scheme
		"proxy.example:8080",     // missing scheme entirely
		"http://[::1",            // malformed URL
		"socks5://",              // no host to dial
		"",                       // empty
	} {
		if _, err := proxyURLToTransport(raw); err == nil {
			t.Errorf("proxyURLToTransport(%q) error = nil, want a rejection", raw)
		}
	}
}

// TestApplyNewClientOptionsIgnoresBadProxy checks the documented degradation:
// a bad proxy is reported and skipped rather than poisoning the client, so a
// mistyped flag value cannot stop prism from starting.
func TestApplyNewClientOptionsIgnoresBadProxy(t *testing.T) {
	t.Parallel()

	options := applyNewClientOptions(
		NewClientOption{ProxyURL: "ftp://proxy.example:21"},
		NewClientOption{ProxyURL: "http://proxy.example:8080"},
	)
	if options.transport == nil {
		t.Fatal("applyNewClientOptions() ignored the valid proxy because of an earlier invalid one")
	}
}

// TestResolveSRVRejectsUnresolvableDomain keeps the failure path honest: a
// domain that cannot be a SRV owner name is rejected rather than silently
// yielding an empty endpoint list the caller would treat as "no endpoints".
func TestResolveSRVRejectsUnresolvableDomain(t *testing.T) {
	t.Parallel()

	if _, err := ResolveSRV("not a valid domain name"); err == nil {
		t.Error("ResolveSRV() error = nil, want a failure for an invalid domain")
	}
}

// TestResolveURLToTransportRoundTrip guards against regressing to rewriting
// the request URL: the proxy is consulted, the request still targets the
// original host.
func TestProxyKeepsOriginalHost(t *testing.T) {
	t.Parallel()

	roundTripper, err := proxyURLToTransport("http://proxy.example:8080")
	if err != nil {
		t.Fatalf("proxyURLToTransport() error = %v", err)
	}
	transport := roundTripper.(*http.Transport)

	target, err := url.Parse("https://rpc.example/health")
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	req := &http.Request{URL: target, Host: "rpc.example"}
	proxyURL, err := transport.Proxy(req)
	if err != nil {
		t.Fatalf("Proxy() error = %v", err)
	}
	if proxyURL.Host != "proxy.example:8080" {
		t.Errorf("Proxy() host = %q, want the configured proxy", proxyURL.Host)
	}
	if req.URL.Host != "rpc.example" || req.URL.Scheme != "https" {
		t.Errorf("Proxy() rewrote the request to %s://%s, want the original target preserved",
			req.URL.Scheme, req.URL.Host)
	}
}

func TestApplyNewClientOptionsMergesHeaders(t *testing.T) {
	t.Parallel()

	options := applyNewClientOptions(
		NewClientOption{
			ProxyURL: "ftp://proxy.example:21",
			Headers:  http.Header{"X-Api-Key": {"first"}, "X-Keep": {"yes"}},
		},
		NewClientOption{
			Headers: http.Header{"x-api-key": {"second"}},
		},
	)
	if !slices.Equal(options.headers["X-Api-Key"], []string{"second"}) {
		t.Errorf("X-Api-Key = %v, want [second]", options.headers["X-Api-Key"])
	}
	if !slices.Equal(options.headers["X-Keep"], []string{"yes"}) {
		t.Errorf("X-Keep = %v, want [yes]", options.headers["X-Keep"])
	}
}
