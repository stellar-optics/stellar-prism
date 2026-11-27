package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/stellar-optics/stellar-prism/internal/cli"
)

// pprofServer serves the Go runtime profiles over HTTP and joins the serving
// goroutine when it is closed.
//
// Operator guide: docs/performance/tuning_and_profiling.md covers capturing
// CPU, heap and allocation profiles with go tool pprof, reading them, and
// GOGC/GOMEMLIMIT tuning. Baseline numbers live in docs/benchmarks.md.
type pprofServer struct {
	server    *http.Server
	serveErr  chan error
	addr      string
	closeOnce sync.Once
	closeErr  error
}

var _ cli.Profiler = (*pprofServer)(nil)

// startPProf starts a loopback-only profiling server. Port zero binds an
// ephemeral port, which tests use; --pprof-port zero disables profiling before
// this function is reached.
func startPProf(port int, stderr io.Writer) (*pprofServer, error) {
	if port < 0 || port > 65535 {
		return nil, fmt.Errorf("pprof port %d must be between 0 and 65535", port)
	}

	mux := http.NewServeMux()
	registerPProfHandlers(mux)

	listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("listening for pprof on 127.0.0.1:%d: %w", port, err)
	}

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       time.Minute,
	}
	p := &pprofServer{
		server:   server,
		serveErr: make(chan error, 1),
		addr:     listener.Addr().String(),
	}
	go func() {
		p.serveErr <- server.Serve(listener)
	}()

	if _, err := fmt.Fprintf(stderr, "prism: serving pprof on http://%s/debug/pprof/\n", p.addr); err != nil {
		return nil, errors.Join(fmt.Errorf("reporting the pprof address: %w", err), p.Close())
	}
	return p, nil
}

// newPProfProfiler adapts startPProf to the CLI's profiler seam.
func newPProfProfiler(port int, stderr io.Writer) (cli.Profiler, error) {
	return startPProf(port, stderr)
}

func registerPProfHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", httppprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", httppprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", httppprof.Trace)
	for _, name := range []string{"allocs", "block", "goroutine", "heap", "mutex", "threadcreate"} {
		mux.Handle("/debug/pprof/"+name, httppprof.Handler(name))
	}
}

// Addr reports the address the profiling server is listening on.
func (p *pprofServer) Addr() string { return p.addr }

// Close shuts the server down, closes any lingering connections and waits for
// the serving goroutine to return. It is safe to call more than once and from
// more than one goroutine.
func (p *pprofServer) Close() error {
	p.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		closeErr := p.server.Shutdown(ctx)
		if closeErr != nil {
			closeErr = errors.Join(closeErr, p.server.Close())
		}

		if serveErr := <-p.serveErr; serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			closeErr = errors.Join(closeErr, serveErr)
		}
		p.closeErr = closeErr
	})
	return p.closeErr
}
