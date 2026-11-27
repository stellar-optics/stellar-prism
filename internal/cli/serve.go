package cli

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
	prismpb "github.com/stellar-optics/stellar-prism/proto"
)

// serve reuses the event command's RPC filters and cursor lifecycle.
func newServeCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	cmd := newEventsCmd(g, stdout, stderr)
	cmd.Use = "serve"
	cmd.Short = "Broadcast live contract events over SSE or WebSocket"
	cmd.Long = "Serve live contract events at /events over HTTP SSE or WebSocket. WebSockets also accept /ws."
	cmd.Example = "  prism serve --ws 127.0.0.1:8081\n  prism serve --sse 127.0.0.1:8080 --contract CBGSB...\n  prism serve --sse 127.0.0.1:8443 --tls-cert /path/to/cert.pem --tls-key /path/to/key.pem --tls-auto-reload"
	var maxRestarts int
	cmd.Flags().IntVar(&maxRestarts, "max-restarts", 0,
		"maximum stream restarts after panics (0 means unlimited)")

	cmd.Flags().StringVar(&g.tlsCert, "tls-cert", "", "path to TLS certificate file (PEM)")
	cmd.Flags().StringVar(&g.tlsKey, "tls-key", "", "path to TLS private key file (PEM)")
	cmd.Flags().BoolVar(&g.tlsAutoReload, "tls-auto-reload", false, "dynamically reload TLS certificate on file change")
	flag := cmd.Flags().Lookup("follow")
	flag.DefValue = "true"
	if err := flag.Value.Set("true"); err != nil {
		panic(err)
	}
	run := cmd.RunE
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if g.serveSSE == "" && g.serveWS == "" && g.serveGRPC == "" {
			return fmt.Errorf("serve requires --sse, --ws or --serve-grpc")
		}
		return superviseServe(cmd.Context(), maxRestarts, stderr, func() error {
			return run(cmd, args)
		}, waitServeRestart)
	}
	return cmd
}

const (
	initialServeRestartDelay = 5 * time.Second
	maxServeRestartDelay     = 5 * time.Minute
)

func superviseServe(
	ctx context.Context,
	maxRestarts int,
	stderr io.Writer,
	run func() error,
	wait func(context.Context, time.Duration) error,
) error {
	if maxRestarts < 0 {
		return fmt.Errorf("--max-restarts must not be negative")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if stderr == nil {
		stderr = io.Discard
	}

	restarts := 0
	for {
		panicValue, panicked, err := runServeAttempt(run)
		if !panicked {
			return err
		}
		if maxRestarts > 0 && restarts >= maxRestarts {
			return fmt.Errorf("serve: restart limit %d reached after panic: %v", maxRestarts, panicValue)
		}
		if ctx.Err() != nil {
			return fmt.Errorf("serve: interrupted after panic: %v", panicValue)
		}

		restarts++
		delay := serveRestartDelay(restarts)
		fmt.Fprintf(stderr, "prism: recovered panic in serve loop; restarting (%d) after %s: %v\n",
			restarts, delay, panicValue)
		if err := wait(ctx, delay); err != nil {
			return fmt.Errorf("serve: waiting to restart after panic: %w", err)
		}
	}
}

func runServeAttempt(run func() error) (panicValue any, panicked bool, err error) {
	defer func() {
		if value := recover(); value != nil {
			panicValue = value
			panicked = true
		}
	}()
	err = run()
	return nil, false, err
}

func serveRestartDelay(restarts int) time.Duration {
	delay := initialServeRestartDelay
	for i := 1; i < restarts && delay < maxServeRestartDelay; i++ {
		delay *= 2
		if delay > maxServeRestartDelay {
			delay = maxServeRestartDelay
		}
	}
	return delay
}

func waitServeRestart(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type broadcastOutput struct {
	next       stream.Sink
	broadcasts []stream.Sink
}

func (s *broadcastOutput) Emit(r stream.Record) error {
	if err := s.next.Emit(r); err != nil {
		return err
	}
	for _, out := range s.broadcasts {
		if err := out.Emit(r); err != nil {
			return err
		}
	}
	return nil
}
func (s *broadcastOutput) Flush() error { return s.next.Flush() }

// startBroadcasts binds synchronously so invalid addresses and occupied ports
// fail immediately. The caller closes the local batch sink before shutdown.
func startBroadcasts(ctx context.Context, g *globalFlags, out stream.Sink, stderr io.Writer, cancel context.CancelCauseFunc) (stream.Sink, func(), error) {
	type endpoint struct {
		address string
		handler http.Handler
		output  stream.Sink
		close   func() error
	}
	var endpoints []endpoint
	if g.serveSSE != "" {
		s := sink.NewSSE()
		endpoints = append(endpoints, endpoint{g.serveSSE, s, s, s.Close})
	}
	if g.serveWS != "" {
		s := sink.NewWebSocket()
		endpoints = append(endpoints, endpoint{g.serveWS, s, s, s.Close})
	}
	var servers []*http.Server
	var listeners []net.Listener
	var grpcServers []*grpc.Server
	var grpcSinks []*sink.GRPCSink
	cleanup := func() {
		for _, e := range endpoints {
			if err := e.close(); err != nil {
				fmt.Fprintf(stderr, "prism: closing broadcast: %v\n", err)
			}
		}
		shutdownCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		for _, server := range servers {
			if err := server.Shutdown(shutdownCtx); err != nil {
				_ = server.Close()
			}
		}
		for _, listener := range listeners {
			_ = listener.Close()
		}
		// Closing the sinks ends every active StreamEvents/StreamLedgers
		// handler; GracefulStop waits for those handlers, so without this a
		// connected client would keep shutdown waiting indefinitely.
		for _, s := range grpcSinks {
			_ = s.Close()
		}
		for _, server := range grpcServers {
			server.GracefulStop()
		}
	}

	var tlsCfg *tls.Config
	if g.tlsCert != "" || g.tlsKey != "" {
		if g.tlsCert == "" || g.tlsKey == "" {
			cleanup()
			return nil, nil, fmt.Errorf("both --tls-cert and --tls-key must be provided for TLS")
		}
		reloader, err := sink.NewDynamicTLSReloader(g.tlsCert, g.tlsKey, g.tlsAutoReload, func(format string, args ...any) {
			fmt.Fprintf(stderr, format+"\n", args...)
		})
		if err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("tls setup: %w", err)
		}
		tlsCfg = reloader.TLSConfig()
		prevCleanup := cleanup
		cleanup = func() {
			_ = reloader.Close()
			prevCleanup()
		}
	}

	combined := &broadcastOutput{next: out}
	for _, e := range endpoints {
		listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", e.address)
		if err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("broadcast listen %s: %w", e.address, err)
		}
		if tlsCfg != nil {
			listener = tls.NewListener(listener, tlsCfg)
		}
		server := &http.Server{
			Handler:           e.handler,
			TLSConfig:         tlsCfg,
			ReadHeaderTimeout: 5 * time.Second,
			IdleTimeout:       60 * time.Second,
			MaxHeaderBytes:    16 << 10,
		}
		listeners = append(listeners, listener)
		servers = append(servers, server)
		combined.broadcasts = append(combined.broadcasts, e.output)
		proto := "http"
		if tlsCfg != nil {
			proto = "https"
		}
		fmt.Fprintf(stderr, "prism: broadcast listening at %s://%s/events\n", proto, listener.Addr())
		go func() {
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				cancel(fmt.Errorf("broadcast server: %w", err))
			}
		}()
	}
	if g.serveGRPC != "" {
		listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", g.serveGRPC)
		if err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("grpc listen %s: %w", g.serveGRPC, err)
		}
		grpcSink := sink.NewGRPCSink()
		var grpcOpts []grpc.ServerOption
		if tlsCfg != nil {
			grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tlsCfg)))
		}
		server := grpc.NewServer(grpcOpts...)
		prismpb.RegisterPrismStreamServer(server, grpcSink)
		prismpb.RegisterEventStreamServiceServer(server, grpcSink)
		listeners = append(listeners, listener)
		grpcServers = append(grpcServers, server)
		grpcSinks = append(grpcSinks, grpcSink)
		combined.broadcasts = append(combined.broadcasts, grpcSink)
		go func() {
			if err := server.Serve(listener); err != nil {
				cancel(fmt.Errorf("grpc server: %w", err))
			}
		}()
	}
	return combined, cleanup, nil
}
