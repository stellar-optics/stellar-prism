package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

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
	cmd.Example = "  prism serve --ws 127.0.0.1:8081\n  prism serve --sse 127.0.0.1:8080 --contract CBGSB..."
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
		return run(cmd, args)
	}
	return cmd
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
		for _, server := range grpcServers {
			server.GracefulStop()
		}
	}
	combined := &broadcastOutput{next: out}
	for _, e := range endpoints {
		listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", e.address)
		if err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("broadcast listen %s: %w", e.address, err)
		}
		server := &http.Server{Handler: e.handler, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
		listeners = append(listeners, listener)
		servers = append(servers, server)
		combined.broadcasts = append(combined.broadcasts, e.output)
		fmt.Fprintf(stderr, "prism: broadcast listening at %s/events\n", listener.Addr())
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
		server := grpc.NewServer()
		prismpb.RegisterPrismStreamServer(server, grpcSink)
		prismpb.RegisterEventStreamServiceServer(server, grpcSink)
		listeners = append(listeners, listener)
		grpcServers = append(grpcServers, server)
		combined.broadcasts = append(combined.broadcasts, grpcSink)
		go func() {
			if err := server.Serve(listener); err != nil {
				cancel(fmt.Errorf("grpc server: %w", err))
			}
		}()
	}
	return combined, cleanup, nil
}
