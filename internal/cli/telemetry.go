package cli

import (
	"context"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// telemetryShutdownTimeout bounds how long the final span flush may take. A
// collector that is unreachable must not hold a finished command open, so the
// batch processor is given a short grace period and then abandoned.
const telemetryShutdownTimeout = 5 * time.Second

// startTelemetry installs the trace provider the whole command runs under.
//
// It is called from the root's PersistentPreRunE, after the profiler, so a
// configuration mistake (an unsupported endpoint scheme, a sample rate outside
// 0 to 1) fails before any RPC call is made.
func (g *globalFlags) startTelemetry(ctx context.Context) error {
	shutdown, err := stream.InitTelemetry(ctx, g.otelEndpoint, g.otelSampleRate)
	if err != nil {
		return err
	}
	g.telemetryShutdown = shutdown
	return nil
}

// stopTelemetry flushes buffered spans and shuts the provider down. It is a
// no-op when tracing was never enabled, which is the default.
func (g *globalFlags) stopTelemetry() error {
	if g.telemetryShutdown == nil {
		return nil
	}
	shutdown := g.telemetryShutdown
	g.telemetryShutdown = nil

	ctx, cancel := context.WithTimeout(context.Background(), telemetryShutdownTimeout)
	defer cancel()
	return shutdown(ctx)
}
