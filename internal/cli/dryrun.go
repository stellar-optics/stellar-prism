package cli

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/strkey"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

type readinessProbe struct {
	name string
	run  func(context.Context) (string, error)
}

func runDryRunCheck(
	ctx context.Context,
	cmd *cobra.Command,
	client rpc.Client,
	sf *streamFlags,
	g *globalFlags,
	stdout, stderr io.Writer,
) error {
	var validator sink.ReadinessValidator

	validator.Add("filter syntax", filterSummary(sf.filter), nil)
	validator.Run("contract addresses", func() (string, error) {
		return checkContractFlags(cmd)
	})
	validator.Run("rpc health", func() (string, error) {
		health, ok := client.(rpc.HealthClient)
		if !ok {
			return "getHealth not supported; latest-ledger probe used", nil
		}
		resp, err := health.GetHealth(ctx)
		if err != nil {
			return "", err
		}
		if resp.Status != "" && !strings.EqualFold(resp.Status, "healthy") {
			return "", fmt.Errorf("status %q", resp.Status)
		}
		return fmt.Sprintf("status %s, ledger %d", resp.Status, resp.LatestLedger), nil
	})
	validator.Run("latest ledger", func() (string, error) {
		ledger, err := latestLedger(ctx, client)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("head %d", ledger), nil
	})

	for _, probe := range dryRunProbes(sf, g.timeout) {
		validator.Run(probe.name, func() (string, error) {
			return probe.run(ctx)
		})
	}

	out := stdout
	if out == nil {
		out = stderr
	}
	if err := validator.Write(out); err != nil {
		return err
	}
	return validator.Err()
}

func filterSummary(filter stream.RecordFilter) string {
	switch f := filter.(type) {
	case nil:
		return "no filters configured"
	case stream.FilterSet:
		return fmt.Sprintf("%d filters compiled", len(f))
	default:
		return "filter compiled"
	}
}

func checkContractFlags(cmd *cobra.Command) (string, error) {
	if cmd == nil || cmd.Flags().Lookup("contract") == nil {
		return "no contracts configured", nil
	}
	ids, err := cmd.Flags().GetStringSlice("contract")
	if err != nil {
		return "", err
	}
	for _, id := range ids {
		if _, err := strkey.Decode(strkey.VersionByteContract, id); err != nil {
			return "", fmt.Errorf("invalid contract address %q: %w", id, err)
		}
	}
	if cmd.Flags().Lookup("contracts-file") != nil {
		path, err := cmd.Flags().GetString("contracts-file")
		if err != nil {
			return "", err
		}
		if path != "" {
			info, err := os.Stat(path)
			if err != nil {
				return "", fmt.Errorf("reading contracts file: %w", err)
			}
			if info.IsDir() {
				return "", fmt.Errorf("contracts file %q is a directory", path)
			}
			return fmt.Sprintf("%d contract IDs, watchlist %s", len(ids), path), nil
		}
	}
	return fmt.Sprintf("%d contract IDs valid", len(ids)), nil
}

func dryRunProbes(sf *streamFlags, timeout time.Duration) []readinessProbe {
	probes := make([]readinessProbe, 0, 8)

	if sf.output != "" {
		path := sf.output
		probes = append(probes, readinessProbe{name: "output sink", run: func(context.Context) (string, error) {
			return probeWritablePath(path)
		}})
	}
	if sf.dlqFile != "" {
		path := sf.dlqFile
		probes = append(probes, readinessProbe{name: "dead letter queue", run: func(context.Context) (string, error) {
			return probeWritablePath(path)
		}})
	}
	if sf.sinks.auditFile != "" {
		dir := filepath.Dir(sf.sinks.auditFile)
		probes = append(probes, readinessProbe{name: "audit sink", run: func(context.Context) (string, error) {
			return probeWritableDir(dir)
		}})
	}
	if sf.sinks.parquetDir != "" {
		dir := sf.sinks.parquetDir
		probes = append(probes, readinessProbe{name: "parquet sink", run: func(context.Context) (string, error) {
			return probeWritableDir(dir)
		}})
	}
	if sf.sinks.webhookURL != "" {
		url := sf.sinks.webhookURL
		probes = append(probes, readinessProbe{name: "webhook sink", run: func(ctx context.Context) (string, error) {
			return probeHTTP(ctx, url, timeout)
		}})
	}
	if sf.sinks.esURL != "" {
		url := sf.sinks.esURL
		probes = append(probes, readinessProbe{name: "elasticsearch sink", run: func(ctx context.Context) (string, error) {
			return probeHTTP(ctx, url, timeout)
		}})
	}

	probes = append(probes, readinessProbe{name: "sink configuration", run: func(context.Context) (string, error) {
		out, err := sf.sinks.addTo(sink.NewDryRunSink(io.Discard), false, nil)
		if err != nil {
			return "", err
		}
		closeSink(out)
		return configuredSinkSummary(sf), nil
	}})
	return probes
}

func configuredSinkSummary(sf *streamFlags) string {
	names := make([]string, 0, 8)
	if sf.output != "" {
		names = append(names, "output file")
	}
	if sf.dlqFile != "" {
		names = append(names, "dead letter queue")
	}
	if sf.kafkaTopic != "" {
		names = append(names, "kafka")
	}
	if sf.amqpQueue != "" {
		names = append(names, "amqp")
	}
	if sf.sinks.metricsAddr != "" {
		names = append(names, "prometheus")
	}
	if sf.sinks.auditFile != "" {
		names = append(names, "audit")
	}
	if sf.sinks.webhookURL != "" {
		names = append(names, "webhook")
	}
	if sf.sinks.cloudURL != "" || sf.sinks.cloudBucket != "" {
		names = append(names, "cloud storage")
	}
	if sf.sinks.syslogAddress != "" {
		names = append(names, "syslog")
	}
	if sf.sinks.esURL != "" {
		names = append(names, "elasticsearch")
	}
	if sf.sinks.natsURL != "" {
		names = append(names, "nats")
	}
	if sf.sinks.zmqEndpoint != "" {
		names = append(names, "zeromq")
	}
	if sf.sinks.parquetDir != "" {
		names = append(names, "parquet")
	}
	if sf.sinks.sqsQueue != "" {
		names = append(names, "sqs")
	}
	if sf.sinks.snsTopic != "" {
		names = append(names, "sns")
	}
	if sf.sinks.pubsubTopic != "" {
		names = append(names, "pubsub")
	}
	if len(names) == 0 {
		return "stdout only"
	}
	return strings.Join(names, ", ")
}

func probeWritablePath(path string) (string, error) {
	info, err := os.Stat(path)
	switch {
	case err == nil:
		if info.IsDir() {
			return "", fmt.Errorf("%q is a directory", path)
		}
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			return "", err
		}
		if err := file.Close(); err != nil {
			return "", err
		}
		return fmt.Sprintf("append access to %s", path), nil
	case os.IsNotExist(err):
		return probeWritableDir(filepath.Dir(path))
	default:
		return "", err
	}
}

func probeWritableDir(dir string) (string, error) {
	if dir == "" {
		dir = "."
	}
	info, err := os.Stat(dir)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%q is not a directory", dir)
	}
	file, err := os.CreateTemp(dir, ".prism-dry-run-*")
	if err != nil {
		return "", err
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	if err := os.Remove(path); err != nil {
		return "", err
	}
	return fmt.Sprintf("writable directory %s", dir), nil
}

func probeHTTP(ctx context.Context, url string, timeout time.Duration) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return "", err
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusInternalServerError {
		return "", fmt.Errorf("HTTP %s", resp.Status)
	}
	return fmt.Sprintf("HTTP %s", resp.Status), nil
}
