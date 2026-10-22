// Package cli implements the prism command-line interface.
//
// Commands are thin: they resolve configuration, construct a Source and a
// Sink, and hand both to a stream.Tailer. Anything worth testing lives in
// pkg/stream or pkg/sink so it is reachable without a process or a network.
//
// Flag names deliberately mirror stellar-xdr-lens where the concept is
// shared (--json, --color), so the two tools feel like one family.
package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// Version is the build version, overridden at release time via ldflags:
//
//	go build -ldflags "-X github.com/stellar-optics/stellar-prism/internal/cli.Version=v0.1.0"
var Version = "dev"

// newClient constructs the RPC client used by every command.
//
// More than one URL produces a stream.Pool, so every command gains failover
// without knowing about it. It is a variable so tests can substitute an
// in-memory fake and exercise the commands end to end without a network.
// Production code never reassigns it.
var newClient = func(urls []string, timeout time.Duration) (rpc.Client, error) {
	if len(urls) == 1 {
		return rpc.New(urls[0], timeout), nil
	}
	return stream.NewPool(urls, stream.WithClientFactory(func(url string) rpc.Client {
		return rpc.New(url, timeout)
	}))
}

// client resolves the configured endpoints and builds the client every
// command shares.
func client(g *globalFlags) (rpc.Client, error) {
	return newClient(rpc.ResolveURLs(g.rpcURLs), g.timeout)
}

// openClient resolves the configured endpoints, builds the client and returns
// the deferrable cleanup every command uses.
func openClient(g *globalFlags, stderr io.Writer) (rpc.Client, func(), error) {
	c, err := client(g)
	if err != nil {
		return nil, nil, err
	}
	return c, func() { closeClient(c, stderr) }, nil
}

// closeClient releases a client and reports a failure on stderr. Closing a
// pool stops its health probe; for a single client it simply releases HTTP
// resources, so the failure is not worth aborting an otherwise finished
// command over.
func closeClient(c rpc.Client, stderr io.Writer) {
	closer, ok := c.(io.Closer)
	if !ok {
		return
	}
	if err := closer.Close(); err != nil {
		fmt.Fprintf(stderr, "prism: closing rpc client: %v\n", err)
	}
}

// globalFlags holds options shared by every subcommand.
type globalFlags struct {
	serveSSE string
	serveWS  string
	// color is one of "auto", "always" or "never", matching lens.
	color string
	// rpcURLs overrides the endpoints; empty falls back to the environment.
	// Repeating the flag builds a failover pool.
	rpcURLs []string
	// timeout bounds a single RPC call.
	timeout time.Duration
}

// Execute builds and runs the root command, returning an error rather than
// exiting so main keeps control of the exit code.
//
// extra commands are attached to the root before execution, which lets the
// binary add commands whose implementation belongs in package main (such as
// the documentation generator) without growing this package's dependencies.
func Execute(args []string, stdout, stderr io.Writer, extra ...*cobra.Command) error {
	root := newRootCmd(stdout, stderr)
	root.AddCommand(extra...)
	root.SetArgs(args)
	return root.Execute()
}

func newRootCmd(stdout, stderr io.Writer) *cobra.Command {
	g := &globalFlags{}

	root := &cobra.Command{
		Use:   "prism",
		Short: "Tail, filter and decode live Stellar and Soroban data",
		Long: `prism streams Stellar and Soroban data from RPC and decodes it as it arrives.

It follows contract events and ledger closes the way tail -f follows a file,
reconnecting through dropped connections without losing or repeating records,
and emitting NDJSON so a stream pipes straight into jq.

XDR is decoded and rendered by stellar-xdr-lens, so values look exactly as
they do under ` + "`lens decode`" + `.

For a one-shot look at events with contract-spec-decoded parameters, the
official ` + "`stellar events`" + ` command does that better. prism is for watching.`,
		Example: `  # Follow a contract's events as they happen
  prism events --contract CBGSB... --follow

  # Pipe a live stream into jq
  prism events --contract CBGSB... --follow --json | jq -r '.value.value.U32'

  # Backfill from a ledger, then keep following
  prism events --contract CBGSB... --from 1234567 --follow

  # Explain a transaction, through the lens explain path
  prism tx 3389e9f0...`,
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			_, err := parseColorMode(g.color)
			return err
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	root.SetOut(stdout)
	root.SetErr(stderr)

	root.PersistentFlags().StringVar(&g.color, "color", "auto",
		`when to colourise output: "auto", "always" or "never"`)
	root.PersistentFlags().StringArrayVar(&g.rpcURLs, "rpc-url", nil,
		"Soroban RPC endpoint; repeat for a failover pool (default $"+rpc.URLEnvVar+", else "+rpc.DefaultURL+")")
	root.PersistentFlags().DurationVar(&g.timeout, "rpc-timeout", 30*time.Second,
		"timeout for a single RPC call")
	registerFlagCompletion(root, "color", string(colorAuto), string(colorAlways), string(colorNever))

	root.PersistentFlags().StringVar(&g.serveSSE, "serve-sse", "", "broadcast SSE at this listen address (/events); retains 1024 records for replay")
	root.PersistentFlags().StringVar(&g.serveSSE, "sse", "", "alias for --serve-sse")
	root.PersistentFlags().StringVar(&g.serveWS, "ws", "", "broadcast WebSocket events at this listen address (/events or /ws)")
	root.AddCommand(
		newServeCmd(g, stdout, stderr),
		newEventsCmd(g, stdout, stderr),
		newLedgersCmd(g, stdout, stderr),
		newTransactionsCmd(g, stdout, stderr),
		newOperationsCmd(g, stdout, stderr),
		newSimulateCmd(g, stdout, stderr),
		newABICmd(g, stdout, stderr),
		newStorageCmd(g, stdout, stderr),
		newWASMCmd(g, stdout, stderr),
		newFeeStatsCmd(g, stdout, stderr),
		newContractsCmd(g, stdout, stderr),
		newTradesCmd(g, stdout, stderr),
		newHealthCmd(g, stdout, stderr),
		newTxCmd(g, stdout, stderr),
		newDiffCmd(g, stdout, stderr),
		newGenerateTypesCmd(g, stdout, stderr),
		newAuditCmd(g, stdout, stderr),
		newAuditVerifyCmd(stdout, stderr),
		newVersionCmd(stdout),
		newCompletionCmd(stdout),
	)
	return root
}

func newVersionCmd(stdout io.Writer) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the prism version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := fmt.Fprintln(stdout, Version)
			return err
		},
	}
}

// ExitCode maps an error to a process exit code, and reports whether it
// should be printed.
// The meanings match lens: 0 success, 1 error, 2 a specific negative outcome
// the caller may want to branch on. Here 2 means the stream gapped, which is
// distinguishable from an ordinary failure so CI can treat it differently.
