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
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// Version is the build version, overridden at release time via ldflags:
//
//	go build -ldflags "-X github.com/stellar-optics/stellar-prism/internal/cli.Version=v0.1.0"
var Version = "dev"

// newClient constructs the RPC client used by every command.
//
// It is a variable so tests can substitute an in-memory fake and exercise the
// commands end to end without a network. Production code never reassigns it.
var newClient = func(url string, timeout time.Duration) rpc.Client {
	return rpc.New(url, timeout)
}

// globalFlags holds options shared by every subcommand.
type globalFlags struct {
	// color is one of "auto", "always" or "never", matching lens.
	color string
	// rpcURL overrides the endpoint; empty falls back to the environment.
	rpcURL string
	// timeout bounds a single RPC call.
	timeout time.Duration
}

// Execute builds and runs the root command, returning an error rather than
// exiting so main keeps control of the exit code.
func Execute(args []string, stdout, stderr io.Writer) error {
	root := newRootCmd(stdout, stderr)
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
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	root.SetOut(stdout)
	root.SetErr(stderr)

	root.PersistentFlags().StringVar(&g.color, "color", "auto",
		`when to colourise output: "auto", "always" or "never"`)
	root.PersistentFlags().StringVar(&g.rpcURL, "rpc-url", "",
		"Soroban RPC endpoint (default $"+rpc.URLEnvVar+", else "+rpc.DefaultURL+")")
	root.PersistentFlags().DurationVar(&g.timeout, "rpc-timeout", 30*time.Second,
		"timeout for a single RPC call")

	root.AddCommand(
		newEventsCmd(g, stdout, stderr),
		newLedgersCmd(g, stdout, stderr),
		newTransactionsCmd(g, stdout, stderr),
		newTxCmd(g, stdout),
		newVersionCmd(stdout),
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

// exitError carries an explicit process exit code.
type exitError struct {
	code int
	msg  string
}

func (e *exitError) Error() string { return e.msg }

// ExitCode maps an error to a process exit code, and reports whether it
// should be printed.
//
// The meanings match lens: 0 success, 1 error, 2 a specific negative outcome
// the caller may want to branch on. Here 2 means the stream gapped, which is
// distinguishable from an ordinary failure so CI can treat it differently.
func ExitCode(err error) (code int, printable bool) {
	if err == nil {
		return 0, false
	}
	var ee *exitError
	if errors.As(err, &ee) {
		return ee.code, false
	}
	return 1, true
}
