package main

import (
	"io"

	"github.com/spf13/cobra"
	"github.com/stellar-optics/stellar-prism/internal/cli"
)

// newReplayCmd creates the `prism replay` command for package main.
func newReplayCmd(stdout, stderr io.Writer) *cobra.Command {
	return cli.NewReplayCmd(nil, stdout, stderr)
}
