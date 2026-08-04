// Command prism tails, filters and decodes live Stellar and Soroban data.
//
// It follows contract events and ledger closes the way tail -f follows a
// file, reconnecting through dropped connections without losing or repeating
// records, and emits NDJSON so a stream pipes straight into jq.
//
// XDR is decoded and rendered by stellar-xdr-lens, so values look exactly as
// they do under `lens decode`.
//
// Usage:
//
//	prism events  [--contract ID] [--topic F] [--follow]  stream contract events
//	prism ledgers [--follow]                              stream ledger closes
//	prism tx <hash>                                       explain a transaction
//
// Exit codes match stellar-xdr-lens: 0 success, 1 error, 2 a specific
// negative outcome — a gapped stream, or a failed transaction under
// --fail-on-error.
//
// See https://github.com/stellar-optics/stellar-prism for full documentation.
package main

import (
	"fmt"
	"os"

	"github.com/stellar-optics/stellar-prism/internal/cli"
)

func main() {
	err := cli.Execute(os.Args[1:], os.Stdout, os.Stderr)
	code, printable := cli.ExitCode(err)
	if printable {
		fmt.Fprintln(os.Stderr, "prism:", err)
	}
	os.Exit(code)
}
