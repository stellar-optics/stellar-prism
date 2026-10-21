package cli

import (
	"io"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func newWASMCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var (
		sf                streamFlags
		includeFailed     bool
		networkPassphrase string
	)

	cmd := &cobra.Command{
		Use:     "wasm",
		Aliases: []string{"deployments"},
		Short:   "Stream WASM uploads and contract deployments",
		Long: `Stream WASM uploads and contract creations by decoding transaction
envelopes.

Soroban emits no system event for deployment, so these records come from the
InvokeHostFunction operation itself. Contract IDs are derived from the network
passphrase; use --network-passphrase when streaming a network other than
testnet.

Failed transactions are skipped unless --include-failed is set.`,
		Example: `  # Follow deployments and uploads on testnet
  prism wasm --follow

  # Mainnet, including attempts that failed
  prism wasm --follow --include-failed \
    --network-passphrase "Public Global Stellar Network ; September 2015"

  # Just the JSON stream
  prism wasm --follow --json | jq -r 'select(.action=="create") | .contract'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := sf.validate(); err != nil {
				return err
			}

			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()

			from := sf.from
			if from == 0 {
				latest, err := latestLedger(cmd.Context(), client)
				if err != nil {
					return err
				}
				from = latest
			}

			newSource := func(startLedger uint32) (stream.Source, error) {
				return stream.NewWASMSource(client, stream.WASMQuery{
					StartLedger:       startLedger,
					PageLimit:         sf.pageSize,
					NetworkPassphrase: networkPassphrase,
					IncludeFailed:     includeFailed,
				})
			}
			return runStream(cmd.Context(), newSource, from, &sf, g, stdout, stderr)
		},
	}

	cmd.Flags().BoolVar(&includeFailed, "include-failed", false,
		"include uploads and creations from failed transactions")
	cmd.Flags().StringVar(&networkPassphrase, "network-passphrase",
		stream.DefaultNetworkPassphrase,
		"network passphrase used to derive contract IDs")
	sf.register(cmd, stream.DefaultTransactionPageLimit)

	return cmd
}
