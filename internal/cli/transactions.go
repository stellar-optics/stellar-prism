package cli

import (
	"io"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func newTransactionsCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var sf streamFlags

	cmd := &cobra.Command{
		Use:   "transactions",
		Short: "Stream transactions",
		Long: `Stream transactions as ledgers close.

Transactions can be streamed continuously by using the --follow flag.`,
		Example: `  prism transactions --follow
  prism transactions --from 1234567 --limit 5 --json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client := newClient(rpc.ResolveURL(g.rpcURL), g.timeout)

			from := sf.from
			if from == 0 {
				latest, err := latestLedger(cmd.Context(), client)
				if err != nil {
					return err
				}
				from = latest
			}

			src := stream.NewTransactionSource(client, stream.TransactionQuery{
				StartLedger: from,
				PageLimit:   sf.pageSize,
			})
			return runStream(cmd.Context(), src, &sf, g, stdout, stderr)
		},
	}

	sf.register(cmd, stream.DefaultTransactionPageLimit)
	return cmd
}
