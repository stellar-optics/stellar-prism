package cli

import (
	"io"

	"github.com/spf13/cobra"

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
				return stream.NewTransactionSource(client, stream.TransactionQuery{
					StartLedger: startLedger,
					PageLimit:   sf.pageSize,
				}), nil
			}
			return runStream(cmd.Context(), newSource, from, &sf, g, stdout, stderr)
		},
	}

	sf.register(cmd, stream.DefaultTransactionPageLimit)
	registerFlagCompletion(cmd, "gap-strategy", string(stream.GapFail), string(stream.GapResume))
	return cmd
}
