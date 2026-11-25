package cli

import (
	"io"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func newFeeStatsCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var sf streamFlags
	cmd := &cobra.Command{Use: "fee-stats", Short: "Stream Stellar ledger fee metrics", Args: cobra.NoArgs,
		Long: `Stream fee-market telemetry for each ledger close: base fee, base reserve,
fee pool, circulating coins, rolling fee averages, capacity usage and surge
pricing status, plus per-operation inclusion fee distributions (min, mode,
p10, p50, p90, p95, p99, max) for both the fee charged and the fee bid.

The table output shows min/mode/p10/p50/p95 of the inclusion fee charged;
--json emits the full distributions under inclusionFeeCharged and
inclusionFeeBid for automated bidders.`,
		Example: "  prism fee-stats --follow\n  prism fee-stats --follow --json\n  prism fee-stats --from 1234567 --json",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := sf.validate(cmd); err != nil {
				return err
			}
			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()
			factory := func(start uint32) (stream.Source, error) {
				return stream.NewFeeStatsSource(client, stream.LedgerQuery{StartLedger: start, PageLimit: sf.pageSize})
			}
			return runStream(cmd, client, factory, sf.from, &sf, g, stdout, stderr)
		},
	}
	sf.register(cmd, stream.DefaultLedgerPageLimit)
	return cmd
}
