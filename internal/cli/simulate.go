package cli

import (
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// newSimulateCmd constructs the "prism simulate" command.
//
// See docs/guides/offline_simulation.md for resource budget metrics,
// preflight vs. on-chain differences, and CI validation recipes.
func newSimulateCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var (
		sf       streamFlags
		envelope string
	)

	cmd := &cobra.Command{
		Use:   "simulate",
		Short: "Continuously simulate a transaction template",
		Long: `Simulate a base64 TransactionEnvelope at each newly observed ledger
head and report its return value, resource use, footprint, authorization
requirements and diagnostic events. Simulation failures are included verbatim.`,
		Example: `  prism simulate --tx-envelope AAA... --follow
  prism simulate --envelope AAA... --json --limit 1`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if envelope == "" {
				return fmt.Errorf("--tx-envelope is required")
			}
			if err := sf.validate(cmd); err != nil {
				return err
			}
			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()
			sf.stateKey = stateKey("simulate", fmt.Sprintf("envelope=%x", sha256.Sum256([]byte(envelope))))
			newSource := func(startLedger uint32) (stream.Source, error) {
				return stream.NewSimulationSource(client, stream.SimulationQuery{
					TransactionEnvelope: envelope, StartLedger: startLedger,
				})
			}
			return runStream(cmd, client, newSource, sf.from, &sf, g, stdout, stderr)
		},
	}
	sf.register(cmd, 1)
	cmd.Flags().StringVar(&envelope, "tx-envelope", "", "base64 TransactionEnvelope to simulate")
	cmd.Flags().StringVar(&envelope, "envelope", "", "alias for --tx-envelope")
	registerFlagCompletion(cmd, "gap-strategy", string(stream.GapFail), string(stream.GapResume))
	return cmd
}
