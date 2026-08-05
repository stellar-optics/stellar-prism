package cli

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-xdr-lens/pkg/lens"
	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

func newTxCmd(g *globalFlags, stdout io.Writer) *cobra.Command {
	var (
		asJSON    bool
		verbose   bool
		failOnErr bool
	)

	cmd := &cobra.Command{
		Use:   "tx <hash>",
		Short: "Fetch a transaction and explain it in plain English",
		Long: `Fetch a transaction by hash and render it through the lens explain path.

The envelope and result are paired, so the output names which operation
failed and why — the same explanation ` + "`lens explain --result`" + ` produces,
without the copy-and-paste.`,
		Example: `  prism tx 3389e9f0a1b2c3...
  prism tx 3389e9f0a1b2c3... --json | jq -r '.headline'
  prism tx 3389e9f0a1b2c3... --fail-on-error`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := newClient(rpc.ResolveURL(g.rpcURL), g.timeout)

			resp, err := client.GetTransaction(cmd.Context(),
				protocol.GetTransactionRequest{Hash: args[0]})
			if err != nil {
				return err
			}

			switch resp.Status {
			case protocol.TransactionStatusNotFound:
				return fmt.Errorf(
					"transaction %s not found; RPC retains only recent history, so an older transaction needs Horizon or an archive",
					args[0])
			case "":
				return fmt.Errorf("server returned no status for transaction %s", args[0])
			}

			summary, err := explainTransaction(resp)
			if err != nil {
				return err
			}

			if asJSON {
				enc := json.NewEncoder(stdout)
				enc.SetIndent("", "  ")
				enc.SetEscapeHTML(false)
				if err := enc.Encode(summary); err != nil {
					return fmt.Errorf("encoding summary: %w", err)
				}
			} else {
				f := lensformat.NewSummaryFormatter(
					lensformat.WithSummaryPalette(lensformat.PaletteFor(stdout, g.color)),
					lensformat.WithVerbose(verbose),
				)
				if err := f.Format(stdout, summary); err != nil {
					return err
				}
			}

			if failOnErr && summary.Outcome != nil && !summary.Outcome.Success {
				return &exitError{code: 2, msg: "transaction failed"}
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the summary as JSON")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false,
		"include the underlying XDR result-code constants")
	cmd.Flags().BoolVar(&failOnErr, "fail-on-error", false,
		"exit with a non-zero status when the transaction failed")

	return cmd
}

// explainTransaction pairs the envelope with the result so the explanation
// can attribute a failure to the operation that caused it.
//
// When the server omits the result — which happens for a transaction still
// being applied — the envelope alone is explained rather than failing, since
// "here is what this transaction was trying to do" is still useful.
func explainTransaction(resp protocol.GetTransactionResponse) (*lens.Summary, error) {
	if resp.EnvelopeXDR == "" {
		return nil, fmt.Errorf("server returned no envelope for this transaction")
	}

	envelope, err := lens.DecodeAs(resp.EnvelopeXDR, "TransactionEnvelope")
	if err != nil {
		return nil, fmt.Errorf("decoding transaction envelope: %w", err)
	}

	if resp.ResultXDR == "" {
		summary, err := lens.Explain(envelope)
		if err != nil {
			return nil, fmt.Errorf("explaining transaction: %w", err)
		}
		return summary, nil
	}

	result, err := lens.DecodeAs(resp.ResultXDR, "TransactionResult")
	if err != nil {
		return nil, fmt.Errorf("decoding transaction result: %w", err)
	}

	summary, err := lens.ExplainPair(envelope, result)
	if err != nil {
		return nil, fmt.Errorf("explaining transaction: %w", err)
	}
	return summary, nil
}
