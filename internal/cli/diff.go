package cli

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
	lens "github.com/stellar-optics/stellar-xdr-lens/pkg/lens"
	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// newDiffCmd implements the `prism diff` command.
//
// prism diff <txHash1> <txHash2>
// prism diff <txHash1> <txHash2> --envelope-only
// prism diff <txHash1> <txHash2> --meta-only
//
// Fetches both transactions from RPC concurrently,
// compares their envelopes, results, and transaction metadata using stellar-xdr-lens,
// and renders a colorized structural diff showing exactly which fields, parameters,
// or return values changed.
func newDiffCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var (
		envelopeOnly bool
		metaOnly     bool
		contractID   string
		sf           streamFlags
	)

	cmd := &cobra.Command{
		Use:   "diff [<txHash1> <txHash2>]",
		Short: "Diff transactions or follow contract storage changes",
		Long: `Diff two transactions fetched from RPC, or follow path-level changes to
one contract's storage as new ledger updates arrive.

Transaction mode compares envelopes, results, and transaction metadata using
stellar-xdr-lens. Contract mode uses transaction ledger metadata to compare
successive contract data entries.

--envelope-only  Compare only the transaction envelopes (XDR).
--meta-only      Compare only the transaction metadata (results).

Example:

  # Diff two transactions
  prism diff TXHASH1 TXHASH2

  # Diff envelopes only
  prism diff TXHASH1 TXHASH2 --envelope-only

  # Diff metadata only
  prism diff TXHASH1 TXHASH2 --meta-only

  # Follow path-level changes to contract storage
  prism diff --contract CDLZ... --follow`,
		Args: func(cmd *cobra.Command, args []string) error {
			if contractID != "" {
				if len(args) != 0 {
					return fmt.Errorf("--contract mode does not accept transaction hashes")
				}
				return nil
			}
			if sf.follow {
				return fmt.Errorf("--follow requires --contract")
			}
			return cobra.ExactArgs(2)(cmd, args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if envelopeOnly && metaOnly {
				return fmt.Errorf("--envelope-only and --meta-only are mutually exclusive")
			}
			if contractID != "" {
				if envelopeOnly || metaOnly {
					return fmt.Errorf("--envelope-only and --meta-only apply only to transaction diff mode")
				}
				if err := sf.validate(cmd); err != nil {
					return err
				}
				client, closeClient, err := openClient(g, stderr)
				if err != nil {
					return err
				}
				defer closeClient()

				sf.stateKey = stateKey("diff", filterPart("contract", []string{contractID}))
				newSource := func(startLedger uint32) (stream.Source, error) {
					return stream.NewContractStateDiffSource(client, stream.StorageChangeQuery{
						StartLedger: startLedger,
						PageLimit:   sf.pageSize,
						ContractIDs: []string{contractID},
					})
				}
				return runStream(cmd, client, newSource, sf.from, &sf, g, stdout, stderr)
			}

			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()

			hash1 := args[0]
			hash2 := args[1]

			// Fetch both transactions concurrently
			resp1, err := client.GetTransaction(cmd.Context(), protocol.GetTransactionRequest{Hash: hash1})
			if err != nil {
				return fmt.Errorf("fetching transaction %s: %w", hash1, err)
			}
			resp2, err := client.GetTransaction(cmd.Context(), protocol.GetTransactionRequest{Hash: hash2})
			if err != nil {
				return fmt.Errorf("fetching transaction %s: %w", hash2, err)
			}

			// Diff based on selected mode
			if envelopeOnly {
				return diffEnvelopes(stdout, resp1, resp2)
			}
			if metaOnly {
				return diffMetadata(stdout, resp1, resp2)
			}
			return diffFull(stdout, resp1, resp2)
		},
	}

	cmd.Flags().BoolVar(&envelopeOnly, "envelope-only", false, "Compare only the transaction envelopes")
	cmd.Flags().BoolVar(&metaOnly, "meta-only", false, "Compare only the transaction metadata")
	cmd.Flags().StringVar(&contractID, "contract", "", "follow structural storage diffs for this contract")
	sf.register(cmd, stream.DefaultStorageChangePageLimit)

	return cmd
}

// diffEnvelopes compares only the transaction envelopes.
func diffEnvelopes(w io.Writer, resp1, resp2 protocol.GetTransactionResponse) error {
	var envelope1, envelope2 string

	if resp1.EnvelopeXDR != "" {
		envelope1 = resp1.EnvelopeXDR
	} else {
		return fmt.Errorf("transaction %s has no envelope", "hash1") // nolint: goerr113
	}

	if resp2.EnvelopeXDR != "" {
		envelope2 = resp2.EnvelopeXDR
	} else {
		return fmt.Errorf("transaction %s has no envelope", "hash2") // nolint: goerr113
	}

	left, err := lens.DecodeAs(envelope1, "TransactionEnvelope")
	if err != nil {
		return fmt.Errorf("decoding envelope for %s: %w", "hash1", err)
	}
	right, err := lens.DecodeAs(envelope2, "TransactionEnvelope")
	if err != nil {
		return fmt.Errorf("decoding envelope for %s: %w", "hash2", err)
	}

	diffResult, err := lens.Diff(left, right)
	if err != nil {
		return fmt.Errorf("diffing envelopes: %w", err)
	}

	formatter := lensformat.NewDiffFormatter()
	return formatter.Format(w, diffResult)
}

// diffMetadata compares only the transaction metadata (results).
func diffMetadata(w io.Writer, resp1, resp2 protocol.GetTransactionResponse) error {
	var result1, result2 string

	if resp1.ResultXDR != "" {
		result1 = resp1.ResultXDR
	} else {
		return fmt.Errorf("transaction %s has no result", "hash1") // nolint: goerr113
	}

	if resp2.ResultXDR != "" {
		result2 = resp2.ResultXDR
	} else {
		return fmt.Errorf("transaction %s has no result", "hash2") // nolint: goerr113
	}

	left, err := lens.DecodeAs(result1, "TransactionResult")
	if err != nil {
		return fmt.Errorf("decoding result for %s: %w", "hash1", err)
	}
	right, err := lens.DecodeAs(result2, "TransactionResult")
	if err != nil {
		return fmt.Errorf("decoding result for %s: %w", "hash2", err)
	}

	diffResult, err := lens.Diff(left, right)
	if err != nil {
		return fmt.Errorf("diffing results: %w", err)
	}

	formatter := lensformat.NewDiffFormatter()
	return formatter.Format(w, diffResult)
}

// diffFull compares both envelopes and results.
func diffFull(w io.Writer, resp1, resp2 protocol.GetTransactionResponse) error {
	// Diff envelopes
	if resp1.EnvelopeXDR != "" && resp2.EnvelopeXDR != "" {
		left, err := lens.DecodeAs(resp1.EnvelopeXDR, "TransactionEnvelope")
		if err != nil {
			return fmt.Errorf("decoding envelope for %s: %w", "hash1", err)
		}
		right, err := lens.DecodeAs(resp2.EnvelopeXDR, "TransactionEnvelope")
		if err != nil {
			return fmt.Errorf("decoding envelope for %s: %w", "hash2", err)
		}

		envelopeDiff, err := lens.Diff(left, right)
		if err != nil {
			return fmt.Errorf("diffing envelopes: %w", err)
		}

		formatter := lensformat.NewDiffFormatter()
		if err := formatter.Format(w, envelopeDiff); err != nil {
			return err
		}
	}

	// Diff results
	if resp1.ResultXDR != "" && resp2.ResultXDR != "" {
		left, err := lens.DecodeAs(resp1.ResultXDR, "TransactionResult")
		if err != nil {
			return fmt.Errorf("decoding result for %s: %w", "hash1", err)
		}
		right, err := lens.DecodeAs(resp2.ResultXDR, "TransactionResult")
		if err != nil {
			return fmt.Errorf("decoding result for %s: %w", "hash2", err)
		}

		resultDiff, err := lens.Diff(left, right)
		if err != nil {
			return fmt.Errorf("diffing results: %w", err)
		}

		formatter := lensformat.NewDiffFormatter()
		if err := formatter.Format(w, resultDiff); err != nil {
			return err
		}
	}

	return nil
}
