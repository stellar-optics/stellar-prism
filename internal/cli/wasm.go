package cli

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/spf13/cobra"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	prismrpc "github.com/stellar-optics/stellar-prism/pkg/rpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func fetchMaxContractSizeBytes(ctx context.Context, client prismrpc.Client) (uint32, error) {
	entries, ok := client.(prismrpc.LedgerEntriesClient)
	if !ok {
		return 0, fmt.Errorf("RPC client does not support getLedgerEntries, which is required for the contract size limit")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	key := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeConfigSetting,
		ConfigSetting: &xdr.LedgerKeyConfigSetting{
			ConfigSettingId: xdr.ConfigSettingIdConfigSettingContractMaxSizeBytes,
		},
	}
	keyXDR, err := xdr.MarshalBase64(key)
	if err != nil {
		return 0, fmt.Errorf("encoding config setting ledger key: %w", err)
	}
	resp, err := entries.GetLedgerEntries(ctx, protocol.GetLedgerEntriesRequest{Keys: []string{keyXDR}})
	if err != nil {
		return 0, fmt.Errorf("fetching contract size limit: %w", err)
	}
	for _, result := range resp.Entries {
		if result.DataXDR == "" {
			continue
		}
		var entry xdr.LedgerEntry
		if err := xdr.SafeUnmarshalBase64(result.DataXDR, &entry); err != nil {
			return 0, fmt.Errorf("decoding config setting entry: %w", err)
		}
		if entry.Data.Type != xdr.LedgerEntryTypeConfigSetting || entry.Data.ConfigSetting == nil {
			continue
		}
		setting := entry.Data.ConfigSetting
		if setting.ConfigSettingId != xdr.ConfigSettingIdConfigSettingContractMaxSizeBytes || setting.ContractMaxSizeBytes == nil {
			continue
		}
		return uint32(*setting.ContractMaxSizeBytes), nil
	}
	return 0, fmt.Errorf("RPC did not return the CONFIG_SETTING_CONTRACT_MAX_SIZE_BYTES config setting")
}

func formatWASMSize(size int, limit uint32, percent float64) string {
	if limit == 0 {
		return fmt.Sprintf("%d bytes", size)
	}
	return fmt.Sprintf("%.1f KB / %.1f KB (%.1f%%)", float64(size)/1024, float64(limit)/1024, percent)
}

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

Failed transactions are skipped unless --include-failed is set.

With --state-file the position reached is recorded after every page, and a
background pruner keeps that file from growing without bound. Add --resume to
start the next run where the last one stopped.`,
		Example: `  # Follow deployments and uploads on testnet
  prism wasm --follow

  # Mainnet, including attempts that failed
  prism wasm --follow --include-failed \
    --network-passphrase "Public Global Stellar Network ; September 2015"

  # Just the JSON stream
  prism wasm --follow --json | jq -r 'select(.action=="create") | .contract'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := sf.validate(cmd); err != nil {
				return err
			}

			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()

			var maxSize uint32
			if !g.dryRun {
				var err error
				maxSize, err = fetchMaxContractSizeBytes(cmd.Context(), client)
				if err != nil {
					fmt.Fprintf(stderr, "warning: contract size limit unavailable: %v\n", err)
				}
			}

			sf.stateKey = stateKey("wasm", "failed="+strconv.FormatBool(includeFailed))

			newSource := func(startLedger uint32) (stream.Source, error) {
				return stream.NewWASMSource(client, stream.WASMQuery{
					StartLedger:          startLedger,
					PageLimit:            sf.pageSize,
					NetworkPassphrase:    networkPassphrase,
					IncludeFailed:        includeFailed,
					MaxContractSizeBytes: maxSize,
				})
			}
			return runStream(cmd, client, newSource, sf.from, &sf, g, stdout, stderr)
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
