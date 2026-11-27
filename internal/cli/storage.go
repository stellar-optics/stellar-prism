package cli

import (
	"io"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func newStorageCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var (
		sf          streamFlags
		contractIDs []string
		changeTypes []string
		entryTypes  []string
		keys        []string
	)

	cmd := &cobra.Command{
		Use:   "storage",
		Short: "Stream contract storage changes",
		Long: `Stream contract data, contract code and TTL mutations extracted from
transaction metadata.

Every record names the change type, the ledger entry type and, for contract
data, the contract and durability. State-only entries are omitted: they record
what a transaction read, not what it changed.

Contract code and TTL entries are not tied to a contract address, so --contract
omits them and shows only that contract's data. --key narrows contract data to
specific storage keys, given as base64 XDR ScVals; it likewise omits code and
TTL entries. TTL records carry liveUntilLedger, remainingTtl and a ttlWarning
flag when fewer than 1000 ledgers remain.

With --state-file the position reached is recorded after every page, and a
background pruner keeps that file from growing without bound. Add --resume to
start the next run where the last one stopped.`,
		Example: `  # Follow every contract storage mutation
  prism storage --follow

  # Only one contract's persistent data, created or updated
  prism storage --contract CBGSB... --change created --change updated --follow

  # Only the contract instance entry (admin, config) of one contract
  prism storage --contract CBGSB... --key AAAAFA== --follow

  # Contract code and TTL changes, NDJSON into jq
  prism storage --entry contract_code --entry ttl --json | jq -r '.wasmHash'`,
		// Guide: docs/guides/storage_change_streaming.md covers the record
		// schema, durability types, --key filtering, state diffing and TTL
		// projections.
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

			sf.stateKey = stateKey("storage",
				filterPart("contract", contractIDs),
				filterPart("change", changeTypes),
				filterPart("entry", entryTypes),
				filterPart("key", keys))

			newSource := func(startLedger uint32) (stream.Source, error) {
				return stream.NewStorageChangeSource(client, stream.StorageChangeQuery{
					StartLedger: startLedger,
					PageLimit:   sf.pageSize,
					ContractIDs: contractIDs,
					ChangeTypes: changeTypes,
					EntryTypes:  entryTypes,
					Keys:        keys,
				})
			}
			return runStream(cmd, client, newSource, sf.from, &sf, g, stdout, stderr)
		},
	}

	cmd.Flags().StringSliceVar(&contractIDs, "contract", nil,
		"contract ID to watch; repeatable")
	cmd.Flags().StringSliceVar(&changeTypes, "change", nil,
		"change type to include: created, updated, removed or restored (default: all)")
	cmd.Flags().StringSliceVar(&entryTypes, "entry", nil,
		"entry type to include: contract_data, contract_code or ttl (default: all)")
	cmd.Flags().StringSliceVar(&keys, "key", nil,
		"contract data storage key to include, as a base64 XDR ScVal; repeatable (AAAAFA== is the instance key)")
	sf.register(cmd, stream.DefaultStorageChangePageLimit)

	return cmd
}
