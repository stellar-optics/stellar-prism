package stream

import (
	"context"
	"fmt"
	"strings"

	"github.com/stellar-optics/stellar-xdr-lens/pkg/lens"
	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// ContractStateDiffSource renders each contract data mutation as a structural
// diff from the preceding entry state.
type ContractStateDiffSource struct {
	storage *StorageChangeSource
}

// NewContractStateDiffSource creates a diff source for one contract.
func NewContractStateDiffSource(client rpc.Client, query StorageChangeQuery) (*ContractStateDiffSource, error) {
	if len(query.ContractIDs) != 1 || strings.TrimSpace(query.ContractIDs[0]) == "" {
		return nil, fmt.Errorf("contract diff source: exactly one contract ID is required")
	}
	query.EntryTypes = []string{StorageEntryContractData}
	storage, err := NewStorageChangeSource(client, query)
	if err != nil {
		return nil, err
	}
	return &ContractStateDiffSource{storage: storage}, nil
}

// Name implements Source.
func (s *ContractStateDiffSource) Name() string { return "contract_diff" }

// Rewind implements Rewinder.
func (s *ContractStateDiffSource) Rewind(ledger uint32) { s.storage.Rewind(ledger) }

// CursorLedger implements Source.
func (s *ContractStateDiffSource) CursorLedger(cursor string) (uint32, bool) {
	return s.storage.CursorLedger(cursor)
}

// Fetch implements Source.
func (s *ContractStateDiffSource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	batch, err := s.storage.Fetch(ctx, cursor)
	if err != nil {
		return Batch{}, err
	}
	for i := range batch.Records {
		change := batch.Records[i].StorageChange
		if change == nil {
			continue
		}
		change.StructuralDiff, err = formatContractEntryDiff(change)
		if err != nil {
			return Batch{}, fmt.Errorf("contract diff source: ledger %d key %s: %w",
				batch.Records[i].Sequence, change.KeyXDR, err)
		}
	}
	return batch, nil
}

func formatContractEntryDiff(change *StorageChange) (string, error) {
	switch change.ChangeType {
	case StorageChangeCreated, StorageChangeRestored:
		return "entry created", nil
	case StorageChangeRemoved:
		return "entry removed", nil
	case StorageChangeUpdated:
		if change.PreviousEntryXDR == "" || change.EntryXDR == "" {
			return "previous entry state unavailable", nil
		}
	default:
		return "", fmt.Errorf("unsupported storage change type %q", change.ChangeType)
	}

	before, err := lens.DecodeAs(change.PreviousEntryXDR, "LedgerEntry")
	if err != nil {
		return "", fmt.Errorf("decoding previous entry: %w", err)
	}
	after, err := lens.DecodeAs(change.EntryXDR, "LedgerEntry")
	if err != nil {
		return "", fmt.Errorf("decoding current entry: %w", err)
	}
	diff, err := lens.Diff(before, after)
	if err != nil {
		return "", fmt.Errorf("comparing entries: %w", err)
	}
	var rendered strings.Builder
	if err := lensformat.NewDiffFormatter().Format(&rendered, diff); err != nil {
		return "", fmt.Errorf("formatting entry diff: %w", err)
	}
	return rendered.String(), nil
}
