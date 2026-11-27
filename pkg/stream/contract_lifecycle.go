package stream

import (
	"context"
	"fmt"
	"strings"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// ContractLifecycleQuery selects contracts whose storage and deployment
// activity should be followed. TTL records are included because their ledger
// key hash cannot be joined to a contract ID from RPC transaction metadata.
type ContractLifecycleQuery struct {
	StartLedger       uint32
	PageLimit         uint
	ContractIDs       []string
	NetworkPassphrase string
}

type ContractLifecycleSource struct {
	tx        *TransactionSource
	storage   *StorageChangeSource
	wasm      *WASMSource
	contracts map[string]bool
}

func NewContractLifecycleSource(client rpc.Client, q ContractLifecycleQuery) (*ContractLifecycleSource, error) {
	if client == nil {
		return nil, fmt.Errorf("contract lifecycle source: rpc client is required")
	}
	storage, err := NewStorageChangeSource(client, StorageChangeQuery{StartLedger: q.StartLedger, PageLimit: q.PageLimit})
	if err != nil {
		return nil, err
	}
	wasm, err := NewWASMSource(client, WASMQuery{StartLedger: q.StartLedger, PageLimit: q.PageLimit, NetworkPassphrase: q.NetworkPassphrase})
	if err != nil {
		return nil, err
	}
	ids := make(map[string]bool, len(q.ContractIDs))
	for _, id := range q.ContractIDs {
		ids[strings.TrimSpace(id)] = true
	}
	return &ContractLifecycleSource{tx: NewTransactionSource(client, TransactionQuery{StartLedger: q.StartLedger, PageLimit: q.PageLimit}), storage: storage, wasm: wasm, contracts: ids}, nil
}
func (s *ContractLifecycleSource) Name() string                         { return "contract_lifecycle" }
func (s *ContractLifecycleSource) Rewind(ledger uint32)                 { s.tx.Rewind(ledger) }
func (s *ContractLifecycleSource) CursorLedger(c string) (uint32, bool) { return s.tx.CursorLedger(c) }
func (s *ContractLifecycleSource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	b, err := s.tx.Fetch(ctx, cursor)
	if err != nil {
		return Batch{}, err
	}
	out := Batch{Records: []Record{}, NextCursor: b.NextCursor, Latest: b.Latest, LatestCloseTime: b.LatestCloseTime, Oldest: b.Oldest, Full: b.Full}
	for _, record := range b.Records {
		tx := record.Transaction
		if tx == nil {
			continue
		}
		changes, err := s.storage.extract(&record)
		if err != nil {
			return Batch{}, fmt.Errorf("contract lifecycle source: transaction %s: %w", tx.Hash, err)
		}
		for _, change := range changes {
			p := change.StorageChange
			if len(s.contracts) > 0 && p.EntryType == StorageEntryContractData && !s.contracts[p.ContractID] {
				continue
			}
			out.Records = append(out.Records, change)
		}
		if !tx.Successful {
			continue
		}
		deployments, err := s.wasm.extract(tx, record.Sequence, record.Time)
		if err != nil {
			return Batch{}, fmt.Errorf("contract lifecycle source: transaction %s: %w", tx.Hash, err)
		}
		for _, deployment := range deployments {
			if len(s.contracts) == 0 || deployment.WASMDeployment.ContractID == "" || s.contracts[deployment.WASMDeployment.ContractID] {
				out.Records = append(out.Records, deployment)
			}
		}
	}
	return out, nil
}
