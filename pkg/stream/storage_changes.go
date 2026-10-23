package stream

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// DefaultStorageChangePageLimit is the page size requested for storage
// change streams. It matches the transaction page size because changes are
// extracted from transaction metadata.
const DefaultStorageChangePageLimit = DefaultTransactionPageLimit

// Storage change types exposed to filters and sinks.
const (
	StorageChangeCreated  = "created"
	StorageChangeUpdated  = "updated"
	StorageChangeRemoved  = "removed"
	StorageChangeRestored = "restored"

	StorageEntryContractData = "contract_data"
	StorageEntryContractCode = "contract_code"
	StorageEntryTTL          = "ttl"
)

// StorageChangeQuery describes which storage mutations to stream.
type StorageChangeQuery struct {
	// StartLedger is where a fresh stream begins. Zero means start from the
	// server's newest ledger.
	StartLedger uint32
	// PageLimit is the page size to request.
	PageLimit uint
	// ContractIDs limits results to these strkey contract addresses. Contract
	// code and TTL entries are not tied to a contract address and are
	// therefore omitted when this filter is set.
	ContractIDs []string
	// ChangeTypes limits results to created, updated, removed or restored.
	// Empty means all.
	ChangeTypes []string
	// EntryTypes limits results to contract_data, contract_code or ttl.
	// Empty means all three.
	EntryTypes []string
}

// StorageChangeSource streams contract-related ledger entry mutations by
// decoding the metadata attached to each transaction.
//
// It wraps TransactionSource rather than calling RPC itself, so transaction
// pagination and cursor semantics are shared and the tailer's delivery
// guarantee applies unchanged.
type StorageChangeSource struct {
	tx      *TransactionSource
	query   StorageChangeQuery
	changes map[string]bool
	entries map[string]bool
}

// NewStorageChangeSource returns a Source over the given RPC client.
//
// Filters are validated up front: a typo in --change or --entry is reported
// immediately rather than producing a stream that silently matches nothing.
func NewStorageChangeSource(client rpc.Client, q StorageChangeQuery) (*StorageChangeSource, error) {
	if client == nil {
		return nil, fmt.Errorf("storage change source: rpc client is required")
	}
	if q.PageLimit == 0 {
		q.PageLimit = DefaultStorageChangePageLimit
	}

	changes, err := normalizeSet(q.ChangeTypes,
		StorageChangeCreated, StorageChangeUpdated, StorageChangeRemoved, StorageChangeRestored)
	if err != nil {
		return nil, fmt.Errorf("storage change source: %w", err)
	}
	entries, err := normalizeSet(q.EntryTypes,
		StorageEntryContractData, StorageEntryContractCode, StorageEntryTTL)
	if err != nil {
		return nil, fmt.Errorf("storage change source: %w", err)
	}

	return &StorageChangeSource{
		tx: NewTransactionSource(client, TransactionQuery{
			StartLedger: q.StartLedger,
			PageLimit:   q.PageLimit,
		}),
		query:   q,
		changes: changes,
		entries: entries,
	}, nil
}

func normalizeSet(values []string, allowed ...string) (map[string]bool, error) {
	if len(values) == 0 {
		return nil, nil
	}

	valid := make(map[string]bool, len(allowed))
	for _, value := range allowed {
		valid[value] = true
	}

	set := make(map[string]bool, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if !valid[value] {
			return nil, fmt.Errorf("unknown value %q (want one of %s)", value, strings.Join(allowed, ", "))
		}
		set[value] = true
	}
	if len(set) == 0 {
		return nil, nil
	}
	return set, nil
}

// Name implements Source.
func (s *StorageChangeSource) Name() string { return "storage_changes" }

// Rewind implements Rewinder, repositioning a fresh fetch at ledger.
func (s *StorageChangeSource) Rewind(ledger uint32) {
	s.query.StartLedger = ledger
	s.tx.Rewind(ledger)
}

// CursorLedger implements Source.
func (s *StorageChangeSource) CursorLedger(cursor string) (uint32, bool) {
	return s.tx.CursorLedger(cursor)
}

// Fetch implements Source.
func (s *StorageChangeSource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	batch, err := s.tx.Fetch(ctx, cursor)
	if err != nil {
		return Batch{}, err
	}

	out := Batch{
		Records:    make([]Record, 0, len(batch.Records)),
		NextCursor: batch.NextCursor,
		Latest:     batch.Latest,
		Oldest:     batch.Oldest,
		Full:       batch.Full,
	}
	for i := range batch.Records {
		tx := batch.Records[i].Transaction
		if tx == nil {
			continue
		}
		changes, err := s.extract(&batch.Records[i])
		if err != nil {
			return Batch{}, fmt.Errorf("storage change source: transaction %s: %w", tx.Hash, err)
		}
		out.Records = append(out.Records, changes...)
	}
	return out, nil
}

// extract decodes one transaction's metadata and returns the mutations that
// pass the query filters.
func (s *StorageChangeSource) extract(record *Record) ([]Record, error) {
	tx := record.Transaction
	if tx == nil {
		return nil, nil
	}
	if tx.ResultMetaXDR == "" {
		return nil, nil
	}

	var meta xdr.TransactionMeta
	if err := xdr.SafeUnmarshalBase64(tx.ResultMetaXDR, &meta); err != nil {
		return nil, fmt.Errorf("decoding result meta: %w", err)
	}

	groups, err := metaChanges(&meta)
	if err != nil {
		return nil, err
	}

	var records []Record
	for _, group := range groups {
		for i := range group {
			change := &group[i]
			if change.Type == xdr.LedgerEntryChangeTypeLedgerEntryState {
				// State changes restate an entry the transaction read; they
				// are not mutations and would duplicate the stream.
				continue
			}

			payload, ok, err := storageChangePayload(change)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			if !s.matches(payload) {
				continue
			}
			if payload.EntryType == StorageEntryTTL && payload.EntryXDR != "" {
				var entry xdr.LedgerEntry
				if err := xdr.SafeUnmarshalBase64(payload.EntryXDR, &entry); err == nil && entry.Data.Ttl != nil {
					payload.LiveUntilLedger = uint32(entry.Data.Ttl.LiveUntilLedgerSeq)
					if payload.LiveUntilLedger > record.Sequence {
						payload.RemainingTTL = payload.LiveUntilLedger - record.Sequence
					}
					payload.TTLWarning = payload.RemainingTTL < 1000
				}
			}

			id := fmt.Sprintf("%010d-%06d-%06d", record.Sequence, tx.Index, len(records))
			records = append(records, Record{
				ID:            id,
				Sequence:      record.Sequence,
				Time:          record.Time,
				Kind:          KindStorageChange,
				StorageChange: payload,
			})
		}
	}
	return records, nil
}

// metaChanges returns the ledger entry change groups of a transaction in
// application order: before, per operation, then after.
func metaChanges(meta *xdr.TransactionMeta) ([][]xdr.LedgerEntryChange, error) {
	var opGroups [][]xdr.LedgerEntryChange

	switch meta.V {
	case 0:
		if meta.Operations == nil {
			return nil, nil
		}
		for _, op := range *meta.Operations {
			opGroups = append(opGroups, op.Changes)
		}
		return opGroups, nil
	case 1:
		if meta.V1 == nil {
			return nil, fmt.Errorf("transaction meta version 1 has no body")
		}
		groups := [][]xdr.LedgerEntryChange{meta.V1.TxChanges}
		for _, op := range meta.V1.Operations {
			groups = append(groups, op.Changes)
		}
		return groups, nil
	case 2:
		if meta.V2 == nil {
			return nil, fmt.Errorf("transaction meta version 2 has no body")
		}
		groups := [][]xdr.LedgerEntryChange{meta.V2.TxChangesBefore}
		for _, op := range meta.V2.Operations {
			groups = append(groups, op.Changes)
		}
		return append(groups, meta.V2.TxChangesAfter), nil
	case 3:
		if meta.V3 == nil {
			return nil, fmt.Errorf("transaction meta version 3 has no body")
		}
		groups := [][]xdr.LedgerEntryChange{meta.V3.TxChangesBefore}
		for _, op := range meta.V3.Operations {
			groups = append(groups, op.Changes)
		}
		return append(groups, meta.V3.TxChangesAfter), nil
	case 4:
		if meta.V4 == nil {
			return nil, fmt.Errorf("transaction meta version 4 has no body")
		}
		groups := [][]xdr.LedgerEntryChange{meta.V4.TxChangesBefore}
		for _, op := range meta.V4.Operations {
			groups = append(groups, op.Changes)
		}
		return append(groups, meta.V4.TxChangesAfter), nil
	default:
		return nil, fmt.Errorf("unsupported transaction meta version %d", meta.V)
	}
}

func storageChangePayload(change *xdr.LedgerEntryChange) (*StorageChange, bool, error) {
	payload := &StorageChange{ChangeType: changeTypeName(change.Type)}

	key, err := change.LedgerKey()
	if err != nil {
		return nil, false, fmt.Errorf("reading ledger key: %w", err)
	}
	switch key.Type {
	case xdr.LedgerEntryTypeContractData:
		payload.EntryType = StorageEntryContractData
		payload.ContractID, err = key.ContractData.Contract.String()
		if err != nil {
			return nil, false, fmt.Errorf("encoding contract address: %w", err)
		}
		switch key.ContractData.Durability {
		case xdr.ContractDataDurabilityPersistent:
			payload.Durability = "persistent"
		case xdr.ContractDataDurabilityTemporary:
			payload.Durability = "temporary"
		}
	case xdr.LedgerEntryTypeContractCode:
		payload.EntryType = StorageEntryContractCode
		payload.WASMHash = hex.EncodeToString(key.ContractCode.Hash[:])
	case xdr.LedgerEntryTypeTtl:
		payload.EntryType = StorageEntryTTL
		payload.KeyHash = hex.EncodeToString(key.Ttl.KeyHash[:])
	default:
		return nil, false, nil
	}

	if payload.ChangeType == "" {
		return nil, false, fmt.Errorf("unsupported ledger entry change type %d", change.Type)
	}

	keyXDR, err := key.MarshalBinaryBase64()
	if err != nil {
		return nil, false, fmt.Errorf("encoding ledger key: %w", err)
	}
	payload.KeyXDR = keyXDR

	if entry, ok := change.GetLedgerEntry(); ok {
		entryXDR, err := xdr.MarshalBase64(entry)
		if err != nil {
			return nil, false, fmt.Errorf("encoding ledger entry: %w", err)
		}
		payload.EntryXDR = entryXDR
	}

	changeXDR, err := change.MarshalBinaryBase64()
	if err != nil {
		return nil, false, fmt.Errorf("encoding ledger entry change: %w", err)
	}
	payload.ChangeXDR = changeXDR

	return payload, true, nil
}

func changeTypeName(t xdr.LedgerEntryChangeType) string {
	switch t {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
		return StorageChangeCreated
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		return StorageChangeUpdated
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		return StorageChangeRemoved
	case xdr.LedgerEntryChangeTypeLedgerEntryRestored:
		return StorageChangeRestored
	default:
		return ""
	}
}

func (s *StorageChangeSource) matches(change *StorageChange) bool {
	if len(s.changes) > 0 && !s.changes[change.ChangeType] {
		return false
	}
	if len(s.entries) > 0 && !s.entries[change.EntryType] {
		return false
	}
	if len(s.query.ContractIDs) > 0 {
		for _, id := range s.query.ContractIDs {
			if id == change.ContractID {
				return true
			}
		}
		return false
	}
	return true
}
