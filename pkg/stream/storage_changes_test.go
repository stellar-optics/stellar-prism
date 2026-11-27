package stream_test

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func testContractAddress(seed byte) xdr.ScAddress {
	var id xdr.ContractId
	for i := range id {
		id[i] = seed + byte(i)
	}
	return xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &id,
	}
}

func testU32(v uint32) xdr.ScVal {
	value := xdr.Uint32(v)
	return xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &value}
}

func testContractDataEntry(addr xdr.ScAddress, key uint32) xdr.LedgerEntry {
	return xdr.LedgerEntry{
		LastModifiedLedgerSeq: 100,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractData,
			ContractData: &xdr.ContractDataEntry{
				Contract:   addr,
				Key:        testU32(key),
				Durability: xdr.ContractDataDurabilityPersistent,
				Val:        testU32(key * 10),
			},
		},
	}
}

func testContractCodeEntry(seed byte) xdr.LedgerEntry {
	var hash xdr.Hash
	for i := range hash {
		hash[i] = seed + byte(i)
	}
	return xdr.LedgerEntry{
		LastModifiedLedgerSeq: 100,
		Data: xdr.LedgerEntryData{
			Type:         xdr.LedgerEntryTypeContractCode,
			ContractCode: &xdr.ContractCodeEntry{Hash: hash, Code: []byte{0x00, 0x61, 0x73, 0x6d}},
		},
	}
}

func testTTLEntry(seed byte) xdr.LedgerEntry {
	var hash xdr.Hash
	for i := range hash {
		hash[i] = seed + byte(i)
	}
	return xdr.LedgerEntry{
		LastModifiedLedgerSeq: 100,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTtl,
			Ttl:  &xdr.TtlEntry{KeyHash: hash, LiveUntilLedgerSeq: 500},
		},
	}
}

func testAccountEntry() xdr.LedgerEntry {
	accountID := xdr.MustAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")
	return xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type:    xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{AccountId: accountID, Balance: 1},
		},
	}
}

func createdChange(t *testing.T, entry xdr.LedgerEntry) xdr.LedgerEntryChange {
	t.Helper()
	return xdr.LedgerEntryChange{Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated, Created: &entry}
}

func updatedChange(t *testing.T, entry xdr.LedgerEntry) xdr.LedgerEntryChange {
	t.Helper()
	return xdr.LedgerEntryChange{Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated, Updated: &entry}
}

func stateChange(t *testing.T, entry xdr.LedgerEntry) xdr.LedgerEntryChange {
	t.Helper()
	return xdr.LedgerEntryChange{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: &entry}
}

func restoredChange(t *testing.T, entry xdr.LedgerEntry) xdr.LedgerEntryChange {
	t.Helper()
	return xdr.LedgerEntryChange{Type: xdr.LedgerEntryChangeTypeLedgerEntryRestored, Restored: &entry}
}

func removedChange(t *testing.T, entry xdr.LedgerEntry) xdr.LedgerEntryChange {
	t.Helper()
	key, err := entry.LedgerKey()
	if err != nil {
		t.Fatalf("LedgerKey() error = %v", err)
	}
	return xdr.LedgerEntryChange{Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved, Removed: &key}
}

func encodeMeta(t *testing.T, meta xdr.TransactionMeta) string {
	t.Helper()
	b, err := meta.MarshalBinary()
	if err != nil {
		t.Fatalf("marshalling meta: %v", err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func testMeta(t *testing.T, addr xdr.ScAddress) xdr.TransactionMeta {
	t.Helper()
	return xdr.TransactionMeta{
		V: 4,
		V4: &xdr.TransactionMetaV4{
			TxChangesBefore: xdr.LedgerEntryChanges{
				stateChange(t, testContractDataEntry(addr, 1)),
				createdChange(t, testContractDataEntry(addr, 2)),
			},
			Operations: []xdr.OperationMetaV2{{
				Changes: xdr.LedgerEntryChanges{
					updatedChange(t, testContractDataEntry(addr, 3)),
					removedChange(t, testContractCodeEntry(7)),
					createdChange(t, testAccountEntry()),
				},
			}},
			TxChangesAfter: xdr.LedgerEntryChanges{
				restoredChange(t, testTTLEntry(9)),
			},
		},
	}
}

func testTransaction(ledger uint32, order int32, hash, meta string) protocol.TransactionInfo {
	return protocol.TransactionInfo{
		TransactionDetails: protocol.TransactionDetails{
			Status:           protocol.TransactionStatusSuccess,
			TransactionHash:  hash,
			ApplicationOrder: order,
			Ledger:           ledger,
			ResultMetaXDR:    meta,
		},
		LedgerCloseTime: 1_700_000_000,
	}
}

func fetchStorage(t *testing.T, src *stream.StorageChangeSource) []stream.Record {
	t.Helper()
	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	return batch.Records
}

func TestStorageChangeSourceExtractsContractEntries(t *testing.T) {
	t.Parallel()

	addr := testContractAddress(1)
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		testTransaction(123, 4, "abc", encodeMeta(t, testMeta(t, addr))),
	})
	src, err := stream.NewStorageChangeSource(srv, stream.StorageChangeQuery{StartLedger: 123})
	if err != nil {
		t.Fatalf("NewStorageChangeSource() error = %v", err)
	}

	records := fetchStorage(t, src)
	if len(records) != 4 {
		t.Fatalf("got %d records, want 4 (state and non-contract entries excluded): %+v", len(records), records)
	}

	wantTypes := []string{"created", "updated", "removed", "restored"}
	wantEntries := []string{"contract_data", "contract_data", "contract_code", "ttl"}
	contractID, _ := addr.String()
	seen := make(map[string]bool)
	for i, rec := range records {
		if rec.Kind != stream.KindStorageChange {
			t.Errorf("record %d kind = %q, want %q", i, rec.Kind, stream.KindStorageChange)
		}
		if rec.Sequence != 123 {
			t.Errorf("record %d ledger = %d, want 123", i, rec.Sequence)
		}
		if rec.ID == "" || seen[rec.ID] {
			t.Errorf("record %d has empty or duplicate ID %q", i, rec.ID)
		}
		seen[rec.ID] = true
		if rec.StorageChange == nil {
			t.Fatalf("record %d has no storage change payload", i)
		}
		if got := rec.StorageChange.ChangeType; got != wantTypes[i] {
			t.Errorf("record %d change = %q, want %q", i, got, wantTypes[i])
		}
		if got := rec.StorageChange.EntryType; got != wantEntries[i] {
			t.Errorf("record %d entry = %q, want %q", i, got, wantEntries[i])
		}
		if rec.StorageChange.ChangeXDR == "" || rec.StorageChange.KeyXDR == "" {
			t.Errorf("record %d is missing key or change XDR", i)
		}
	}

	first := records[0].StorageChange
	if first.ContractID != contractID {
		t.Errorf("contract ID = %q, want %q", first.ContractID, contractID)
	}
	if first.Durability != "persistent" {
		t.Errorf("durability = %q, want persistent", first.Durability)
	}
	if first.EntryXDR == "" {
		t.Error("created record is missing its ledger entry XDR")
	}

	removed := records[2].StorageChange
	if removed.EntryXDR != "" {
		t.Errorf("removed record carries an entry XDR: %q", removed.EntryXDR)
	}
	if removed.WASMHash == "" {
		t.Error("contract code record is missing the wasm hash")
	}

	ttl := records[3].StorageChange
	if ttl.KeyHash == "" {
		t.Error("ttl record is missing the key hash")
	}
	if ttl.LiveUntilLedger != 500 || ttl.RemainingTTL != 377 || !ttl.TTLWarning {
		t.Errorf("ttl expiration = live until %d, remaining %d, warning %v; want 500, 377, true", ttl.LiveUntilLedger, ttl.RemainingTTL, ttl.TTLWarning)
	}
}

func TestStorageChangeSourceFilters(t *testing.T) {
	t.Parallel()

	addr := testContractAddress(2)
	other := testContractAddress(90)
	meta := encodeMeta(t, testMeta(t, addr))

	tests := []struct {
		name  string
		query stream.StorageChangeQuery
		want  int
	}{
		{
			name:  "change type",
			query: stream.StorageChangeQuery{ChangeTypes: []string{"created"}},
			want:  1,
		},
		{
			name:  "entry type",
			query: stream.StorageChangeQuery{EntryTypes: []string{"ttl"}},
			want:  1,
		},
		{
			name:  "matching contract",
			query: stream.StorageChangeQuery{ContractIDs: []string{mustContractID(t, addr)}},
			want:  2,
		},
		{
			name:  "other contract",
			query: stream.StorageChangeQuery{ContractIDs: []string{mustContractID(t, other)}},
			want:  0,
		},
		{
			name:  "combined",
			query: stream.StorageChangeQuery{EntryTypes: []string{"contract_data"}, ChangeTypes: []string{"updated"}},
			want:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
				testTransaction(200, 1, "abc", meta),
			})
			src, err := stream.NewStorageChangeSource(srv, tc.query)
			if err != nil {
				t.Fatalf("NewStorageChangeSource() error = %v", err)
			}
			if got := len(fetchStorage(t, src)); got != tc.want {
				t.Errorf("got %d records, want %d", got, tc.want)
			}
		})
	}
}

func mustContractID(t *testing.T, addr xdr.ScAddress) string {
	t.Helper()
	id, err := addr.String()
	if err != nil {
		t.Fatalf("ScAddress.String() error = %v", err)
	}
	return id
}

func TestStorageChangeSourceRejectsUnknownFilters(t *testing.T) {
	t.Parallel()

	srv := fakerpc.NewTransactionServer(nil)
	if _, err := stream.NewStorageChangeSource(srv, stream.StorageChangeQuery{ChangeTypes: []string{"upserted"}}); err == nil {
		t.Error("unknown change type accepted")
	}
	if _, err := stream.NewStorageChangeSource(srv, stream.StorageChangeQuery{EntryTypes: []string{"account"}}); err == nil {
		t.Error("unknown entry type accepted")
	}
	if _, err := stream.NewStorageChangeSource(nil, stream.StorageChangeQuery{}); err == nil {
		t.Error("nil client accepted")
	}
}

func TestStorageChangeSourceSupportsAllMetaVersions(t *testing.T) {
	t.Parallel()

	addr := testContractAddress(3)
	changes := xdr.LedgerEntryChanges{createdChange(t, testContractDataEntry(addr, 1))}

	tests := []struct {
		name string
		meta xdr.TransactionMeta
	}{
		{
			name: "v0",
			meta: xdr.TransactionMeta{
				V:          0,
				Operations: &[]xdr.OperationMeta{{Changes: changes}},
			},
		},
		{
			name: "v1",
			meta: xdr.TransactionMeta{
				V: 1,
				V1: &xdr.TransactionMetaV1{
					TxChanges:  changes,
					Operations: []xdr.OperationMeta{{Changes: changes}},
				},
			},
		},
		{
			name: "v2",
			meta: xdr.TransactionMeta{
				V: 2,
				V2: &xdr.TransactionMetaV2{
					TxChangesBefore: changes,
					Operations:      []xdr.OperationMeta{{Changes: changes}},
				},
			},
		},
		{
			name: "v3",
			meta: xdr.TransactionMeta{
				V: 3,
				V3: &xdr.TransactionMetaV3{
					TxChangesBefore: changes,
					Operations:      []xdr.OperationMeta{{Changes: changes}},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
				testTransaction(300, 1, "abc", encodeMeta(t, tc.meta)),
			})
			src, err := stream.NewStorageChangeSource(srv, stream.StorageChangeQuery{})
			if err != nil {
				t.Fatalf("NewStorageChangeSource() error = %v", err)
			}
			if got := len(fetchStorage(t, src)); got == 0 {
				t.Error("no records extracted")
			}
		})
	}
}

func TestStorageChangeSourceRejectsMalformedMeta(t *testing.T) {
	t.Parallel()

	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		testTransaction(400, 1, "abc", "not-base64!"),
	})
	src, err := stream.NewStorageChangeSource(srv, stream.StorageChangeQuery{})
	if err != nil {
		t.Fatalf("NewStorageChangeSource() error = %v", err)
	}
	if _, err := src.Fetch(context.Background(), ""); err == nil {
		t.Fatal("Fetch() error = nil, want a decoding failure")
	} else if !strings.Contains(err.Error(), "decoding result meta") {
		t.Errorf("error = %v, want it to name the meta decode", err)
	}
}

func TestStorageChangeSourcePaginatesAndPreservesCursor(t *testing.T) {
	t.Parallel()

	addr := testContractAddress(4)
	meta := encodeMeta(t, testMeta(t, addr))
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		testTransaction(500, 1, "a", meta),
		testTransaction(500, 2, "b", meta),
	}, fakerpc.WithPageLimit(1))
	src, err := stream.NewStorageChangeSource(srv, stream.StorageChangeQuery{PageLimit: 1})
	if err != nil {
		t.Fatalf("NewStorageChangeSource() error = %v", err)
	}

	first, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("first Fetch() error = %v", err)
	}
	if len(first.Records) != 4 {
		t.Fatalf("first page has %d records, want 4", len(first.Records))
	}
	if first.NextCursor == "" || !first.Full {
		t.Fatalf("first page cursor = %q, full = %v; want a continuation", first.NextCursor, first.Full)
	}
	if ledger, ok := src.CursorLedger(first.NextCursor); !ok || ledger != 500 {
		t.Errorf("CursorLedger(%q) = %d, %v; want 500, true", first.NextCursor, ledger, ok)
	}

	second, err := src.Fetch(context.Background(), first.NextCursor)
	if err != nil {
		t.Fatalf("second Fetch() error = %v", err)
	}
	if len(second.Records) == 0 {
		t.Fatal("second page is empty, want the remaining transaction")
	}
	if second.Records[0].ID <= first.Records[len(first.Records)-1].ID {
		t.Errorf("second page ID %q does not follow first page ID %q",
			second.Records[0].ID, first.Records[len(first.Records)-1].ID)
	}
}

func TestStorageChangeSourceSkipsMissingMeta(t *testing.T) {
	t.Parallel()

	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		testTransaction(600, 1, "abc", ""),
	})
	src, err := stream.NewStorageChangeSource(srv, stream.StorageChangeQuery{})
	if err != nil {
		t.Fatalf("NewStorageChangeSource() error = %v", err)
	}
	records := fetchStorage(t, src)
	if len(records) != 0 {
		t.Errorf("got %d records for a transaction without meta, want 0", len(records))
	}
}

func TestStorageChangeSourceName(t *testing.T) {
	t.Parallel()

	src, err := stream.NewStorageChangeSource(fakerpc.NewTransactionServer(nil), stream.StorageChangeQuery{})
	if err != nil {
		t.Fatalf("NewStorageChangeSource() error = %v", err)
	}
	if got := src.Name(); got != "storage_changes" {
		t.Errorf("Name() = %q, want %q", got, "storage_changes")
	}
}

func TestStorageChangeSourceFiltersByStorageKey(t *testing.T) {
	t.Parallel()

	addr := testContractAddress(3)
	meta := encodeMeta(t, testMeta(t, addr))
	encodeKey := func(v xdr.ScVal) string {
		t.Helper()
		s, err := xdr.MarshalBase64(v)
		if err != nil {
			t.Fatalf("MarshalBase64() error = %v", err)
		}
		return s
	}

	tests := []struct {
		name string
		keys []string
		want []uint32
	}{
		{name: "created key", keys: []string{encodeKey(testU32(2))}, want: []uint32{2}},
		{name: "updated key", keys: []string{encodeKey(testU32(3))}, want: []uint32{3}},
		{name: "both keys", keys: []string{encodeKey(testU32(2)), encodeKey(testU32(3))}, want: []uint32{2, 3}},
		// Key 1 only appears as a state change, which is never a mutation.
		{name: "state-only key", keys: []string{encodeKey(testU32(1))}, want: nil},
		{name: "instance key", keys: []string{"AAAAFA=="}, want: nil},
		{name: "surrounding space", keys: []string{"  " + encodeKey(testU32(2)) + " "}, want: []uint32{2}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
				testTransaction(200, 1, "abc", meta),
			})
			src, err := stream.NewStorageChangeSource(srv, stream.StorageChangeQuery{Keys: tc.keys})
			if err != nil {
				t.Fatalf("NewStorageChangeSource() error = %v", err)
			}
			records := fetchStorage(t, src)
			if len(records) != len(tc.want) {
				t.Fatalf("got %d records, want %d", len(records), len(tc.want))
			}
			for i, record := range records {
				c := record.StorageChange
				if c == nil || c.EntryType != stream.StorageEntryContractData {
					t.Fatalf("record %d is not a contract data change: %+v", i, c)
				}
				var entry xdr.LedgerEntry
				if err := xdr.SafeUnmarshalBase64(c.EntryXDR, &entry); err != nil {
					t.Fatalf("decoding entry: %v", err)
				}
				if got := uint32(*entry.Data.ContractData.Key.U32); got != tc.want[i] {
					t.Errorf("record %d key = %d, want %d", i, got, tc.want[i])
				}
			}
		})
	}
}

func TestStorageChangeSourceInstanceKeyConstant(t *testing.T) {
	t.Parallel()

	got, err := xdr.MarshalBase64(xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance})
	if err != nil {
		t.Fatalf("MarshalBase64() error = %v", err)
	}
	if got != "AAAAFA==" {
		t.Fatalf("instance key = %q, want AAAAFA== (documented in the storage guide)", got)
	}
}

func TestStorageChangeSourceRejectsMalformedKey(t *testing.T) {
	t.Parallel()

	srv := fakerpc.NewTransactionServer(nil)
	_, err := stream.NewStorageChangeSource(srv, stream.StorageChangeQuery{Keys: []string{"not-xdr"}})
	if err == nil || !strings.Contains(err.Error(), "--key") {
		t.Fatalf("NewStorageChangeSource() error = %v, want a --key error", err)
	}
}
