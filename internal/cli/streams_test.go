package cli

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

func withFakeCapturingURLs(t *testing.T, srv *fakerpc.Server) *[]string {
	t.Helper()
	var got []string
	original := newClient
	newClient = func(urls []string, _ time.Duration) (rpc.Client, error) {
		got = append([]string(nil), urls...)
		return srv, nil
	}
	t.Cleanup(func() { newClient = original })
	return &got
}

func cliContractAddress() xdr.ScAddress {
	var id xdr.ContractId
	for i := range id {
		id[i] = byte(i + 1)
	}
	return xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &id,
	}
}

func cliStorageMeta(t *testing.T) string {
	t.Helper()

	value := xdr.Uint32(7)
	entry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 9000,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractData,
			ContractData: &xdr.ContractDataEntry{
				Contract:   cliContractAddress(),
				Key:        xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &value},
				Durability: xdr.ContractDataDurabilityPersistent,
				Val:        xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &value},
			},
		},
	}
	change := xdr.LedgerEntryChange{
		Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Created: &entry,
	}
	meta := xdr.TransactionMeta{
		V: 4,
		V4: &xdr.TransactionMetaV4{
			Operations: []xdr.OperationMetaV2{{
				Changes: xdr.LedgerEntryChanges{change},
			}},
		},
	}

	b, err := meta.MarshalBinary()
	if err != nil {
		t.Fatalf("marshalling meta: %v", err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func cliStorageUpdateMeta(t *testing.T) string {
	t.Helper()

	makeEntry := func(value uint32) xdr.LedgerEntry {
		key := xdr.Uint32(1)
		val := xdr.Uint32(value)
		return xdr.LedgerEntry{
			LastModifiedLedgerSeq: 9000,
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractData,
				ContractData: &xdr.ContractDataEntry{
					Contract:   cliContractAddress(),
					Key:        xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &key},
					Durability: xdr.ContractDataDurabilityPersistent,
					Val:        xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &val},
				},
			},
		}
	}
	before, after := makeEntry(7), makeEntry(9)
	meta := xdr.TransactionMeta{
		V: 4,
		V4: &xdr.TransactionMetaV4{
			TxChangesBefore: xdr.LedgerEntryChanges{{
				Type:  xdr.LedgerEntryChangeTypeLedgerEntryState,
				State: &before,
			}},
			Operations: []xdr.OperationMetaV2{{Changes: xdr.LedgerEntryChanges{{
				Type:    xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
				Updated: &after,
			}}}},
		},
	}
	b, err := meta.MarshalBinary()
	if err != nil {
		t.Fatalf("marshalling meta: %v", err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func cliUploadEnvelope(t *testing.T, wasm []byte) string {
	t.Helper()

	fn := xdr.HostFunction{
		Type: xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm,
		Wasm: &wasm,
	}
	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: xdr.MustMuxedAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"),
				Operations: []xdr.Operation{{
					Body: xdr.OperationBody{
						Type:                 xdr.OperationTypeInvokeHostFunction,
						InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{HostFunction: fn},
					},
				}},
			},
		},
	}
	b, err := envelope.MarshalBinary()
	if err != nil {
		t.Fatalf("marshalling envelope: %v", err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func cliTransaction(ledger uint32, hash, status, envelope, meta string) protocol.TransactionInfo {
	return protocol.TransactionInfo{
		TransactionDetails: protocol.TransactionDetails{
			Status:           status,
			TransactionHash:  hash,
			ApplicationOrder: 1,
			Ledger:           ledger,
			EnvelopeXDR:      envelope,
			ResultMetaXDR:    meta,
		},
		LedgerCloseTime: 1_700_000_000,
	}
}

func TestStorageCommandNDJSON(t *testing.T) {
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		cliTransaction(9000, "abc", protocol.TransactionStatusSuccess, "", cliStorageMeta(t)),
	})
	withFake(t, srv)

	stdout, _, err := run(t, "storage", "--from", "9000", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	lines := nonEmptyLines(stdout)
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1\n%s", len(lines), stdout)
	}

	var rec struct {
		Kind       string `json:"kind"`
		ChangeType string `json:"changeType"`
		EntryType  string `json:"entryType"`
		Contract   string `json:"contract"`
		Key        struct {
			Type string `json:"type"`
		} `json:"key"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("not valid JSON: %v\n%s", err, lines[0])
	}
	if rec.Kind != "storage_change" || rec.ChangeType != "created" || rec.EntryType != "contract_data" {
		t.Errorf("record = %+v, want a created contract_data change", rec)
	}
	want, err := cliContractAddress().String()
	if err != nil {
		t.Fatalf("ScAddress.String() error = %v", err)
	}
	if rec.Contract != want {
		t.Errorf("contract = %q, want %q", rec.Contract, want)
	}
	if rec.Key.Type == "" {
		t.Error("key was not decoded through lens")
	}
}

func TestDiffCommandStreamsContractStateDiff(t *testing.T) {
	contractID, err := cliContractAddress().String()
	if err != nil {
		t.Fatal(err)
	}
	withFake(t, fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		cliTransaction(9000, "diff", protocol.TransactionStatusSuccess, "", cliStorageUpdateMeta(t)),
	}))

	stdout, _, err := run(t, "diff", "--contract", contractID, "--from", "9000", "--follow", "--limit", "1", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(stdout, `"kind":"storage_change"`) ||
		!strings.Contains(stdout, `"structuralDiff"`) ||
		!strings.Contains(stdout, "Val") {
		t.Fatalf("contract diff output lacks path-level storage changes:\n%s", stdout)
	}
}

func TestStorageCommandFilterAndRaw(t *testing.T) {
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		cliTransaction(9100, "abc", protocol.TransactionStatusSuccess, "", cliStorageMeta(t)),
	})
	withFake(t, srv)

	stdout, _, err := run(t, "storage", "--from", "9100", "--json", "--change", "removed")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(nonEmptyLines(stdout)); got != 0 {
		t.Errorf("got %d records for a non-matching filter, want 0", got)
	}

	withFake(t, srv)
	stdout, _, err = run(t, "storage", "--from", "9100", "--json", "--raw")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(stdout, "changeXdr") || strings.Contains(stdout, `"change":{`) {
		t.Errorf("--raw did not preserve base64 XDR\n%s", stdout)
	}
}

func TestStorageCommandRejectsUnknownFilter(t *testing.T) {
	srv := fakerpc.NewTransactionServer(nil)
	withFake(t, srv)

	if _, _, err := run(t, "storage", "--from", "9200", "--change", "upserted"); err == nil {
		t.Fatal("Execute() error = nil, want a validation failure")
	}
}

func TestWASMCommand(t *testing.T) {
	wasm := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00}
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		cliTransaction(9300, "abc", protocol.TransactionStatusSuccess, cliUploadEnvelope(t, wasm), ""),
	})
	withFake(t, srv)

	stdout, _, err := run(t, "wasm", "--from", "9300", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	lines := nonEmptyLines(stdout)
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1\n%s", len(lines), stdout)
	}
	var rec struct {
		Kind     string `json:"kind"`
		Action   string `json:"action"`
		WASMHash string `json:"wasmHash"`
		WASMSize int    `json:"wasmSize"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("not valid JSON: %v\n%s", err, lines[0])
	}
	if rec.Kind != "wasm_deployment" || rec.Action != "upload" {
		t.Errorf("record = %+v, want a wasm upload", rec)
	}
	sum := sha256.Sum256(wasm)
	if want := hex.EncodeToString(sum[:]); rec.WASMHash != want {
		t.Errorf("wasm hash = %q, want %q", rec.WASMHash, want)
	}
	if rec.WASMSize != len(wasm) {
		t.Errorf("wasm size = %d, want %d", rec.WASMSize, len(wasm))
	}
}

func TestWASMCommandIncludeFailed(t *testing.T) {
	wasm := []byte{0x00, 0x61, 0x73, 0x6d}
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		cliTransaction(9400, "bad", protocol.TransactionStatusFailed, cliUploadEnvelope(t, wasm), ""),
	})

	withFake(t, srv)
	stdout, _, err := run(t, "wasm", "--from", "9400", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(nonEmptyLines(stdout)); got != 0 {
		t.Errorf("got %d failed records by default, want 0", got)
	}

	withFake(t, srv)
	stdout, _, err = run(t, "wasm", "--from", "9400", "--json", "--include-failed")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(nonEmptyLines(stdout)); got != 1 {
		t.Errorf("got %d records with --include-failed, want 1", got)
	}
}

func TestOutputFileRotates(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(9500, 5), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	path := filepath.Join(t.TempDir(), "events.ndjson")
	stdout, _, err := run(t, "events", "--from", "9500", "--json",
		"--output-file", path,
		"--rotate-size", "1",
		"--rotate-compress",
		"--rotate-keep", "2")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if strings.TrimSpace(stdout) != "" {
		t.Errorf("records went to stdout as well as the file\n%s", stdout)
	}

	active, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading active file: %v", err)
	}
	if got := len(nonEmptyLines(string(active))); got != 1 {
		t.Errorf("active file has %d records, want the last one", got)
	}

	backups, err := filepath.Glob(path + ".*.gz")
	if err != nil {
		t.Fatalf("globbing backups: %v", err)
	}
	if len(backups) != 2 {
		t.Errorf("got %d compressed backups, want --rotate-keep=2", len(backups))
	}
}

func TestRotateFlagsRequireOutputFile(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(9600, 1), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	if _, _, err := run(t, "events", "--from", "9600", "--rotate-size", "10"); err == nil {
		t.Fatal("Execute() error = nil, want --output-file to be required")
	}
}

func TestRPCPoolURLsFromRepeatedFlag(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(9700, 1), fakerpc.WithPageLimit(10))
	urls := withFakeCapturingURLs(t, srv)

	_, _, err := run(t, "events", "--from", "9700",
		"--rpc-url", "https://a.example",
		"--rpc-url", "https://b.example")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(*urls) != 2 || (*urls)[0] != "https://a.example" || (*urls)[1] != "https://b.example" {
		t.Errorf("newClient got %v, want both repeated URLs in order", *urls)
	}
}

func TestRPCPoolURLsFromEnvironment(t *testing.T) {
	t.Setenv(rpc.URLEnvVar, "https://a.example, https://b.example")
	srv := fakerpc.NewServer(fakerpc.MakeEvents(9800, 1), fakerpc.WithPageLimit(10))
	urls := withFakeCapturingURLs(t, srv)

	if _, _, err := run(t, "events", "--from", "9800"); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(*urls) != 2 || (*urls)[0] != "https://a.example" || (*urls)[1] != "https://b.example" {
		t.Errorf("newClient got %v, want the comma-separated environment URLs", *urls)
	}
}
