package cli

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
)

func TestOperationsCommandNDJSON(t *testing.T) {
	envelope := cliUploadEnvelope(t, []byte{0, 97, 115, 109})
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		cliTransaction(9900, "tx", protocol.TransactionStatusSuccess, envelope, ""),
	})
	withFake(t, srv)

	stdout, _, err := run(t, "operations", "--from", "9900", "--type", "invoke_host_function", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	lines := nonEmptyLines(stdout)
	if len(lines) != 1 {
		t.Fatalf("got %d operation lines, want 1\n%s", len(lines), stdout)
	}
	var record struct {
		Kind       string          `json:"kind"`
		ID         string          `json:"id"`
		Type       string          `json:"type"`
		Parameters json.RawMessage `json:"parameters"`
		ParamsXDR  string          `json:"parametersXdr"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &record); err != nil {
		t.Fatalf("invalid operation JSON: %v", err)
	}
	if record.Kind != "operation" || record.Type != "invoke_host_function" || record.ID == "" || len(record.Parameters) == 0 && record.ParamsXDR == "" {
		t.Errorf("operation record = %+v, want demuxed metadata", record)
	}
}

func TestSimulateCommandNDJSON(t *testing.T) {
	srv := fakerpc.NewLedgerServer(fakerpc.MakeLedgers(9950, 1))
	account := xdr.MustAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")
	key, err := xdr.NewLedgerKey(xdr.LedgerEntryTypeAccount, xdr.LedgerKeyAccount{AccountId: account})
	if err != nil {
		t.Fatal(err)
	}
	dataXDR, err := xdr.MarshalBase64(xdr.SorobanTransactionData{
		Resources: xdr.SorobanResources{
			Instructions: 42, DiskReadBytes: 10, WriteBytes: 3,
			Footprint: xdr.LedgerFootprint{ReadOnly: []xdr.LedgerKey{key}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv.SetSimulations([]protocol.SimulateTransactionResponse{{
		TransactionDataXDR: dataXDR,
		MinResourceFee:     17,
		Error:              "HostError: contract error 42",
		Results: []protocol.SimulateHostFunctionResult{{
			ReturnValueJSON: json.RawMessage(`{"value":4}`),
			AuthJSON:        []json.RawMessage{json.RawMessage(`{"credential":"required"}`)},
		}},
		EventsJSON: []json.RawMessage{json.RawMessage(`{"type":"contract"}`)},
	}})
	withFake(t, srv)

	stdout, _, err := run(t, "simulate", "--envelope", "AAA=", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, expected := range []string{
		`"kind":"simulation"`, `"minResourceFee":17`, `"instructions":42`,
		`"error":"HostError: contract error 42"`, `"returnValue":{"value":4}`,
		`"auth":[{"credential":"required"}]`, `"events":[{"type":"contract"}]`,
		`"readOnlyFootprint":[`,
	} {
		if !strings.Contains(stdout, expected) {
			t.Errorf("simulation output missing %s:\n%s", expected, stdout)
		}
	}
}

func TestSimulateCommandRequiresEnvelope(t *testing.T) {
	if _, _, err := run(t, "simulate"); err == nil || !strings.Contains(err.Error(), "--tx-envelope") {
		t.Fatalf("Execute() error = %v, want missing envelope validation", err)
	}
}

func TestNewStreamsValidateSinceFlags(t *testing.T) {
	for _, args := range [][]string{
		{"operations", "--from", "1", "--since", "1h"},
		{"simulate", "--tx-envelope", "AAA=", "--from", "1", "--since", "1h"},
	} {
		if _, _, err := run(t, args...); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
			t.Errorf("%v: error = %v, want mutually exclusive flags", args, err)
		}
	}
}

func TestOperationsResumesWithFilterSpecificState(t *testing.T) {
	envelope := cliUploadEnvelope(t, []byte{0, 97, 115, 109})
	transaction := func(ledger uint32) protocol.TransactionInfo {
		return cliTransaction(ledger, "tx", protocol.TransactionStatusSuccess, envelope, "")
	}
	withFake(t, fakerpc.NewTransactionServer([]protocol.TransactionInfo{transaction(9900)}))
	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	args := []string{"operations", "--type", "invoke_host_function", "--json", "--state-file", path}
	if _, _, err := run(t, append(args, "--from", "9900")...); err != nil {
		t.Fatal(err)
	}
	entries := stateEntries(t, path, "operations|type=invoke_host_function")
	if len(entries) != 1 {
		t.Fatalf("got %d operation checkpoints, want 1", len(entries))
	}
	withFake(t, fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		transaction(9900), transaction(9901), transaction(9902),
	}))
	stdout, _, err := run(t, append(args, "--resume")...)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(nonEmptyLines(stdout)); got != 2 {
		t.Fatalf("resumed %d records, want both unseen ledgers: %s", got, stdout)
	}
}
