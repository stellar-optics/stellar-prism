package stream

import (
	"context"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
)

func TestReorderBufferFlushesOnlyWhenNextLedgerArrives(t *testing.T) {
	b := NewReorderBuffer()
	ready := b.Add([]Record{
		{ID: "0000000010-0001-00-01", Sequence: 10, Kind: KindEvent, Event: &Event{TxIndex: 1, OpIndex: 0, EventIndex: 1}},
		{ID: "0000000010-0001-00-00", Sequence: 10, Kind: KindEvent, Event: &Event{TxIndex: 1, OpIndex: 0, EventIndex: 0}},
	})
	if len(ready) != 0 {
		t.Fatalf("Add() returned %d active-ledger records, want 0", len(ready))
	}
	ready = b.Add([]Record{{ID: "0000000011-0000-00-00", Sequence: 11}})
	if len(ready) != 2 || ready[0].ID != "0000000010-0001-00-00" || ready[1].ID != "0000000010-0001-00-01" {
		t.Fatalf("flushed records = %v, want event-index order", ready)
	}
	if got := b.Flush(); len(got) != 1 || got[0].Sequence != 11 {
		t.Fatalf("Flush() = %v, want the active ledger", got)
	}
}

func TestOperationSourceExtractsAndFiltersOperations(t *testing.T) {
	source := xdr.MustMuxedAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")
	destination := xdr.MustMuxedAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")
	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{Tx: xdr.Transaction{
			SourceAccount: source,
			Operations: []xdr.Operation{{Body: xdr.OperationBody{
				Type:      xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{Destination: destination, Asset: xdr.MustNewNativeAsset(), Amount: 1},
			}}},
		}},
	}
	envelopeXDR, err := xdr.MarshalBase64(envelope)
	if err != nil {
		t.Fatalf("encoding envelope: %v", err)
	}
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{{
		TransactionDetails: protocol.TransactionDetails{Ledger: 700, ApplicationOrder: 2, TransactionHash: "hash", EnvelopeXDR: envelopeXDR},
	}})
	src, err := NewOperationSource(srv, OperationQuery{StartLedger: 700, Types: []string{"payment"}})
	if err != nil {
		t.Fatalf("NewOperationSource() error = %v", err)
	}
	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(batch.Records) != 1 {
		t.Fatalf("got %d operation records, want 1", len(batch.Records))
	}
	got := batch.Records[0]
	if got.ID != "0000000700-0002-00" || got.Operation.Type != "payment" || got.Operation.SourceAccount == "" || got.Operation.ParametersXDR == "" {
		t.Fatalf("operation = %+v, want deterministic payment metadata", got.Operation)
	}
}

func TestFootprintAnalyzerAnnotatesReadWriteCollisions(t *testing.T) {
	var keyID xdr.Hash
	keyID[0] = 7
	key := xdr.LedgerKey{Type: xdr.LedgerEntryTypeContractCode, ContractCode: &xdr.LedgerKeyContractCode{Hash: keyID}}
	data := xdr.SorobanTransactionData{Resources: xdr.SorobanResources{Footprint: xdr.LedgerFootprint{ReadWrite: []xdr.LedgerKey{key}}}}
	dataXDR, err := xdr.MarshalBase64(data)
	if err != nil {
		t.Fatalf("encoding soroban data: %v", err)
	}
	makeEnvelope := func() string {
		envelope := xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: &xdr.TransactionV1Envelope{Tx: xdr.Transaction{
			SourceAccount: xdr.MustMuxedAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"),
			Ext:           xdr.TransactionExt{V: 1, SorobanData: &data},
		}}}
		encoded, _ := xdr.MarshalBase64(envelope)
		return encoded
	}
	_ = dataXDR // keep the fixture's XDR encoding exercised
	records := []Record{
		{Sequence: 800, Kind: KindTransaction, Transaction: &Transaction{Hash: "tx-a", EnvelopeXDR: makeEnvelope()}},
		{Sequence: 800, Kind: KindTransaction, Transaction: &Transaction{Hash: "tx-b", EnvelopeXDR: makeEnvelope()}},
	}
	got, err := NewFootprintAnalyzer().Analyze(records)
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	for _, record := range got {
		if len(record.Transaction.Conflicts) != 1 || len(record.Transaction.Conflicts[0].Transactions) != 2 {
			t.Fatalf("transaction %s conflicts = %+v, want symmetric collision", record.Transaction.Hash, record.Transaction.Conflicts)
		}
	}
}

func TestSimulationSourceEmitsResourceEstimate(t *testing.T) {
	srv := fakerpc.NewLedgerServer(fakerpc.MakeLedgers(900, 1))
	returnValue := xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: func() *xdr.Uint32 { v := xdr.Uint32(4); return &v }()}
	returnXDR, err := xdr.MarshalBase64(returnValue)
	if err != nil {
		t.Fatalf("encoding return value: %v", err)
	}
	data := xdr.SorobanTransactionData{Resources: xdr.SorobanResources{Instructions: 42, DiskReadBytes: 10, WriteBytes: 3}}
	dataXDR, err := xdr.MarshalBase64(data)
	if err != nil {
		t.Fatalf("encoding resources: %v", err)
	}
	srv.SetSimulations([]protocol.SimulateTransactionResponse{{
		TransactionDataXDR: dataXDR, MinResourceFee: 99,
		Results: []protocol.SimulateHostFunctionResult{{ReturnValueXDR: &returnXDR}},
	}})
	src, err := NewSimulationSource(srv, SimulationQuery{TransactionEnvelope: "envelope"})
	if err != nil {
		t.Fatalf("NewSimulationSource() error = %v", err)
	}
	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(batch.Records) != 1 || batch.Records[0].Simulation.Instructions != 42 || batch.Records[0].Simulation.MinResourceFee != 99 {
		t.Fatalf("simulation = %+v, want decoded resources", batch.Records[0].Simulation)
	}
}

func TestSimulationSourceRetriesLedgerAfterMalformedTransactionData(t *testing.T) {
	srv := fakerpc.NewLedgerServer(fakerpc.MakeLedgers(901, 1))
	srv.SetSimulations([]protocol.SimulateTransactionResponse{{TransactionDataXDR: "not-xdr"}})
	src, err := NewSimulationSource(srv, SimulationQuery{TransactionEnvelope: "envelope"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.Fetch(context.Background(), ""); err == nil {
		t.Fatal("Fetch() error = nil, want malformed transaction data error")
	}

	srv.SetSimulations([]protocol.SimulateTransactionResponse{{MinResourceFee: 3}})
	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("Fetch() after correcting the response: %v", err)
	}
	if len(batch.Records) != 1 || batch.Records[0].Sequence != 901 {
		t.Fatalf("records = %+v, want the same ledger retried", batch.Records)
	}
}
