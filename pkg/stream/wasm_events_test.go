package stream_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func hostFunctionOp(fn xdr.HostFunction) xdr.Operation {
	return xdr.Operation{
		Body: xdr.OperationBody{
			Type:                 xdr.OperationTypeInvokeHostFunction,
			InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{HostFunction: fn},
		},
	}
}

func uploadFunction(wasm []byte) xdr.HostFunction {
	return xdr.HostFunction{
		Type: xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm,
		Wasm: &wasm,
	}
}

func createFunction(preimage xdr.ContractIdPreimage, executable xdr.ContractExecutable, v2 bool) xdr.HostFunction {
	if v2 {
		return xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeCreateContractV2,
			CreateContractV2: &xdr.CreateContractArgsV2{
				ContractIdPreimage: preimage,
				Executable:         executable,
			},
		}
	}
	return xdr.HostFunction{
		Type: xdr.HostFunctionTypeHostFunctionTypeCreateContract,
		CreateContract: &xdr.CreateContractArgs{
			ContractIdPreimage: preimage,
			Executable:         executable,
		},
	}
}

func addressPreimage(addr xdr.ScAddress, seed byte) xdr.ContractIdPreimage {
	var salt xdr.Uint256
	for i := range salt {
		salt[i] = seed + byte(i)
	}
	return xdr.ContractIdPreimage{
		Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
		FromAddress: &xdr.ContractIdPreimageFromAddress{
			Address: addr,
			Salt:    salt,
		},
	}
}

func wasmExecutable(seed byte) xdr.ContractExecutable {
	var hash xdr.Hash
	for i := range hash {
		hash[i] = seed + byte(i)
	}
	return xdr.ContractExecutable{
		Type:     xdr.ContractExecutableTypeContractExecutableWasm,
		WasmHash: &hash,
	}
}

func encodeEnvelope(t *testing.T, envelope xdr.TransactionEnvelope) string {
	t.Helper()
	b, err := envelope.MarshalBinary()
	if err != nil {
		t.Fatalf("marshalling envelope: %v", err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func v1Envelope(operations ...xdr.Operation) xdr.TransactionEnvelope {
	return xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: xdr.MustMuxedAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"),
				Operations:    operations,
			},
		},
	}
}

func wasmTransaction(ledger uint32, order int32, hash, envelope, status string) protocol.TransactionInfo {
	return protocol.TransactionInfo{
		TransactionDetails: protocol.TransactionDetails{
			Status:           status,
			TransactionHash:  hash,
			ApplicationOrder: order,
			Ledger:           ledger,
			EnvelopeXDR:      envelope,
		},
		LedgerCloseTime: 1_700_000_100,
	}
}

func fetchWASM(t *testing.T, src *stream.WASMSource) []stream.Record {
	t.Helper()
	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	return batch.Records
}

func TestWASMSourceEmitsUpload(t *testing.T) {
	t.Parallel()

	wasm := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		wasmTransaction(10, 1, "abc", encodeEnvelope(t, v1Envelope(hostFunctionOp(uploadFunction(wasm)))), protocol.TransactionStatusSuccess),
	})
	src, err := stream.NewWASMSource(srv, stream.WASMQuery{})
	if err != nil {
		t.Fatalf("NewWASMSource() error = %v", err)
	}

	records := fetchWASM(t, src)
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}

	rec := records[0]
	if rec.Kind != stream.KindWASMDeployment || rec.WASMDeployment == nil {
		t.Fatalf("record = %+v, want a wasm deployment", rec)
	}
	event := rec.WASMDeployment
	if event.Action != stream.WASMActionUpload {
		t.Errorf("action = %q, want upload", event.Action)
	}
	sum := sha256.Sum256(wasm)
	if want := hex.EncodeToString(sum[:]); event.WASMHash != want {
		t.Errorf("wasm hash = %q, want %q", event.WASMHash, want)
	}
	if event.WASMSize != len(wasm) {
		t.Errorf("wasm size = %d, want %d", event.WASMSize, len(wasm))
	}
	if event.ContractID != "" {
		t.Errorf("upload has contract ID %q, want none", event.ContractID)
	}
	if event.TxHash != "abc" || !event.Successful {
		t.Errorf("transaction metadata = %+v, want hash abc and successful", event)
	}
}

func TestWASMSourceEmitsCreate(t *testing.T) {
	t.Parallel()

	addr := testContractAddress(1)
	preimage := addressPreimage(addr, 20)
	executable := wasmExecutable(40)
	envelope := v1Envelope(hostFunctionOp(createFunction(preimage, executable, false)))

	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		wasmTransaction(20, 3, "def", encodeEnvelope(t, envelope), protocol.TransactionStatusSuccess),
	})
	src, err := stream.NewWASMSource(srv, stream.WASMQuery{})
	if err != nil {
		t.Fatalf("NewWASMSource() error = %v", err)
	}

	records := fetchWASM(t, src)
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	event := records[0].WASMDeployment
	if event.Action != stream.WASMActionCreate {
		t.Errorf("action = %q, want create", event.Action)
	}
	if event.Executable != "wasm" {
		t.Errorf("executable = %q, want wasm", event.Executable)
	}
	if !strings.HasPrefix(event.ContractID, "C") || len(event.ContractID) != 56 {
		t.Errorf("contract ID = %q, want a 56-character strkey", event.ContractID)
	}
	if _, err := strkey.Decode(strkey.VersionByteContract, event.ContractID); err != nil {
		t.Errorf("contract ID does not decode: %v", err)
	}
	if event.WASMHash == "" {
		t.Error("create record is missing the executable wasm hash")
	}
}

func TestWASMSourceCreateV2AndStellarAsset(t *testing.T) {
	t.Parallel()

	native := xdr.MustNewNativeAsset()
	assetPreimage := xdr.ContractIdPreimage{
		Type:      xdr.ContractIdPreimageTypeContractIdPreimageFromAsset,
		FromAsset: &native,
	}
	executable := xdr.ContractExecutable{
		Type: xdr.ContractExecutableTypeContractExecutableStellarAsset,
	}
	envelope := v1Envelope(hostFunctionOp(createFunction(assetPreimage, executable, true)))

	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		wasmTransaction(30, 1, "ghi", encodeEnvelope(t, envelope), protocol.TransactionStatusSuccess),
	})
	src, err := stream.NewWASMSource(srv, stream.WASMQuery{})
	if err != nil {
		t.Fatalf("NewWASMSource() error = %v", err)
	}

	records := fetchWASM(t, src)
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	event := records[0].WASMDeployment
	if event.Executable != "stellar_asset" {
		t.Errorf("executable = %q, want stellar_asset", event.Executable)
	}

	id, err := native.ContractID(stream.DefaultNetworkPassphrase)
	if err != nil {
		t.Fatalf("Asset.ContractID() error = %v", err)
	}
	want, err := strkey.Encode(strkey.VersionByteContract, id[:])
	if err != nil {
		t.Fatalf("strkey.Encode() error = %v", err)
	}
	if event.ContractID != want {
		t.Errorf("contract ID = %q, want %q", event.ContractID, want)
	}
}

func TestWASMSourceFailedTransactions(t *testing.T) {
	t.Parallel()

	wasm := []byte{0x00, 0x61, 0x73, 0x6d}
	envelope := encodeEnvelope(t, v1Envelope(hostFunctionOp(uploadFunction(wasm))))
	txs := []protocol.TransactionInfo{
		wasmTransaction(40, 1, "ok", envelope, protocol.TransactionStatusSuccess),
		wasmTransaction(40, 2, "bad", envelope, protocol.TransactionStatusFailed),
	}

	t.Run("skipped by default", func(t *testing.T) {
		srv := fakerpc.NewTransactionServer(txs)
		src, err := stream.NewWASMSource(srv, stream.WASMQuery{})
		if err != nil {
			t.Fatalf("NewWASMSource() error = %v", err)
		}
		records := fetchWASM(t, src)
		if len(records) != 1 {
			t.Fatalf("got %d records, want the successful one only", len(records))
		}
		if records[0].WASMDeployment.TxHash != "ok" {
			t.Errorf("emitted %q, want the successful transaction", records[0].WASMDeployment.TxHash)
		}
	})

	t.Run("included on request", func(t *testing.T) {
		srv := fakerpc.NewTransactionServer(txs)
		src, err := stream.NewWASMSource(srv, stream.WASMQuery{IncludeFailed: true})
		if err != nil {
			t.Fatalf("NewWASMSource() error = %v", err)
		}
		records := fetchWASM(t, src)
		if len(records) != 2 {
			t.Fatalf("got %d records, want both", len(records))
		}
		if records[1].WASMDeployment.Successful {
			t.Error("failed transaction was marked successful")
		}
	})
}

func TestWASMSourceEnvelopeVariants(t *testing.T) {
	t.Parallel()

	wasm := []byte{0x00, 0x61, 0x73, 0x6d}
	ops := []xdr.Operation{hostFunctionOp(uploadFunction(wasm))}

	tests := []struct {
		name     string
		envelope xdr.TransactionEnvelope
	}{
		{
			name: "v0",
			envelope: xdr.TransactionEnvelope{
				Type: xdr.EnvelopeTypeEnvelopeTypeTxV0,
				V0:   &xdr.TransactionV0Envelope{Tx: xdr.TransactionV0{Operations: ops}},
			},
		},
		{
			name: "fee bump",
			envelope: xdr.TransactionEnvelope{
				Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
				FeeBump: &xdr.FeeBumpTransactionEnvelope{
					Tx: xdr.FeeBumpTransaction{
						FeeSource: xdr.MustMuxedAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"),
						InnerTx: xdr.FeeBumpTransactionInnerTx{
							Type: xdr.EnvelopeTypeEnvelopeTypeTx,
							V1: &xdr.TransactionV1Envelope{Tx: xdr.Transaction{
								SourceAccount: xdr.MustMuxedAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"),
								Operations:    ops,
							}},
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
				wasmTransaction(50, 1, "abc", encodeEnvelope(t, tc.envelope), protocol.TransactionStatusSuccess),
			})
			src, err := stream.NewWASMSource(srv, stream.WASMQuery{})
			if err != nil {
				t.Fatalf("NewWASMSource() error = %v", err)
			}
			if got := len(fetchWASM(t, src)); got != 1 {
				t.Errorf("got %d records, want 1", got)
			}
		})
	}
}

func TestWASMSourceSkipsNonInvokeOperations(t *testing.T) {
	t.Parallel()

	payment := xdr.Operation{
		Body: xdr.OperationBody{
			Type: xdr.OperationTypePayment,
			PaymentOp: &xdr.PaymentOp{
				Destination: xdr.MustMuxedAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"),
				Asset:       xdr.MustNewNativeAsset(),
				Amount:      1,
			},
		},
	}
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		wasmTransaction(60, 1, "abc", encodeEnvelope(t, v1Envelope(payment)), protocol.TransactionStatusSuccess),
	})
	src, err := stream.NewWASMSource(srv, stream.WASMQuery{})
	if err != nil {
		t.Fatalf("NewWASMSource() error = %v", err)
	}
	if got := len(fetchWASM(t, src)); got != 0 {
		t.Errorf("got %d records for a payment, want 0", got)
	}
}

func TestWASMSourceRejectsMalformedEnvelope(t *testing.T) {
	t.Parallel()

	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		wasmTransaction(70, 1, "abc", "not-base64!", protocol.TransactionStatusSuccess),
	})
	src, err := stream.NewWASMSource(srv, stream.WASMQuery{})
	if err != nil {
		t.Fatalf("NewWASMSource() error = %v", err)
	}
	if _, err := src.Fetch(context.Background(), ""); err == nil {
		t.Fatal("Fetch() error = nil, want a decoding failure")
	} else if !strings.Contains(err.Error(), "decoding envelope") {
		t.Errorf("error = %v, want it to name the envelope decode", err)
	}
}

func TestWASMSourceRequiresClient(t *testing.T) {
	t.Parallel()

	if _, err := stream.NewWASMSource(nil, stream.WASMQuery{}); err == nil {
		t.Error("nil client accepted")
	}
}

func TestWASMSourceNameAndCursor(t *testing.T) {
	t.Parallel()

	src, err := stream.NewWASMSource(fakerpc.NewTransactionServer(nil), stream.WASMQuery{})
	if err != nil {
		t.Fatalf("NewWASMSource() error = %v", err)
	}
	if got := src.Name(); got != "wasm_deployments" {
		t.Errorf("Name() = %q, want %q", got, "wasm_deployments")
	}
	if ledger, ok := src.CursorLedger("000000000000000123-0000000000"); !ok || ledger != 123 {
		t.Errorf("CursorLedger() = %d, %v; want 123, true", ledger, ok)
	}
}

func TestWASMSourceAssetPreimageAndExternalRef(t *testing.T) {
	t.Parallel()

	native := xdr.MustNewNativeAsset()
	assetPreimage := xdr.ContractIdPreimage{
		Type:      xdr.ContractIdPreimageTypeContractIdPreimageFromAsset,
		FromAsset: &native,
	}
	owner := xdr.MustAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")
	executable := xdr.ContractExecutable{
		Type: xdr.ContractExecutableTypeContractExecutableExternalRef,
		ExternalRef: &xdr.ContractExecutableExternalRef{
			ExecutableOwner: xdr.ScAddress{
				Type:      xdr.ScAddressTypeScAddressTypeAccount,
				AccountId: &owner,
			},
		},
	}
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		wasmTransaction(80, 1, "abc", encodeEnvelope(t, v1Envelope(
			hostFunctionOp(createFunction(assetPreimage, executable, false)),
		)), protocol.TransactionStatusSuccess),
	})
	src, err := stream.NewWASMSource(srv, stream.WASMQuery{})
	if err != nil {
		t.Fatalf("NewWASMSource() error = %v", err)
	}
	records := fetchWASM(t, src)
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	event := records[0].WASMDeployment
	if event.Executable != "external_ref" {
		t.Errorf("Executable = %q, want %q", event.Executable, "external_ref")
	}
	if event.ContractID == "" {
		t.Error("ContractID is empty, want a derived strkey")
	}
}

func TestWASMSourceRejectsInvalidEnvelopeUnion(t *testing.T) {
	t.Parallel()

	// EnvelopeType OpId (6) has no arm in TransactionEnvelope, so the SDK
	// cannot marshal it. Four big-endian bytes are the complete XDR for that
	// enum, which is exactly what a hostile or buggy producer could send.
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		wasmTransaction(90, 1, "abc", base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 6}), protocol.TransactionStatusSuccess),
	})
	src, err := stream.NewWASMSource(srv, stream.WASMQuery{})
	if err != nil {
		t.Fatalf("NewWASMSource() error = %v", err)
	}
	if _, err := src.Fetch(context.Background(), ""); err == nil {
		t.Error("Fetch() error = nil, want an envelope decoding failure")
	} else if !strings.Contains(err.Error(), "decoding envelope") {
		t.Errorf("error = %v, want it to name the envelope decode", err)
	}
}

func TestWASMSourceDefaultsPageLimitAndPassphrase(t *testing.T) {
	t.Parallel()

	wasm := []byte{0x00, 0x61, 0x73, 0x6d}
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		wasmTransaction(100, 1, "abc", encodeEnvelope(t, v1Envelope(hostFunctionOp(uploadFunction(wasm)))), protocol.TransactionStatusSuccess),
	})
	src, err := stream.NewWASMSource(srv, stream.WASMQuery{})
	if err != nil {
		t.Fatalf("NewWASMSource() error = %v", err)
	}
	records := fetchWASM(t, src)
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	sum := sha256.Sum256(wasm)
	if got := records[0].WASMDeployment.WASMHash; got != hex.EncodeToString(sum[:]) {
		t.Errorf("WASMHash = %q, want the sha256 of the uploaded code", got)
	}
}

func TestWASMSourceAnnotatesSizeAgainstLimit(t *testing.T) {
	t.Parallel()

	wasm := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}
	tests := []struct {
		name     string
		limit    uint32
		warning  bool
		exceeded bool
	}{
		{name: "warning", limit: 10, warning: true},
		{name: "exceeded", limit: 7, warning: true, exceeded: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
				wasmTransaction(10, 1, "abc", encodeEnvelope(t, v1Envelope(hostFunctionOp(uploadFunction(wasm)))), protocol.TransactionStatusSuccess),
			})
			src, err := stream.NewWASMSource(srv, stream.WASMQuery{MaxContractSizeBytes: tc.limit})
			if err != nil {
				t.Fatalf("NewWASMSource() error = %v", err)
			}
			records := fetchWASM(t, src)
			if len(records) != 1 {
				t.Fatalf("got %d records, want 1", len(records))
			}
			event := records[0].WASMDeployment
			if event.WASMSizeLimitBytes != tc.limit {
				t.Errorf("WASMSizeLimitBytes = %d, want %d", event.WASMSizeLimitBytes, tc.limit)
			}
			if event.WASMSizeWarning != tc.warning {
				t.Errorf("WASMSizeWarning = %t, want %t", event.WASMSizeWarning, tc.warning)
			}
			if event.WASMSizeExceeded != tc.exceeded {
				t.Errorf("WASMSizeExceeded = %t, want %t", event.WASMSizeExceeded, tc.exceeded)
			}
			wantPercent := float64(len(wasm)) / float64(tc.limit) * 100
			if event.WASMSizePercent != wantPercent {
				t.Errorf("WASMSizePercent = %f, want %f", event.WASMSizePercent, wantPercent)
			}
		})
	}
}
