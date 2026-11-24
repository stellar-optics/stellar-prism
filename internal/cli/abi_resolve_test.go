package cli

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// testContractID is a syntactically valid contract strkey.
func testContractID(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 32)
	raw[0] = 0x42
	id, err := strkey.Encode(strkey.VersionByteContract, raw)
	if err != nil {
		t.Fatalf("encoding contract ID: %v", err)
	}
	return id
}

func uleb(value uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], value)
	return buf[:n]
}

// testWasm wraps spec entries in the usual WASM header and contractspecv0
// custom section, exactly as soroban-sdk emits them.
func testWasm(t *testing.T, entries ...xdr.ScSpecEntry) []byte {
	t.Helper()

	var spec bytes.Buffer
	for _, entry := range entries {
		encoded, err := entry.MarshalBinary()
		if err != nil {
			t.Fatalf("encoding spec entry: %v", err)
		}
		spec.Write(encoded)
	}

	var payload bytes.Buffer
	payload.Write(uleb(uint64(len(contractSpecSection))))
	payload.WriteString(contractSpecSection)
	payload.Write(spec.Bytes())

	var wasm bytes.Buffer
	wasm.Write([]byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00})
	wasm.WriteByte(0)
	wasm.Write(uleb(uint64(payload.Len())))
	wasm.Write(payload.Bytes())
	return wasm.Bytes()
}

func addressType() xdr.ScSpecTypeDef {
	return xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeAddress}
}

func u32Type() xdr.ScSpecTypeDef {
	return xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeU32}
}

func i128Type() xdr.ScSpecTypeDef {
	return xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeI128}
}

func stringType() xdr.ScSpecTypeDef {
	return xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeString}
}

// sampleSpec covers every entry kind the renderer knows about.
func sampleSpec() []xdr.ScSpecEntry {
	return []xdr.ScSpecEntry{
		{
			Kind: xdr.ScSpecEntryKindScSpecEntryFunctionV0,
			FunctionV0: &xdr.ScSpecFunctionV0{
				Doc:  "Says hello to a contract.",
				Name: "hello",
				Inputs: []xdr.ScSpecFunctionInputV0{
					{Doc: "recipient", Name: "to", Type: addressType()},
				},
				Outputs: []xdr.ScSpecTypeDef{u32Type()},
			},
		},
		{
			Kind: xdr.ScSpecEntryKindScSpecEntryFunctionV0,
			FunctionV0: &xdr.ScSpecFunctionV0{
				Name: "set_count",
				Inputs: []xdr.ScSpecFunctionInputV0{
					{Name: "count", Type: u32Type()},
				},
			},
		},
		{
			Kind: xdr.ScSpecEntryKindScSpecEntryFunctionV0,
			FunctionV0: &xdr.ScSpecFunctionV0{
				Name: "transfer",
				Inputs: []xdr.ScSpecFunctionInputV0{
					{Name: "from", Type: addressType()},
					{Name: "to", Type: addressType()},
					{Name: "amount", Type: i128Type()},
				},
				Outputs: []xdr.ScSpecTypeDef{{Type: xdr.ScSpecTypeScSpecTypeVoid}},
			},
		},
		{
			Kind: xdr.ScSpecEntryKindScSpecEntryUdtStructV0,
			UdtStructV0: &xdr.ScSpecUdtStructV0{
				Name: "Profile",
				Fields: []xdr.ScSpecUdtStructFieldV0{
					{Name: "name", Type: stringType()},
					{Name: "age", Type: u32Type()},
				},
			},
		},
		{
			Kind: xdr.ScSpecEntryKindScSpecEntryUdtUnionV0,
			UdtUnionV0: &xdr.ScSpecUdtUnionV0{
				Name: "Choice",
				Cases: []xdr.ScSpecUdtUnionCaseV0{
					{
						Kind: xdr.ScSpecUdtUnionCaseV0KindScSpecUdtUnionCaseVoidV0,
						VoidCase: &xdr.ScSpecUdtUnionCaseVoidV0{
							Name: "None",
						},
					},
					{
						Kind: xdr.ScSpecUdtUnionCaseV0KindScSpecUdtUnionCaseTupleV0,
						TupleCase: &xdr.ScSpecUdtUnionCaseTupleV0{
							Name: "Some",
							Type: []xdr.ScSpecTypeDef{u32Type()},
						},
					},
				},
			},
		},
		{
			Kind: xdr.ScSpecEntryKindScSpecEntryUdtEnumV0,
			UdtEnumV0: &xdr.ScSpecUdtEnumV0{
				Name: "Color",
				Cases: []xdr.ScSpecUdtEnumCaseV0{
					{Name: "Red", Value: 0},
					{Name: "Blue", Value: 1},
				},
			},
		},
		{
			Kind: xdr.ScSpecEntryKindScSpecEntryUdtErrorEnumV0,
			UdtErrorEnumV0: &xdr.ScSpecUdtErrorEnumV0{
				Name: "Failure",
				Cases: []xdr.ScSpecUdtErrorEnumCaseV0{
					{Name: "NotFound", Value: 7},
				},
			},
		},
		{
			Kind: xdr.ScSpecEntryKindScSpecEntryEventV0,
			EventV0: &xdr.ScSpecEventV0{
				Name: "Greeted",
				Params: []xdr.ScSpecEventParamV0{
					{Name: "who", Type: addressType(), Location: xdr.ScSpecEventParamLocationV0ScSpecEventParamLocationTopicList},
					{Name: "message", Type: stringType(), Location: xdr.ScSpecEventParamLocationV0ScSpecEventParamLocationData},
				},
				DataFormat: xdr.ScSpecEventDataFormatScSpecEventDataFormatSingleValue,
			},
		},
	}
}

func fakeABIContract(t *testing.T) (contractID string, srv *fakerpc.Server) {
	t.Helper()
	contractID = testContractID(t)
	srv = fakerpc.NewServer(nil)
	if err := srv.SetContractWasm(contractID, testWasm(t, sampleSpec()...)); err != nil {
		t.Fatalf("installing contract WASM: %v", err)
	}
	return contractID, srv
}

func TestABICommandRendersFullSpec(t *testing.T) {
	contractID, srv := fakeABIContract(t)
	withFake(t, srv)

	stdout, _, err := run(t, "abi", contractID)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	for _, want := range []string{
		"contract: " + contractID,
		"wasm:",
		"hello(to: Address) -> u32",
		"Says hello to a contract.",
		"set_count(count: u32) -> ()",
		"transfer(from: Address, to: Address, amount: i128) -> ()",
		"struct Profile",
		"name: String",
		"union Choice",
		"None",
		"Some(u32)",
		"enum Color",
		"Blue = 1",
		"error Failure",
		"NotFound = 7",
		"event Greeted",
		"who: Address [topic]",
		"message: String [data]",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("output does not contain %q\n%s", want, stdout)
		}
	}
}

func TestABICommandFunctionAndArguments(t *testing.T) {
	contractID, srv := fakeABIContract(t)
	withFake(t, srv)

	stdout, _, err := run(t, "abi", contractID, "--function", "set_count", "--arg", "AAAAAwAAAAc=")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, want := range []string{
		"set_count(count: u32) -> ()",
		"arguments:",
		"count: u32",
		"ScVal",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("output does not contain %q\n%s", want, stdout)
		}
	}
	// The decoded u32 must be present as a value, not just as an opaque
	// base64 blob.
	if !strings.Contains(stdout, "7") {
		t.Errorf("argument value was not decoded\n%s", stdout)
	}
	// Narrowing to a function must not print the rest of the ABI.
	if strings.Contains(stdout, "struct Profile") {
		t.Errorf("--function still printed unrelated entries\n%s", stdout)
	}
}

func TestABICommandRejectsBadInvocation(t *testing.T) {
	contractID, srv := fakeABIContract(t)
	withFake(t, srv)

	tests := []struct {
		name string
		args []string
	}{
		{
			name: "argument without function",
			args: []string{"abi", contractID, "--arg", "AAAAAwAAAAc="},
		},
		{
			name: "unknown function",
			args: []string{"abi", contractID, "--function", "missing"},
		},
		{
			name: "too many arguments",
			args: []string{"abi", contractID, "--function", "set_count", "--arg", "AAAAAwAAAAc=", "--arg", "AAAAAwAAAAc="},
		},
		{
			name: "argument is not XDR",
			args: []string{"abi", contractID, "--function", "set_count", "--arg", "not-xdr"},
		},
		{
			name: "invalid contract ID",
			args: []string{"abi", "not-a-contract"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := run(t, tc.args...); err == nil {
				t.Fatal("Execute() error = nil, want a failure")
			}
		})
	}
}

func TestABICommandContractNotFound(t *testing.T) {
	withFake(t, fakerpc.NewServer(nil))

	_, _, err := run(t, "abi", testContractID(t))
	if err == nil {
		t.Fatal("Execute() error = nil, want a not-found failure")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error = %q, want it to say the contract was not found", err)
	}
}

func TestABICommandWASMWithoutSpec(t *testing.T) {
	contractID := testContractID(t)
	plainWASM := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}
	srv := fakerpc.NewServer(nil)
	if err := srv.SetContractWasm(contractID, plainWASM); err != nil {
		t.Fatalf("installing contract WASM: %v", err)
	}
	withFake(t, srv)

	_, _, err := run(t, "abi", contractID)
	if err == nil {
		t.Fatal("Execute() error = nil, want a missing-spec failure")
	}
	if !strings.Contains(err.Error(), contractSpecSection) {
		t.Errorf("error = %q, want it to mention %s", err, contractSpecSection)
	}
}

func TestParseContractSpecRejectsMalformedWASM(t *testing.T) {
	tests := []struct {
		name string
		wasm []byte
	}{
		{name: "empty", wasm: nil},
		{name: "bad magic", wasm: []byte("not wasm at all")},
		{
			name: "truncated section",
			wasm: []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x00, 0x7f},
		},
		{
			name: "custom section without name",
			wasm: []byte{
				0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
				0x00, 0x01, 0xff,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := parseContractSpec(tc.wasm); err == nil {
				t.Fatal("parseContractSpec() error = nil, want a failure")
			}
		})
	}
}

func TestParseContractSpecRejectsTruncatedEntries(t *testing.T) {
	wasm := testWasm(t, sampleSpec()...)
	// Truncating inside the spec section leaves an incomplete XDR value.
	if _, err := parseContractSpec(wasm[:len(wasm)-1]); err == nil {
		t.Fatal("parseContractSpec() error = nil, want a truncated-entry failure")
	}
}

func TestFormatSpecType(t *testing.T) {
	tests := []struct {
		name string
		typ  xdr.ScSpecTypeDef
		want string
	}{
		{name: "u32", typ: u32Type(), want: "u32"},
		{name: "address", typ: addressType(), want: "Address"},
		{
			name: "option",
			typ: xdr.ScSpecTypeDef{
				Type:   xdr.ScSpecTypeScSpecTypeOption,
				Option: &xdr.ScSpecTypeOption{ValueType: u32Type()},
			},
			want: "Option<u32>",
		},
		{
			name: "vec",
			typ: xdr.ScSpecTypeDef{
				Type: xdr.ScSpecTypeScSpecTypeVec,
				Vec:  &xdr.ScSpecTypeVec{ElementType: u32Type()},
			},
			want: "Vec<u32>",
		},
		{
			name: "map",
			typ: xdr.ScSpecTypeDef{
				Type: xdr.ScSpecTypeScSpecTypeMap,
				Map:  &xdr.ScSpecTypeMap{KeyType: stringType(), ValueType: u32Type()},
			},
			want: "Map<String, u32>",
		},
		{
			name: "tuple",
			typ: xdr.ScSpecTypeDef{
				Type:  xdr.ScSpecTypeScSpecTypeTuple,
				Tuple: &xdr.ScSpecTypeTuple{ValueTypes: []xdr.ScSpecTypeDef{u32Type(), stringType()}},
			},
			want: "(u32, String)",
		},
		{
			name: "bytesN",
			typ: xdr.ScSpecTypeDef{
				Type:   xdr.ScSpecTypeScSpecTypeBytesN,
				BytesN: &xdr.ScSpecTypeBytesN{N: 32},
			},
			want: "BytesN<32>",
		},
		{
			name: "udt",
			typ: xdr.ScSpecTypeDef{
				Type: xdr.ScSpecTypeScSpecTypeUdt,
				Udt:  &xdr.ScSpecTypeUdt{Name: "Profile"},
			},
			want: "Profile",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := formatSpecType(tc.typ); got != tc.want {
				t.Errorf("formatSpecType() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFormatSpecTypePrimitivesAndMissingPayloads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		typ  xdr.ScSpecType
		want string
	}{
		{xdr.ScSpecTypeScSpecTypeVal, "Val"},
		{xdr.ScSpecTypeScSpecTypeBool, "Bool"},
		{xdr.ScSpecTypeScSpecTypeVoid, "()"},
		{xdr.ScSpecTypeScSpecTypeError, "Error"},
		{xdr.ScSpecTypeScSpecTypeI32, "i32"},
		{xdr.ScSpecTypeScSpecTypeU64, "u64"},
		{xdr.ScSpecTypeScSpecTypeI64, "i64"},
		{xdr.ScSpecTypeScSpecTypeTimepoint, "Timepoint"},
		{xdr.ScSpecTypeScSpecTypeDuration, "Duration"},
		{xdr.ScSpecTypeScSpecTypeU128, "u128"},
		{xdr.ScSpecTypeScSpecTypeI128, "i128"},
		{xdr.ScSpecTypeScSpecTypeU256, "u256"},
		{xdr.ScSpecTypeScSpecTypeI256, "i256"},
		{xdr.ScSpecTypeScSpecTypeBytes, "Bytes"},
		{xdr.ScSpecTypeScSpecTypeSymbol, "Symbol"},
		{xdr.ScSpecTypeScSpecTypeMuxedAddress, "MuxedAddress"},
	}
	for _, tc := range tests {
		if got := formatSpecType(xdr.ScSpecTypeDef{Type: tc.typ}); got != tc.want {
			t.Errorf("formatSpecType(%s) = %q, want %q", tc.typ, got, tc.want)
		}
	}

	if got := formatSpecType(xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeResult}); got != "Result<?, ?>" {
		t.Errorf("missing Result payload = %q", got)
	}
	if got := formatSpecType(xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeOption}); got != "Option<?>" {
		t.Errorf("missing Option payload = %q", got)
	}
	if got := formatSpecType(xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeVec}); got != "Vec<?>" {
		t.Errorf("missing Vec payload = %q", got)
	}
	if got := formatSpecType(xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeMap}); got != "Map<?, ?>" {
		t.Errorf("missing Map payload = %q", got)
	}
	if got := formatSpecType(xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeTuple}); got != "(?)" {
		t.Errorf("missing Tuple payload = %q", got)
	}
	if got := formatSpecType(xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeBytesN}); got != "BytesN<?>" {
		t.Errorf("missing BytesN payload = %q", got)
	}
	if got := formatSpecType(xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeUdt}); got != "< ?>" && got != "<\u003f>" && got != "<?>" {
		t.Errorf("missing UDT payload = %q", got)
	}

	result := xdr.ScSpecTypeDef{
		Type:   xdr.ScSpecTypeScSpecTypeResult,
		Result: &xdr.ScSpecTypeResult{OkType: u32Type(), ErrorType: stringType()},
	}
	if got := formatSpecType(result); got != "Result<u32, String>" {
		t.Errorf("Result = %q", got)
	}
}

func TestWasmHashFromInstanceRejectsUnsupportedShapes(t *testing.T) {
	t.Parallel()

	contractData := func(value xdr.ScVal) xdr.LedgerEntry {
		return xdr.LedgerEntry{Data: xdr.LedgerEntryData{
			Type:         xdr.LedgerEntryTypeContractData,
			ContractData: &xdr.ContractDataEntry{Val: value},
		}}
	}

	tests := []struct {
		name  string
		entry xdr.LedgerEntry
	}{
		{
			name:  "not contract data",
			entry: xdr.LedgerEntry{Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeAccount}},
		},
		{
			name:  "empty contract data",
			entry: xdr.LedgerEntry{Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeContractData}},
		},
		{
			name:  "not a contract instance",
			entry: contractData(xdr.ScVal{Type: xdr.ScValTypeScvU32}),
		},
		{
			name: "instance with nil payload",
			entry: contractData(xdr.ScVal{
				Type: xdr.ScValTypeScvContractInstance,
			}),
		},
		{
			name: "stellar asset contract",
			entry: contractData(xdr.ScVal{
				Type: xdr.ScValTypeScvContractInstance,
				Instance: &xdr.ScContractInstance{
					Executable: xdr.ContractExecutable{
						Type: xdr.ContractExecutableTypeContractExecutableStellarAsset,
					},
				},
			}),
		},
		{
			name: "external reference",
			entry: contractData(xdr.ScVal{
				Type: xdr.ScValTypeScvContractInstance,
				Instance: &xdr.ScContractInstance{
					Executable: xdr.ContractExecutable{
						Type: xdr.ContractExecutableTypeContractExecutableExternalRef,
					},
				},
			}),
		},
		{
			name: "wasm without hash",
			entry: contractData(xdr.ScVal{
				Type: xdr.ScValTypeScvContractInstance,
				Instance: &xdr.ScContractInstance{
					Executable: xdr.ContractExecutable{
						Type: xdr.ContractExecutableTypeContractExecutableWasm,
					},
				},
			}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := wasmHashFromInstance(tc.entry); err == nil {
				t.Fatal("wasmHashFromInstance() error = nil, want a failure")
			}
		})
	}
}

// ledgerStub implements only the optional ledger-entry capability, which is
// all fetchLedgerEntry needs.
type ledgerStub struct {
	resp protocol.GetLedgerEntriesResponse
	err  error
}

func (s ledgerStub) GetLedgerEntries(context.Context, protocol.GetLedgerEntriesRequest) (protocol.GetLedgerEntriesResponse, error) {
	return s.resp, s.err
}

func TestFetchLedgerEntryFailures(t *testing.T) {
	t.Parallel()
	key := xdr.LedgerKey{Type: xdr.LedgerEntryTypeContractCode, ContractCode: &xdr.LedgerKeyContractCode{}}

	t.Run("rpc error", func(t *testing.T) {
		_, err := fetchLedgerEntry(context.Background(), ledgerStub{err: errors.New("boom")}, key, "C...")
		if err == nil {
			t.Fatal("fetchLedgerEntry() error = nil, want the RPC error")
		}
	})

	t.Run("entry without data", func(t *testing.T) {
		_, err := fetchLedgerEntry(context.Background(), ledgerStub{
			resp: protocol.GetLedgerEntriesResponse{Entries: []protocol.LedgerEntryResult{{}}},
		}, key, "C...")
		if err == nil {
			t.Fatal("fetchLedgerEntry() error = nil, want a not-found failure")
		}
	})

	t.Run("malformed entry", func(t *testing.T) {
		_, err := fetchLedgerEntry(context.Background(), ledgerStub{
			resp: protocol.GetLedgerEntriesResponse{Entries: []protocol.LedgerEntryResult{{DataXDR: "not-xdr"}}},
		}, key, "C...")
		if err == nil {
			t.Fatal("fetchLedgerEntry() error = nil, want a decode failure")
		}
	})
}

// streamOnlyClient satisfies rpc.Client but not rpc.LedgerEntriesClient.
type streamOnlyClient struct{}

func (streamOnlyClient) GetEvents(context.Context, protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	return protocol.GetEventsResponse{}, nil
}

func (streamOnlyClient) GetEventsBatch(context.Context, []protocol.GetEventsRequest) ([]protocol.GetEventsResponse, error) {
	return nil, nil
}

func (streamOnlyClient) GetLedgers(context.Context, protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error) {
	return protocol.GetLedgersResponse{}, nil
}

func (streamOnlyClient) GetTransaction(context.Context, protocol.GetTransactionRequest) (protocol.GetTransactionResponse, error) {
	return protocol.GetTransactionResponse{}, nil
}

func (streamOnlyClient) GetTransactions(context.Context, protocol.GetTransactionsRequest) (protocol.GetTransactionsResponse, error) {
	return protocol.GetTransactionsResponse{}, nil
}

func (streamOnlyClient) SimulateTransaction(context.Context, protocol.SimulateTransactionRequest) (protocol.SimulateTransactionResponse, error) {
	return protocol.SimulateTransactionResponse{}, nil
}

func (streamOnlyClient) GetLatestLedger(context.Context) (protocol.GetLatestLedgerResponse, error) {
	return protocol.GetLatestLedgerResponse{}, nil
}

func TestResolveContractSpecRequiresLedgerEntriesCapability(t *testing.T) {
	t.Parallel()
	_, err := resolveContractSpec(context.Background(), streamOnlyClient{}, testContractID(t))
	if err == nil {
		t.Fatal("resolveContractSpec() error = nil, want an unsupported-client failure")
	}
	if !strings.Contains(err.Error(), "getLedgerEntries") {
		t.Errorf("error = %q, want it to name the missing capability", err)
	}
}

func TestContractSpecAddRejectsMalformedEntries(t *testing.T) {
	t.Parallel()

	kinds := []xdr.ScSpecEntryKind{
		xdr.ScSpecEntryKindScSpecEntryFunctionV0,
		xdr.ScSpecEntryKindScSpecEntryUdtStructV0,
		xdr.ScSpecEntryKindScSpecEntryUdtUnionV0,
		xdr.ScSpecEntryKindScSpecEntryUdtEnumV0,
		xdr.ScSpecEntryKindScSpecEntryUdtErrorEnumV0,
		xdr.ScSpecEntryKindScSpecEntryEventV0,
		xdr.ScSpecEntryKind(99),
	}
	for _, kind := range kinds {
		spec := &contractSpec{}
		if err := spec.add([]xdr.ScSpecEntry{{Kind: kind}}); err == nil {
			t.Errorf("add(entry kind %d) error = nil, want a failure", kind)
		}
	}
}

func TestContractSpecWriteEmptySections(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	spec := &contractSpec{ContractID: "C...", WasmHash: xdr.Hash{1}}
	if err := spec.write(&buf, "", nil, lensformat.NoColor); err != nil {
		t.Fatalf("write() error = %v", err)
	}
	if strings.Contains(buf.String(), "functions") {
		t.Errorf("empty spec printed a functions heading\n%s", buf.String())
	}
}

func TestWriteUnionCaseRejectsMalformedCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		unionCase xdr.ScSpecUdtUnionCaseV0
	}{
		{
			name:      "missing void payload",
			unionCase: xdr.ScSpecUdtUnionCaseV0{Kind: xdr.ScSpecUdtUnionCaseV0KindScSpecUdtUnionCaseVoidV0},
		},
		{
			name:      "missing tuple payload",
			unionCase: xdr.ScSpecUdtUnionCaseV0{Kind: xdr.ScSpecUdtUnionCaseV0KindScSpecUdtUnionCaseTupleV0},
		},
		{
			name:      "unknown kind",
			unionCase: xdr.ScSpecUdtUnionCaseV0{Kind: xdr.ScSpecUdtUnionCaseV0Kind(99)},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeUnionCase(&buf, tc.unionCase, ""); err == nil {
				t.Fatal("writeUnionCase() error = nil, want a failure")
			}
		})
	}
}

func TestABIInspectMarkdown(t *testing.T) {
	contractID, srv := fakeABIContract(t)
	withFake(t, srv)

	stdout, _, err := run(t, "abi", "inspect", contractID, "--color", "always")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if strings.Contains(stdout, "\x1b[") {
		t.Errorf("markdown contains ANSI escapes despite --color always\n%s", stdout)
	}

	for _, want := range []string{
		"# Contract ABI",
		"## Contract",
		"| Contract ID | `" + contractID + "` |",
		"| WASM hash | `",
		"## Functions",
		"| Name | Signature | Returns | Doc |",
		"| `hello` | `hello(to: Address) -> u32` | `u32` | Says hello to a contract. |",
		"| `set_count` | `set_count(count: u32) -> ()` | `()` |",
		"| `transfer` | `transfer(from: Address, to: Address, amount: i128) -> ()` | `()` |",
		"## Arguments",
		"| Function | Name | Type | Doc |",
		"| `hello` | `to` | `Address` | recipient |",
		"| `set_count` | `count` | `u32` |",
		"| `transfer` | `amount` | `i128` |",
		"## Structs",
		"| `Profile` |",
		"| `name` | `String` |",
		"## Unions",
		"| `Some` | `(u32)` |",
		"## Enums",
		"| `Blue` | 1 |",
		"## Errors",
		"| `NotFound` | 7 |",
		"## Events",
		"| `Greeted` |",
		"| `who` | `Address` | topic |",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("markdown output does not contain %q\n%s", want, stdout)
		}
	}

	// Spec entries must stay in their on-chain order in every table.
	positions := make([]int, 0, 3)
	for _, signature := range []string{
		"`hello(to: Address) -> u32`",
		"`set_count(count: u32) -> ()`",
		"`transfer(from: Address, to: Address, amount: i128) -> ()`",
	} {
		pos := strings.Index(stdout, signature)
		if pos < 0 {
			t.Fatalf("markdown output is missing signature %s\n%s", signature, stdout)
		}
		positions = append(positions, pos)
	}
	for i := 1; i < len(positions); i++ {
		if positions[i-1] >= positions[i] {
			t.Fatalf("function signatures are out of order\n%s", stdout)
		}
	}
}

func TestABIInspectJSON(t *testing.T) {
	contractID, srv := fakeABIContract(t)
	withFake(t, srv)

	stdout, _, err := run(t, "abi", "inspect", contractID, "--json", "--color", "always")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if strings.Contains(stdout, "\x1b[") {
		t.Errorf("JSON contains ANSI escapes despite --color always\n%s", stdout)
	}
	if !strings.Contains(stdout, "\n  \"contractId\":") {
		t.Errorf("JSON is not pretty printed with two-space indentation\n%s", stdout)
	}
	if !strings.HasSuffix(stdout, "}\n") {
		t.Errorf("JSON encoder did not end with a newline\n%s", stdout)
	}

	for i, key := range []string{"\"contractId\"", "\"wasmHash\"", "\"functions\""} {
		pos := strings.Index(stdout, key)
		if pos < 0 {
			t.Fatalf("JSON is missing top-level key %s\n%s", key, stdout)
		}
		if i > 0 && strings.Index(stdout, []string{"\"contractId\"", "\"wasmHash\"", "\"functions\""}[i-1]) >= pos {
			t.Fatalf("top-level JSON keys are out of order\n%s", stdout)
		}
	}

	var got struct {
		ContractID string `json:"contractId"`
		WasmHash   string `json:"wasmHash"`
		Functions  []struct {
			Name      string `json:"name"`
			Doc       string `json:"doc"`
			Arguments []struct {
				Name string `json:"name"`
				Type string `json:"type"`
				Doc  string `json:"doc"`
			} `json:"arguments"`
			Returns []string `json:"returns"`
		} `json:"functions"`
		Structs []struct {
			Name   string `json:"name"`
			Doc    string `json:"doc"`
			Fields []struct {
				Name string `json:"name"`
				Type string `json:"type"`
				Doc  string `json:"doc"`
			} `json:"fields"`
		} `json:"structs"`
		Unions []struct {
			Name  string `json:"name"`
			Doc   string `json:"doc"`
			Cases []struct {
				Name  string   `json:"name"`
				Types []string `json:"types"`
				Doc   string   `json:"doc"`
			} `json:"cases"`
		} `json:"unions"`
		Enums []struct {
			Name  string `json:"name"`
			Doc   string `json:"doc"`
			Cases []struct {
				Name  string `json:"name"`
				Value uint32 `json:"value"`
				Doc   string `json:"doc"`
			} `json:"cases"`
		} `json:"enums"`
		Errors []struct {
			Name  string `json:"name"`
			Doc   string `json:"doc"`
			Cases []struct {
				Name  string `json:"name"`
				Value uint32 `json:"value"`
				Doc   string `json:"doc"`
			} `json:"cases"`
		} `json:"errors"`
		Events []struct {
			Name   string `json:"name"`
			Doc    string `json:"doc"`
			Params []struct {
				Name     string `json:"name"`
				Type     string `json:"type"`
				Location string `json:"location"`
				Doc      string `json:"doc"`
			} `json:"params"`
		} `json:"events"`
	}
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("decoding ABI inspection JSON: %v\n%s", err, stdout)
	}

	if got.ContractID != contractID {
		t.Errorf("contractId = %q, want %q", got.ContractID, contractID)
	}
	if len(got.WasmHash) != 64 {
		t.Errorf("wasmHash = %q, want a 64-character hex hash", got.WasmHash)
	}
	if len(got.Functions) != 3 {
		t.Fatalf("got %d functions, want 3\n%s", len(got.Functions), stdout)
	}

	wantNames := []string{"hello", "set_count", "transfer"}
	for i, want := range wantNames {
		if got.Functions[i].Name != want {
			t.Errorf("functions[%d].name = %q, want %q", i, got.Functions[i].Name, want)
		}
	}

	hello := got.Functions[0]
	if hello.Doc != "Says hello to a contract." {
		t.Errorf("hello.doc = %q", hello.Doc)
	}
	if len(hello.Arguments) != 1 {
		t.Fatalf("hello has %d arguments, want 1", len(hello.Arguments))
	}
	if hello.Arguments[0].Name != "to" || hello.Arguments[0].Type != "Address" || hello.Arguments[0].Doc != "recipient" {
		t.Errorf("hello argument = %+v, want to/Address/recipient", hello.Arguments[0])
	}
	if strings.Join(hello.Returns, ",") != "u32" {
		t.Errorf("hello.returns = %v, want [u32]", hello.Returns)
	}
	if got.Functions[1].Doc != "" || len(got.Functions[1].Returns) != 0 {
		t.Errorf("set_count = %+v, want an empty doc and returns", got.Functions[1])
	}
	if strings.Join(got.Functions[2].Returns, ",") != "()" {
		t.Errorf("transfer.returns = %v, want [()]", got.Functions[2].Returns)
	}

	if len(got.Structs) != 1 || got.Structs[0].Name != "Profile" {
		t.Fatalf("structs = %+v, want Profile", got.Structs)
	}
	if len(got.Structs[0].Fields) != 2 ||
		got.Structs[0].Fields[0].Name != "name" || got.Structs[0].Fields[0].Type != "String" ||
		got.Structs[0].Fields[1].Name != "age" || got.Structs[0].Fields[1].Type != "u32" {
		t.Errorf("Profile fields = %+v", got.Structs[0].Fields)
	}
	if len(got.Unions) != 1 || got.Unions[0].Name != "Choice" || len(got.Unions[0].Cases) != 2 {
		t.Fatalf("unions = %+v, want Choice with two cases", got.Unions)
	}
	if got.Unions[0].Cases[0].Name != "None" || len(got.Unions[0].Cases[0].Types) != 0 ||
		got.Unions[0].Cases[1].Name != "Some" || strings.Join(got.Unions[0].Cases[1].Types, ",") != "u32" {
		t.Errorf("Choice cases = %+v", got.Unions[0].Cases)
	}
	if len(got.Enums) != 1 || len(got.Enums[0].Cases) != 2 ||
		got.Enums[0].Cases[1].Name != "Blue" || got.Enums[0].Cases[1].Value != 1 {
		t.Errorf("enums = %+v, want Color/Blue=1", got.Enums)
	}
	if len(got.Errors) != 1 || len(got.Errors[0].Cases) != 1 ||
		got.Errors[0].Cases[0].Name != "NotFound" || got.Errors[0].Cases[0].Value != 7 {
		t.Errorf("errors = %+v, want Failure/NotFound=7", got.Errors)
	}
	if len(got.Events) != 1 || got.Events[0].Name != "Greeted" || len(got.Events[0].Params) != 2 {
		t.Fatalf("events = %+v, want Greeted with two params", got.Events)
	}
	if got.Events[0].Params[0].Name != "who" || got.Events[0].Params[0].Location != "topic" ||
		got.Events[0].Params[1].Location != "data" {
		t.Errorf("Greeted params = %+v", got.Events[0].Params)
	}
}

func TestABIInspectRequiresContractID(t *testing.T) {
	if _, _, err := run(t, "abi", "inspect"); err == nil {
		t.Fatal("Execute() error = nil, want a missing contract ID failure")
	}
}

func TestABIInspectValidatesContractIDBeforeClient(t *testing.T) {
	original := newClient
	clientCalled := false
	newClient = func([]string, time.Duration) (rpc.Client, error) {
		clientCalled = true
		return nil, errors.New("unexpected client construction")
	}
	t.Cleanup(func() { newClient = original })

	_, _, err := run(t, "abi", "inspect", "not-a-contract")
	if err == nil {
		t.Fatal("Execute() error = nil, want an invalid contract ID failure")
	}
	if !strings.Contains(err.Error(), "invalid contract ID") {
		t.Errorf("error = %q, want it to name the invalid contract ID", err)
	}
	if clientCalled {
		t.Error("invalid contract ID opened an RPC client before validation")
	}
}

func TestABIInspectContractNotFound(t *testing.T) {
	withFake(t, fakerpc.NewServer(nil))

	_, _, err := run(t, "abi", "inspect", testContractID(t))
	if err == nil {
		t.Fatal("Execute() error = nil, want a not-found failure")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error = %q, want it to say the contract was not found", err)
	}
}

func TestABIInspectWASMWithoutSpec(t *testing.T) {
	contractID := testContractID(t)
	srv := fakerpc.NewServer(nil)
	if err := srv.SetContractWasm(contractID, []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}); err != nil {
		t.Fatalf("installing contract WASM: %v", err)
	}
	withFake(t, srv)

	_, _, err := run(t, "abi", "inspect", contractID)
	if err == nil {
		t.Fatal("Execute() error = nil, want a missing-spec failure")
	}
	if !strings.Contains(err.Error(), contractSpecSection) {
		t.Errorf("error = %q, want it to mention %s", err, contractSpecSection)
	}
}

var errABIWrite = errors.New("write failed")

type abiFailWriter struct{}

func (abiFailWriter) Write([]byte) (int, error) { return 0, errABIWrite }

func TestABIInspectionWriterErrorsAreWrapped(t *testing.T) {
	t.Parallel()
	spec := &contractSpec{ContractID: "C...", WasmHash: xdr.Hash{1}}

	if err := spec.writeInspectionMarkdown(abiFailWriter{}); !errors.Is(err, errABIWrite) {
		t.Errorf("writeInspectionMarkdown() error = %v, want it to wrap the writer error", err)
	}
	if err := spec.writeInspectionJSON(abiFailWriter{}); !errors.Is(err, errABIWrite) {
		t.Errorf("writeInspectionJSON() error = %v, want it to wrap the writer error", err)
	}
}
