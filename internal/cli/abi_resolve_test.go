package cli

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
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

func writeSpecFixture(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("writing spec fixture: %v", err)
	}
	return path
}

func localSpecFixture(t *testing.T) string {
	t.Helper()
	return writeSpecFixture(t, "contract.wasm", testWasm(t, sampleSpec()...))
}

func TestLoadContractSpecFromFile(t *testing.T) {
	t.Run("wasm", func(t *testing.T) {
		spec, err := loadContractSpec(localSpecFixture(t), "C...")
		if err != nil {
			t.Fatalf("loadContractSpec() error = %v", err)
		}
		if len(spec.Functions) != 3 || len(spec.Events) != 1 {
			t.Fatalf("loaded spec has %d functions and %d events", len(spec.Functions), len(spec.Events))
		}
	})

	t.Run("json", func(t *testing.T) {
		data, err := json.Marshal(sampleSpec())
		if err != nil {
			t.Fatalf("marshalling spec: %v", err)
		}
		spec, err := loadContractSpec(writeSpecFixture(t, "spec.json", data), "C...")
		if err != nil {
			t.Fatalf("loadContractSpec() error = %v", err)
		}
		if len(spec.Functions) != 3 || len(spec.Events) != 1 {
			t.Fatalf("loaded spec has %d functions and %d events", len(spec.Functions), len(spec.Events))
		}
	})
}

func TestLoadContractSpecRejectsInvalidFiles(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		if _, err := loadContractSpec(filepath.Join(t.TempDir(), "missing.wasm"), "C..."); err == nil {
			t.Fatal("loadContractSpec() error = nil, want a missing-file failure")
		}
	})
	t.Run("invalid wasm", func(t *testing.T) {
		path := writeSpecFixture(t, "broken.wasm", []byte("not wasm"))
		if _, err := loadContractSpec(path, "C..."); err == nil {
			t.Fatal("loadContractSpec() error = nil, want a bad-WASM failure")
		}
	})
	t.Run("wasm without spec", func(t *testing.T) {
		path := writeSpecFixture(t, "plain.wasm", []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00})
		if _, err := loadContractSpec(path, "C..."); err == nil {
			t.Fatal("loadContractSpec() error = nil, want a missing-spec failure")
		}
	})
	t.Run("invalid json", func(t *testing.T) {
		path := writeSpecFixture(t, "broken.json", []byte("{"))
		if _, err := loadContractSpec(path, "C..."); err == nil {
			t.Fatal("loadContractSpec() error = nil, want a JSON failure")
		}
	})
	t.Run("empty json", func(t *testing.T) {
		path := writeSpecFixture(t, "empty.json", []byte("[]"))
		if _, err := loadContractSpec(path, "C..."); err == nil {
			t.Fatal("loadContractSpec() error = nil, want an empty-spec failure")
		}
	})
	t.Run("unknown extension", func(t *testing.T) {
		path := writeSpecFixture(t, "spec.bin", testWasm(t, sampleSpec()...))
		if _, err := loadContractSpec(path, "C..."); err == nil {
			t.Fatal("loadContractSpec() error = nil, want an extension failure")
		}
	})
}

func TestABICommandUsesLocalSpecWithoutRPC(t *testing.T) {
	withFake(t, fakerpc.NewServer(nil))
	path := localSpecFixture(t)

	stdout, _, err := run(t, "abi", testContractID(t), "--spec", path)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(stdout, "hello(to: Address) -> u32") {
		t.Fatalf("local spec was not rendered\n%s", stdout)
	}
}

func TestABIEncodeCommand(t *testing.T) {
	withFake(t, fakerpc.NewServer(nil))
	path := localSpecFixture(t)

	stdout, _, err := run(t, "abi", "encode", testContractID(t), "set_count",
		"--arg", "count=7", "--spec", path)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(stdout, "count=AAAAAwAAAAc=") {
		t.Fatalf("encoded output = %q, want base64 ScVal for u32 7", stdout)
	}
}

func TestWriteEncodedArgumentsValidatesInputs(t *testing.T) {
	spec := &contractSpec{}
	if err := spec.add(sampleSpec()); err != nil {
		t.Fatalf("building spec: %v", err)
	}

	tests := []struct {
		name      string
		function  string
		arguments []string
	}{
		{name: "unknown function", function: "missing", arguments: []string{"count=7"}},
		{name: "missing value", function: "set_count", arguments: []string{"count="}},
		{name: "unknown argument", function: "set_count", arguments: []string{"other=7"}},
		{name: "missing argument", function: "set_count"},
		{name: "duplicate argument", function: "set_count", arguments: []string{"count=7", "count=8"}},
		{name: "wrong type", function: "set_count", arguments: []string{"count=abc"}},
		{name: "out of range", function: "set_count", arguments: []string{"count=4294967296"}},
		{name: "invalid address", function: "hello", arguments: []string{"to=not-an-address"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeEncodedArguments(&buf, spec, tc.function, tc.arguments); err == nil {
				t.Fatal("writeEncodedArguments() error = nil, want a validation failure")
			}
		})
	}
}

func TestEncodeScValSupportedTypes(t *testing.T) {
	addr := testContractID(t)
	tests := []struct {
		name string
		typ  xdr.ScSpecTypeDef
		raw  string
		want xdr.ScValType
	}{
		{name: "bool", typ: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeBool}, raw: "true", want: xdr.ScValTypeScvBool},
		{name: "u32", typ: u32Type(), raw: "7", want: xdr.ScValTypeScvU32},
		{name: "i32", typ: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeI32}, raw: "-7", want: xdr.ScValTypeScvI32},
		{name: "u64", typ: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeU64}, raw: "7", want: xdr.ScValTypeScvU64},
		{name: "i64", typ: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeI64}, raw: "-7", want: xdr.ScValTypeScvI64},
		{name: "u128", typ: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeU128}, raw: "123456789012345678901234567890", want: xdr.ScValTypeScvU128},
		{name: "i128", typ: i128Type(), raw: "-1000", want: xdr.ScValTypeScvI128},
		{name: "u256", typ: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeU256}, raw: "123456789012345678901234567890", want: xdr.ScValTypeScvU256},
		{name: "i256", typ: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeI256}, raw: "-1000", want: xdr.ScValTypeScvI256},
		{name: "bytes", typ: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeBytes}, raw: "0xdeadbeef", want: xdr.ScValTypeScvBytes},
		{name: "string", typ: stringType(), raw: "hello", want: xdr.ScValTypeScvString},
		{name: "symbol", typ: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeSymbol}, raw: "hello", want: xdr.ScValTypeScvSymbol},
		{name: "address", typ: addressType(), raw: addr, want: xdr.ScValTypeScvAddress},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			value, err := encodeScVal(tc.typ, tc.raw)
			if err != nil {
				t.Fatalf("encodeScVal() error = %v", err)
			}
			if value.Type != tc.want {
				t.Fatalf("ScVal type = %s, want %s", value.Type, tc.want)
			}
			if _, err := xdr.MarshalBase64(value); err != nil {
				t.Fatalf("MarshalBase64() error = %v", err)
			}
		})
	}
}

func TestEncodeScValRejectsCompoundTypes(t *testing.T) {
	typ := xdr.ScSpecTypeDef{
		Type: xdr.ScSpecTypeScSpecTypeVec,
		Vec:  &xdr.ScSpecTypeVec{ElementType: u32Type()},
	}
	if _, err := encodeScVal(typ, "[1,2]"); err == nil {
		t.Fatal("encodeScVal() error = nil, want an unsupported-type failure")
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

type batchSourceStub struct {
	batch stream.Batch
}

func (s batchSourceStub) Name() string { return "stub" }
func (s batchSourceStub) Fetch(context.Context, string) (stream.Batch, error) {
	return s.batch, nil
}
func (batchSourceStub) CursorLedger(string) (uint32, bool) { return 0, false }

func TestSpecDecodeSourceDecodesMatchingEvent(t *testing.T) {
	contractID := testContractID(t)
	addr, err := encodeScVal(addressType(), contractID)
	if err != nil {
		t.Fatalf("encoding address: %v", err)
	}
	name, err := xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol("Greeted"))
	if err != nil {
		t.Fatalf("encoding event name: %v", err)
	}
	message, err := encodeScVal(stringType(), "hello")
	if err != nil {
		t.Fatalf("encoding message: %v", err)
	}

	record := stream.Record{
		ID:    "1",
		Kind:  stream.KindEvent,
		Event: &stream.Event{ContractID: contractID, TopicXDR: []string{marshalScVal(name), marshalScVal(addr)}, ValueXDR: marshalScVal(message)},
	}
	src := newSpecDecodeSource(batchSourceStub{batch: stream.Batch{Records: []stream.Record{record}}},
		&contractSpec{Events: []xdr.ScSpecEventV0{*sampleSpec()[7].EventV0}}, contractID)

	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	event := batch.Records[0].Event
	if event.SpecName != "Greeted" {
		t.Fatalf("SpecName = %q, want Greeted", event.SpecName)
	}
	if len(event.SpecParams) != 2 {
		t.Fatalf("SpecParams = %d, want 2", len(event.SpecParams))
	}
	if event.SpecParams[0].Name != "who" || event.SpecParams[0].Location != "topic" {
		t.Fatalf("topic param = %+v", event.SpecParams[0])
	}
	if event.SpecParams[1].Name != "message" || event.SpecParams[1].XDR != marshalScVal(message) {
		t.Fatalf("data param = %+v", event.SpecParams[1])
	}
}

func TestSpecDecodeSourceIgnoresOtherContracts(t *testing.T) {
	record := stream.Record{ID: "1", Kind: stream.KindEvent, Event: &stream.Event{ContractID: "other"}}
	src := newSpecDecodeSource(batchSourceStub{batch: stream.Batch{Records: []stream.Record{record}}},
		&contractSpec{Events: []xdr.ScSpecEventV0{*sampleSpec()[7].EventV0}}, testContractID(t))

	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if batch.Records[0].Event.SpecName != "" {
		t.Fatal("event from another contract was decoded with the local spec")
	}
}
