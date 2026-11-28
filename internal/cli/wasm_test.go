package cli

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
)

// wasmModuleWriter assembles a WASM binary section by section.
type wasmModuleWriter struct {
	bytes.Buffer
}

func newWASMModuleWriter() *wasmModuleWriter {
	w := &wasmModuleWriter{}
	w.Write([]byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00})
	return w
}

func (w *wasmModuleWriter) section(id byte, payload []byte) {
	w.WriteByte(id)
	w.Write(uleb(uint64(len(payload))))
	w.Write(payload)
}

// disasmFixtureWasm is a module exercising every section disasm reports: two
// function types, a host function and memory import, two local functions, a
// table, a memory with page limits, three exports and a custom section.
func disasmFixtureWasm() []byte {
	w := newWASMModuleWriter()

	// Type: (i32, i32) -> i32 and () -> ().
	var types bytes.Buffer
	types.Write(uleb(2))
	types.Write([]byte{0x60})
	types.Write(uleb(2))
	types.Write([]byte{0x7F, 0x7F})
	types.Write(uleb(1))
	types.Write([]byte{0x7F})
	types.Write([]byte{0x60})
	types.Write(uleb(0))
	types.Write(uleb(0))
	w.section(wasmSectionType, types.Bytes())

	// Import: env.log_i32 (func, type 0) and env.memory (memory, 1..4 pages).
	var imports bytes.Buffer
	imports.Write(uleb(2))
	imports.Write(uleb(3))
	imports.WriteString("env")
	imports.Write(uleb(7))
	imports.WriteString("log_i32")
	imports.Write([]byte{0x00})
	imports.Write(uleb(0))
	imports.Write(uleb(3))
	imports.WriteString("env")
	imports.Write(uleb(6))
	imports.WriteString("memory")
	imports.Write([]byte{0x02, 0x01})
	imports.Write(uleb(1))
	imports.Write(uleb(4))
	w.section(wasmSectionImport, imports.Bytes())

	// Function: two local functions, using type 0 and type 1.
	var functions bytes.Buffer
	functions.Write(uleb(2))
	functions.Write(uleb(0))
	functions.Write(uleb(1))
	w.section(wasmSectionFunction, functions.Bytes())

	// Table: one funcref table of 1..1 entries.
	var tables bytes.Buffer
	tables.Write(uleb(1))
	tables.Write([]byte{0x70, 0x01})
	tables.Write(uleb(1))
	tables.Write(uleb(1))
	w.section(wasmSectionTable, tables.Bytes())

	// Memory: one memory with a minimum of 2 pages and no maximum.
	var memories bytes.Buffer
	memories.Write(uleb(1))
	memories.Write([]byte{0x00})
	memories.Write(uleb(2))
	w.section(wasmSectionMemory, memories.Bytes())

	// Export: add (local function 0 = module index 1), hello (index 2), and
	// the memory (index 0).
	var exports bytes.Buffer
	exports.Write(uleb(3))
	exports.Write(uleb(3))
	exports.WriteString("add")
	exports.Write([]byte{0x00})
	exports.Write(uleb(1))
	exports.Write(uleb(5))
	exports.WriteString("hello")
	exports.Write([]byte{0x00})
	exports.Write(uleb(2))
	exports.Write(uleb(6))
	exports.WriteString("memory")
	exports.Write([]byte{0x02})
	exports.Write(uleb(0))
	w.section(wasmSectionExport, exports.Bytes())

	// A custom section, as the Soroban SDK emits for the contract spec.
	var custom bytes.Buffer
	custom.Write(uleb(uint64(len(contractSpecSection))))
	custom.WriteString(contractSpecSection)
	custom.Write([]byte{0x01, 0x02, 0x03})
	w.section(wasmSectionCustom, custom.Bytes())

	return w.Bytes()
}

func TestWASMDisasmCommandPrintsModuleSummary(t *testing.T) {
	contractID := testContractID(t)
	wasm := disasmFixtureWasm()
	srv := fakerpc.NewServer(nil)
	if err := srv.SetContractWasm(contractID, wasm); err != nil {
		t.Fatalf("installing contract WASM: %v", err)
	}
	withFake(t, srv)

	stdout, _, err := run(t, "wasm", "disasm", contractID)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	hash := sha256.Sum256(wasm)
	for _, want := range []string{
		"contract: " + contractID,
		"wasm hash: " + hex.EncodeToString(hash[:]),
		"Type",
		"Import",
		"Function",
		"Memory",
		"Export",
		"Custom",
		contractSpecSection,
		"log_i32",
		"(i32, i32) -> i32",
		"() -> ()",
		"add",
		"hello",
		"min 1 page (64 KiB), max 4 pages (256 KiB)",
		"min 2 pages (128 KiB), no maximum",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("output does not contain %q\n%s", want, stdout)
		}
	}
}

func TestWASMDisasmCommandRejectsInvalidWASM(t *testing.T) {
	contractID := testContractID(t)
	srv := fakerpc.NewServer(nil)
	if err := srv.SetContractWasm(contractID, []byte("not wasm")); err != nil {
		t.Fatalf("installing contract WASM: %v", err)
	}
	withFake(t, srv)

	_, _, err := run(t, "wasm", "disasm", contractID)
	if err == nil {
		t.Fatal("Execute() error = nil, want a parse failure")
	}
	if !strings.Contains(err.Error(), "not a WASM module") {
		t.Errorf("error = %q, want it to explain the module is invalid", err)
	}
}

func TestWASMDumpCommandWritesVerifiedBytecode(t *testing.T) {
	contractID := testContractID(t)
	wasm := disasmFixtureWasm()
	srv := fakerpc.NewServer(nil)
	if err := srv.SetContractWasm(contractID, wasm); err != nil {
		t.Fatalf("installing contract WASM: %v", err)
	}
	withFake(t, srv)

	output := filepath.Join(t.TempDir(), "contract.wasm")
	stdout, _, err := run(t, "wasm", "dump", contractID, "--output", output)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	written, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("reading dump: %v", err)
	}
	if !bytes.Equal(written, wasm) {
		t.Errorf("dumped %d bytes, want the %d-byte on-chain module", len(written), len(wasm))
	}

	hash := sha256.Sum256(wasm)
	if !strings.Contains(stdout, hex.EncodeToString(hash[:])) {
		t.Errorf("output does not report the verified hash\n%s", stdout)
	}
}

func TestWASMDumpCommandRejectsHashMismatch(t *testing.T) {
	contractID := testContractID(t)
	srv := fakerpc.NewServer(nil)
	entries, err := fakerpc.ContractWasmEntries(contractID, disasmFixtureWasm())
	if err != nil {
		t.Fatalf("building contract entries: %v", err)
	}

	// Keep the instance's declared hash but store different bytecode, which is
	// exactly the corruption the SHA-256 check exists to catch.
	for key, result := range entries {
		var entry xdr.LedgerEntry
		if err := xdr.SafeUnmarshalBase64(result.DataXDR, &entry); err != nil {
			t.Fatalf("decoding ledger entry: %v", err)
		}
		if entry.Data.Type != xdr.LedgerEntryTypeContractCode {
			continue
		}
		entry.Data.ContractCode.Code = []byte("tampered bytecode")
		data, err := xdr.MarshalBase64(entry)
		if err != nil {
			t.Fatalf("encoding ledger entry: %v", err)
		}
		entries[key] = protocol.LedgerEntryResult{KeyXDR: result.KeyXDR, DataXDR: data}
	}
	srv.SetLedgerEntries(entries)
	withFake(t, srv)

	output := filepath.Join(t.TempDir(), "contract.wasm")
	_, _, err = run(t, "wasm", "dump", contractID, "--output", output)
	if err == nil {
		t.Fatal("Execute() error = nil, want a hash mismatch failure")
	}
	if !strings.Contains(err.Error(), "hash mismatch") {
		t.Errorf("error = %q, want it to report a hash mismatch", err)
	}
	if _, statErr := os.Stat(output); !os.IsNotExist(statErr) {
		t.Errorf("mismatched bytecode was written to %s", output)
	}
}

func TestWASMDumpCommandRequiresOutput(t *testing.T) {
	contractID := testContractID(t)
	srv := fakerpc.NewServer(nil)
	if err := srv.SetContractWasm(contractID, disasmFixtureWasm()); err != nil {
		t.Fatalf("installing contract WASM: %v", err)
	}
	withFake(t, srv)

	_, _, err := run(t, "wasm", "dump", contractID)
	if err == nil {
		t.Fatal("Execute() error = nil, want a missing --output failure")
	}
	if !strings.Contains(err.Error(), "--output") {
		t.Errorf("error = %q, want it to name the missing flag", err)
	}
}

func TestParseWASMModuleRejectsMalformedModules(t *testing.T) {
	header := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}

	tests := []struct {
		name string
		code []byte
	}{
		{name: "empty", code: nil},
		{name: "bad magic", code: []byte("not wasm at all")},
		{name: "unknown section", code: append(append([]byte{}, header...), 0x63, 0x00)},
		{name: "truncated section", code: append(append([]byte{}, header...), wasmSectionType, 0x05, 0x01)},
		{
			name: "malformed type form",
			code: append(append([]byte{}, header...), wasmSectionType, 0x02, 0x01, 0x61),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := parseWASMModule(tc.code); err == nil {
				t.Fatal("parseWASMModule() error = nil, want a failure")
			}
		})
	}
}
