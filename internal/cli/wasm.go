package cli

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"unicode/utf8"

	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/xdr"

	prismrpc "github.com/stellar-optics/stellar-prism/pkg/rpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func newWASMCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var (
		sf                streamFlags
		includeFailed     bool
		networkPassphrase string
	)

	cmd := &cobra.Command{
		Use:     "wasm",
		Aliases: []string{"deployments"},
		Short:   "Stream WASM uploads and contract deployments",
		Long: `Stream WASM uploads and contract creations by decoding transaction
envelopes.

Soroban emits no system event for deployment, so these records come from the
InvokeHostFunction operation itself. Contract IDs are derived from the network
passphrase; use --network-passphrase when streaming a network other than
testnet.

Failed transactions are skipped unless --include-failed is set.

With --state-file the position reached is recorded after every page, and a
background pruner keeps that file from growing without bound. Add --resume to
start the next run where the last one stopped.

To inspect one deployed contract instead of streaming, use the subcommands:
"dump" saves its verified bytecode and "disasm" summarises the module.`,
		Example: `  # Follow deployments and uploads on testnet
  prism wasm --follow

  # Mainnet, including attempts that failed
  prism wasm --follow --include-failed \
    --network-passphrase "Public Global Stellar Network ; September 2015"

  # Just the JSON stream
  prism wasm --follow --json | jq -r 'select(.action=="create") | .contract'

  # Save a deployed contract's verified bytecode
  prism wasm dump CBGSB... --output contract.wasm

  # Summarise its sections, imports, exports and memory limits
  prism wasm disasm CBGSB...`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := sf.validate(cmd); err != nil {
				return err
			}

			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()

			sf.stateKey = stateKey("wasm", "failed="+strconv.FormatBool(includeFailed))

			newSource := func(startLedger uint32) (stream.Source, error) {
				return stream.NewWASMSource(client, stream.WASMQuery{
					StartLedger:       startLedger,
					PageLimit:         sf.pageSize,
					NetworkPassphrase: networkPassphrase,
					IncludeFailed:     includeFailed,
				})
			}
			return runStream(cmd, client, newSource, sf.from, &sf, g, stdout, stderr)
		},
	}

	cmd.Flags().BoolVar(&includeFailed, "include-failed", false,
		"include uploads and creations from failed transactions")
	cmd.Flags().StringVar(&networkPassphrase, "network-passphrase",
		stream.DefaultNetworkPassphrase,
		"network passphrase used to derive contract IDs")
	sf.register(cmd, stream.DefaultTransactionPageLimit)

	cmd.AddCommand(
		newWASMDumpCmd(g, stdout, stderr),
		newWASMDisasmCmd(g, stdout, stderr),
	)
	return cmd
}

// newWASMDumpCmd builds `prism wasm dump <contract-id>`.
func newWASMDumpCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var output string

	cmd := &cobra.Command{
		Use:   "dump <contract-id>",
		Short: "Download a contract's verified WASM bytecode",
		Long: `Fetch the WASM module the contract instance points at and save it to a
file, so it can be audited, decompiled or re-hashed offline.

The lookup is two RPC calls: the contract instance names the WASM hash, and the
code entry carries the bytecode. The SHA-256 of the bytecode must equal that
hash; on any mismatch nothing is written and the command fails.`,
		Example: `  prism wasm dump CBGSB... --output contract.wasm
  prism wasm dump CBGSB... -o contract.wasm`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if output == "" {
				return fmt.Errorf("--output is required")
			}

			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()

			contractID := args[0]
			hash, code, err := contractWASM(cmd.Context(), client, contractID)
			if err != nil {
				return err
			}

			sum := sha256.Sum256(code)
			if xdr.Hash(sum) != hash {
				return fmt.Errorf(
					"contract %s: WASM hash mismatch: instance declares %s but bytecode hashes to %s; refusing to write",
					contractID, hex.EncodeToString(hash[:]), hex.EncodeToString(sum[:]))
			}

			if err := os.WriteFile(output, code, 0o644); err != nil {
				return fmt.Errorf("writing %s: %w", output, err)
			}
			if _, err := fmt.Fprintf(stdout, "wrote %d bytes of verified WASM to %s\nsha256: %s\n",
				len(code), output, hex.EncodeToString(hash[:])); err != nil {
				return fmt.Errorf("writing confirmation: %w", err)
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", "",
		"file to write the verified bytecode to (required)")
	return cmd
}

// newWASMDisasmCmd builds `prism wasm disasm <contract-id>`.
func newWASMDisasmCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	return &cobra.Command{
		Use:   "disasm <contract-id>",
		Short: "Summarise a deployed contract's WASM module",
		Long: `Fetch a contract's bytecode and print its structure: the section table,
function types, host imports, exports, tables and memory page limits.

This is a structural disassembly, not a decompiler: it reports what the module
declares rather than instruction-level code, which is enough to see what a
contract imports from the host environment and how much memory it asks for.`,
		Example: `  prism wasm disasm CBGSB...`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()

			contractID := args[0]
			hash, code, err := contractWASM(cmd.Context(), client, contractID)
			if err != nil {
				return err
			}

			module, err := parseWASMModule(code)
			if err != nil {
				return fmt.Errorf("contract %s: %w", contractID, err)
			}
			return module.write(stdout, contractID, hash, len(code))
		},
	}
}

// contractWASM resolves a contract's instance entry to its code entry and
// returns the declared hash along with the raw bytecode.
//
// It is the shared read path for the ABI resolver, dump and disasm, so the
// two-call lookup and its error messages stay in one place.
func contractWASM(ctx context.Context, client prismrpc.Client, contractID string) (xdr.Hash, []byte, error) {
	entries, ok := client.(prismrpc.LedgerEntriesClient)
	if !ok {
		return xdr.Hash{}, nil, fmt.Errorf("RPC client does not support getLedgerEntries, which is required to fetch contract WASM")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	instanceKey, err := contractInstanceKey(contractID)
	if err != nil {
		return xdr.Hash{}, nil, err
	}
	instanceEntry, err := fetchLedgerEntry(ctx, entries, instanceKey, contractID)
	if err != nil {
		return xdr.Hash{}, nil, err
	}

	hash, err := wasmHashFromInstance(instanceEntry)
	if err != nil {
		return xdr.Hash{}, nil, err
	}

	codeKey := xdr.LedgerKey{
		Type:         xdr.LedgerEntryTypeContractCode,
		ContractCode: &xdr.LedgerKeyContractCode{Hash: hash},
	}
	codeEntry, err := fetchLedgerEntry(ctx, entries, codeKey, contractID)
	if err != nil {
		return xdr.Hash{}, nil, err
	}
	if codeEntry.Data.Type != xdr.LedgerEntryTypeContractCode || codeEntry.Data.ContractCode == nil {
		return xdr.Hash{}, nil, fmt.Errorf("contract %s: WASM code entry has unexpected type %s", contractID, codeEntry.Data.Type)
	}
	return hash, codeEntry.Data.ContractCode.Code, nil
}

// wasmPageSize is the fixed WebAssembly memory page size.
const wasmPageSize = 64 * 1024

// WASM section identifiers from the core specification.
const (
	wasmSectionCustom    = 0
	wasmSectionType      = 1
	wasmSectionImport    = 2
	wasmSectionFunction  = 3
	wasmSectionTable     = 4
	wasmSectionMemory    = 5
	wasmSectionGlobal    = 6
	wasmSectionExport    = 7
	wasmSectionStart     = 8
	wasmSectionElement   = 9
	wasmSectionCode      = 10
	wasmSectionData      = 11
	wasmSectionDataCount = 12
	wasmSectionTag       = 13
)

var wasmSectionNames = map[byte]string{
	wasmSectionCustom:    "Custom",
	wasmSectionType:      "Type",
	wasmSectionImport:    "Import",
	wasmSectionFunction:  "Function",
	wasmSectionTable:     "Table",
	wasmSectionMemory:    "Memory",
	wasmSectionGlobal:    "Global",
	wasmSectionExport:    "Export",
	wasmSectionStart:     "Start",
	wasmSectionElement:   "Element",
	wasmSectionCode:      "Code",
	wasmSectionData:      "Data",
	wasmSectionDataCount: "DataCount",
	wasmSectionTag:       "Tag",
}

// wasmModule is the structural view of a module that disasm prints. Fields
// are only populated for sections the renderer uses.
type wasmModule struct {
	Version   uint32
	Sections  []wasmSection
	Types     []wasmFuncType
	Imports   []wasmImport
	Functions []uint32
	Tables    []wasmLimits
	Memories  []wasmLimits
	Exports   []wasmExport

	// importedFuncs is the number of function imports, needed to translate an
	// export's function index into the local function table.
	importedFuncs int
}

// wasmSection is one entry of the module's section table.
type wasmSection struct {
	ID     byte
	Name   string
	Detail string
	Size   int
}

// wasmFuncType is a function signature declared in the Type section.
type wasmFuncType struct {
	Params  []string
	Results []string
}

func (t wasmFuncType) signature() string {
	return "(" + strings.Join(t.Params, ", ") + ") -> " + wasmResultString(t.Results)
}

func wasmResultString(results []string) string {
	switch len(results) {
	case 0:
		return "()"
	case 1:
		return results[0]
	default:
		return "(" + strings.Join(results, ", ") + ")"
	}
}

// wasmImport is one entry of the Import section.
type wasmImport struct {
	Module    string
	Field     string
	Kind      string
	TypeIndex uint32
	Limits    *wasmLimits
}

// wasmExport is one entry of the Export section.
type wasmExport struct {
	Name  string
	Kind  string
	Index uint32
}

// wasmLimits is a table or memory size declaration, in pages.
type wasmLimits struct {
	Min    uint64
	Max    *uint64
	Shared bool
}

// String renders memory limits in pages, with the equivalent byte size.
func (l wasmLimits) String() string {
	return l.describe(func(n uint64) string { return formatWASMPages(n) })
}

// elements renders table limits, which count entries rather than pages.
func (l wasmLimits) elements() string {
	return l.describe(func(n uint64) string {
		unit := "elements"
		if n == 1 {
			unit = "element"
		}
		return fmt.Sprintf("%d %s", n, unit)
	})
}

func (l wasmLimits) describe(format func(uint64) string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "min %s", format(l.Min))
	if l.Max == nil {
		b.WriteString(", no maximum")
	} else {
		fmt.Fprintf(&b, ", max %s", format(*l.Max))
	}
	if l.Shared {
		b.WriteString(" (shared)")
	}
	return b.String()
}

func formatWASMPages(pages uint64) string {
	unit := "pages"
	if pages == 1 {
		unit = "page"
	}
	return fmt.Sprintf("%d %s (%s)", pages, unit, formatWASMBytes(pages*wasmPageSize))
}

func formatWASMBytes(n uint64) string {
	const (
		kib = 1024
		mib = 1024 * kib
		gib = 1024 * mib
	)
	switch {
	case n == 0:
		return "0 bytes"
	case n%gib == 0:
		return fmt.Sprintf("%d GiB", n/gib)
	case n%mib == 0:
		return fmt.Sprintf("%d MiB", n/mib)
	case n%kib == 0:
		return fmt.Sprintf("%d KiB", n/kib)
	default:
		return fmt.Sprintf("%d bytes", n)
	}
}

// parseWASMModule validates the module header and parses the sections that
// disasm reports.
func parseWASMModule(code []byte) (*wasmModule, error) {
	if len(code) < 8 {
		return nil, fmt.Errorf("not a WASM module: %d bytes is shorter than the 8-byte header", len(code))
	}
	if string(code[:4]) != "\x00asm" {
		return nil, fmt.Errorf("not a WASM module: bad magic")
	}

	module := &wasmModule{Version: binary.LittleEndian.Uint32(code[4:8])}
	p := &wasmParser{data: code, off: 8}

	for !p.eof() {
		id, err := p.u8()
		if err != nil {
			return nil, fmt.Errorf("reading section id: %w", err)
		}
		name, ok := wasmSectionNames[id]
		if !ok {
			return nil, fmt.Errorf("unknown WASM section id %d", id)
		}
		size, err := p.u32()
		if err != nil {
			return nil, fmt.Errorf("%s section: reading length: %w", name, err)
		}
		payload, err := p.take(int(size))
		if err != nil {
			return nil, fmt.Errorf("%s section: length %d exceeds the remaining %d bytes", name, size, p.remaining())
		}

		section := wasmSection{ID: id, Name: name, Size: len(payload)}
		switch id {
		case wasmSectionCustom:
			custom, err := parseWASMCustomSection(payload)
			if err != nil {
				return nil, fmt.Errorf("custom section: %w", err)
			}
			section.Detail = custom
		case wasmSectionType:
			if err := module.parseTypes(payload); err != nil {
				return nil, fmt.Errorf("type section: %w", err)
			}
			section.Detail = wasmCountLabel(len(module.Types), "type", "types")
		case wasmSectionImport:
			if err := module.parseImports(payload); err != nil {
				return nil, fmt.Errorf("import section: %w", err)
			}
			section.Detail = wasmCountLabel(len(module.Imports), "import", "imports")
		case wasmSectionFunction:
			if err := module.parseFunctions(payload); err != nil {
				return nil, fmt.Errorf("function section: %w", err)
			}
			section.Detail = wasmCountLabel(len(module.Functions), "function", "functions")
		case wasmSectionTable:
			if err := module.parseTables(payload); err != nil {
				return nil, fmt.Errorf("table section: %w", err)
			}
			section.Detail = wasmCountLabel(len(module.Tables), "table", "tables")
		case wasmSectionMemory:
			if err := module.parseMemories(payload); err != nil {
				return nil, fmt.Errorf("memory section: %w", err)
			}
			section.Detail = wasmCountLabel(len(module.Memories), "memory", "memories")
		case wasmSectionExport:
			if err := module.parseExports(payload); err != nil {
				return nil, fmt.Errorf("export section: %w", err)
			}
			section.Detail = wasmCountLabel(len(module.Exports), "export", "exports")
		case wasmSectionGlobal, wasmSectionElement, wasmSectionCode, wasmSectionData, wasmSectionTag:
			count, err := parseWASMCount(payload)
			if err != nil {
				return nil, fmt.Errorf("%s section: %w", name, err)
			}
			section.Detail = wasmCountLabel(int(count), "entry", "entries")
		}
		module.Sections = append(module.Sections, section)
	}
	return module, nil
}

// wasmCountLabel renders a count with its singular or plural noun.
func wasmCountLabel(count int, singular, plural string) string {
	if count == 1 {
		return fmt.Sprintf("%d %s", count, singular)
	}
	return fmt.Sprintf("%d %s", count, plural)
}

func parseWASMCustomSection(payload []byte) (string, error) {
	p := &wasmParser{data: payload}
	name, err := p.name()
	if err != nil {
		return "", err
	}
	return name, nil
}

func parseWASMCount(payload []byte) (uint32, error) {
	p := &wasmParser{data: payload}
	return p.u32()
}

func (m *wasmModule) parseTypes(payload []byte) error {
	p := &wasmParser{data: payload}
	count, err := p.u32()
	if err != nil {
		return err
	}
	if err := p.reserve(int(count)); err != nil {
		return err
	}
	for i := uint32(0); i < count; i++ {
		form, err := p.u8()
		if err != nil {
			return fmt.Errorf("type %d: %w", i, err)
		}
		if form != 0x60 {
			return fmt.Errorf("type %d: unsupported form 0x%02x", i, form)
		}
		params, err := p.valueTypes()
		if err != nil {
			return fmt.Errorf("type %d parameters: %w", i, err)
		}
		results, err := p.valueTypes()
		if err != nil {
			return fmt.Errorf("type %d results: %w", i, err)
		}
		m.Types = append(m.Types, wasmFuncType{Params: params, Results: results})
	}
	return p.finish()
}

func (m *wasmModule) parseImports(payload []byte) error {
	p := &wasmParser{data: payload}
	count, err := p.u32()
	if err != nil {
		return err
	}
	if err := p.reserve(int(count)); err != nil {
		return err
	}
	for i := uint32(0); i < count; i++ {
		module, err := p.name()
		if err != nil {
			return fmt.Errorf("import %d module: %w", i, err)
		}
		field, err := p.name()
		if err != nil {
			return fmt.Errorf("import %d name: %w", i, err)
		}
		kind, err := p.u8()
		if err != nil {
			return fmt.Errorf("import %d kind: %w", i, err)
		}

		imp := wasmImport{Module: module, Field: field}
		switch kind {
		case 0x00:
			imp.Kind = "func"
			typeIndex, err := p.u32()
			if err != nil {
				return fmt.Errorf("import %d type index: %w", i, err)
			}
			imp.TypeIndex = typeIndex
			m.importedFuncs++
		case 0x01:
			imp.Kind = "table"
			if _, err := p.u8(); err != nil { // reference type
				return fmt.Errorf("import %d table type: %w", i, err)
			}
			limits, err := p.limits()
			if err != nil {
				return fmt.Errorf("import %d table limits: %w", i, err)
			}
			imp.Limits = &limits
		case 0x02:
			imp.Kind = "memory"
			limits, err := p.limits()
			if err != nil {
				return fmt.Errorf("import %d memory limits: %w", i, err)
			}
			imp.Limits = &limits
		case 0x03:
			imp.Kind = "global"
			if _, err := p.u8(); err != nil { // value type
				return fmt.Errorf("import %d global type: %w", i, err)
			}
			if _, err := p.u8(); err != nil { // mutability
				return fmt.Errorf("import %d global mutability: %w", i, err)
			}
		case 0x04:
			imp.Kind = "tag"
			if _, err := p.u8(); err != nil { // attribute
				return fmt.Errorf("import %d tag attribute: %w", i, err)
			}
			typeIndex, err := p.u32()
			if err != nil {
				return fmt.Errorf("import %d tag type index: %w", i, err)
			}
			imp.TypeIndex = typeIndex
		default:
			return fmt.Errorf("import %d: unknown kind 0x%02x", i, kind)
		}
		m.Imports = append(m.Imports, imp)
	}
	return p.finish()
}

func (m *wasmModule) parseFunctions(payload []byte) error {
	p := &wasmParser{data: payload}
	count, err := p.u32()
	if err != nil {
		return err
	}
	if err := p.reserve(int(count)); err != nil {
		return err
	}
	for i := uint32(0); i < count; i++ {
		typeIndex, err := p.u32()
		if err != nil {
			return fmt.Errorf("function %d: %w", i, err)
		}
		m.Functions = append(m.Functions, typeIndex)
	}
	return p.finish()
}

func (m *wasmModule) parseTables(payload []byte) error {
	p := &wasmParser{data: payload}
	count, err := p.u32()
	if err != nil {
		return err
	}
	if err := p.reserve(int(count)); err != nil {
		return err
	}
	for i := uint32(0); i < count; i++ {
		if _, err := p.u8(); err != nil { // reference type
			return fmt.Errorf("table %d: %w", i, err)
		}
		limits, err := p.limits()
		if err != nil {
			return fmt.Errorf("table %d limits: %w", i, err)
		}
		m.Tables = append(m.Tables, limits)
	}
	return p.finish()
}

func (m *wasmModule) parseMemories(payload []byte) error {
	p := &wasmParser{data: payload}
	count, err := p.u32()
	if err != nil {
		return err
	}
	if err := p.reserve(int(count)); err != nil {
		return err
	}
	for i := uint32(0); i < count; i++ {
		limits, err := p.limits()
		if err != nil {
			return fmt.Errorf("memory %d limits: %w", i, err)
		}
		m.Memories = append(m.Memories, limits)
	}
	return p.finish()
}

func (m *wasmModule) parseExports(payload []byte) error {
	p := &wasmParser{data: payload}
	count, err := p.u32()
	if err != nil {
		return err
	}
	if err := p.reserve(int(count)); err != nil {
		return err
	}
	for i := uint32(0); i < count; i++ {
		name, err := p.name()
		if err != nil {
			return fmt.Errorf("export %d name: %w", i, err)
		}
		kind, err := p.u8()
		if err != nil {
			return fmt.Errorf("export %d kind: %w", i, err)
		}
		index, err := p.u32()
		if err != nil {
			return fmt.Errorf("export %d index: %w", i, err)
		}
		export := wasmExport{Name: name, Index: index}
		switch kind {
		case 0x00:
			export.Kind = "func"
		case 0x01:
			export.Kind = "table"
		case 0x02:
			export.Kind = "memory"
		case 0x03:
			export.Kind = "global"
		case 0x04:
			export.Kind = "tag"
		default:
			return fmt.Errorf("export %d: unknown kind 0x%02x", i, kind)
		}
		m.Exports = append(m.Exports, export)
	}
	return p.finish()
}

// signatureFor renders the type of the function at the given module-wide
// function index, accounting for imported functions coming first.
func (m *wasmModule) signatureFor(index uint32) string {
	var typeIndex uint32
	var found bool

	if int(index) < m.importedFuncs {
		seen := 0
		for _, imp := range m.Imports {
			if imp.Kind != "func" {
				continue
			}
			if seen == int(index) {
				typeIndex, found = imp.TypeIndex, true
				break
			}
			seen++
		}
	} else {
		local := int(index) - m.importedFuncs
		if local < len(m.Functions) {
			typeIndex, found = m.Functions[local], true
		}
	}
	if !found || int(typeIndex) >= len(m.Types) {
		return ""
	}
	return fmt.Sprintf("type[%d]: %s", typeIndex, m.Types[typeIndex].signature())
}

// exportDetail describes what an export points at: a signature for functions,
// page limits for memories and tables.
func (m *wasmModule) exportDetail(export wasmExport) string {
	switch export.Kind {
	case "func":
		return m.signatureFor(export.Index)
	case "memory":
		if int(export.Index) < len(m.Memories) {
			return m.Memories[export.Index].String()
		}
	case "table":
		if int(export.Index) < len(m.Tables) {
			return m.Tables[export.Index].elements()
		}
	}
	return ""
}

// write renders the module summary.
func (m *wasmModule) write(w io.Writer, contractID string, hash xdr.Hash, size int) error {
	header := []string{
		fmt.Sprintf("contract: %s", contractID),
		fmt.Sprintf("wasm hash: %s", hex.EncodeToString(hash[:])),
		fmt.Sprintf("size: %d bytes", size),
		fmt.Sprintf("version: %d", m.Version),
	}
	for _, line := range header {
		if _, err := fmt.Fprintln(w, line); err != nil {
			return fmt.Errorf("writing header: %w", err)
		}
	}

	if err := writeWASMTable(w, fmt.Sprintf("\nsections (%d):", len(m.Sections)),
		[]string{"id", "section", "contents", "size"},
		func() [][]string {
			rows := make([][]string, 0, len(m.Sections))
			for _, section := range m.Sections {
				rows = append(rows, []string{
					strconv.Itoa(int(section.ID)),
					section.Name,
					section.Detail,
					fmt.Sprintf("%d bytes", section.Size),
				})
			}
			return rows
		}()); err != nil {
		return err
	}

	if len(m.Types) > 0 {
		if err := writeWASMTable(w, fmt.Sprintf("\ntypes (%d):", len(m.Types)),
			[]string{"index", "signature"},
			func() [][]string {
				rows := make([][]string, 0, len(m.Types))
				for i, typ := range m.Types {
					rows = append(rows, []string{fmt.Sprintf("type[%d]", i), typ.signature()})
				}
				return rows
			}()); err != nil {
			return err
		}
	}

	if len(m.Imports) > 0 {
		if err := writeWASMTable(w, fmt.Sprintf("\nimports (%d):", len(m.Imports)),
			[]string{"module", "field", "kind", "detail"},
			func() [][]string {
				rows := make([][]string, 0, len(m.Imports))
				for _, imp := range m.Imports {
					detail := ""
					switch imp.Kind {
					case "func":
						if int(imp.TypeIndex) < len(m.Types) {
							detail = fmt.Sprintf("type[%d]: %s",
								imp.TypeIndex, m.Types[imp.TypeIndex].signature())
						}
					case "table", "memory":
						if imp.Limits != nil {
							detail = imp.Limits.String()
						}
					}
					rows = append(rows, []string{imp.Module, imp.Field, imp.Kind, detail})
				}
				return rows
			}()); err != nil {
			return err
		}
	}

	if len(m.Exports) > 0 {
		if err := writeWASMTable(w, fmt.Sprintf("\nexports (%d):", len(m.Exports)),
			[]string{"name", "kind", "detail"},
			func() [][]string {
				rows := make([][]string, 0, len(m.Exports))
				for _, export := range m.Exports {
					rows = append(rows, []string{export.Name, export.Kind, m.exportDetail(export)})
				}
				return rows
			}()); err != nil {
			return err
		}
	}

	if len(m.Memories) > 0 {
		if err := writeWASMTable(w, fmt.Sprintf("\nmemory (%d):", len(m.Memories)),
			[]string{"memory", "pages"},
			func() [][]string {
				rows := make([][]string, 0, len(m.Memories))
				for i, limits := range m.Memories {
					rows = append(rows, []string{fmt.Sprintf("memory[%d]", i), limits.String()})
				}
				return rows
			}()); err != nil {
			return err
		}
	}

	if len(m.Tables) > 0 {
		if err := writeWASMTable(w, fmt.Sprintf("\ntables (%d):", len(m.Tables)),
			[]string{"table", "entries"},
			func() [][]string {
				rows := make([][]string, 0, len(m.Tables))
				for i, limits := range m.Tables {
					rows = append(rows, []string{fmt.Sprintf("table[%d]", i), limits.elements()})
				}
				return rows
			}()); err != nil {
			return err
		}
	}

	return nil
}

// writeWASMTable prints a heading followed by one aligned table.
func writeWASMTable(w io.Writer, heading string, columns []string, rows [][]string) error {
	if _, err := fmt.Fprintln(w, heading); err != nil {
		return fmt.Errorf("writing %q heading: %w", heading, err)
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "  "+strings.Join(columns, "\t")); err != nil {
		return fmt.Errorf("writing table header: %w", err)
	}
	for _, row := range rows {
		if _, err := fmt.Fprintln(tw, "  "+strings.Join(row, "\t")); err != nil {
			return fmt.Errorf("writing table row: %w", err)
		}
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flushing table: %w", err)
	}
	return nil
}

// wasmParser is a bounds-checked cursor over a byte slice.
type wasmParser struct {
	data []byte
	off  int
}

func (p *wasmParser) eof() bool { return p.off >= len(p.data) }

func (p *wasmParser) remaining() int { return len(p.data) - p.off }

func (p *wasmParser) u8() (byte, error) {
	if p.eof() {
		return 0, io.ErrUnexpectedEOF
	}
	value := p.data[p.off]
	p.off++
	return value, nil
}

func (p *wasmParser) u32() (uint32, error) {
	value, n := binary.Uvarint(p.data[p.off:])
	if n <= 0 {
		return 0, fmt.Errorf("malformed LEB128 integer")
	}
	if value > math.MaxUint32 {
		return 0, fmt.Errorf("LEB128 integer overflows uint32")
	}
	p.off += n
	return uint32(value), nil
}

func (p *wasmParser) take(n int) ([]byte, error) {
	if n < 0 || n > p.remaining() {
		return nil, io.ErrUnexpectedEOF
	}
	b := p.data[p.off : p.off+n]
	p.off += n
	return b, nil
}

func (p *wasmParser) name() (string, error) {
	length, err := p.u32()
	if err != nil {
		return "", err
	}
	b, err := p.take(int(length))
	if err != nil {
		return "", err
	}
	if !utf8.Valid(b) {
		return "", fmt.Errorf("name is not valid UTF-8")
	}
	return string(b), nil
}

// reserve rejects a declared item count that could not possibly fit in the
// remaining bytes, which keeps a corrupt module from driving a huge
// allocation before the first item fails.
func (p *wasmParser) reserve(count int) error {
	if count < 0 || count > p.remaining() {
		return fmt.Errorf("%d entries do not fit in the remaining %d bytes", count, p.remaining())
	}
	return nil
}

func (p *wasmParser) finish() error {
	if !p.eof() {
		return fmt.Errorf("%d trailing bytes", p.remaining())
	}
	return nil
}

func (p *wasmParser) valueTypes() ([]string, error) {
	count, err := p.u32()
	if err != nil {
		return nil, err
	}
	if err := p.reserve(int(count)); err != nil {
		return nil, err
	}
	types := make([]string, 0, count)
	for i := uint32(0); i < count; i++ {
		value, err := p.u8()
		if err != nil {
			return nil, err
		}
		types = append(types, wasmValueTypeName(value))
	}
	return types, nil
}

func (p *wasmParser) limits() (wasmLimits, error) {
	flag, err := p.u8()
	if err != nil {
		return wasmLimits{}, err
	}

	var limits wasmLimits
	switch flag {
	case 0x00:
	case 0x01:
	case 0x02:
		limits.Shared = true
	case 0x03:
		limits.Shared = true
	default:
		return wasmLimits{}, fmt.Errorf("unsupported limits flag 0x%02x", flag)
	}

	minimum, err := p.u32()
	if err != nil {
		return wasmLimits{}, err
	}
	limits.Min = uint64(minimum)

	if flag == 0x01 || flag == 0x03 {
		maximum, err := p.u32()
		if err != nil {
			return wasmLimits{}, err
		}
		value := uint64(maximum)
		limits.Max = &value
	}
	return limits, nil
}

func wasmValueTypeName(value byte) string {
	switch value {
	case 0x7F:
		return "i32"
	case 0x7E:
		return "i64"
	case 0x7D:
		return "f32"
	case 0x7C:
		return "f64"
	case 0x7B:
		return "v128"
	case 0x70:
		return "funcref"
	case 0x6F:
		return "externref"
	default:
		return fmt.Sprintf("valtype(0x%02x)", value)
	}
}
