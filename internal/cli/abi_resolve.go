package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-xdr-lens/pkg/lens"
	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"

	prismrpc "github.com/stellar-optics/stellar-prism/pkg/rpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// DefaultSpecCacheDir is the default directory where parsed contract specs are persisted.
const DefaultSpecCacheDir = "~/.cache/prism/specs"

// contractSpecSection is the WASM custom section that Soroban contracts embed
// their ScSpecEntry stream in.
const contractSpecSection = "contractspecv0"

// errNoContractSpec means the bytecode has no embedded spec. This is normal
// for contracts built without the Soroban spec macros.
var errNoContractSpec = fmt.Errorf("contract WASM has no %q custom section", contractSpecSection)

// contractSpec is the resolved, grouped ABI of one contract.
type contractSpec struct {
	ContractID string
	WasmHash   xdr.Hash
	Functions  []xdr.ScSpecFunctionV0
	Structs    []xdr.ScSpecUdtStructV0
	Unions     []xdr.ScSpecUdtUnionV0
	Enums      []xdr.ScSpecUdtEnumV0
	Errors     []xdr.ScSpecUdtErrorEnumV0
	Events     []xdr.ScSpecEventV0
}

// newABICmd builds `prism abi <contract-id>`.
func newABICmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var (
		function     string
		arguments    []string
		specCacheDir string
		specCacheTTL time.Duration
	)

	cmd := &cobra.Command{
		Use:   "abi <contract-id>",
		Short: "Resolve a contract's on-chain ABI spec",
		Long: `Fetch a contract's WASM bytecode through getLedgerEntries, parse the
Soroban spec embedded in its contractspecv0 custom section, and render
functions, types and events with human-readable argument names.

Specs are cached in memory and on disk under --spec-cache-dir with configurable TTL
to avoid redundant RPC requests when watching or re-resolving contracts.

With --function, only that function is shown. Repeating --arg decodes actual
base64 ScVal arguments against the function's parameter names and types, so
an event or invocation can be read without translating the wire shape by
hand.`,
		Example: `  # List everything the contract exposes
  prism abi CBGSB...

  # Show one function and decode its arguments
  prism abi CBGSB... --function transfer \
    --arg AAAAAwAAAAc= --arg AAAAAwAAAAo=`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if function == "" && len(arguments) > 0 {
				return fmt.Errorf("--arg requires --function")
			}
			if _, err := contractInstanceKey(args[0]); err != nil {
				return err
			}

			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()

			var cache *stream.ABICache
			if specCacheDir != "" {
				var err error
				cache, err = stream.NewABICache(stream.ABICacheOptions{
					Dir: specCacheDir,
					TTL: specCacheTTL,
				})
				if err != nil {
					// Fallback to in-memory cache if disk directory cannot be created
					cache, err = stream.NewABICache(stream.ABICacheOptions{
						TTL: specCacheTTL,
					})
					if err != nil {
						return fmt.Errorf("creating in-memory spec cache: %w", err)
					}
				}
			}

			spec, err := resolveContractSpecWithCache(cmd.Context(), client, args[0], cache)
			if err != nil {
				return err
			}

			color, err := parseColorMode(g.color)
			if err != nil {
				return err
			}
			return spec.write(stdout, function, arguments, color.palette(stdout))
		},
	}

	cmd.Flags().StringVar(&function, "function", "",
		"show only this function and decode any --arg values against it")
	cmd.Flags().StringArrayVar(&arguments, "arg", nil,
		"base64 ScVal argument to decode; repeatable and requires --function")
	cmd.Flags().StringVar(&specCacheDir, "spec-cache-dir", DefaultSpecCacheDir,
		"directory to cache parsed contract specs on disk (empty disables disk cache)")
	cmd.Flags().DurationVar(&specCacheTTL, "spec-cache-ttl", stream.DefaultABICacheTTL,
		"TTL for cached contract specs (default: 24h)")
	return cmd
}

// newABIInspectCmd builds `prism abi inspect <contract-id>`.
func newABIInspectCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var asJSON bool

	cmd := &cobra.Command{
		Use:   "inspect <contract-id>",
		Short: "Inspect a contract ABI as markdown or JSON",
		Long: `Resolve a contract's on-chain spec and print its complete interface.

The markdown view is deliberately plain text with no colour codes, so it can
be pasted into an issue or a document as-is. Function and argument docstrings
are shown next to parameter types and return types.

With --json the same information is emitted as a stable object for editors and
other tooling.`,
		Example: `  # Human-readable markdown
  prism abi inspect CBGSB...

  # Structured output for tooling
  prism abi inspect CBGSB... --json | jq '.functions[].name'`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			contractID := args[0]
			if _, err := contractInstanceKey(contractID); err != nil {
				return err
			}

			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()

			spec, err := resolveContractSpec(cmd.Context(), client, contractID)
			if err != nil {
				return err
			}
			if asJSON {
				return spec.writeInspectionJSON(stdout)
			}
			return spec.writeInspectionMarkdown(stdout)
		},
	}

	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the ABI inspection as JSON")
	return cmd
}

// abiInspectionJSON is the stable schema of `prism abi inspect --json`.
// Scalar fields are always present so consumers do not need to distinguish a
// missing field from an empty string or list; optional UDT and event sections
// are omitted when the spec has none.
type abiInspectionJSON struct {
	ContractID string            `json:"contractId"`
	WasmHash   string            `json:"wasmHash"`
	Functions  []abiFunctionJSON `json:"functions"`
	Structs    []abiStructJSON   `json:"structs,omitempty"`
	Unions     []abiUnionJSON    `json:"unions,omitempty"`
	Enums      []abiEnumJSON     `json:"enums,omitempty"`
	Errors     []abiErrorJSON    `json:"errors,omitempty"`
	Events     []abiEventJSON    `json:"events,omitempty"`
}

type abiFunctionJSON struct {
	Name      string            `json:"name"`
	Doc       string            `json:"doc"`
	Arguments []abiArgumentJSON `json:"arguments"`
	Returns   []string          `json:"returns"`
}

type abiArgumentJSON struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Doc  string `json:"doc"`
}

type abiStructJSON struct {
	Name   string               `json:"name"`
	Doc    string               `json:"doc"`
	Fields []abiStructFieldJSON `json:"fields"`
}

type abiStructFieldJSON struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Doc  string `json:"doc"`
}

type abiUnionJSON struct {
	Name  string             `json:"name"`
	Doc   string             `json:"doc"`
	Cases []abiUnionCaseJSON `json:"cases"`
}

type abiUnionCaseJSON struct {
	Name  string   `json:"name"`
	Types []string `json:"types"`
	Doc   string   `json:"doc"`
}

type abiEnumJSON struct {
	Name  string            `json:"name"`
	Doc   string            `json:"doc"`
	Cases []abiEnumCaseJSON `json:"cases"`
}

type abiEnumCaseJSON struct {
	Name  string `json:"name"`
	Value uint32 `json:"value"`
	Doc   string `json:"doc"`
}

type abiErrorJSON struct {
	Name  string             `json:"name"`
	Doc   string             `json:"doc"`
	Cases []abiErrorCaseJSON `json:"cases"`
}

type abiErrorCaseJSON struct {
	Name  string `json:"name"`
	Value uint32 `json:"value"`
	Doc   string `json:"doc"`
}

type abiEventJSON struct {
	Name   string              `json:"name"`
	Doc    string              `json:"doc"`
	Params []abiEventParamJSON `json:"params"`
}

type abiEventParamJSON struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Location string `json:"location"`
	Doc      string `json:"doc"`
}

// resolveContractSpec performs the two ledger-entry lookups and parses the
// embedded spec.
func resolveContractSpec(ctx context.Context, client prismrpc.Client, contractID string) (*contractSpec, error) {
	return resolveContractSpecWithCache(ctx, client, contractID, nil)
}

// resolveContractSpecWithCache resolves the contract spec, checking and updating
// cache if non-nil. When the contract wasm hash is cached, the second RPC call
// for contract bytecode is skipped.
func resolveContractSpecWithCache(ctx context.Context, client prismrpc.Client, contractID string, cache *stream.ABICache) (*contractSpec, error) {
	entries, ok := client.(prismrpc.LedgerEntriesClient)
	if !ok {
		return nil, fmt.Errorf("RPC client does not support getLedgerEntries, which is required to resolve a contract ABI")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	instanceKey, err := contractInstanceKey(contractID)
	if err != nil {
		return nil, err
	}
	instanceEntry, err := fetchLedgerEntry(ctx, entries, instanceKey, contractID)
	if err != nil {
		return nil, err
	}

	hash, err := wasmHashFromInstance(instanceEntry)
	if err != nil {
		return nil, err
	}

	if cache != nil {
		if cached, ok := cache.Get(contractID); ok {
			expectedHash := hex.EncodeToString(hash[:])
			if cached.WasmHash == expectedHash {
				// Cache hit! Skip the second RPC call for WASM code.
				spec := &contractSpec{ContractID: contractID, WasmHash: hash}
				if err := spec.add(cached.Entries()); err == nil {
					return spec, nil
				}
			} else {
				// Code hash changed on-chain, invalidate stale entry.
				cache.Invalidate(contractID)
			}
		}
	}

	codeKey := xdr.LedgerKey{
		Type:         xdr.LedgerEntryTypeContractCode,
		ContractCode: &xdr.LedgerKeyContractCode{Hash: hash},
	}
	codeEntry, err := fetchLedgerEntry(ctx, entries, codeKey, contractID)
	if err != nil {
		return nil, err
	}
	if codeEntry.Data.Type != xdr.LedgerEntryTypeContractCode || codeEntry.Data.ContractCode == nil {
		return nil, fmt.Errorf("contract %s: WASM code entry has unexpected type %s", contractID, codeEntry.Data.Type)
	}
	code := codeEntry.Data.ContractCode.Code

	specEntries, err := parseContractSpec(code)
	if err != nil {
		return nil, fmt.Errorf("contract %s: %w", contractID, err)
	}

	if cache != nil {
		cache.Put(contractID, hash, specEntries)
	}

	spec := &contractSpec{ContractID: contractID, WasmHash: hash}
	if err := spec.add(specEntries); err != nil {
		return nil, fmt.Errorf("contract %s: %w", contractID, err)
	}
	return spec, nil
}

// contractInstanceKey builds the ledger key for a contract's instance entry.
func contractInstanceKey(contractID string) (xdr.LedgerKey, error) {
	raw, err := strkey.Decode(strkey.VersionByteContract, contractID)
	if err != nil {
		return xdr.LedgerKey{}, fmt.Errorf("invalid contract ID %q: %w", contractID, err)
	}
	if len(raw) != len(xdr.Hash{}) {
		return xdr.LedgerKey{}, fmt.Errorf("contract ID %q decodes to %d bytes, want %d",
			contractID, len(raw), len(xdr.Hash{}))
	}

	var id xdr.ContractId
	copy(id[:], raw)

	return xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.LedgerKeyContractData{
			Contract: xdr.ScAddress{
				Type:       xdr.ScAddressTypeScAddressTypeContract,
				ContractId: &id,
			},
			Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
			Durability: xdr.ContractDataDurabilityPersistent,
		},
	}, nil
}

// fetchLedgerEntry makes one getLedgerEntries call and decodes the result.
func fetchLedgerEntry(ctx context.Context, client prismrpc.LedgerEntriesClient, key xdr.LedgerKey, contractID string) (xdr.LedgerEntry, error) {
	keyXDR, err := xdr.MarshalBase64(key)
	if err != nil {
		return xdr.LedgerEntry{}, fmt.Errorf("encoding ledger key: %w", err)
	}

	resp, err := client.GetLedgerEntries(ctx, protocol.GetLedgerEntriesRequest{Keys: []string{keyXDR}})
	if err != nil {
		return xdr.LedgerEntry{}, fmt.Errorf("fetching ledger entry for contract %s: %w", contractID, err)
	}

	for _, result := range resp.Entries {
		if result.DataXDR == "" {
			continue
		}
		var entry xdr.LedgerEntry
		if err := xdr.SafeUnmarshalBase64(result.DataXDR, &entry); err != nil {
			return xdr.LedgerEntry{}, fmt.Errorf("decoding ledger entry for contract %s: %w", contractID, err)
		}
		return entry, nil
	}
	return xdr.LedgerEntry{}, fmt.Errorf(
		"contract %s not found on this network; check the ID and that the contract has not been pruned", contractID)
}

// wasmHashFromInstance extracts the WASM hash from a contract instance entry.
func wasmHashFromInstance(entry xdr.LedgerEntry) (xdr.Hash, error) {
	if entry.Data.Type != xdr.LedgerEntryTypeContractData || entry.Data.ContractData == nil {
		return xdr.Hash{}, fmt.Errorf("contract instance entry has unexpected type %s", entry.Data.Type)
	}
	value := entry.Data.ContractData.Val
	if value.Type != xdr.ScValTypeScvContractInstance || value.Instance == nil {
		return xdr.Hash{}, fmt.Errorf("contract instance entry does not contain a contract instance")
	}

	executable := value.Instance.Executable
	switch executable.Type {
	case xdr.ContractExecutableTypeContractExecutableWasm:
		if executable.WasmHash == nil {
			return xdr.Hash{}, fmt.Errorf("contract instance has an empty WASM hash")
		}
		return *executable.WasmHash, nil
	case xdr.ContractExecutableTypeContractExecutableStellarAsset:
		return xdr.Hash{}, fmt.Errorf("contract is the Stellar asset contract and has no user-defined ABI")
	default:
		return xdr.Hash{}, fmt.Errorf("contract executable type %s has no embedded WASM spec", executable.Type)
	}
}

// parseContractSpec extracts and decodes the spec entries from WASM bytecode.
func parseContractSpec(wasm []byte) ([]xdr.ScSpecEntry, error) {
	section, err := wasmCustomSection(wasm, contractSpecSection)
	if err != nil {
		return nil, err
	}
	if len(section) == 0 {
		return nil, fmt.Errorf("%q section is empty", contractSpecSection)
	}

	decoder := xdr.NewBytesDecoder()
	var entries []xdr.ScSpecEntry
	for offset := 0; offset < len(section); {
		var entry xdr.ScSpecEntry
		n, err := decoder.DecodeBytes(&entry, section[offset:])
		if err != nil {
			return nil, fmt.Errorf("decoding spec entry %d at offset %d: %w", len(entries), offset, err)
		}
		if n <= 0 || offset+n > len(section) {
			return nil, fmt.Errorf("decoding spec entry %d at offset %d: invalid length %d", len(entries), offset, n)
		}
		offset += n
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("%q section contains no spec entries", contractSpecSection)
	}
	return entries, nil
}

// wasmCustomSection returns the payload of the named custom section.
func wasmCustomSection(wasm []byte, name string) ([]byte, error) {
	if len(wasm) < 8 {
		return nil, fmt.Errorf("WASM bytecode is too short: %d bytes", len(wasm))
	}
	if !bytes.Equal(wasm[:4], []byte{0x00, 0x61, 0x73, 0x6d}) {
		return nil, fmt.Errorf("not a WASM module: bad magic")
	}

	offset := 8
	for offset < len(wasm) {
		sectionID := wasm[offset]
		offset++

		size, n := binary.Uvarint(wasm[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("malformed WASM section %d length", sectionID)
		}
		offset += n
		if size > math.MaxUint32 || size > uint64(len(wasm)-offset) {
			return nil, fmt.Errorf("malformed WASM section %d: length %d exceeds remaining %d bytes",
				sectionID, size, len(wasm)-offset)
		}
		payload := wasm[offset : offset+int(size)] //nolint:gosec // bounded above
		offset += int(size)

		if sectionID != 0 {
			continue
		}
		nameLen, n := binary.Uvarint(payload)
		if n <= 0 || nameLen > uint64(len(payload)-n) {
			return nil, fmt.Errorf("malformed WASM custom section name")
		}
		sectionName := string(payload[n : n+int(nameLen)]) //nolint:gosec // bounded above
		if sectionName == name {
			return payload[n+int(nameLen):], nil
		}
	}
	return nil, errNoContractSpec
}

// add groups decoded entries by kind.
func (s *contractSpec) add(entries []xdr.ScSpecEntry) error {
	for i, entry := range entries {
		switch entry.Kind {
		case xdr.ScSpecEntryKindScSpecEntryFunctionV0:
			if entry.FunctionV0 == nil {
				return fmt.Errorf("spec entry %d: function payload is missing", i)
			}
			s.Functions = append(s.Functions, *entry.FunctionV0)
		case xdr.ScSpecEntryKindScSpecEntryUdtStructV0:
			if entry.UdtStructV0 == nil {
				return fmt.Errorf("spec entry %d: struct payload is missing", i)
			}
			s.Structs = append(s.Structs, *entry.UdtStructV0)
		case xdr.ScSpecEntryKindScSpecEntryUdtUnionV0:
			if entry.UdtUnionV0 == nil {
				return fmt.Errorf("spec entry %d: union payload is missing", i)
			}
			s.Unions = append(s.Unions, *entry.UdtUnionV0)
		case xdr.ScSpecEntryKindScSpecEntryUdtEnumV0:
			if entry.UdtEnumV0 == nil {
				return fmt.Errorf("spec entry %d: enum payload is missing", i)
			}
			s.Enums = append(s.Enums, *entry.UdtEnumV0)
		case xdr.ScSpecEntryKindScSpecEntryUdtErrorEnumV0:
			if entry.UdtErrorEnumV0 == nil {
				return fmt.Errorf("spec entry %d: error enum payload is missing", i)
			}
			s.Errors = append(s.Errors, *entry.UdtErrorEnumV0)
		case xdr.ScSpecEntryKindScSpecEntryEventV0:
			if entry.EventV0 == nil {
				return fmt.Errorf("spec entry %d: event payload is missing", i)
			}
			s.Events = append(s.Events, *entry.EventV0)
		default:
			return fmt.Errorf("spec entry %d: unknown kind %s", i, entry.Kind)
		}
	}
	return nil
}

// write renders the spec, optionally narrowing to one function and decoding
// supplied arguments.
func (s *contractSpec) write(w io.Writer, function string, arguments []string, palette lensformat.Palette) error {
	if _, err := fmt.Fprintf(w, "contract: %s\nwasm:     %s\n", s.ContractID, hex.EncodeToString(s.WasmHash[:])); err != nil {
		return fmt.Errorf("writing contract header: %w", err)
	}

	if function != "" {
		return s.writeFunction(w, function, arguments, palette)
	}
	return s.writeAll(w)
}

func (s *contractSpec) writeAll(w io.Writer) error {
	if err := writeSection(w, "functions", len(s.Functions), func() error {
		for _, fn := range s.Functions {
			if err := writeFunctionSignature(w, fn, "  "); err != nil {
				return err
			}
			if err := writeDoc(w, fn.Doc, "    "); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	if err := writeSection(w, "structs", len(s.Structs), func() error {
		for _, item := range s.Structs {
			if _, err := fmt.Fprintf(w, "  struct %s\n", item.Name); err != nil {
				return fmt.Errorf("writing struct %s: %w", item.Name, err)
			}
			if err := writeDoc(w, item.Doc, "    "); err != nil {
				return err
			}
			for _, field := range item.Fields {
				if _, err := fmt.Fprintf(w, "    %s: %s\n", field.Name, formatSpecType(field.Type)); err != nil {
					return fmt.Errorf("writing struct field %s: %w", field.Name, err)
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	if err := writeSection(w, "unions", len(s.Unions), func() error {
		for _, item := range s.Unions {
			if _, err := fmt.Fprintf(w, "  union %s\n", item.Name); err != nil {
				return fmt.Errorf("writing union %s: %w", item.Name, err)
			}
			if err := writeDoc(w, item.Doc, "    "); err != nil {
				return err
			}
			for _, unionCase := range item.Cases {
				if err := writeUnionCase(w, unionCase, "    "); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	if err := writeSection(w, "enums", len(s.Enums), func() error {
		for _, item := range s.Enums {
			if _, err := fmt.Fprintf(w, "  enum %s\n", item.Name); err != nil {
				return fmt.Errorf("writing enum %s: %w", item.Name, err)
			}
			if err := writeDoc(w, item.Doc, "    "); err != nil {
				return err
			}
			for _, enumCase := range item.Cases {
				if _, err := fmt.Fprintf(w, "    %s = %d\n", enumCase.Name, enumCase.Value); err != nil {
					return fmt.Errorf("writing enum case %s: %w", enumCase.Name, err)
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	if err := writeSection(w, "errors", len(s.Errors), func() error {
		for _, item := range s.Errors {
			if _, err := fmt.Fprintf(w, "  error %s\n", item.Name); err != nil {
				return fmt.Errorf("writing error enum %s: %w", item.Name, err)
			}
			if err := writeDoc(w, item.Doc, "    "); err != nil {
				return err
			}
			for _, errorCase := range item.Cases {
				if _, err := fmt.Fprintf(w, "    %s = %d\n", errorCase.Name, errorCase.Value); err != nil {
					return fmt.Errorf("writing error case %s: %w", errorCase.Name, err)
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	return writeSection(w, "events", len(s.Events), func() error {
		for _, event := range s.Events {
			if _, err := fmt.Fprintf(w, "  event %s\n", event.Name); err != nil {
				return fmt.Errorf("writing event %s: %w", event.Name, err)
			}
			if err := writeDoc(w, event.Doc, "    "); err != nil {
				return err
			}
			for _, param := range event.Params {
				if _, err := fmt.Fprintf(w, "    %s: %s%s\n",
					param.Name, formatSpecType(param.Type), eventParamLocation(param)); err != nil {
					return fmt.Errorf("writing event param %s: %w", param.Name, err)
				}
			}
		}
		return nil
	})
}

// writeInspectionMarkdown renders the spec as plain markdown tables. It never
// consults the colour palette: ANSI escapes would make the result invalid
// markdown, including when the caller passes --color always.
func (s *contractSpec) writeInspectionMarkdown(w io.Writer) error {
	if _, err := fmt.Fprintln(w, "# Contract ABI"); err != nil {
		return fmt.Errorf("writing ABI inspection title: %w", err)
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return fmt.Errorf("writing ABI inspection separator: %w", err)
	}

	if err := writeMarkdownTable(w, "Contract", []string{"Field", "Value"}, [][]string{
		{"Contract ID", markdownCode(s.ContractID)},
		{"WASM hash", markdownCode(hex.EncodeToString(s.WasmHash[:]))},
	}); err != nil {
		return err
	}

	functionRows := make([][]string, 0, len(s.Functions))
	argumentRows := make([][]string, 0)
	for _, fn := range s.Functions {
		functionRows = append(functionRows, []string{
			markdownCode(string(fn.Name)),
			markdownCode(functionSignature(fn)),
			markdownCode(functionReturns(fn)),
			fn.Doc,
		})
		for _, input := range fn.Inputs {
			argumentRows = append(argumentRows, []string{
				markdownCode(string(fn.Name)),
				markdownCode(input.Name),
				markdownCode(formatSpecType(input.Type)),
				input.Doc,
			})
		}
	}
	if err := writeMarkdownTable(w, "Functions",
		[]string{"Name", "Signature", "Returns", "Doc"}, functionRows); err != nil {
		return err
	}
	if err := writeMarkdownTable(w, "Arguments",
		[]string{"Function", "Name", "Type", "Doc"}, argumentRows); err != nil {
		return err
	}

	if err := s.writeInspectionTypesMarkdown(w); err != nil {
		return err
	}
	return s.writeInspectionEventsMarkdown(w)
}

func (s *contractSpec) writeInspectionTypesMarkdown(w io.Writer) error {
	if len(s.Structs) > 0 {
		rows := make([][]string, 0)
		for _, item := range s.Structs {
			if len(item.Fields) == 0 {
				rows = append(rows, []string{markdownCode(item.Name), item.Doc, "", "", ""})
				continue
			}
			for _, field := range item.Fields {
				rows = append(rows, []string{
					markdownCode(item.Name),
					item.Doc,
					markdownCode(field.Name),
					markdownCode(formatSpecType(field.Type)),
					field.Doc,
				})
			}
		}
		if err := writeMarkdownTable(w, "Structs",
			[]string{"Struct", "Struct Doc", "Field", "Type", "Field Doc"}, rows); err != nil {
			return err
		}
	}

	if len(s.Unions) > 0 {
		rows := make([][]string, 0)
		for _, item := range s.Unions {
			if len(item.Cases) == 0 {
				rows = append(rows, []string{markdownCode(item.Name), item.Doc, "", "", ""})
				continue
			}
			for _, unionCase := range item.Cases {
				detail, err := newSpecUnionCaseDetail(unionCase)
				if err != nil {
					return fmt.Errorf("describing union case: %w", err)
				}
				typeCell := ""
				if len(detail.Types) > 0 {
					typeCell = markdownCode("(" + strings.Join(detail.Types, ", ") + ")")
				}
				rows = append(rows, []string{
					markdownCode(item.Name),
					item.Doc,
					markdownCode(detail.Name),
					typeCell,
					detail.Doc,
				})
			}
		}
		if err := writeMarkdownTable(w, "Unions",
			[]string{"Union", "Union Doc", "Case", "Types", "Case Doc"}, rows); err != nil {
			return err
		}
	}

	if len(s.Enums) > 0 {
		rows := make([][]string, 0)
		for _, item := range s.Enums {
			if len(item.Cases) == 0 {
				rows = append(rows, []string{markdownCode(item.Name), item.Doc, "", "", ""})
				continue
			}
			for _, enumCase := range item.Cases {
				rows = append(rows, []string{
					markdownCode(item.Name),
					item.Doc,
					markdownCode(enumCase.Name),
					fmt.Sprintf("%d", enumCase.Value),
					enumCase.Doc,
				})
			}
		}
		if err := writeMarkdownTable(w, "Enums",
			[]string{"Enum", "Enum Doc", "Case", "Value", "Case Doc"}, rows); err != nil {
			return err
		}
	}

	return nil
}

func (s *contractSpec) writeInspectionEventsMarkdown(w io.Writer) error {
	if len(s.Errors) > 0 {
		rows := make([][]string, 0)
		for _, item := range s.Errors {
			if len(item.Cases) == 0 {
				rows = append(rows, []string{markdownCode(item.Name), item.Doc, "", "", ""})
				continue
			}
			for _, errorCase := range item.Cases {
				rows = append(rows, []string{
					markdownCode(item.Name),
					item.Doc,
					markdownCode(errorCase.Name),
					fmt.Sprintf("%d", errorCase.Value),
					errorCase.Doc,
				})
			}
		}
		if err := writeMarkdownTable(w, "Errors",
			[]string{"Error", "Error Doc", "Case", "Value", "Case Doc"}, rows); err != nil {
			return err
		}
	}

	if len(s.Events) == 0 {
		return nil
	}
	rows := make([][]string, 0)
	for _, event := range s.Events {
		if len(event.Params) == 0 {
			rows = append(rows, []string{markdownCode(string(event.Name)), event.Doc, "", "", "", ""})
			continue
		}
		for _, param := range event.Params {
			rows = append(rows, []string{
				markdownCode(string(event.Name)),
				event.Doc,
				markdownCode(param.Name),
				markdownCode(formatSpecType(param.Type)),
				eventParamLocationName(param),
				param.Doc,
			})
		}
	}
	return writeMarkdownTable(w, "Events",
		[]string{"Event", "Event Doc", "Param", "Type", "Location", "Param Doc"}, rows)
}

func writeMarkdownTable(w io.Writer, title string, headers []string, rows [][]string) error {
	if _, err := fmt.Fprintf(w, "## %s\n\n", title); err != nil {
		return fmt.Errorf("writing %s heading: %w", title, err)
	}
	if _, err := fmt.Fprintf(w, "| %s |\n", strings.Join(headers, " | ")); err != nil {
		return fmt.Errorf("writing %s table header: %w", title, err)
	}
	separators := make([]string, len(headers))
	for i := range separators {
		separators[i] = "---"
	}
	if _, err := fmt.Fprintf(w, "| %s |\n", strings.Join(separators, " | ")); err != nil {
		return fmt.Errorf("writing %s table separator: %w", title, err)
	}
	for _, row := range rows {
		if len(row) != len(headers) {
			return fmt.Errorf("writing %s row: got %d cells, want %d", title, len(row), len(headers))
		}
		cells := make([]string, len(row))
		for i, cell := range row {
			cells[i] = markdownCell(cell)
		}
		if _, err := fmt.Fprintf(w, "| %s |\n", strings.Join(cells, " | ")); err != nil {
			return fmt.Errorf("writing %s row: %w", title, err)
		}
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return fmt.Errorf("writing %s separator: %w", title, err)
	}
	return nil
}

// markdownCell keeps docstrings from breaking a GFM table. Newlines become
// line breaks and pipes are escaped, while ordinary text is left untouched.
func markdownCell(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "<br>")
	s = strings.ReplaceAll(s, "\n", "<br>")
	s = strings.ReplaceAll(s, "\r", "<br>")
	return strings.ReplaceAll(s, "|", `\|`)
}

func markdownCode(s string) string {
	return "`" + markdownCell(s) + "`"
}

// writeInspectionJSON renders the spec as a stable, pretty-printed object in
// the same style as `prism tx --json`.
func (s *contractSpec) writeInspectionJSON(w io.Writer) error {
	doc, err := s.inspectionJSON()
	if err != nil {
		return fmt.Errorf("building ABI inspection: %w", err)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(doc); err != nil {
		return fmt.Errorf("encoding ABI inspection: %w", err)
	}
	return nil
}

func (s *contractSpec) inspectionJSON() (abiInspectionJSON, error) {
	out := abiInspectionJSON{
		ContractID: s.ContractID,
		WasmHash:   hex.EncodeToString(s.WasmHash[:]),
		Functions:  make([]abiFunctionJSON, 0, len(s.Functions)),
	}
	for _, fn := range s.Functions {
		item := abiFunctionJSON{
			Name:      string(fn.Name),
			Doc:       fn.Doc,
			Arguments: make([]abiArgumentJSON, 0, len(fn.Inputs)),
			Returns:   make([]string, 0, len(fn.Outputs)),
		}
		for _, input := range fn.Inputs {
			item.Arguments = append(item.Arguments, abiArgumentJSON{
				Name: input.Name,
				Type: formatSpecType(input.Type),
				Doc:  input.Doc,
			})
		}
		for _, output := range fn.Outputs {
			item.Returns = append(item.Returns, formatSpecType(output))
		}
		out.Functions = append(out.Functions, item)
	}

	if len(s.Structs) > 0 {
		out.Structs = make([]abiStructJSON, 0, len(s.Structs))
		for _, item := range s.Structs {
			entry := abiStructJSON{
				Name:   item.Name,
				Doc:    item.Doc,
				Fields: make([]abiStructFieldJSON, 0, len(item.Fields)),
			}
			for _, field := range item.Fields {
				entry.Fields = append(entry.Fields, abiStructFieldJSON{
					Name: field.Name,
					Type: formatSpecType(field.Type),
					Doc:  field.Doc,
				})
			}
			out.Structs = append(out.Structs, entry)
		}
	}

	if len(s.Unions) > 0 {
		out.Unions = make([]abiUnionJSON, 0, len(s.Unions))
		for _, item := range s.Unions {
			entry := abiUnionJSON{
				Name:  item.Name,
				Doc:   item.Doc,
				Cases: make([]abiUnionCaseJSON, 0, len(item.Cases)),
			}
			for _, unionCase := range item.Cases {
				detail, err := newSpecUnionCaseDetail(unionCase)
				if err != nil {
					return abiInspectionJSON{}, fmt.Errorf("describing union case: %w", err)
				}
				entry.Cases = append(entry.Cases, abiUnionCaseJSON{
					Name:  detail.Name,
					Types: detail.Types,
					Doc:   detail.Doc,
				})
			}
			out.Unions = append(out.Unions, entry)
		}
	}

	if len(s.Enums) > 0 {
		out.Enums = make([]abiEnumJSON, 0, len(s.Enums))
		for _, item := range s.Enums {
			entry := abiEnumJSON{
				Name:  item.Name,
				Doc:   item.Doc,
				Cases: make([]abiEnumCaseJSON, 0, len(item.Cases)),
			}
			for _, enumCase := range item.Cases {
				entry.Cases = append(entry.Cases, abiEnumCaseJSON{
					Name:  enumCase.Name,
					Value: uint32(enumCase.Value),
					Doc:   enumCase.Doc,
				})
			}
			out.Enums = append(out.Enums, entry)
		}
	}

	if len(s.Errors) > 0 {
		out.Errors = make([]abiErrorJSON, 0, len(s.Errors))
		for _, item := range s.Errors {
			entry := abiErrorJSON{
				Name:  item.Name,
				Doc:   item.Doc,
				Cases: make([]abiErrorCaseJSON, 0, len(item.Cases)),
			}
			for _, errorCase := range item.Cases {
				entry.Cases = append(entry.Cases, abiErrorCaseJSON{
					Name:  errorCase.Name,
					Value: uint32(errorCase.Value),
					Doc:   errorCase.Doc,
				})
			}
			out.Errors = append(out.Errors, entry)
		}
	}

	if len(s.Events) > 0 {
		out.Events = make([]abiEventJSON, 0, len(s.Events))
		for _, event := range s.Events {
			entry := abiEventJSON{
				Name:   string(event.Name),
				Doc:    event.Doc,
				Params: make([]abiEventParamJSON, 0, len(event.Params)),
			}
			for _, param := range event.Params {
				entry.Params = append(entry.Params, abiEventParamJSON{
					Name:     param.Name,
					Type:     formatSpecType(param.Type),
					Location: eventParamLocationName(param),
					Doc:      param.Doc,
				})
			}
			out.Events = append(out.Events, entry)
		}
	}
	return out, nil
}

// writeSection prints a heading only when the section has content, so a
// contract without events does not show an empty list.
func writeSection(w io.Writer, name string, count int, body func() error) error {
	if count == 0 {
		return nil
	}
	if _, err := fmt.Fprintf(w, "\n%s (%d):\n", name, count); err != nil {
		return fmt.Errorf("writing %s heading: %w", name, err)
	}
	return body()
}

func (s *contractSpec) writeFunction(w io.Writer, name string, arguments []string, palette lensformat.Palette) error {
	var fn *xdr.ScSpecFunctionV0
	for i := range s.Functions {
		if string(s.Functions[i].Name) == name {
			fn = &s.Functions[i]
			break
		}
	}
	if fn == nil {
		names := make([]string, 0, len(s.Functions))
		for _, candidate := range s.Functions {
			names = append(names, string(candidate.Name))
		}
		return fmt.Errorf("function %q not found; available: %s", name, strings.Join(names, ", "))
	}
	if len(arguments) > len(fn.Inputs) {
		return fmt.Errorf("function %s takes %d argument(s), got %d", name, len(fn.Inputs), len(arguments))
	}

	if _, err := fmt.Fprintln(w); err != nil {
		return fmt.Errorf("writing separator: %w", err)
	}
	if err := writeFunctionSignature(w, *fn, "  "); err != nil {
		return err
	}
	if fn.Doc != "" {
		if _, err := fmt.Fprintf(w, "    %s\n", fn.Doc); err != nil {
			return fmt.Errorf("writing documentation: %w", err)
		}
	}

	if len(arguments) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w, "\narguments:"); err != nil {
		return fmt.Errorf("writing arguments heading: %w", err)
	}
	for i, raw := range arguments {
		input := fn.Inputs[i]
		value, err := lens.DecodeAs(raw, "ScVal")
		if err != nil {
			return fmt.Errorf("decoding argument %s: %w", input.Name, err)
		}
		if _, err := fmt.Fprintf(w, "  %s: %s =\n", input.Name, formatSpecType(input.Type)); err != nil {
			return fmt.Errorf("writing argument %s: %w", input.Name, err)
		}
		if err := writeIndentedValue(w, value, palette, "    "); err != nil {
			return fmt.Errorf("formatting argument %s: %w", input.Name, err)
		}
	}
	return nil
}

func writeFunctionSignature(w io.Writer, fn xdr.ScSpecFunctionV0, indent string) error {
	if _, err := fmt.Fprintln(w, indent+functionSignature(fn)); err != nil {
		return fmt.Errorf("writing function %s: %w", fn.Name, err)
	}
	return nil
}

// functionSignature renders a function declaration without any indentation.
func functionSignature(fn xdr.ScSpecFunctionV0) string {
	var b strings.Builder
	b.WriteString(string(fn.Name))
	b.WriteString("(")
	for i, input := range fn.Inputs {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(input.Name)
		b.WriteString(": ")
		b.WriteString(formatSpecType(input.Type))
	}
	b.WriteString(") -> ")
	b.WriteString(functionReturns(fn))
	return b.String()
}

// functionReturns renders the return side of a signature. Void is spelled ()
// to match the rest of prism's ABI output; JSON uses an empty array instead.
func functionReturns(fn xdr.ScSpecFunctionV0) string {
	switch len(fn.Outputs) {
	case 0:
		return "()"
	case 1:
		return formatSpecType(fn.Outputs[0])
	default:
		parts := make([]string, 0, len(fn.Outputs))
		for _, output := range fn.Outputs {
			parts = append(parts, formatSpecType(output))
		}
		return "(" + strings.Join(parts, ", ") + ")"
	}
}

type specUnionCaseDetail struct {
	Name  string
	Doc   string
	Types []string
}

func newSpecUnionCaseDetail(unionCase xdr.ScSpecUdtUnionCaseV0) (specUnionCaseDetail, error) {
	switch unionCase.Kind {
	case xdr.ScSpecUdtUnionCaseV0KindScSpecUdtUnionCaseVoidV0:
		if unionCase.VoidCase == nil {
			return specUnionCaseDetail{}, fmt.Errorf("union case is missing its void payload")
		}
		return specUnionCaseDetail{
			Name:  unionCase.VoidCase.Name,
			Doc:   unionCase.VoidCase.Doc,
			Types: make([]string, 0),
		}, nil
	case xdr.ScSpecUdtUnionCaseV0KindScSpecUdtUnionCaseTupleV0:
		if unionCase.TupleCase == nil {
			return specUnionCaseDetail{}, fmt.Errorf("union case is missing its tuple payload")
		}
		types := make([]string, 0, len(unionCase.TupleCase.Type))
		for _, typ := range unionCase.TupleCase.Type {
			types = append(types, formatSpecType(typ))
		}
		return specUnionCaseDetail{
			Name:  unionCase.TupleCase.Name,
			Doc:   unionCase.TupleCase.Doc,
			Types: types,
		}, nil
	default:
		return specUnionCaseDetail{}, fmt.Errorf("union case has unknown kind %s", unionCase.Kind)
	}
}

func writeUnionCase(w io.Writer, unionCase xdr.ScSpecUdtUnionCaseV0, indent string) error {
	detail, err := newSpecUnionCaseDetail(unionCase)
	if err != nil {
		return err
	}

	line := indent + detail.Name
	if len(detail.Types) > 0 {
		line += "(" + strings.Join(detail.Types, ", ") + ")"
	}
	if _, err := fmt.Fprintln(w, line); err != nil {
		return fmt.Errorf("writing union case %s: %w", detail.Name, err)
	}
	return nil
}

// writeDoc prints a non-empty documentation string at the given indent.
func writeDoc(w io.Writer, doc, indent string) error {
	if doc == "" {
		return nil
	}
	if _, err := fmt.Fprintf(w, "%s%s\n", indent, doc); err != nil {
		return fmt.Errorf("writing documentation: %w", err)
	}
	return nil
}

func eventParamLocation(param xdr.ScSpecEventParamV0) string {
	if param.Location == xdr.ScSpecEventParamLocationV0ScSpecEventParamLocationTopicList {
		return " [topic]"
	}
	return " [data]"
}

func eventParamLocationName(param xdr.ScSpecEventParamV0) string {
	if param.Location == xdr.ScSpecEventParamLocationV0ScSpecEventParamLocationTopicList {
		return "topic"
	}
	return "data"
}

// formatSpecType renders an ScSpecTypeDef in Soroban's familiar type syntax.
func formatSpecType(t xdr.ScSpecTypeDef) string {
	switch t.Type {
	case xdr.ScSpecTypeScSpecTypeVal:
		return "Val"
	case xdr.ScSpecTypeScSpecTypeBool:
		return "Bool"
	case xdr.ScSpecTypeScSpecTypeVoid:
		return "()"
	case xdr.ScSpecTypeScSpecTypeError:
		return "Error"
	case xdr.ScSpecTypeScSpecTypeU32:
		return "u32"
	case xdr.ScSpecTypeScSpecTypeI32:
		return "i32"
	case xdr.ScSpecTypeScSpecTypeU64:
		return "u64"
	case xdr.ScSpecTypeScSpecTypeI64:
		return "i64"
	case xdr.ScSpecTypeScSpecTypeTimepoint:
		return "Timepoint"
	case xdr.ScSpecTypeScSpecTypeDuration:
		return "Duration"
	case xdr.ScSpecTypeScSpecTypeU128:
		return "u128"
	case xdr.ScSpecTypeScSpecTypeI128:
		return "i128"
	case xdr.ScSpecTypeScSpecTypeU256:
		return "u256"
	case xdr.ScSpecTypeScSpecTypeI256:
		return "i256"
	case xdr.ScSpecTypeScSpecTypeBytes:
		return "Bytes"
	case xdr.ScSpecTypeScSpecTypeString:
		return "String"
	case xdr.ScSpecTypeScSpecTypeSymbol:
		return "Symbol"
	case xdr.ScSpecTypeScSpecTypeAddress:
		return "Address"
	case xdr.ScSpecTypeScSpecTypeMuxedAddress:
		return "MuxedAddress"
	case xdr.ScSpecTypeScSpecTypeOption:
		if t.Option == nil {
			return "Option<?>"
		}
		return "Option<" + formatSpecType(t.Option.ValueType) + ">"
	case xdr.ScSpecTypeScSpecTypeResult:
		if t.Result == nil {
			return "Result<?, ?>"
		}
		return "Result<" + formatSpecType(t.Result.OkType) + ", " + formatSpecType(t.Result.ErrorType) + ">"
	case xdr.ScSpecTypeScSpecTypeVec:
		if t.Vec == nil {
			return "Vec<?>"
		}
		return "Vec<" + formatSpecType(t.Vec.ElementType) + ">"
	case xdr.ScSpecTypeScSpecTypeMap:
		if t.Map == nil {
			return "Map<?, ?>"
		}
		return "Map<" + formatSpecType(t.Map.KeyType) + ", " + formatSpecType(t.Map.ValueType) + ">"
	case xdr.ScSpecTypeScSpecTypeTuple:
		if t.Tuple == nil {
			return "(?)"
		}
		parts := make([]string, 0, len(t.Tuple.ValueTypes))
		for _, typ := range t.Tuple.ValueTypes {
			parts = append(parts, formatSpecType(typ))
		}
		return "(" + strings.Join(parts, ", ") + ")"
	case xdr.ScSpecTypeScSpecTypeBytesN:
		if t.BytesN == nil {
			return "BytesN<?>"
		}
		return fmt.Sprintf("BytesN<%d>", t.BytesN.N)
	case xdr.ScSpecTypeScSpecTypeUdt:
		if t.Udt == nil {
			return "<?>"
		}
		return t.Udt.Name
	default:
		return t.Type.String()
	}
}

// writeIndentedValue renders a lens value and indents every line, so decoded
// arguments line up under their parameter name.
func writeIndentedValue(w io.Writer, value *lens.Value, palette lensformat.Palette, indent string) error {
	var buf bytes.Buffer
	formatter := lensformat.NewTreeFormatter(lensformat.WithPalette(palette))
	if err := formatter.Format(&buf, value); err != nil {
		return err
	}
	for _, line := range strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n") {
		if _, err := fmt.Fprintln(w, indent+line); err != nil {
			return err
		}
	}
	return nil
}

// specDecodeSink decorates event records with their emitting contract's ABI
// before handing them to the next sink.
//
// Resolution is attempted once per contract and cached in memory; a contract
// whose spec cannot be fetched or has none is remembered as unresolved and its
// events pass through with the wire shape intact, so a missing spec never
// fails the stream.
type specDecodeSink struct {
	next   stream.Sink
	client prismrpc.Client
	cache  *stream.ABICache

	mu       sync.Mutex
	resolved map[string]*contractSpec
	failed   map[string]struct{}
}

// specResolveTimeout bounds the RPC round trip for one contract, so an
// unreachable endpoint degrades to the wire shape instead of stalling output.
const specResolveTimeout = 10 * time.Second

func newSpecDecodeSink(next stream.Sink, client prismrpc.Client) (*specDecodeSink, error) {
	cache, err := stream.NewABICache(stream.ABICacheOptions{})
	if err != nil {
		return nil, fmt.Errorf("initialising contract spec cache: %w", err)
	}
	return &specDecodeSink{
		next:     next,
		client:   client,
		cache:    cache,
		resolved: make(map[string]*contractSpec),
		failed:   make(map[string]struct{}),
	}, nil
}

// Emit implements stream.Sink.
func (s *specDecodeSink) Emit(r stream.Record) error {
	if r.Event != nil {
		if typed := s.decode(r.Event); typed != nil {
			r.Event.Typed = typed
		}
	}
	return s.next.Emit(r)
}

// Flush implements stream.Sink.
func (s *specDecodeSink) Flush() error { return s.next.Flush() }

// Close closes the wrapped sink when it owns a resource.
func (s *specDecodeSink) Close() error {
	if closer, ok := s.next.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (s *specDecodeSink) decode(e *stream.Event) *stream.TypedEvent {
	return s.specFor(e.ContractID).decodeEvent(e)
}

// specFor returns the resolved spec for a contract, or nil. Nil receivers make
// the call site a no-op when resolution failed.
func (s *specDecodeSink) specFor(contractID string) *contractSpec {
	if contractID == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if spec, ok := s.resolved[contractID]; ok {
		return spec
	}
	if _, ok := s.failed[contractID]; ok {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), specResolveTimeout)
	defer cancel()
	spec, err := resolveContractSpecWithCache(ctx, s.client, contractID, s.cache)
	if err != nil {
		s.failed[contractID] = struct{}{}
		return nil
	}
	s.resolved[contractID] = spec
	return spec
}

// decodeEvent applies the contract spec to one event. It returns nil when the
// event is not declared in the spec, which leaves the wire shape in place.
func (s *contractSpec) decodeEvent(e *stream.Event) *stream.TypedEvent {
	if s == nil || e == nil || len(s.Events) == 0 || len(e.TopicXDR) == 0 {
		return nil
	}

	topicVals := make([]xdr.ScVal, 0, len(e.TopicXDR))
	for _, raw := range e.TopicXDR {
		value, err := decodeScValBase64(raw)
		if err != nil {
			return nil
		}
		topicVals = append(topicVals, value)
	}
	// Contract events lead with the event name as a symbol topic.
	if topicVals[0].Type != xdr.ScValTypeScvSymbol || topicVals[0].Sym == nil {
		return nil
	}
	name := string(*topicVals[0].Sym)

	var spec *xdr.ScSpecEventV0
	for i := range s.Events {
		if string(s.Events[i].Name) == name {
			spec = &s.Events[i]
			break
		}
	}
	if spec == nil {
		return nil
	}

	var data *xdr.ScVal
	if e.ValueXDR != "" {
		if value, err := decodeScValBase64(e.ValueXDR); err == nil {
			data = &value
		}
	}

	dataParams := 0
	for _, param := range spec.Params {
		if param.Location != xdr.ScSpecEventParamLocationV0ScSpecEventParamLocationTopicList {
			dataParams++
		}
	}

	typed := &stream.TypedEvent{Name: name}
	topicIndex := 1
	for _, param := range spec.Params {
		if param.Location == xdr.ScSpecEventParamLocationV0ScSpecEventParamLocationTopicList {
			if topicIndex >= len(topicVals) {
				continue
			}
			typed.Fields = append(typed.Fields, stream.TypedField{
				Name:  param.Name,
				Value: formatScValRaw(e.TopicXDR[topicIndex]),
			})
			topicIndex++
			continue
		}
		if data == nil {
			continue
		}
		// A single data parameter is the payload itself; several are packed
		// into a map keyed by parameter name.
		if dataParams <= 1 {
			typed.Fields = append(typed.Fields, stream.TypedField{
				Name:  param.Name,
				Value: formatScValValue(*data),
			})
			continue
		}
		if data.Type == xdr.ScValTypeScvMap && data.Map != nil {
			if value := scMapEntryValue(**data.Map, param.Name); value != nil {
				typed.Fields = append(typed.Fields, stream.TypedField{
					Name:  param.Name,
					Value: formatScValValue(*value),
				})
			}
		}
	}
	if len(typed.Fields) == 0 {
		return nil
	}
	return typed
}

func decodeScValBase64(raw string) (xdr.ScVal, error) {
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("decoding ScVal base64: %w", err)
	}
	var value xdr.ScVal
	if err := xdr.SafeUnmarshal(decoded, &value); err != nil {
		return xdr.ScVal{}, fmt.Errorf("unmarshalling ScVal: %w", err)
	}
	return value, nil
}

func scMapEntryValue(m xdr.ScMap, name string) *xdr.ScVal {
	for i := range m {
		key := m[i].Key
		if key.Type == xdr.ScValTypeScvSymbol && key.Sym != nil && string(*key.Sym) == name {
			return &m[i].Val
		}
	}
	return nil
}

func formatScValValue(value xdr.ScVal) string {
	encoded, err := scValBase64(value)
	if err != nil {
		return value.Type.String()
	}
	return formatScValRaw(encoded)
}

func scValBase64(value xdr.ScVal) (string, error) {
	var buf bytes.Buffer
	if _, err := xdr.Marshal(&buf, value); err != nil {
		return "", fmt.Errorf("marshalling ScVal: %w", err)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

// formatScValRaw renders one ScVal as a single line, so a typed field fits on
// its own output line.
func formatScValRaw(raw string) string {
	value, err := lens.DecodeAs(raw, "ScVal")
	if err != nil {
		return raw
	}
	var buf bytes.Buffer
	if err := lensformat.NewTreeFormatter().Format(&buf, value); err != nil {
		return raw
	}
	return strings.Join(strings.Fields(buf.String()), " ")
}
