package cli

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"strings"
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
	var b strings.Builder
	b.WriteString(indent)
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
	b.WriteString(")")

	switch len(fn.Outputs) {
	case 0:
		b.WriteString(" -> ()")
	case 1:
		b.WriteString(" -> ")
		b.WriteString(formatSpecType(fn.Outputs[0]))
	default:
		b.WriteString(" -> (")
		for i, output := range fn.Outputs {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(formatSpecType(output))
		}
		b.WriteString(")")
	}

	if _, err := fmt.Fprintln(w, b.String()); err != nil {
		return fmt.Errorf("writing function %s: %w", fn.Name, err)
	}
	return nil
}

func writeUnionCase(w io.Writer, unionCase xdr.ScSpecUdtUnionCaseV0, indent string) error {
	var name string
	var types []string
	switch unionCase.Kind {
	case xdr.ScSpecUdtUnionCaseV0KindScSpecUdtUnionCaseVoidV0:
		if unionCase.VoidCase == nil {
			return fmt.Errorf("union case is missing its void payload")
		}
		name = unionCase.VoidCase.Name
	case xdr.ScSpecUdtUnionCaseV0KindScSpecUdtUnionCaseTupleV0:
		if unionCase.TupleCase == nil {
			return fmt.Errorf("union case is missing its tuple payload")
		}
		name = unionCase.TupleCase.Name
		for _, typ := range unionCase.TupleCase.Type {
			types = append(types, formatSpecType(typ))
		}
	default:
		return fmt.Errorf("union case has unknown kind %s", unionCase.Kind)
	}

	line := indent + name
	if len(types) > 0 {
		line += "(" + strings.Join(types, ", ") + ")"
	}
	if _, err := fmt.Fprintln(w, line); err != nil {
		return fmt.Errorf("writing union case %s: %w", name, err)
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
