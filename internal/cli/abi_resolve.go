package cli

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-xdr-lens/pkg/lens"
	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"

	prismrpc "github.com/stellar-optics/stellar-prism/pkg/rpc"
)

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

func loadContractSpec(path, contractID string) (*contractSpec, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("spec path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading spec file %q: %w", path, err)
	}

	var entries []xdr.ScSpecEntry
	switch ext := strings.ToLower(filepath.Ext(path)); ext {
	case ".wasm":
		entries, err = parseContractSpec(data)
		if err != nil {
			return nil, fmt.Errorf("parsing WASM spec %q: %w", path, err)
		}
	case ".json":
		if err := json.Unmarshal(data, &entries); err != nil {
			return nil, fmt.Errorf("parsing JSON spec %q: %w", path, err)
		}
		if len(entries) == 0 {
			return nil, fmt.Errorf("spec file %q contains no entries", path)
		}
	default:
		return nil, fmt.Errorf("spec file %q must end in .wasm or .json", path)
	}

	spec := &contractSpec{ContractID: contractID}
	if err := spec.add(entries); err != nil {
		return nil, fmt.Errorf("spec file %q: %w", path, err)
	}
	return spec, nil
}

func specForArgs(ctx context.Context, g *globalFlags, stderr io.Writer, specPath, contractID string) (*contractSpec, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if specPath != "" {
		spec, err := loadContractSpec(specPath, contractID)
		return spec, func() {}, err
	}
	client, closeClient, err := openClient(g, stderr)
	if err != nil {
		return nil, nil, err
	}
	spec, err := resolveContractSpec(ctx, client, contractID)
	if err != nil {
		closeClient()
		return nil, nil, err
	}
	return spec, closeClient, nil
}

// newABICmd builds `prism abi <contract-id>`.
func newABICmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var (
		function  string
		arguments []string
		specPath  string
	)

	cmd := &cobra.Command{
		Use:   "abi <contract-id>",
		Short: "Resolve a contract's on-chain ABI spec",
		Long: `Fetch a contract's WASM bytecode through getLedgerEntries, parse the
Soroban spec embedded in its contractspecv0 custom section, and render
functions, types and events with human-readable argument names.

The lookup is two RPC calls: the contract instance names the WASM hash, and
the code entry carries the bytecode. No other network access is made, and
nothing is cached between runs. --spec replaces the lookup with a local
WASM or contract spec JSON file.

With --function, only that function is shown. Repeating --arg decodes actual
base64 ScVal arguments against the function's parameter names and types, so
an event or invocation can be read without translating the wire shape by
hand.`,
		Example: `  # List everything the contract exposes
  prism abi CBGSB...

  # Show one function and decode its arguments
  prism abi CBGSB... --function transfer \
    --arg AAAAAwAAAAc= --arg AAAAAwAAAAo=

  # Read a local build artifact without touching RPC
  prism abi CBGSB... --spec ./target/wasm32-unknown-unknown/release/c.wasm`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if function == "" && len(arguments) > 0 {
				return fmt.Errorf("--arg requires --function")
			}

			spec, closeSpec, err := specForArgs(cmd.Context(), g, stderr, specPath, args[0])
			if err != nil {
				return err
			}
			defer closeSpec()

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
	cmd.Flags().StringVar(&specPath, "spec", "",
		"read the contract spec from a local .wasm or .json file instead of RPC")
	cmd.AddCommand(newABIEncodeCmd(g, stdout, stderr))
	return cmd
}

func newABIEncodeCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var (
		arguments []string
		specPath  string
	)

	cmd := &cobra.Command{
		Use:   "encode <contract-id> <function>",
		Short: "Encode human-readable arguments as base64 ScVals",
		Long: `Encode --arg name=value pairs against a function's spec types and print
base64 ScVals ready to paste into stellar contract invoke or a test harness.
Arguments may be given in any order, but every input needs exactly one value.
--spec reads the contract spec from a local .wasm or .json file.`,
		Example: `  prism abi encode CBGSB... set_count --arg count=7
  prism abi encode CBGSB... transfer --arg to=CBGSB... --arg amount=1000`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			spec, closeSpec, err := specForArgs(cmd.Context(), g, stderr, specPath, args[0])
			if err != nil {
				return err
			}
			defer closeSpec()
			return writeEncodedArguments(stdout, spec, args[1], arguments)
		},
	}

	cmd.Flags().StringArrayVar(&arguments, "arg", nil,
		"argument as name=value; repeatable and checked against the function spec")
	cmd.Flags().StringVar(&specPath, "spec", "",
		"read the contract spec from a local .wasm or .json file instead of RPC")
	return cmd
}

func writeEncodedArguments(w io.Writer, spec *contractSpec, function string, arguments []string) error {
	var fn *xdr.ScSpecFunctionV0
	for i := range spec.Functions {
		if string(spec.Functions[i].Name) == function {
			fn = &spec.Functions[i]
			break
		}
	}
	if fn == nil {
		names := make([]string, 0, len(spec.Functions))
		for _, candidate := range spec.Functions {
			names = append(names, string(candidate.Name))
		}
		return fmt.Errorf("function %q not found; available: %s", function, strings.Join(names, ", "))
	}

	values := make(map[string]string, len(arguments))
	for _, raw := range arguments {
		name, value, ok := strings.Cut(raw, "=")
		if !ok || name == "" {
			return fmt.Errorf("invalid --arg %q: want name=value", raw)
		}
		if _, exists := values[name]; exists {
			return fmt.Errorf("argument %q specified more than once", name)
		}
		values[name] = value
	}
	if len(values) != len(fn.Inputs) {
		return fmt.Errorf("function %s takes %d argument(s), got %d", function, len(fn.Inputs), len(values))
	}

	encoded := make([]string, len(fn.Inputs))
	for i, input := range fn.Inputs {
		value, ok := values[input.Name]
		if !ok {
			return fmt.Errorf("missing argument %q for function %s", input.Name, function)
		}
		scval, err := encodeScVal(input.Type, value)
		if err != nil {
			return fmt.Errorf("argument %s (%s): %w", input.Name, formatSpecType(input.Type), err)
		}
		encoded[i], err = xdr.MarshalBase64(scval)
		if err != nil {
			return fmt.Errorf("encoding argument %s: %w", input.Name, err)
		}
	}
	for i, input := range fn.Inputs {
		if _, err := fmt.Fprintf(w, "%s=%s\n", input.Name, encoded[i]); err != nil {
			return fmt.Errorf("writing argument %s: %w", input.Name, err)
		}
	}
	return nil
}

func encodeScVal(typ xdr.ScSpecTypeDef, value string) (xdr.ScVal, error) {
	switch typ.Type {
	case xdr.ScSpecTypeScSpecTypeBool:
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return xdr.ScVal{}, fmt.Errorf("%q is not a boolean: %w", value, err)
		}
		return xdr.NewScVal(xdr.ScValTypeScvBool, parsed)
	case xdr.ScSpecTypeScSpecTypeU32:
		return parseUintScVal(value, 32, xdr.ScValTypeScvU32, func(v uint64) any { return xdr.Uint32(v) })
	case xdr.ScSpecTypeScSpecTypeI32:
		return parseIntScVal(value, 32, xdr.ScValTypeScvI32, func(v int64) any { return xdr.Int32(v) })
	case xdr.ScSpecTypeScSpecTypeU64, xdr.ScSpecTypeScSpecTypeTimepoint, xdr.ScSpecTypeScSpecTypeDuration:
		var scType xdr.ScValType
		switch typ.Type {
		case xdr.ScSpecTypeScSpecTypeTimepoint:
			scType = xdr.ScValTypeScvTimepoint
		case xdr.ScSpecTypeScSpecTypeDuration:
			scType = xdr.ScValTypeScvDuration
		default:
			scType = xdr.ScValTypeScvU64
		}
		return parseUintScVal(value, 64, scType, func(v uint64) any {
			switch scType {
			case xdr.ScValTypeScvTimepoint:
				return xdr.TimePoint(v)
			case xdr.ScValTypeScvDuration:
				return xdr.Duration(v)
			default:
				return xdr.Uint64(v)
			}
		})
	case xdr.ScSpecTypeScSpecTypeI64:
		return parseIntScVal(value, 64, xdr.ScValTypeScvI64, func(v int64) any { return xdr.Int64(v) })
	case xdr.ScSpecTypeScSpecTypeU128:
		parts, err := integerParts(value, 128, false)
		if err != nil {
			return xdr.ScVal{}, err
		}
		return xdr.NewScVal(xdr.ScValTypeScvU128, xdr.UInt128Parts{Hi: xdr.Uint64(parts[0]), Lo: xdr.Uint64(parts[1])})
	case xdr.ScSpecTypeScSpecTypeI128:
		parts, err := integerParts(value, 128, true)
		if err != nil {
			return xdr.ScVal{}, err
		}
		return xdr.NewScVal(xdr.ScValTypeScvI128, xdr.Int128Parts{Hi: xdr.Int64(parts[0]), Lo: xdr.Uint64(parts[1])})
	case xdr.ScSpecTypeScSpecTypeU256:
		parts, err := integerParts(value, 256, false)
		if err != nil {
			return xdr.ScVal{}, err
		}
		return xdr.NewScVal(xdr.ScValTypeScvU256, xdr.UInt256Parts{
			HiHi: xdr.Uint64(parts[0]), HiLo: xdr.Uint64(parts[1]), LoHi: xdr.Uint64(parts[2]), LoLo: xdr.Uint64(parts[3]),
		})
	case xdr.ScSpecTypeScSpecTypeI256:
		parts, err := integerParts(value, 256, true)
		if err != nil {
			return xdr.ScVal{}, err
		}
		return xdr.NewScVal(xdr.ScValTypeScvI256, xdr.Int256Parts{
			HiHi: xdr.Int64(parts[0]), HiLo: xdr.Uint64(parts[1]), LoHi: xdr.Uint64(parts[2]), LoLo: xdr.Uint64(parts[3]),
		})
	case xdr.ScSpecTypeScSpecTypeBytes:
		raw, err := hex.DecodeString(strings.TrimPrefix(value, "0x"))
		if err != nil {
			return xdr.ScVal{}, fmt.Errorf("%q is not hexadecimal: %w", value, err)
		}
		return xdr.NewScVal(xdr.ScValTypeScvBytes, xdr.ScBytes(raw))
	case xdr.ScSpecTypeScSpecTypeString:
		return xdr.NewScVal(xdr.ScValTypeScvString, xdr.ScString(value))
	case xdr.ScSpecTypeScSpecTypeSymbol:
		return xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol(value))
	case xdr.ScSpecTypeScSpecTypeAddress:
		return encodeAddress(value, false)
	case xdr.ScSpecTypeScSpecTypeMuxedAddress:
		return encodeAddress(value, true)
	default:
		return xdr.ScVal{}, fmt.Errorf("cannot encode %s from a CLI string", formatSpecType(typ))
	}
}

func parseUintScVal(value string, bits int, scType xdr.ScValType, wrap func(uint64) any) (xdr.ScVal, error) {
	parsed, err := strconv.ParseUint(value, 0, bits)
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("%q is not a uint%d: %w", value, bits, err)
	}
	return xdr.NewScVal(scType, wrap(parsed))
}

func parseIntScVal(value string, bits int, scType xdr.ScValType, wrap func(int64) any) (xdr.ScVal, error) {
	parsed, err := strconv.ParseInt(value, 0, bits)
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("%q is not an int%d: %w", value, bits, err)
	}
	return xdr.NewScVal(scType, wrap(parsed))
}

func integerParts(value string, bits int, signed bool) ([]uint64, error) {
	n := new(big.Int)
	if _, ok := n.SetString(strings.TrimPrefix(value, "0x"), 0); !ok {
		return nil, fmt.Errorf("%q is not an integer", value)
	}
	width := bits / 64
	if n.Sign() < 0 {
		if !signed {
			return nil, fmt.Errorf("%q is negative but the type is unsigned", value)
		}
		limit := new(big.Int).Lsh(big.NewInt(1), uint(bits))
		if n.Cmp(new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), uint(bits-1)))) < 0 {
			return nil, fmt.Errorf("%q is below the int%d range", value, bits)
		}
		n.Add(n, limit)
	} else {
		limit := new(big.Int).Lsh(big.NewInt(1), uint(bits))
		if signed {
			limit.Rsh(limit, 1)
		}
		if n.Cmp(limit) >= 0 {
			kind := "uint"
			if signed {
				kind = "int"
			}
			return nil, fmt.Errorf("%q is above the %s%d range", value, kind, bits)
		}
	}
	raw := n.FillBytes(make([]byte, width*8))
	parts := make([]uint64, width)
	for i := range parts {
		parts[i] = binary.BigEndian.Uint64(raw[i*8 : i*8+8])
	}
	return parts, nil
}

func encodeAddress(value string, muxed bool) (xdr.ScVal, error) {
	if muxed {
		raw, err := strkey.Decode(strkey.VersionByteMuxedAccount, value)
		if err != nil {
			return xdr.ScVal{}, fmt.Errorf("%q is not a muxed account address: %w", value, err)
		}
		if len(raw) != 40 {
			return xdr.ScVal{}, fmt.Errorf("%q decodes to %d bytes, want 40", value, len(raw))
		}
		var key xdr.Uint256
		copy(key[:], raw[:32])
		return xdr.NewScVal(xdr.ScValTypeScvAddress, xdr.ScAddress{
			Type: xdr.ScAddressTypeScAddressTypeMuxedAccount,
			MuxedAccount: &xdr.MuxedEd25519Account{
				Id:      xdr.Uint64(binary.BigEndian.Uint64(raw[32:])),
				Ed25519: key,
			},
		})
	}

	version := strkey.VersionByteAccountID
	if strings.HasPrefix(value, "C") {
		version = strkey.VersionByteContract
	}
	raw, err := strkey.Decode(version, value)
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("%q is not a Stellar address: %w", value, err)
	}
	if len(raw) != 32 {
		return xdr.ScVal{}, fmt.Errorf("%q decodes to %d bytes, want 32", value, len(raw))
	}
	if version == strkey.VersionByteContract {
		var id xdr.ContractId
		copy(id[:], raw)
		return xdr.NewScVal(xdr.ScValTypeScvAddress, xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &id,
		})
	}
	var key xdr.Uint256
	copy(key[:], raw)
	return xdr.NewScVal(xdr.ScValTypeScvAddress, xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: &xdr.AccountId{Type: xdr.PublicKeyTypePublicKeyTypeEd25519, Ed25519: &key},
	})
}

// resolveContractSpec performs the two ledger-entry lookups and parses the
// embedded spec. It is the only networked path in this file.
func resolveContractSpec(ctx context.Context, client prismrpc.Client, contractID string) (*contractSpec, error) {
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

	specEntries, err := parseContractSpec(codeEntry.Data.ContractCode.Code)
	if err != nil {
		return nil, fmt.Errorf("contract %s: %w", contractID, err)
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
