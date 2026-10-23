package stream

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// DefaultNetworkPassphrase is the testnet passphrase. Contract IDs are
// derived from the network passphrase, so callers streaming mainnet must set
// it explicitly.
const DefaultNetworkPassphrase = "Test SDF Network ; September 2015"

// WASM deployment actions.
const (
	WASMActionUpload = "upload"
	WASMActionCreate = "create"
)

// WASMQuery describes which deployment host functions to stream.
type WASMQuery struct {
	// StartLedger is where a fresh stream begins. Zero means start from the
	// server's newest ledger.
	StartLedger uint32
	// PageLimit is the page size to request.
	PageLimit uint
	// NetworkPassphrase is used to derive contract IDs. Empty means testnet;
	// set it to the passphrase of the network being streamed.
	NetworkPassphrase string
	// IncludeFailed emits events from failed transactions too. They did not
	// change the ledger, but seeing attempted deployments is useful when
	// debugging.
	IncludeFailed bool
}

// WASMSource streams WASM uploads and contract creations by decoding each
// transaction envelope.
//
// Soroban does not emit a system event for deployment, so these records are
// derived from the InvokeHostFunction operation rather than from the event
// stream. Like StorageChangeSource it wraps TransactionSource so pagination
// and cursor semantics stay shared.
type WASMSource struct {
	tx  *TransactionSource
	cfg WASMQuery
}

// NewWASMSource returns a Source over the given RPC client.
func NewWASMSource(client rpc.Client, q WASMQuery) (*WASMSource, error) {
	if client == nil {
		return nil, fmt.Errorf("wasm source: rpc client is required")
	}
	if q.PageLimit == 0 {
		q.PageLimit = DefaultTransactionPageLimit
	}
	if q.NetworkPassphrase == "" {
		q.NetworkPassphrase = DefaultNetworkPassphrase
	}

	return &WASMSource{
		tx: NewTransactionSource(client, TransactionQuery{
			StartLedger: q.StartLedger,
			PageLimit:   q.PageLimit,
		}),
		cfg: q,
	}, nil
}

// Name implements Source.
func (s *WASMSource) Name() string { return "wasm_deployments" }

// Rewind implements Rewinder, repositioning a fresh fetch at ledger.
func (s *WASMSource) Rewind(ledger uint32) {
	s.cfg.StartLedger = ledger
	s.tx.Rewind(ledger)
}

// CursorLedger implements Source.
func (s *WASMSource) CursorLedger(cursor string) (uint32, bool) {
	return s.tx.CursorLedger(cursor)
}

// Fetch implements Source.
func (s *WASMSource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	batch, err := s.tx.Fetch(ctx, cursor)
	if err != nil {
		return Batch{}, err
	}

	out := Batch{
		Records:    make([]Record, 0, len(batch.Records)),
		NextCursor: batch.NextCursor,
		Latest:     batch.Latest,
		Oldest:     batch.Oldest,
		Full:       batch.Full,
	}
	for i := range batch.Records {
		record := &batch.Records[i]
		tx := record.Transaction
		if tx == nil {
			continue
		}
		if !tx.Successful && !s.cfg.IncludeFailed {
			continue
		}

		events, err := s.extract(tx, record.Sequence, record.Time)
		if err != nil {
			return Batch{}, fmt.Errorf("wasm source: transaction %s: %w", tx.Hash, err)
		}
		out.Records = append(out.Records, events...)
	}
	return out, nil
}

func (s *WASMSource) extract(tx *Transaction, ledger uint32, closeTime time.Time) ([]Record, error) {
	if tx.EnvelopeXDR == "" {
		return nil, nil
	}

	var envelope xdr.TransactionEnvelope
	if err := xdr.SafeUnmarshalBase64(tx.EnvelopeXDR, &envelope); err != nil {
		return nil, fmt.Errorf("decoding envelope: %w", err)
	}

	operations, err := envelopeOperations(&envelope)
	if err != nil {
		return nil, err
	}

	var records []Record
	for opIndex := range operations {
		op := &operations[opIndex]
		if op.Body.Type != xdr.OperationTypeInvokeHostFunction || op.Body.InvokeHostFunctionOp == nil {
			continue
		}

		event, ok, err := s.hostFunctionEvent(&op.Body.InvokeHostFunctionOp.HostFunction)
		if err != nil {
			return nil, fmt.Errorf("operation %d: %w", opIndex, err)
		}
		if !ok {
			continue
		}

		event.TxHash = tx.Hash
		event.OpIndex = uint32(opIndex)
		event.Successful = tx.Successful
		records = append(records, Record{
			ID:             fmt.Sprintf("%010d-%06d-%06d", ledger, tx.Index, opIndex),
			Sequence:       ledger,
			Time:           closeTime,
			Kind:           KindWASMDeployment,
			WASMDeployment: event,
		})
	}
	return records, nil
}

func envelopeOperations(envelope *xdr.TransactionEnvelope) ([]xdr.Operation, error) {
	switch envelope.Type {
	case xdr.EnvelopeTypeEnvelopeTypeTxV0:
		if envelope.V0 == nil {
			return nil, fmt.Errorf("envelope has no v0 transaction")
		}
		return envelope.V0.Tx.Operations, nil
	case xdr.EnvelopeTypeEnvelopeTypeTx:
		if envelope.V1 == nil {
			return nil, fmt.Errorf("envelope has no v1 transaction")
		}
		return envelope.V1.Tx.Operations, nil
	case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
		if envelope.FeeBump == nil {
			return nil, fmt.Errorf("envelope has no fee bump transaction")
		}
		inner := envelope.FeeBump.Tx.InnerTx
		if inner.V1 == nil {
			return nil, fmt.Errorf("fee bump has no inner v1 transaction")
		}
		return inner.V1.Tx.Operations, nil
	default:
		return nil, fmt.Errorf("unsupported envelope type %d", envelope.Type)
	}
}

func (s *WASMSource) hostFunctionEvent(fn *xdr.HostFunction) (*WASMEvent, bool, error) {
	switch fn.Type {
	case xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm:
		wasm, ok := fn.GetWasm()
		if !ok {
			return nil, false, fmt.Errorf("upload host function has no wasm")
		}
		sum := sha256.Sum256(wasm)
		return &WASMEvent{
			Action:   WASMActionUpload,
			WASMHash: hex.EncodeToString(sum[:]),
			WASMSize: len(wasm),
		}, true, nil

	case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
		args, ok := fn.GetCreateContract()
		if !ok {
			return nil, false, fmt.Errorf("create host function has no arguments")
		}
		return s.createEvent(args.ContractIdPreimage, args.Executable)

	case xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
		args, ok := fn.GetCreateContractV2()
		if !ok {
			return nil, false, fmt.Errorf("create-v2 host function has no arguments")
		}
		return s.createEvent(args.ContractIdPreimage, args.Executable)

	case xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
		args := fn.InvokeContract
		if args == nil || string(args.FunctionName) != "update_wasm" {
			return nil, false, nil
		}
		contract, ok := args.ContractAddress.GetContractId()
		if !ok {
			return nil, false, fmt.Errorf("update_wasm target is not a contract")
		}
		contractID, err := strkey.Encode(strkey.VersionByteContract, contract[:])
		if err != nil {
			return nil, false, fmt.Errorf("encoding contract address: %w", err)
		}
		if len(args.Args) == 0 {
			return nil, false, fmt.Errorf("update_wasm has no wasm hash argument")
		}
		hash, ok := args.Args[0].GetBytes()
		if !ok || len(hash) != 32 {
			return nil, false, fmt.Errorf("update_wasm has invalid wasm hash")
		}
		return &WASMEvent{Action: "upgrade", ContractID: contractID, WASMHash: hex.EncodeToString(hash)}, true, nil

	default:
		return nil, false, nil
	}
}

func (s *WASMSource) createEvent(preimage xdr.ContractIdPreimage, executable xdr.ContractExecutable) (*WASMEvent, bool, error) {
	contractID, err := contractID(preimage, s.cfg.NetworkPassphrase)
	if err != nil {
		return nil, false, fmt.Errorf("deriving contract ID: %w", err)
	}

	event := &WASMEvent{
		Action:     WASMActionCreate,
		ContractID: contractID,
	}
	switch executable.Type {
	case xdr.ContractExecutableTypeContractExecutableWasm:
		event.Executable = "wasm"
		if hash, ok := executable.GetWasmHash(); ok {
			event.WASMHash = hex.EncodeToString(hash[:])
		}
	case xdr.ContractExecutableTypeContractExecutableStellarAsset:
		event.Executable = "stellar_asset"
	case xdr.ContractExecutableTypeContractExecutableExternalRef:
		event.Executable = "external_ref"
	default:
		event.Executable = fmt.Sprintf("unknown_%d", executable.Type)
	}
	return event, true, nil
}

func contractID(preimage xdr.ContractIdPreimage, passphrase string) (string, error) {
	switch preimage.Type {
	case xdr.ContractIdPreimageTypeContractIdPreimageFromAddress:
		if preimage.FromAddress == nil {
			return "", fmt.Errorf("address preimage has no address")
		}
		networkID := xdr.Hash(sha256.Sum256([]byte(passphrase)))
		image := xdr.HashIdPreimage{
			Type: xdr.EnvelopeTypeEnvelopeTypeContractId,
			ContractId: &xdr.HashIdPreimageContractId{
				NetworkId:          networkID,
				ContractIdPreimage: preimage,
			},
		}
		raw, err := image.MarshalBinary()
		if err != nil {
			return "", fmt.Errorf("encoding contract ID preimage: %w", err)
		}
		sum := sha256.Sum256(raw)
		return strkey.Encode(strkey.VersionByteContract, sum[:])

	case xdr.ContractIdPreimageTypeContractIdPreimageFromAsset:
		if preimage.FromAsset == nil {
			return "", fmt.Errorf("asset preimage has no asset")
		}
		id, err := preimage.FromAsset.ContractID(passphrase)
		if err != nil {
			return "", err
		}
		return strkey.Encode(strkey.VersionByteContract, id[:])

	default:
		return "", fmt.Errorf("unsupported contract ID preimage type %d", preimage.Type)
	}
}
