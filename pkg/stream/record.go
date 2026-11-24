// Package stream implements prism's streaming core: the cursor-driven state
// machine that turns a poll-only RPC into a continuous, gap-free record
// stream, plus the Source and Sink seams that new inputs and outputs plug
// into.
//
// Soroban RPC offers no subscription or websocket mechanism, so "streaming"
// here means adaptive polling with exact cursor continuity. The design work
// is in never losing or repeating a record across a reconnect; see
// docs/architecture.md for the delivery guarantee and how it is proven.
package stream

import "time"

// Kind identifies which sort of record a Record carries.
type Kind string

const (
	// KindEvent is a Soroban contract, system or diagnostic event.
	KindEvent Kind = "event"
	// KindLedger is ledger close metadata.
	KindLedger Kind = "ledger"
	// KindTransaction is a Stellar transaction.
	KindTransaction Kind = "transaction"
	// KindStorageChange is a contract data, contract code or TTL entry
	// mutation observed in transaction metadata.
	KindStorageChange Kind = "storage_change"
	// KindWASMDeployment is a WASM upload or contract creation derived from a
	// transaction envelope.
	KindWASMDeployment Kind = "wasm_deployment"
	KindFeeStats       Kind = "fee_stats"
	KindTrade          Kind = "trade"
	// KindOperation is one operation extracted from a transaction envelope.
	KindOperation Kind = "operation"
	// KindSimulation is a simulation estimate for a template transaction.
	KindSimulation Kind = "simulation"
)

// Record is one item in a stream, whatever its source.
//
// Sinks render Records without knowing which Source produced them, which is
// what lets a new source reuse every output format.
type Record struct {
	// ID orders the stream. It must be fixed-width and zero-padded so that
	// lexicographic comparison matches chronological order — the whole
	// de-duplication scheme rests on that property, so a new Source must
	// preserve it. See Source for the contract.
	ID string
	// Sequence is the ledger the record belongs to.
	Sequence uint32
	// Time is the ledger close time.
	Time time.Time
	// Kind selects which of the payload pointers below is set.
	Kind Kind

	// Event is set when Kind is KindEvent.
	Event *Event
	// Close is set when Kind is KindLedger.
	Close *LedgerClose
	// Transaction is set when Kind is KindTransaction.
	Transaction *Transaction
	// StorageChange is set when Kind is KindStorageChange.
	StorageChange *StorageChange
	// WASMDeployment is set when Kind is KindWASMDeployment.
	WASMDeployment *WASMEvent
	FeeStats       *FeeStats
	Trade          *Trade
	// Operation is set when Kind is KindOperation.
	Operation *Operation
	// Simulation is set when Kind is KindSimulation.
	Simulation *Simulation
}

// Event is a Soroban event as delivered by RPC. The XDR fields are kept in
// their base64 form so sinks can re-decode them; SAC holds a decoded view when
// the event matches a built-in Stellar Asset Contract token event.
type Event struct {
	// Type is "contract", "system" or "diagnostic".
	Type string
	// ContractID is the strkey contract address that emitted the event.
	ContractID string
	// TxHash is the hex hash of the emitting transaction.
	TxHash string
	// TxIndex and OpIndex locate the event within its ledger.
	TxIndex uint32
	OpIndex uint32
	// EventIndex locates the event within an operation.
	EventIndex uint32
	// TopicXDR holds the base64 ScVal topics, in order.
	TopicXDR []string
	// ValueXDR is the base64 ScVal payload.
	ValueXDR string
	// SAC is the decoded built-in Stellar Asset Contract token event, or nil
	// for events that do not follow a SAC token shape.
	SAC *SACEvent
	// CallDepth is the zero-based invocation depth when call trace metadata is
	// available. RPC getEvents currently omits this trace and leaves it zero.
	CallDepth int
	// ParentContractID identifies the immediate caller when known.
	ParentContractID string
	// InvocationPath lists the contract IDs from the root invocation to the
	// contract that emitted this event, when known.
	InvocationPath []string
}

// LedgerClose is ledger close metadata as delivered by RPC.
type LedgerClose struct {
	// Consensus is populated when consensus diagnostics are requested.
	Consensus *ConsensusMetrics
	// Hash is the hex ledger hash.
	Hash string
	// HeaderXDR is the base64 LedgerHeaderHistoryEntry.
	HeaderXDR string
	// MetadataXDR is the base64 LedgerCloseMeta.
	MetadataXDR string
}

// Transaction is a transaction as delivered by RPC.
type Transaction struct {
	// Memo is decoded and sanitized when the envelope is valid.
	Memo *SanitizedMemo
	// MemoError describes a malformed envelope without aborting the stream.
	MemoError string
	// Hash is the hex transaction hash.
	Hash string
	// Index is the zero-based index of the transaction within its ledger.
	Index uint32
	// Successful indicates whether the transaction succeeded.
	Successful bool
	// EnvelopeXDR is the base64 TransactionEnvelope.
	EnvelopeXDR string
	// ResultXDR is the base64 TransactionResult.
	ResultXDR string
	// ResultMetaXDR is the base64 TransactionMeta.
	ResultMetaXDR string
	// FailureReason is a short human-readable description of the failure.
	FailureReason string
	// ErrorCode is the normalized transaction or operation result code.
	ErrorCode     string
	FailedOpIndex *uint32
	FailedOpType  string
	// Conflicts contains Soroban footprint collisions detected in this ledger.
	Conflicts []FootprintConflict

	// Status is "success" or "failed" once the transaction has been
	// classified. It is empty on a record produced without
	// TransactionQuery.Classify, which is what the derived sources use.
	Status string
	// FeeAccount is the strkey account that paid the fee: the fee-bump
	// source for a wrapped transaction, and the transaction's own source
	// otherwise. It is set during classification.
	FeeAccount string
	// FeeBump is set when the transaction envelope is a FeeBumpTransaction.
	FeeBump *FeeBump
	// Failure explains a failed transaction's decoded result. It is nil for
	// a successful transaction and for an unclassified record.
	Failure *Failure
}

// FeeStats contains ledger fee-market measurements and rolling fee averages.
type FeeStats struct {
	BaseFee            uint32  `json:"baseFee"`
	BaseReserve        uint32  `json:"baseReserve"`
	FeePool            int64   `json:"feePool"`
	CoinsInCirculation int64   `json:"coinsInCirculation"`
	AverageFee10       float64 `json:"averageFee10"`
	AverageFee50       float64 `json:"averageFee50"`
	Transactions       uint32  `json:"transactions"`
	SorobanResourceFee int64   `json:"sorobanResourceFee"`
	SorobanFeeBid      int64   `json:"sorobanFeeBid"`
}

// Trade describes a classic or Soroban asset exchange in one operation.
type Trade struct {
	Protocol   string `json:"protocol"`
	TxHash     string `json:"tx"`
	Trader     string `json:"trader,omitempty"`
	Maker      string `json:"maker,omitempty"`
	Taker      string `json:"taker,omitempty"`
	Operation  string `json:"operation"`
	AssetIn    string `json:"assetIn,omitempty"`
	AmountIn   string `json:"amountIn,omitempty"`
	AmountUnit string `json:"amountUnit,omitempty"`
	AssetOut   string `json:"assetOut,omitempty"`
	AmountOut  string `json:"amountOut,omitempty"`
	Price      string `json:"price,omitempty"`
	OpIndex    uint32 `json:"opIndex"`
}

// StorageChange is a single contract-related ledger entry mutation extracted
// from a transaction's result metadata.
//
// The XDR fields stay base64-encoded for the same reason as Event: only the
// sinks that render them need to decode.
type StorageChange struct {
	// ChangeType is "created", "updated", "removed" or "restored".
	ChangeType string
	// EntryType is "contract_data", "contract_code" or "ttl".
	EntryType string
	// ContractID is the strkey contract address for contract data entries.
	ContractID string
	// Durability is "persistent" or "temporary" for contract data entries.
	Durability string
	// KeyHash is the hex ledger key hash for TTL entries.
	KeyHash string
	// WASMHash is the hex contract code hash for contract code entries.
	WASMHash string
	// KeyXDR is the base64 LedgerKey, absent for created entries whose key
	// only exists in the entry itself.
	KeyXDR string
	// EntryXDR is the base64 LedgerEntry, absent for removed entries.
	EntryXDR string
	// ChangeXDR is the base64 LedgerEntryChange exactly as the ledger emitted
	// it, so consumers can re-decode without reconstructing anything.
	ChangeXDR       string
	LiveUntilLedger uint32
	RemainingTTL    uint32
	TTLWarning      bool
}

// WASMEvent is a WASM upload or contract deployment derived from a successful
// (or, when requested, failed) transaction envelope.
type WASMEvent struct {
	// Action is "upload" or "create".
	Action string
	// ContractID is the strkey contract address for create actions.
	ContractID string
	// WASMHash is the hex sha256 of the uploaded code for upload actions, or
	// the executable's code hash for create actions when known.
	WASMHash string
	// WASMSize is the byte length of the uploaded code, when available.
	WASMSize int
	// Executable describes how the contract was created: "wasm", "stellar_asset"
	// or another future executable type.
	Executable string
	// TxHash is the hex hash of the transaction that carried the host
	// function.
	TxHash string
	// OpIndex is the zero-based operation index within the transaction.
	OpIndex uint32
	// Successful indicates whether the transaction succeeded.
	Successful bool
}

// Operation is one operation extracted from a transaction envelope.
type Operation struct {
	Index           uint32
	TxIndex         uint32
	Type            string
	SourceAccount   string
	ParametersXDR   string
	ResultCode      string
	ResultXDR       string
	TransactionHash string
}

// FootprintConflict describes a ledger key accessed by multiple transactions
// where at least one access is read-write.
type FootprintConflict struct {
	KeyXDR       string
	ContractID   string
	Transactions []string
}

// Simulation is the resource estimate returned by simulateTransaction.
type Simulation struct {
	Ledger         uint32
	Error          string
	ReturnValueXDR string
	Instructions   uint32
	DiskReadBytes  uint32
	WriteBytes     uint32
	MinResourceFee int64
	Changed        []string
	Previous       *SimulationSnapshot
}

// SimulationSnapshot contains the comparable values from the previous head.
type SimulationSnapshot struct {
	ReturnValueXDR string
	Instructions   uint32
	DiskReadBytes  uint32
	WriteBytes     uint32
	MinResourceFee int64
}
