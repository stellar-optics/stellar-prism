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

import (
	"encoding/json"
	"sync"
	"time"
)

var recordBufferPool = sync.Pool{
	New: func() any {
		records := make([]Record, 0, DefaultPageLimit)
		return &records
	},
}

var eventIDSetPool = sync.Pool{
	New: func() any {
		return make(map[string]struct{}, DefaultPageLimit)
	},
}

func getRecordBuffer(minCapacity int) []Record {
	if minCapacity > DefaultPageLimit {
		return make([]Record, 0, minCapacity)
	}
	if records, ok := recordBufferPool.Get().(*[]Record); ok && cap(*records) >= minCapacity {
		return (*records)[:0]
	}
	return make([]Record, 0, DefaultPageLimit)
}

func putRecordBuffer(records []Record) {
	if cap(records) == 0 || cap(records) > DefaultPageLimit {
		return
	}
	clear(records[:cap(records)])
	reusable := records[:0]
	recordBufferPool.Put(&reusable)
}

func getEventIDSet() map[string]struct{} {
	if seen, ok := eventIDSetPool.Get().(map[string]struct{}); ok {
		return seen
	}
	return make(map[string]struct{}, DefaultPageLimit)
}

func putEventIDSet(seen map[string]struct{}) {
	clear(seen)
	eventIDSetPool.Put(seen)
}

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
// their base64 form so that decoding happens in the sink, only for the output
// formats that need it.
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
//
// The percentile, capacity and surge fields are derived per ledger; see
// FeeStatsSource and docs/guides/fee_stats_streaming.md for the methodology.
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

	// Operations is the number of operations in the ledger's transactions,
	// counting a fee-bump wrapper as one extra operation, as fees do.
	Operations uint32 `json:"operations"`
	// ClassicOperations is the part of Operations outside Soroban
	// transactions, which is what MaxTxSetSize limits.
	ClassicOperations uint32 `json:"classicOperations"`
	// MaxTxSetSize is the ledger header's classic capacity, in operations.
	MaxTxSetSize uint32 `json:"maxTxSetSize"`
	// CapacityUsage is ClassicOperations / MaxTxSetSize: 1 means the classic
	// lane was full.
	CapacityUsage float64 `json:"capacityUsage"`
	// InclusionFeeCharged is the distribution of the inclusion fee each
	// transaction actually paid, per operation, in stroops. Soroban resource
	// fees are excluded. Nil for a ledger with no transactions.
	InclusionFeeCharged *FeeDistribution `json:"inclusionFeeCharged,omitempty"`
	// InclusionFeeBid is the distribution of the maximum inclusion fee each
	// transaction offered, per operation, in stroops.
	InclusionFeeBid *FeeDistribution `json:"inclusionFeeBid,omitempty"`
	// SurgePricing reports that the ledger closed under surge pricing:
	// inclusion was competitive rather than available at the base fee.
	SurgePricing bool `json:"surgePricing"`
	// SurgeReason names the trigger: SurgeReasonCapacity,
	// SurgeReasonFee, or both joined with "+". Empty when not surging.
	SurgeReason string `json:"surgeReason,omitempty"`
}

// Surge pricing triggers reported in FeeStats.SurgeReason.
const (
	// SurgeReasonCapacity: the classic lane reached MaxTxSetSize.
	SurgeReasonCapacity = "capacity"
	// SurgeReasonFee: every transaction paid more than the base fee per
	// operation, which the network only charges when bids compete.
	SurgeReasonFee = "fee"
)

// FeeDistribution summarises per-operation fees across one ledger's
// transactions, in stroops. Percentiles use the nearest-rank method over
// one value per transaction.
type FeeDistribution struct {
	Min  int64 `json:"min"`
	Mode int64 `json:"mode"`
	P10  int64 `json:"p10"`
	P50  int64 `json:"p50"`
	P90  int64 `json:"p90"`
	P95  int64 `json:"p95"`
	P99  int64 `json:"p99"`
	Max  int64 `json:"max"`
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
	// PreviousEntryXDR is the base64 LedgerEntry immediately before this
	// mutation, when transaction metadata provides it or the source observed it.
	PreviousEntryXDR string
	// ChangeXDR is the base64 LedgerEntryChange exactly as the ledger emitted
	// it, so consumers can re-decode without reconstructing anything.
	ChangeXDR string
	// StructuralDiff is populated by the contract diff source.
	StructuralDiff  string
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
	Ledger             uint32
	Error              string
	ReturnValueXDR     string
	ReturnValueJSON    json.RawMessage
	Instructions       uint32
	DiskReadBytes      uint32
	WriteBytes         uint32
	MinResourceFee     int64
	ReadOnlyFootprint  []string
	ReadWriteFootprint []string
	AuthXDR            []string
	AuthJSON           []json.RawMessage
	EventsXDR          []string
	EventsJSON         []json.RawMessage
	Changed            []string
	Previous           *SimulationSnapshot
}

// SimulationSnapshot contains the comparable values from the previous head.
type SimulationSnapshot struct {
	ReturnValueXDR     string
	Instructions       uint32
	DiskReadBytes      uint32
	WriteBytes         uint32
	MinResourceFee     int64
	ReadOnlyFootprint  []string
	ReadWriteFootprint []string
	AuthXDR            []string
	EventsXDR          []string
}
