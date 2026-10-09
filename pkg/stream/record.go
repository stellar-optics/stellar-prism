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
	// TopicXDR holds the base64 ScVal topics, in order.
	TopicXDR []string
	// ValueXDR is the base64 ScVal payload.
	ValueXDR string
}

// LedgerClose is ledger close metadata as delivered by RPC.
type LedgerClose struct {
	// Hash is the hex ledger hash.
	Hash string
	// HeaderXDR is the base64 LedgerHeaderHistoryEntry.
	HeaderXDR string
	// MetadataXDR is the base64 LedgerCloseMeta.
	MetadataXDR string
}

// Transaction is a transaction as delivered by RPC.
type Transaction struct {
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
}
