package stream

import (
	"fmt"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// TrappedFilter accepts only Soroban transactions whose host function
// trapped or panicked.
//
// "Trapped" is the protocol's word for a contract execution that aborted:
// the WASM host caught the failure and reported it as a result code rather
// than crashing the network. It is distinct from other failure modes such as
// a malformed invocation or an exceeded resource limit, and distinct from a
// transaction that failed for a non-Soroban reason.
//
// The filter reads the decoded Failure on a classified transaction, so it
// must be applied through TransactionFilterSource (or another path that has
// called EnrichTransaction).
func TrappedFilter() TransactionFilter {
	return func(tx *Transaction) bool {
		return tx != nil && tx.Failure.Trapped()
	}
}

// TrappedTxFilter is an alias for TrappedFilter.
func TrappedTxFilter() TransactionFilter {
	return TrappedFilter()
}

// TrapQuery describes a stream of trapped Soroban transactions.
type TrapQuery struct {
	// StartLedger is where a fresh stream begins. Zero means start from the
	// server's newest ledger.
	StartLedger uint32
	// PageLimit is the page size to request.
	PageLimit uint
}

// TrapSource is the trapped-contract-execution view of a transaction stream.
type TrapSource = TransactionFilterSource

// NewTrapSource returns a Source emitting only Soroban transactions that
// trapped, with the failing operation and decoded reason attached.
func NewTrapSource(client rpc.Client, q TrapQuery) (*TrapSource, error) {
	src, err := NewTransactionFilterSource(client, TransactionFilterQuery{
		StartLedger: q.StartLedger,
		PageLimit:   q.PageLimit,
		Filters:     []TransactionFilter{TrappedFilter()},
	})
	if err != nil {
		return nil, fmt.Errorf("trap source: %w", err)
	}
	return src, nil
}

// IsTrapped reports whether a transaction record contains a trapped Soroban
// host function. It is the predicate NewTrapSource uses, exported for callers
// that already hold a classified record.
func IsTrapped(tx *Transaction) bool {
	return tx != nil && tx.Failure.Trapped()
}
