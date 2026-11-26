package stream

import (
	"context"
	"fmt"
	"strings"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-xdr-lens/pkg/lens"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// Transaction statuses accepted by --status and by TxStatusQuery.
const (
	// TxStatusSuccess selects transactions that applied cleanly.
	TxStatusSuccess = "success"
	// TxStatusFailed selects transactions that were applied but failed.
	TxStatusFailed = "failed"
)

// FailureDetail is one decoded result code, in plain English.
//
// It is a copy of the fields of lens.Reason rather than a lens type, so a
// record stays a neutral value the stream package owns and sinks do not have
// to import lens to describe a failure.
type FailureDetail struct {
	// Code is the snake_case short form, e.g. "tx_failed" or
	// "invoke_host_function_trapped".
	Code string
	// Constant is the generated XDR constant name, useful when
	// cross-referencing the protocol definition.
	Constant string
	// Summary explains the code in one sentence.
	Summary string
	// Hint suggests what to do about it; empty when no advice applies.
	Hint string
}

// OperationFailure describes the first operation that failed in a transaction.
type OperationFailure struct {
	// Index is the zero-based operation position.
	Index int
	// Type is the operation type, e.g. "invoke_host_function".
	Type string
	// Detail is the operation's decoded result code.
	Detail FailureDetail
	// Trapped reports a Soroban host function that trapped or panicked.
	// It is the predicate behind `prism transactions --trapped`.
	Trapped bool
}

// Failure explains why a transaction failed.
//
// A failure can be reported at three levels, and all of them are kept because
// the most specific one is not always the most useful: a fee-bump wrapper
// reports its own code while the inner transaction carries the real cause,
// and a failed transaction reports `tx_failed` while the failing operation
// carries the meaningful code.
type Failure struct {
	// Detail is the transaction-level result code.
	Detail FailureDetail
	// Inner is the inner transaction's result code for a fee-bump wrapper.
	// It is nil for an ordinary transaction.
	Inner *FailureDetail
	// Operation is the first failing operation, when the result reports one.
	Operation *OperationFailure
	// Headline is one sentence naming what failed, as lens rendered it.
	Headline string
}

// Trapped reports whether the failure is a Soroban host function trap or
// panic, regardless of whether the code lives on the operation or the inner
// transaction.
func (f *Failure) Trapped() bool {
	return f != nil && f.Operation != nil && f.Operation.Trapped
}

// ParseTransactionStatuses validates a list of status names and returns the
// set to match. An empty list means "no status filter" and returns a nil set,
// which keeps filtering on one code path.
func ParseTransactionStatuses(values []string) (map[string]bool, error) {
	if len(values) == 0 {
		return nil, nil
	}
	set := make(map[string]bool, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		switch value {
		case TxStatusSuccess, TxStatusFailed:
			set[value] = true
		default:
			return nil, fmt.Errorf(
				"unknown transaction status %q; expected %q or %q",
				value, TxStatusSuccess, TxStatusFailed)
		}
	}
	if len(set) == 0 {
		return nil, nil
	}
	return set, nil
}

// EnrichTransaction fills Status, FeeAccount, FeeBump and Failure on a
// transaction record by decoding its envelope and result.
//
// Classification is opt-in through TransactionQuery.Classify because it costs
// two XDR decodes per record. It is idempotent: enriching an already-enriched
// record replaces the derived fields rather than accumulating them.
func EnrichTransaction(tx *Transaction) error {
	if tx == nil {
		return fmt.Errorf("classifying transaction: nil payload")
	}
	if tx.EnvelopeXDR == "" {
		// Some backends omit the envelope for a transaction they have not
		// fully ingested. The RPC status is still authoritative, so keep the
		// record and skip the derived fields rather than stopping a stream
		// over an enrichment that was never required.
		if tx.Successful {
			tx.Status = TxStatusSuccess
		} else {
			tx.Status = TxStatusFailed
		}
		return nil
	}

	envelope, err := lens.DecodeAs(tx.EnvelopeXDR, "TransactionEnvelope")
	if err != nil {
		return fmt.Errorf("classifying transaction %s: decoding envelope: %w", tx.Hash, err)
	}
	env, ok := envelope.Raw.(*xdr.TransactionEnvelope)
	if !ok {
		return fmt.Errorf("classifying transaction %s: envelope did not decode to a TransactionEnvelope", tx.Hash)
	}

	feeAccount := env.FeeAccount()
	tx.FeeAccount = feeAccount.ToAccountId().Address()

	var (
		result      *xdr.TransactionResult
		resultValue *lens.Value
	)
	if tx.ResultXDR != "" {
		decoded, err := lens.DecodeAs(tx.ResultXDR, "TransactionResult")
		if err != nil {
			return fmt.Errorf("classifying transaction %s: decoding result: %w", tx.Hash, err)
		}
		parsed, ok := decoded.Raw.(*xdr.TransactionResult)
		if !ok {
			return fmt.Errorf("classifying transaction %s: result did not decode to a TransactionResult", tx.Hash)
		}
		result = parsed
		resultValue = decoded
	}

	tx.FeeBump = classifyFeeBump(env, result)

	if tx.Successful {
		tx.Status = TxStatusSuccess
		tx.Failure = nil
		return nil
	}
	tx.Status = TxStatusFailed
	tx.Failure = nil

	if result == nil {
		// A transaction the server reports as failed but did not return a
		// result for cannot be explained further. The status itself is
		// still correct and useful.
		return nil
	}

	summary, err := lens.ExplainPair(envelope, resultValue)
	if err != nil {
		return fmt.Errorf("classifying transaction %s: explaining result: %w", tx.Hash, err)
	}
	tx.Failure = failureFromSummary(summary)
	return nil
}

// failureFromSummary converts lens's explanation into the record's neutral
// failure shape, preferring the most specific code available.
func failureFromSummary(summary *lens.Summary) *Failure {
	if summary == nil || summary.Outcome == nil {
		return nil
	}

	failure := &Failure{
		Detail:   failureDetail(summary.Outcome.Reason),
		Headline: summary.Headline,
	}
	if summary.Outcome.InnerReason != nil {
		inner := failureDetail(*summary.Outcome.InnerReason)
		failure.Inner = &inner
	}

	if len(summary.Outcome.FailedOps) == 0 {
		return failure
	}

	index := summary.Outcome.FailedOps[0]
	op := OperationFailure{Index: index}
	if index >= 0 && index < len(summary.Operations) {
		op.Type = summary.Operations[index].Type
		if reason := summary.Operations[index].Result; reason != nil {
			op.Detail = failureDetail(*reason)
			op.Trapped = reason.Constant == xdr.InvokeHostFunctionResultCodeInvokeHostFunctionTrapped.String()
		}
	}
	failure.Operation = &op
	return failure
}

func failureDetail(reason lens.Reason) FailureDetail {
	return FailureDetail{
		Code:     reason.Code,
		Constant: reason.Constant,
		Summary:  reason.Summary,
		Hint:     reason.Hint,
	}
}

// TxStatusQuery describes a transaction stream filtered by status.
type TxStatusQuery struct {
	// StartLedger is where a fresh stream begins. Zero means start from the
	// server's newest ledger.
	StartLedger uint32
	// PageLimit is the page size to request.
	PageLimit uint
	// Statuses limits results to "success" and/or "failed". Empty means all.
	Statuses []string
}

// TransactionFilter decides whether an enriched transaction belongs in a
// filtered stream. Filters are applied to classified records, so they can
// rely on Status, FeeBump and Failure being populated.
type TransactionFilter func(*Transaction) bool

// TransactionFilterQuery describes a transaction stream narrowed by status
// and/or a set of predicates.
type TransactionFilterQuery struct {
	// StartLedger is where a fresh stream begins. Zero means start from the
	// server's newest ledger.
	StartLedger uint32
	// PageLimit is the page size to request.
	PageLimit uint
	// Statuses limits results to "success" and/or "failed". Empty means all.
	Statuses []string
	// Filters are ANDed: a transaction is emitted only when every filter
	// accepts it. Empty means no predicate beyond Statuses.
	Filters []TransactionFilter
}

// TransactionFilterSource streams classified transactions that match a
// status filter and zero or more predicates.
//
// It wraps TransactionSource rather than calling RPC itself, so pagination,
// cursor semantics and gap detection stay shared. Filtering happens after the
// page is fetched: the cursor still advances across suppressed records, which
// is what makes "only failures" a view of the stream rather than a second
// stream that could skip history.
type TransactionFilterSource struct {
	tx       *TransactionSource
	statuses map[string]bool
	filters  []TransactionFilter
}

// TxStatusSource is the status-only view of a filtered transaction stream.
type TxStatusSource = TransactionFilterSource

// NewTxStatusSource returns a Source emitting only transactions whose status
// matches q.Statuses, with failures decoded.
func NewTxStatusSource(client rpc.Client, q TxStatusQuery) (*TxStatusSource, error) {
	return NewTransactionFilterSource(client, TransactionFilterQuery{
		StartLedger: q.StartLedger,
		PageLimit:   q.PageLimit,
		Statuses:    q.Statuses,
	})
}

// NewTransactionFilterSource returns a Source over the given RPC client.
//
// Every record is classified before filtering, so an invalid status name or
// an unusable predicate fails at construction rather than producing a stream
// that silently matches nothing.
func NewTransactionFilterSource(client rpc.Client, q TransactionFilterQuery) (*TransactionFilterSource, error) {
	if client == nil {
		return nil, fmt.Errorf("transaction filter source: rpc client is required")
	}
	statuses, err := ParseTransactionStatuses(q.Statuses)
	if err != nil {
		return nil, fmt.Errorf("transaction filter source: %w", err)
	}
	if q.PageLimit == 0 {
		q.PageLimit = DefaultTransactionPageLimit
	}
	return &TransactionFilterSource{
		tx: NewTransactionSource(client, TransactionQuery{
			StartLedger: q.StartLedger,
			PageLimit:   q.PageLimit,
			Classify:    true,
		}),
		statuses: statuses,
		filters:  q.Filters,
	}, nil
}

// Name implements Source.
func (s *TransactionFilterSource) Name() string { return "transactions" }

// Rewind implements Rewinder, so --gap-strategy resume still works behind the
// filter.
func (s *TransactionFilterSource) Rewind(ledger uint32) { s.tx.Rewind(ledger) }

// CursorLedger implements Source.
func (s *TransactionFilterSource) CursorLedger(cursor string) (uint32, bool) {
	return s.tx.CursorLedger(cursor)
}

// Fetch implements Source.
func (s *TransactionFilterSource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	batch, err := s.tx.Fetch(ctx, cursor)
	if err != nil {
		return Batch{}, err
	}

	out := Batch{
		Records:    make([]Record, 0, len(batch.Records)),
		NextCursor: batch.NextCursor,
		Latest:     batch.Latest, LatestCloseTime: batch.LatestCloseTime,
		Oldest: batch.Oldest,
		Full:   batch.Full,
	}
	for _, record := range batch.Records {
		if record.Transaction == nil {
			continue
		}
		if !s.matches(record.Transaction) {
			continue
		}
		out.Records = append(out.Records, record)
	}
	return out, nil
}

func (s *TransactionFilterSource) matches(tx *Transaction) bool {
	if len(s.statuses) > 0 && !s.statuses[tx.Status] {
		return false
	}
	for _, filter := range s.filters {
		if filter != nil && !filter(tx) {
			return false
		}
	}
	return true
}
