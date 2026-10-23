package stream

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// DefaultTransactionPageLimit is the page size requested for transaction streams.
const DefaultTransactionPageLimit = 100

// TransactionQuery describes which transactions to stream.
type TransactionQuery struct {
	// Memo matches a prefix of the sanitized text memo; empty disables filtering.
	Memo string
	// StartLedger is where a fresh stream begins. Zero means start from the
	// server's newest ledger.
	StartLedger uint32
	// PageLimit is the page size to request.
	PageLimit uint
	// Classify decodes each envelope and result to fill Status, FeeAccount,
	// FeeBump and Failure. It costs two XDR decodes per record, so it is
	// opt-in; the filtered transaction sources enable it automatically.
	Classify bool
}

// TransactionSource streams transactions from RPC.
type TransactionSource struct {
	client rpc.Client
	query  TransactionQuery
}

// NewTransactionSource returns a Source over the given RPC client.
func NewTransactionSource(client rpc.Client, q TransactionQuery) *TransactionSource {
	if q.PageLimit == 0 {
		q.PageLimit = DefaultTransactionPageLimit
	}
	return &TransactionSource{client: client, query: q}
}

// Name implements Source.
func (s *TransactionSource) Name() string { return "transactions" }

// Rewind implements Rewinder, repositioning a fresh fetch at ledger.
func (s *TransactionSource) Rewind(ledger uint32) { s.query.StartLedger = ledger }

// CursorLedger implements Source.
//
// The getTransactions cursor is a SEP-35 total-order ID in decimal: one int64
// packing the ledger sequence, the transaction's application order within
// that ledger, and an operation index. It is emphatically not a ledger
// sequence. transactionCursorLedger is what turns it back into one, and gap
// detection depends on getting that right: a TOID for a recent ledger is
// roughly 10^13, so comparing it directly against the server's retention
// floor is always false and detection silently becomes a no-op.
func (s *TransactionSource) CursorLedger(cursor string) (uint32, bool) {
	if cursor == "" {
		return 0, false
	}
	// Try parsing as a full cursor (has the form ledger-tx-op-event). RPC
	// cursors are TOID-encoded, while the derived sources synthesise a plain
	// zero-padded ledger in the first segment; both must be understood.
	if parts := strings.Split(cursor, "-"); len(parts) >= 2 {
		// First part may be toid-encoded; parse it and extract the ledger
		// sequence. Legacy transaction cursors carry the ledger directly
		// ("%010d-%06d"), so fall back to the plain value when the TOID
		// payload has no ledger in it.
		idInt, err := strconv.ParseInt(parts[0], 10, 64)
		if err == nil {
			if parsed := toid.Parse(idInt); parsed.LedgerSequence > 0 {
				return uint32(parsed.LedgerSequence), true
			}
			if n, err := strconv.ParseUint(parts[0], 10, 32); err == nil {
				return uint32(n), true
			}
		idInt, err := strconv.ParseInt(parts[0], 10, 64)
		if err == nil {
			parsed := toid.Parse(idInt)
			// A valid ledger sequence is never zero, so a zero here means the
			// first segment is a plain ledger rather than a TOID.
			if parsed.LedgerSequence != 0 {
				return uint32(parsed.LedgerSequence), true
			}
		}
		if n, err := strconv.ParseUint(parts[0], 10, 32); err == nil {
			return uint32(n), true
		}
		// Fall back: try the second part as a plain ledger number.
		if n, err := strconv.ParseUint(parts[1], 10, 32); err == nil {
			return uint32(n), true
		}
		return 0, false
	}
	// No dash in cursor; try parsing as a plain decimal ledger.
	n, err := strconv.ParseUint(cursor, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(n), true
}

// Fetch implements Source.
func (s *TransactionSource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	req := protocol.GetTransactionsRequest{
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: s.query.PageLimit,
		},
	}
	if cursor == "" {
		req.StartLedger = s.query.StartLedger
	} else {
		// A corrupt cursor is unfixable by retrying, so it must not be
		// classified as transient: the tailer would back off until its retry
		// budget ran out and then report the wrong cause.
		if _, ok := transactionCursorLedger(cursor); !ok {
			return Batch{}, rpc.Permanent(fmt.Errorf("invalid transaction cursor %q", cursor))
		}
		req.Pagination.Cursor = cursor
	}

	resp, err := s.client.GetTransactions(ctx, req)
	if err != nil {
		return Batch{}, err
	}

	batch := Batch{
		Records:    make([]Record, 0, len(resp.Transactions)),
		NextCursor: resp.Cursor,
		Latest:     resp.LatestLedger,
		Oldest:     resp.OldestLedger,
		Full:       transactionPageIsFull(resp, s.query.PageLimit),
	}
	for _, tx := range resp.Transactions {
		record := transactionRecord(tx)
		if s.query.Classify {
			if err := EnrichTransaction(record.Transaction); err != nil {
				return Batch{}, fmt.Errorf("transaction source: %w", err)
			}
		}
		if s.query.Memo != "" && (record.Transaction.Memo == nil || record.Transaction.Memo.Type != "text" || !strings.HasPrefix(record.Transaction.Memo.Value, s.query.Memo)) {
			continue
		}
		batch.Records = append(batch.Records, record)
	}
	return batch, nil
}

// transactionRecord converts an RPC transaction into a neutral Record.
func transactionRecord(tx protocol.TransactionInfo) Record {
	transaction := &Transaction{
		Hash:          tx.TransactionHash,
		Index:         uint32(tx.ApplicationOrder),
		Successful:    tx.Status == protocol.TransactionStatusSuccess,
		EnvelopeXDR:   tx.EnvelopeXDR,
		ResultXDR:     tx.ResultXDR,
		ResultMetaXDR: tx.ResultMetaXDR,
	}
	if !transaction.Successful {
		transaction.ErrorCode, transaction.FailureReason, transaction.FailedOpIndex, transaction.FailedOpType = failureSummary(tx.ResultXDR)
	}
	r := Record{
		// Seven digits is the width of TOID's transaction-order field, so the
		// fixed-width ordering contract holds for a whole ledger's worth of
		// transactions rather than the first 10,000.
		ID:          fmt.Sprintf("%010d-%07d", tx.Ledger, tx.ApplicationOrder),
		Sequence:    tx.Ledger,
		Time:        time.Unix(tx.LedgerCloseTime, 0).UTC(),
		Kind:        KindTransaction,
		Transaction: transaction,
	}
	if memo, err := DecodeMemo(tx.EnvelopeXDR); err == nil {
		r.Transaction.Memo = &memo
	} else {
		r.Transaction.MemoError = err.Error()
	}
	return r
}

// transactionCursorLedger extracts the ledger a cursor refers to, accepting
// both shapes that exist in practice: the RPC's SEP-35 decimal token, and the
// compound "ledger-..." form derived sources synthesise.
//
// A plain ledger sequence fits comfortably below 2^32 while every real TOID is
// at least ledger<<32, so the two cannot be confused.
func transactionCursorLedger(cursor string) (uint32, bool) {
	if cursor == "" {
		return 0, false
	}

	if n, err := strconv.ParseInt(cursor, 10, 64); err == nil && n > 0 {
		if n <= math.MaxUint32 {
			return uint32(n), true
		}
		if ledger := toid.Parse(n).LedgerSequence; ledger > 0 {
			return uint32(ledger), true
		}
		return 0, false
	}

	first, _, ok := strings.Cut(cursor, "-")
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(first, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(n), true
}

// transactionPageIsFull reports whether more records are already waiting.
//
// A full page is the obvious case. The subtler one is a short page that stops
// well before the server's head: the RPC caps how many ledgers one call
// scans, so backfilling a sparse range yields a short page with plenty left
// behind it. Treating that page as terminal would end a backfill in the
// middle of the range, so it only counts as terminal once it reaches the head.
// An empty page is always terminal — the cursor cannot advance past nothing.
func transactionPageIsFull(resp protocol.GetTransactionsResponse, pageLimit uint) bool {
	raw := resp.Transactions
	if len(raw) == 0 || resp.Cursor == "" {
		return false
	}
	if uint(len(raw)) >= pageLimit {
		return true
	}
	return raw[len(raw)-1].Ledger < resp.LatestLedger
}

// failureSummary extracts the failed transaction or operation code without
// making malformed optional result XDR fatal to the stream.
func failureSummary(resultXDR string) (code, detail string, opIndex *uint32, opType string) {
	if resultXDR == "" {
		return
	}
	var result xdr.TransactionResult
	if err := xdr.SafeUnmarshalBase64(resultXDR, &result); err != nil {
		return
	}
	resultCode := normalizedCode(result.Result.Code.String())
	var operationResults *[]xdr.OperationResult
	if result.Result.Results != nil {
		operationResults = result.Result.Results
	}
	if result.Result.Code == xdr.TransactionResultCodeTxFeeBumpInnerFailed && result.Result.InnerResultPair != nil {
		inner := result.Result.InnerResultPair.Result
		resultCode = normalizedCode(inner.Result.Code.String())
		operationResults = inner.Result.Results
	}
	code, detail = resultCode, humanizeCode(resultCode)
	if operationResults == nil {
		return
	}
	for i, operation := range *operationResults {
		if operation.Code != xdr.OperationResultCodeOpInner || operation.Tr == nil {
			if operation.Code != xdr.OperationResultCodeOpInner {
				n := uint32(i)
				opIndex = &n
				code = normalizedCode(operation.Code.String())
				detail = humanizeCode(code)
			}
			if opIndex != nil {
				return
			}
			continue
		}
		tr := operation.Tr
		raw := operationErrorCode(tr)
		if raw != "" && !strings.HasSuffix(raw, "Success") {
			n := uint32(i)
			opIndex = &n
			opType = strings.TrimPrefix(strings.ToLower(tr.Type.String()), "operationtype")
			code = normalizedCode(raw)
			detail = humanizeCode(code)
			return
		}
	}
	return
}

func operationErrorCode(tr *xdr.OperationResultTr) string {
	if tr == nil {
		return ""
	}
	name, ok := tr.ArmForSwitch(int32(tr.Type))
	if !ok || name == "" {
		return ""
	}
	value := reflect.ValueOf(tr).Elem().FieldByName(name)
	if !value.IsValid() || value.IsNil() {
		return ""
	}
	code := value.Elem().FieldByName("Code")
	if !code.IsValid() || !code.CanInterface() {
		return ""
	}
	stringer, ok := code.Interface().(fmt.Stringer)
	if !ok {
		return ""
	}
	return stringer.String()
}

func normalizedCode(raw string) string {
	prefix := ""
	if i := strings.Index(raw, "ResultCode"); i >= 0 {
		enumName := raw[:i]
		raw = raw[i+len("ResultCode"):]
		switch enumName {
		case "Transaction":
			prefix = "tx_"
		case "Operation":
			prefix = "op_"
		default:
			prefix = camelSnake(enumName) + "_"
		}
		raw = strings.TrimPrefix(raw, enumName)
		if enumName == "Transaction" {
			raw = strings.TrimPrefix(raw, "Tx")
		}
		if enumName == "Operation" {
			raw = strings.TrimPrefix(raw, "Op")
		}
	}
	return prefix + camelSnake(raw)
}

func camelSnake(raw string) string {
	var b strings.Builder
	for i, r := range raw {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
		}
		b.WriteRune(r)
	}
	return strings.ToLower(b.String())
}

func humanizeCode(code string) string { return strings.ReplaceAll(code, "_", " ") }
