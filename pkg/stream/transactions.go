package stream

import (
	"context"
	"fmt"
	"strconv"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// DefaultTransactionPageLimit is the page size requested for transaction streams.
const DefaultTransactionPageLimit = 100

// TransactionQuery describes which transactions to stream.
type TransactionQuery struct {
	// StartLedger is where a fresh stream begins. Zero means start from the
	// server's newest ledger.
	StartLedger uint32
	// PageLimit is the page size to request.
	PageLimit uint
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

// CursorLedger implements Source.
func (s *TransactionSource) CursorLedger(cursor string) (uint32, bool) {
	if cursor == "" {
		return 0, false
	}
	// The transaction cursor is just a decimal ledger sequence in some RPC versions,
	// but according to protocol it might be identical to LedgerPaginationOptions cursor.
	// We'll parse it out. Since the events cursor is a compound string like "12345-1",
	// let's try to parse a ledger from it. If it fails, fallback to simple decimal.
	var ledger uint32
	if _, err := fmt.Sscanf(cursor, "%d-", &ledger); err == nil {
		return ledger, true
	}
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
		Full:       uint(len(resp.Transactions)) >= s.query.PageLimit,
	}
	for _, tx := range resp.Transactions {
		batch.Records = append(batch.Records, transactionRecord(tx))
	}
	return batch, nil
}

// transactionRecord converts an RPC transaction into a neutral Record.
func transactionRecord(tx protocol.TransactionInfo) Record {
	return Record{
		ID:       fmt.Sprintf("%010d-%04d", tx.Ledger, tx.ApplicationOrder),
		Sequence: tx.Ledger,
		Time:     time.Unix(tx.LedgerCloseTime, 0).UTC(),
		Kind:     KindTransaction,
		Transaction: &Transaction{
			Hash:           tx.TransactionHash,
			Index:          uint32(tx.ApplicationOrder),
			Successful:     tx.Status == protocol.TransactionStatusSuccess,
			EnvelopeXDR:    tx.EnvelopeXDR,
			ResultXDR:      tx.ResultXDR,
			ResultMetaXDR:  tx.ResultMetaXDR,
		},
	}
}
