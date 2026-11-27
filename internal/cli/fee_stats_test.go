package cli

import (
	"encoding/json"
	"strings"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// feeFixtureTx is one transaction of a sample ledger: how many operations it
// carried, what it bid in total and what it was charged in total.
type feeFixtureTx struct {
	ops     int
	bid     uint32
	charged int64
}

// feeFixtureLedger builds a decodable ledger header with the given base fee
// and classic capacity.
func feeFixtureLedger(t *testing.T, seq uint32, baseFee, maxTxSetSize uint32) protocol.LedgerInfo {
	t.Helper()
	header := xdr.LedgerHeaderHistoryEntry{Header: xdr.LedgerHeader{
		LedgerSeq:          xdr.Uint32(seq),
		BaseFee:            xdr.Uint32(baseFee),
		BaseReserve:        5_000_000,
		MaxTxSetSize:       xdr.Uint32(maxTxSetSize),
		TotalCoins:         1_000_000_000,
		FeePool:            42,
		LedgerVersion:      23,
		ScpValue:           xdr.StellarValue{CloseTime: xdr.TimePoint(1_700_000_000 + int64(seq))},
		SkipList:           [4]xdr.Hash{},
		PreviousLedgerHash: xdr.Hash{},
	}}
	encoded, err := xdr.MarshalBase64(header)
	if err != nil {
		t.Fatalf("encoding ledger header: %v", err)
	}
	return protocol.LedgerInfo{
		Hash:            strings.Repeat("ab", 32),
		Sequence:        seq,
		LedgerCloseTime: 1_700_000_000 + int64(seq),
		LedgerHeader:    encoded,
	}
}

// feeFixtureTxs builds decodable classic transactions for one ledger.
func feeFixtureTxs(t *testing.T, seq uint32, txs []feeFixtureTx) []protocol.TransactionInfo {
	t.Helper()
	out := make([]protocol.TransactionInfo, 0, len(txs))
	for i, tx := range txs {
		ops := make([]xdr.Operation, tx.ops)
		for j := range ops {
			ops[j] = xdr.Operation{Body: xdr.OperationBody{Type: xdr.OperationTypeInflation}}
		}
		env := xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{Tx: xdr.Transaction{
				SourceAccount: xdr.MuxedAccount{Type: xdr.CryptoKeyTypeKeyTypeEd25519, Ed25519: &xdr.Uint256{}},
				Fee:           xdr.Uint32(tx.bid),
				SeqNum:        xdr.SequenceNumber(i + 1),
				Cond:          xdr.Preconditions{Type: xdr.PreconditionTypePrecondNone},
				Memo:          xdr.Memo{Type: xdr.MemoTypeMemoNone},
				Operations:    ops,
			}},
		}
		envXDR, err := xdr.MarshalBase64(env)
		if err != nil {
			t.Fatalf("encoding envelope: %v", err)
		}
		results := []xdr.OperationResult{}
		result := xdr.TransactionResult{
			FeeCharged: xdr.Int64(tx.charged),
			Result:     xdr.TransactionResultResult{Code: xdr.TransactionResultCodeTxSuccess, Results: &results},
		}
		resXDR, err := xdr.MarshalBase64(result)
		if err != nil {
			t.Fatalf("encoding result: %v", err)
		}
		out = append(out, protocol.TransactionInfo{
			TransactionDetails: protocol.TransactionDetails{
				Status:      protocol.TransactionStatusSuccess,
				EnvelopeXDR: envXDR,
				ResultXDR:   resXDR,
				Ledger:      seq,
				// Application order is 1-based within a ledger.
				ApplicationOrder: int32(i + 1), //nolint:gosec // test data
			},
		})
	}
	return out
}

// feeStatsFixture is two sample ledgers: a calm one where everybody paid the
// base fee, and a congested one where the classic lane was full and every
// transaction paid more than the base fee.
func feeStatsFixture(t *testing.T) *fakerpc.Server {
	t.Helper()
	ledgers := []protocol.LedgerInfo{
		feeFixtureLedger(t, 1000, 100, 50),
		feeFixtureLedger(t, 1001, 100, 10),
	}
	calm := feeFixtureTxs(t, 1000, []feeFixtureTx{
		{ops: 1, bid: 100, charged: 100},
		{ops: 2, bid: 1000, charged: 200},
		{ops: 1, bid: 300, charged: 100},
		{ops: 1, bid: 5000, charged: 100},
	})
	var surgeTxs []feeFixtureTx
	for _, charged := range []int64{150, 150, 150, 200, 200, 200, 200, 300, 400, 1000} {
		surgeTxs = append(surgeTxs, feeFixtureTx{ops: 1, bid: uint32(charged * 2), charged: charged}) //nolint:gosec // test data
	}
	surge := feeFixtureTxs(t, 1001, surgeTxs)

	srv := fakerpc.NewLedgerServer(ledgers)
	srv.SetTransactions(protocol.GetTransactionsResponse{
		Transactions: append(calm, surge...),
		LatestLedger: 1001,
		OldestLedger: 1000,
	})
	return srv
}

// TestFeeStatsJSONEmitsPercentilesPerLedger checks the structured output an
// automated bidder consumes: one record per ledger, with the min, mode,
// median and 95th percentile of the per-operation inclusion fee, and the
// surge flag.
func TestFeeStatsJSONEmitsPercentilesPerLedger(t *testing.T) {
	withFake(t, feeStatsFixture(t))

	stdout, stderr, err := run(t, "fee-stats", "--from", "1000", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v\nstderr: %s", err, stderr)
	}
	lines := nonEmptyLines(stdout)
	if len(lines) != 2 {
		t.Fatalf("emitted %d records, want one per ledger (2)\n%s", len(lines), stdout)
	}

	type record struct {
		Kind   string `json:"kind"`
		Ledger uint32 `json:"ledger"`
		stream.FeeStats
	}
	var got []record
	for _, line := range lines {
		var r record
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("not valid JSON: %v\n%s", err, line)
		}
		got = append(got, r)
	}

	calm := got[0]
	if calm.Kind != string(stream.KindFeeStats) || calm.Ledger != 1000 {
		t.Fatalf("first record = %s @ %d, want fee_stats @ 1000", calm.Kind, calm.Ledger)
	}
	if calm.BaseFee != 100 || calm.Transactions != 4 {
		t.Errorf("calm ledger base fee/tx = %d/%d, want 100/4", calm.BaseFee, calm.Transactions)
	}
	// Charged per op: 100, 100, 100, 100.
	if d := calm.InclusionFeeCharged; d == nil || d.Min != 100 || d.Mode != 100 || d.P50 != 100 || d.P95 != 100 {
		t.Errorf("calm charged = %+v, want everything at the base fee", d)
	}
	// Bid per op: 100, 500, 300, 5000.
	if d := calm.InclusionFeeBid; d == nil || d.Min != 100 || d.P50 != 300 || d.P95 != 5000 || d.Max != 5000 {
		t.Errorf("calm bid = %+v, want min 100, median 300, p95 5000", d)
	}
	if calm.SurgePricing {
		t.Errorf("calm ledger reported surge pricing (%s)", calm.SurgeReason)
	}

	congested := got[1]
	if congested.Ledger != 1001 || congested.Transactions != 10 {
		t.Fatalf("second record = ledger %d with %d tx, want 1001 with 10", congested.Ledger, congested.Transactions)
	}
	// Charged per op, sorted: 150 150 150 200 200 200 200 300 400 1000.
	d := congested.InclusionFeeCharged
	if d == nil {
		t.Fatal("congested ledger has no charged distribution")
	}
	if d.Min != 150 || d.Mode != 200 || d.P10 != 150 || d.P50 != 200 || d.P90 != 400 || d.P95 != 1000 || d.Max != 1000 {
		t.Errorf("congested charged = %+v, want min 150, mode 200, p10 150, p50 200, p90 400, p95 1000", d)
	}
	if congested.CapacityUsage != 1 {
		t.Errorf("capacity usage = %v, want 1 (10 ops in a 10-op lane)", congested.CapacityUsage)
	}
	if !congested.SurgePricing || congested.SurgeReason != stream.SurgeReasonCapacity+"+"+stream.SurgeReasonFee {
		t.Errorf("surge = %v (%q), want capacity+fee", congested.SurgePricing, congested.SurgeReason)
	}
}

// TestFeeStatsTableShowsPercentiles checks the human-readable table: one line
// per ledger carrying the base fee, the inclusion fee percentiles and the
// surge status.
func TestFeeStatsTableShowsPercentiles(t *testing.T) {
	withFake(t, feeStatsFixture(t))

	stdout, stderr, err := run(t, "fee-stats", "--from", "1000")
	if err != nil {
		t.Fatalf("Execute() error = %v\nstderr: %s", err, stderr)
	}
	lines := nonEmptyLines(stdout)
	if len(lines) != 2 {
		t.Fatalf("printed %d lines, want one per ledger (2)\n%s", len(lines), stdout)
	}
	for _, want := range []string{"ledger 1000", "base fee 100 stroops", "min/mode/p10/p50/p95 100/100/100/100/100", "surge no"} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("calm line lacks %q\n%s", want, lines[0])
		}
	}
	for _, want := range []string{"ledger 1001", "min/mode/p10/p50/p95 150/200/150/200/1000", "capacity 100%", "surge yes (capacity+fee)"} {
		if !strings.Contains(lines[1], want) {
			t.Errorf("congested line lacks %q\n%s", want, lines[1])
		}
	}
}

// TestFeeStatsLimitStopsAfterOneLedger checks that --limit bounds the
// ledgers summarised, which is how --follow is exercised offline.
func TestFeeStatsLimitStopsAfterOneLedger(t *testing.T) {
	withFake(t, feeStatsFixture(t))

	stdout, stderr, err := run(t, "fee-stats", "--from", "1000", "--follow", "--json", "--limit", "1")
	if err != nil {
		t.Fatalf("Execute() error = %v\nstderr: %s", err, stderr)
	}
	if lines := nonEmptyLines(stdout); len(lines) != 1 {
		t.Fatalf("emitted %d records with --limit 1, want 1\n%s", len(lines), stdout)
	}
}
