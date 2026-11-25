package stream

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// FeeStatsSource streams one fee-market summary per closed ledger.
//
// For each ledger it reads the header (base fee, base reserve, capacity) and
// the ledger's transactions, and derives:
//
//   - InclusionFeeCharged and InclusionFeeBid: nearest-rank percentiles of
//     the per-operation inclusion fee, one sample per transaction, with
//     Soroban resource fees removed so classic and Soroban fees compare;
//   - CapacityUsage: classic operations over the header's MaxTxSetSize;
//   - SurgePricing: true when the classic lane is full, or when every
//     transaction paid more than the base fee per operation.
//
// The methodology, the payload schema and client fee recipes are documented
// in docs/guides/fee_stats_streaming.md; keep that guide in step with any
// change to the derivation here.
type FeeStatsSource struct {
	client  rpc.Client
	ledgers *LedgerSource
	window  map[uint32][]int64
}

func NewFeeStatsSource(client rpc.Client, q LedgerQuery) (*FeeStatsSource, error) {
	if client == nil {
		return nil, fmt.Errorf("fee stats source: rpc client is required")
	}
	return &FeeStatsSource{client: client, ledgers: NewLedgerSource(client, q), window: make(map[uint32][]int64)}, nil
}
func (s *FeeStatsSource) Name() string { return "fee_stats" }
func (s *FeeStatsSource) Rewind(ledger uint32) {
	s.ledgers.Rewind(ledger)
	for seq := range s.window {
		if seq >= ledger {
			delete(s.window, seq)
		}
	}
}
func (s *FeeStatsSource) CursorLedger(cursor string) (uint32, bool) {
	return s.ledgers.CursorLedger(cursor)
}

func (s *FeeStatsSource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	b, err := s.ledgers.Fetch(ctx, cursor)
	if err != nil {
		return Batch{}, err
	}
	out := Batch{Records: make([]Record, 0, len(b.Records)), NextCursor: b.NextCursor, Latest: b.Latest, Oldest: b.Oldest, Full: b.Full}
	for _, ledger := range b.Records {
		if ledger.Close == nil || ledger.Close.HeaderXDR == "" {
			continue
		}
		var header xdr.LedgerHeaderHistoryEntry
		if err := xdr.SafeUnmarshalBase64(ledger.Close.HeaderXDR, &header); err != nil {
			return Batch{}, fmt.Errorf("fee stats source: ledger %d header: %w", ledger.Sequence, err)
		}
		resp, err := s.client.GetTransactions(ctx, protocol.GetTransactionsRequest{StartLedger: ledger.Sequence, Pagination: &protocol.LedgerPaginationOptions{Limit: 1000}})
		if err != nil {
			return Batch{}, fmt.Errorf("fee stats source: ledger %d transactions: %w", ledger.Sequence, err)
		}
		var fees []int64
		var samples []feeSample
		var resourceFee, feeBid int64
		for _, tx := range resp.Transactions {
			if tx.Ledger != ledger.Sequence {
				continue
			}
			sample := feeSample{charged: -1}
			var result xdr.TransactionResult
			if tx.ResultXDR != "" && xdr.SafeUnmarshalBase64(tx.ResultXDR, &result) == nil {
				fees = append(fees, int64(result.FeeCharged))
				sample.charged = int64(result.FeeCharged)
			}
			txResourceFee := sorobanResourceFee(tx.ResultMetaXDR)
			resourceFee += txResourceFee
			sample.resourceCharged = txResourceFee
			var env xdr.TransactionEnvelope
			if tx.EnvelopeXDR != "" && xdr.SafeUnmarshalBase64(tx.EnvelopeXDR, &env) == nil {
				if t, ok := envelopeTransaction(env); ok {
					if sd, ok := t.Ext.GetSorobanData(); ok {
						feeBid += int64(sd.ResourceFee)
						sample.soroban = true
						sample.resourceBid = int64(sd.ResourceFee)
					}
				}
				sample.ops, sample.bid = envelopeFeeTerms(env)
			}
			samples = append(samples, sample)
		}
		s.window[ledger.Sequence] = fees
		if len(s.window) > 50 {
			var seqs []uint32
			for seq := range s.window {
				seqs = append(seqs, seq)
			}
			sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
			for _, seq := range seqs[:len(seqs)-50] {
				delete(s.window, seq)
			}
		}
		stats := &FeeStats{BaseFee: uint32(header.Header.BaseFee), BaseReserve: uint32(header.Header.BaseReserve), FeePool: int64(header.Header.FeePool), CoinsInCirculation: int64(header.Header.TotalCoins), Transactions: uint32(len(fees)), SorobanResourceFee: resourceFee, SorobanFeeBid: feeBid}
		stats.AverageFee10 = averageFees(s.window, 10)
		stats.AverageFee50 = averageFees(s.window, 50)
		summarizeFeeMarket(stats, uint32(header.Header.MaxTxSetSize), samples)
		out.Records = append(out.Records, Record{ID: fmt.Sprintf("%010d", ledger.Sequence), Sequence: ledger.Sequence, Time: ledger.Time, Kind: KindFeeStats, FeeStats: stats})
	}
	return out, nil
}

func sorobanResourceFee(metaXDR string) int64 {
	if metaXDR == "" {
		return 0
	}
	var meta xdr.TransactionMeta
	if xdr.SafeUnmarshalBase64(metaXDR, &meta) != nil {
		return 0
	}
	var ext xdr.SorobanTransactionMetaExt
	if meta.V == 3 && meta.V3 != nil && meta.V3.SorobanMeta != nil {
		ext = meta.V3.SorobanMeta.Ext
	}
	if meta.V == 4 && meta.V4 != nil && meta.V4.SorobanMeta != nil {
		ext = meta.V4.SorobanMeta.Ext
	}
	if ext.V == 1 && ext.V1 != nil {
		return int64(ext.V1.TotalNonRefundableResourceFeeCharged + ext.V1.TotalRefundableResourceFeeCharged)
	}
	return 0
}

func averageFees(samples map[uint32][]int64, ledgers int) float64 {
	var sum int64
	var n int
	seqs := make([]uint32, 0, len(samples))
	for seq := range samples {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] > seqs[j] })
	if len(seqs) > ledgers {
		seqs = seqs[:ledgers]
	}
	for _, seq := range seqs {
		for _, fee := range samples[seq] {
			sum += fee
			n++
		}
	}
	if n == 0 {
		return 0
	}
	return float64(sum) / float64(n)
}

func envelopeTransaction(e xdr.TransactionEnvelope) (xdr.Transaction, bool) {
	switch e.Type {
	case xdr.EnvelopeTypeEnvelopeTypeTx:
		if e.V1 != nil {
			return e.V1.Tx, true
		}
	case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
		if e.FeeBump != nil && e.FeeBump.Tx.InnerTx.V1 != nil {
			return e.FeeBump.Tx.InnerTx.V1.Tx, true
		}
	}
	return xdr.Transaction{}, false
}

// feeSample is one transaction's contribution to a ledger's fee
// distributions.
type feeSample struct {
	// ops is the operation count fees are divided by: the transaction's
	// operations, plus one for a fee-bump wrapper. Zero means the envelope
	// could not be read, and the sample is skipped.
	ops int64
	// bid is the envelope's total fee bid; the outer fee for a fee bump.
	bid int64
	// charged is the fee charged, from the result. -1 means unknown.
	charged int64
	// soroban marks a transaction carrying Soroban resource data.
	soroban bool
	// resourceBid is the Soroban resource fee the envelope declared.
	resourceBid int64
	// resourceCharged is the Soroban resource fee actually charged.
	resourceCharged int64
}

// envelopeFeeTerms returns the operation count fees are divided by and the
// total fee bid. A fee bump counts as one extra operation, which is how the
// network prices it.
func envelopeFeeTerms(e xdr.TransactionEnvelope) (ops, bid int64) {
	switch e.Type {
	case xdr.EnvelopeTypeEnvelopeTypeTx:
		if e.V1 != nil {
			return int64(len(e.V1.Tx.Operations)), int64(e.V1.Tx.Fee)
		}
	case xdr.EnvelopeTypeEnvelopeTypeTxV0:
		if e.V0 != nil {
			return int64(len(e.V0.Tx.Operations)), int64(e.V0.Tx.Fee)
		}
	case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
		if e.FeeBump != nil && e.FeeBump.Tx.InnerTx.V1 != nil {
			return int64(len(e.FeeBump.Tx.InnerTx.V1.Tx.Operations)) + 1, int64(e.FeeBump.Tx.Fee)
		}
	}
	return 0, 0
}

// summarizeFeeMarket fills the capacity, distribution and surge fields of
// stats from one ledger's samples. stats.BaseFee must already be set.
//
// Each transaction contributes one per-operation value to each distribution:
// its inclusion fee (the fee with any Soroban resource fee removed) divided
// by its operation count, rounded down as the network rounds.
func summarizeFeeMarket(stats *FeeStats, maxTxSetSize uint32, samples []feeSample) {
	stats.MaxTxSetSize = maxTxSetSize
	var charged, bids []int64
	for _, s := range samples {
		if s.ops <= 0 {
			continue
		}
		stats.Operations += uint32(s.ops)
		if !s.soroban {
			stats.ClassicOperations += uint32(s.ops)
		}
		if s.charged >= 0 {
			inclusion := s.charged
			if s.soroban {
				inclusion -= s.resourceCharged
			}
			charged = append(charged, max(inclusion, 0)/s.ops)
		}
		bid := s.bid
		if s.soroban {
			bid -= s.resourceBid
		}
		bids = append(bids, max(bid, 0)/s.ops)
	}
	if maxTxSetSize > 0 {
		stats.CapacityUsage = float64(stats.ClassicOperations) / float64(maxTxSetSize)
	}
	stats.InclusionFeeCharged = feeDistribution(charged)
	stats.InclusionFeeBid = feeDistribution(bids)
	stats.SurgePricing, stats.SurgeReason = surgePricing(stats.BaseFee, stats.CapacityUsage, stats.InclusionFeeCharged)
}

// surgePricing derives the surge flag from the two signals a closed ledger
// carries:
//
//   - capacity: the classic lane reached the header's MaxTxSetSize, so more
//     was competing for inclusion than fitted;
//   - fee: even the cheapest transaction paid more than the base fee per
//     operation. Outside surge pricing the network charges every included
//     transaction exactly the base fee, so this only happens when bids
//     compete.
func surgePricing(baseFee uint32, capacityUsage float64, charged *FeeDistribution) (bool, string) {
	var reasons []string
	if capacityUsage >= 1 {
		reasons = append(reasons, SurgeReasonCapacity)
	}
	if charged != nil && baseFee > 0 && charged.Min > int64(baseFee) {
		reasons = append(reasons, SurgeReasonFee)
	}
	return len(reasons) > 0, strings.Join(reasons, "+")
}

// feeDistribution summarises values, or returns nil when there are none.
func feeDistribution(values []int64) *FeeDistribution {
	if len(values) == 0 {
		return nil
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	return &FeeDistribution{
		Min:  sorted[0],
		Mode: modeOf(sorted),
		P10:  nearestRank(sorted, 10),
		P50:  nearestRank(sorted, 50),
		P90:  nearestRank(sorted, 90),
		P95:  nearestRank(sorted, 95),
		P99:  nearestRank(sorted, 99),
		Max:  sorted[len(sorted)-1],
	}
}

// nearestRank returns the p-th percentile of an ascending slice by the
// nearest-rank method: the value at 1-based rank ceil(p/100 * n). It always
// returns an observed value, never an interpolation.
func nearestRank(sorted []int64, p int) int64 {
	rank := (p*len(sorted) + 99) / 100
	if rank < 1 {
		rank = 1
	}
	return sorted[rank-1]
}

// modeOf returns the most frequent value of an ascending slice; on a tie the
// smallest such value wins, so the result is deterministic.
func modeOf(sorted []int64) int64 {
	best, bestRun := sorted[0], 0
	for i := 0; i < len(sorted); {
		j := i
		for j < len(sorted) && sorted[j] == sorted[i] {
			j++
		}
		if run := j - i; run > bestRun {
			best, bestRun = sorted[i], run
		}
		i = j
	}
	return best
}
