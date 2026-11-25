package stream

import (
	"reflect"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestAverageFeesUsesMostRecentLedgers(t *testing.T) {
	samples := map[uint32][]int64{10: {100, 200}, 11: {300}, 12: {900}}
	if got := averageFees(samples, 2); got != 600 {
		t.Fatalf("two-ledger average = %v, want 600", got)
	}
	if got := averageFees(samples, 10); got != 375 {
		t.Fatalf("all-ledger average = %v, want 375", got)
	}
}

func TestNearestRank(t *testing.T) {
	t.Parallel()
	ten := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	twenty := make([]int64, 20)
	for i := range twenty {
		twenty[i] = int64(i + 1)
	}
	tests := []struct {
		name   string
		sorted []int64
		p      int
		want   int64
	}{
		{name: "single value at every percentile", sorted: []int64{100}, p: 99, want: 100},
		{name: "p10 of ten", sorted: ten, p: 10, want: 1},
		{name: "p50 of ten", sorted: ten, p: 50, want: 5},
		{name: "p90 of ten", sorted: ten, p: 90, want: 9},
		{name: "p95 of ten rounds up to the last", sorted: ten, p: 95, want: 10},
		{name: "p95 of twenty is the nineteenth", sorted: twenty, p: 95, want: 19},
		{name: "p99 of ten rounds up to the last", sorted: ten, p: 99, want: 10},
		{name: "p50 of an odd count is the middle", sorted: []int64{1, 2, 3}, p: 50, want: 2},
		{name: "p0 clamps to the first rank", sorted: ten, p: 0, want: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := nearestRank(tc.sorted, tc.p); got != tc.want {
				t.Fatalf("nearestRank(p%d) = %d, want %d", tc.p, got, tc.want)
			}
		})
	}
}

func TestFeeDistribution(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		values []int64
		want   *FeeDistribution
	}{
		{name: "no values", values: nil, want: nil},
		{
			name:   "unsorted input with a clear mode",
			values: []int64{500, 100, 100, 100, 200, 1000},
			want:   &FeeDistribution{Min: 100, Mode: 100, P10: 100, P50: 100, P90: 1000, P95: 1000, P99: 1000, Max: 1000},
		},
		{
			name:   "tied mode resolves to the smallest value",
			values: []int64{300, 200, 300, 200},
			want:   &FeeDistribution{Min: 200, Mode: 200, P10: 200, P50: 200, P90: 300, P95: 300, P99: 300, Max: 300},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := append([]int64(nil), tc.values...)
			got := feeDistribution(in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("feeDistribution = %+v, want %+v", got, tc.want)
			}
			if !reflect.DeepEqual(in, tc.values) && tc.values != nil {
				t.Fatalf("feeDistribution modified its input: %v", in)
			}
		})
	}
}

func TestSurgePricing(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		baseFee    uint32
		capacity   float64
		charged    *FeeDistribution
		wantSurge  bool
		wantReason string
	}{
		{name: "empty ledger", baseFee: 100},
		{name: "everyone paid the base fee", baseFee: 100, capacity: 0.4, charged: &FeeDistribution{Min: 100}},
		{name: "cheapest paid above base", baseFee: 100, capacity: 0.4, charged: &FeeDistribution{Min: 150}, wantSurge: true, wantReason: SurgeReasonFee},
		{name: "classic lane full", baseFee: 100, capacity: 1, charged: &FeeDistribution{Min: 100}, wantSurge: true, wantReason: SurgeReasonCapacity},
		{name: "both triggers", baseFee: 100, capacity: 1, charged: &FeeDistribution{Min: 200}, wantSurge: true, wantReason: SurgeReasonCapacity + "+" + SurgeReasonFee},
		{name: "unknown base fee never triggers on fee", baseFee: 0, capacity: 0.1, charged: &FeeDistribution{Min: 200}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			surge, reason := surgePricing(tc.baseFee, tc.capacity, tc.charged)
			if surge != tc.wantSurge || reason != tc.wantReason {
				t.Fatalf("surgePricing = (%v, %q), want (%v, %q)", surge, reason, tc.wantSurge, tc.wantReason)
			}
		})
	}
}

func TestSummarizeFeeMarket(t *testing.T) {
	t.Parallel()
	samples := []feeSample{
		// Classic, two operations, bid 1000 total, charged base fee.
		{ops: 2, bid: 1000, charged: 200},
		// Classic fee bump: one inner operation plus the wrapper.
		{ops: 2, bid: 600, charged: 400},
		// Soroban: resource fees are removed before the per-op division.
		{ops: 1, bid: 50_100, charged: 30_100, soroban: true, resourceBid: 50_000, resourceCharged: 30_000},
		// Unreadable envelope: skipped entirely.
		{ops: 0, bid: 0, charged: 100},
		// Result missing: contributes a bid but no charged sample.
		{ops: 1, bid: 300, charged: -1},
	}
	stats := &FeeStats{BaseFee: 100}
	summarizeFeeMarket(stats, 10, samples)

	if stats.Operations != 6 || stats.ClassicOperations != 5 {
		t.Fatalf("operations = %d (classic %d), want 6 (classic 5)", stats.Operations, stats.ClassicOperations)
	}
	if stats.MaxTxSetSize != 10 || stats.CapacityUsage != 0.5 {
		t.Fatalf("capacity = %d / %v, want 10 / 0.5", stats.MaxTxSetSize, stats.CapacityUsage)
	}
	// Charged per op: 100, 200, 100.
	wantCharged := &FeeDistribution{Min: 100, Mode: 100, P10: 100, P50: 100, P90: 200, P95: 200, P99: 200, Max: 200}
	if !reflect.DeepEqual(stats.InclusionFeeCharged, wantCharged) {
		t.Fatalf("charged = %+v, want %+v", stats.InclusionFeeCharged, wantCharged)
	}
	// Bid per op: 500, 300, 100, 300.
	wantBid := &FeeDistribution{Min: 100, Mode: 300, P10: 100, P50: 300, P90: 500, P95: 500, P99: 500, Max: 500}
	if !reflect.DeepEqual(stats.InclusionFeeBid, wantBid) {
		t.Fatalf("bid = %+v, want %+v", stats.InclusionFeeBid, wantBid)
	}
	if stats.SurgePricing {
		t.Fatalf("surge = %v (%q), want false: the cheapest paid the base fee and the lane was half full", stats.SurgePricing, stats.SurgeReason)
	}
}

func TestSummarizeFeeMarketEmptyLedger(t *testing.T) {
	t.Parallel()
	stats := &FeeStats{BaseFee: 100}
	summarizeFeeMarket(stats, 1000, nil)
	if stats.InclusionFeeCharged != nil || stats.InclusionFeeBid != nil || stats.SurgePricing || stats.CapacityUsage != 0 {
		t.Fatalf("empty ledger = %+v, want no distributions, no surge, zero usage", stats)
	}
}

func TestEnvelopeFeeTerms(t *testing.T) {
	t.Parallel()
	ops := func(n int) []xdr.Operation { return make([]xdr.Operation, n) }
	tests := []struct {
		name    string
		env     xdr.TransactionEnvelope
		wantOps int64
		wantBid int64
	}{
		{
			name:    "v1 transaction",
			env:     xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: &xdr.TransactionV1Envelope{Tx: xdr.Transaction{Fee: 300, Operations: ops(3)}}},
			wantOps: 3, wantBid: 300,
		},
		{
			name:    "v0 transaction",
			env:     xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTxV0, V0: &xdr.TransactionV0Envelope{Tx: xdr.TransactionV0{Fee: 100, Operations: ops(1)}}},
			wantOps: 1, wantBid: 100,
		},
		{
			name: "fee bump counts the wrapper as an operation",
			env: xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump, FeeBump: &xdr.FeeBumpTransactionEnvelope{Tx: xdr.FeeBumpTransaction{
				Fee:     900,
				InnerTx: xdr.FeeBumpTransactionInnerTx{Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: &xdr.TransactionV1Envelope{Tx: xdr.Transaction{Operations: ops(2)}}},
			}}},
			wantOps: 3, wantBid: 900,
		},
		{name: "missing body", env: xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTx}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotOps, gotBid := envelopeFeeTerms(tc.env)
			if gotOps != tc.wantOps || gotBid != tc.wantBid {
				t.Fatalf("envelopeFeeTerms = (%d, %d), want (%d, %d)", gotOps, gotBid, tc.wantOps, tc.wantBid)
			}
		})
	}
}
