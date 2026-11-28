package stream

import (
	"sync"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// createMockLedgerCloseMeta builds a LedgerCloseMeta with high Soroban event activity.
func createMockLedgerCloseMeta(txCount, eventsPerTx int) *xdr.LedgerCloseMeta {
	txProcessing := make([]xdr.TransactionResultMeta, txCount)

	dummyContractID := xdr.ContractId([32]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
	})

	for i := 0; i < txCount; i++ {
		events := make([]xdr.ContractEvent, eventsPerTx)
		for j := 0; j < eventsPerTx; j++ {
			sym1 := xdr.ScSymbol("transfer")
			sym2 := xdr.ScSymbol("token")
			sym3 := xdr.ScSymbol("payment")
			valI64 := xdr.Int64(1000000 + i*100 + j)

			topics := []xdr.ScVal{
				{Type: xdr.ScValTypeScvSymbol, Sym: &sym1},
				{Type: xdr.ScValTypeScvSymbol, Sym: &sym2},
				{Type: xdr.ScValTypeScvSymbol, Sym: &sym3},
				{Type: xdr.ScValTypeScvI64, I64: &valI64},
			}

			dataVal := xdr.Int64(99999999)
			events[j] = xdr.ContractEvent{
				Ext:        xdr.ExtensionPoint{V: 0},
				ContractId: &dummyContractID,
				Type:       xdr.ContractEventTypeContract,
				Body: xdr.ContractEventBody{
					V: 0,
					V0: &xdr.ContractEventV0{
						Topics: topics,
						Data: xdr.ScVal{
							Type: xdr.ScValTypeScvI64,
							I64:  &dataVal,
						},
					},
				},
			}
		}

		txHash := xdr.Hash([32]byte{byte(i), 0xaa, 0xbb, 0xcc})
		txProcessing[i] = xdr.TransactionResultMeta{
			Result: xdr.TransactionResultPair{
				TransactionHash: txHash,
			},
			TxApplyProcessing: xdr.TransactionMeta{
				V: 3,
				V3: &xdr.TransactionMetaV3{
					SorobanMeta: &xdr.SorobanTransactionMeta{
						Events: events,
					},
				},
			},
		}
	}

	header := xdr.LedgerHeaderHistoryEntry{
		Hash: xdr.Hash([32]byte{0x01}),
		Header: xdr.LedgerHeader{
			LedgerSeq: 1234567,
			ScpValue: xdr.StellarValue{
				CloseTime: xdr.TimePoint(time.Now().Unix()),
			},
		},
	}

	return &xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: header,
			TxProcessing: txProcessing,
		},
	}
}

func TestExtractEventsEquivalence(t *testing.T) {
	meta := createMockLedgerCloseMeta(5, 4)

	standard := ExtractEventsFromLedgerCloseMetaStandard(meta)
	zeroCopy := ExtractEventsFromLedgerCloseMeta(meta)

	if len(standard) != len(zeroCopy) {
		t.Fatalf("length mismatch: standard=%d, zeroCopy=%d", len(standard), len(zeroCopy))
	}

	for i := range standard {
		s := standard[i]
		z := zeroCopy[i]

		if s.ID != z.ID {
			t.Errorf("item %d ID mismatch: %s vs %s", i, s.ID, z.ID)
		}
		if s.Sequence != z.Sequence {
			t.Errorf("item %d Sequence mismatch: %d vs %d", i, s.Sequence, z.Sequence)
		}
		if s.Event.ContractID != z.Event.ContractID {
			t.Errorf("item %d ContractID mismatch: %s vs %s", i, s.Event.ContractID, z.Event.ContractID)
		}
		if s.Event.Type != z.Event.Type {
			t.Errorf("item %d Type mismatch: %s vs %s", i, s.Event.Type, z.Event.Type)
		}
		if s.Event.TxHash != z.Event.TxHash {
			t.Errorf("item %d TxHash mismatch: %s vs %s", i, s.Event.TxHash, z.Event.TxHash)
		}
		if len(s.Event.TopicXDR) != len(z.Event.TopicXDR) {
			t.Fatalf("item %d TopicXDR length mismatch: %d vs %d", i, len(s.Event.TopicXDR), len(z.Event.TopicXDR))
		}
		for j := range s.Event.TopicXDR {
			if s.Event.TopicXDR[j] != z.Event.TopicXDR[j] {
				t.Errorf("item %d topic %d mismatch: %s vs %s", i, j, s.Event.TopicXDR[j], z.Event.TopicXDR[j])
			}
		}
		if s.Event.ValueXDR != z.Event.ValueXDR {
			t.Errorf("item %d ValueXDR mismatch: %s vs %s", i, s.Event.ValueXDR, z.Event.ValueXDR)
		}
	}
}

func TestExtractEventsAllocationReduction(t *testing.T) {
	meta := createMockLedgerCloseMeta(10, 5) // 50 events total

	allocsStandard := testing.AllocsPerRun(100, func() {
		_ = ExtractEventsFromLedgerCloseMetaStandard(meta)
	})

	allocsZeroCopy := testing.AllocsPerRun(100, func() {
		_ = ExtractEventsFromLedgerCloseMeta(meta)
	})

	allocsPooled := testing.AllocsPerRun(100, func() {
		records := ExtractEventsFromLedgerCloseMetaPooled(meta)
		ReleaseEvents(records)
	})

	reductionZeroCopy := (allocsStandard - allocsZeroCopy) / allocsStandard * 100
	reductionPooled := (allocsStandard - allocsPooled) / allocsStandard * 100

	t.Logf("Allocs per run - Standard: %.1f, ZeroCopy: %.1f (%.1f%% reduction), Pooled: %.1f (%.1f%% reduction)",
		allocsStandard, allocsZeroCopy, reductionZeroCopy, allocsPooled, reductionPooled)

	if reductionZeroCopy < 35.0 && reductionPooled < 35.0 {
		t.Fatalf("expected > 35%% reduction in heap allocations, got ZeroCopy=%.1f%%, Pooled=%.1f%%",
			reductionZeroCopy, reductionPooled)
	}
}

func TestExtractEventsZeroRace(t *testing.T) {
	meta := createMockLedgerCloseMeta(10, 5)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				records := ExtractEventsFromLedgerCloseMeta(meta)
				if len(records) != 50 {
					t.Errorf("expected 50 records, got %d", len(records))
				}

				pooled := ExtractEventsFromLedgerCloseMetaPooled(meta)
				if len(pooled) != 50 {
					t.Errorf("expected 50 pooled records, got %d", len(pooled))
				}
				ReleaseEvents(pooled)
			}
		}()
	}
	wg.Wait()
}

func BenchmarkExtractEvents_Standard(b *testing.B) {
	meta := createMockLedgerCloseMeta(50, 10) // 500 events
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		records := ExtractEventsFromLedgerCloseMetaStandard(meta)
		if len(records) == 0 {
			b.Fatal("no records extracted")
		}
	}
}

func BenchmarkExtractEvents_ZeroCopy(b *testing.B) {
	meta := createMockLedgerCloseMeta(50, 10) // 500 events
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		records := ExtractEventsFromLedgerCloseMeta(meta)
		if len(records) == 0 {
			b.Fatal("no records extracted")
		}
	}
}

func BenchmarkExtractEvents_Pooled(b *testing.B) {
	meta := createMockLedgerCloseMeta(50, 10) // 500 events
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		records := ExtractEventsFromLedgerCloseMetaPooled(meta)
		if len(records) == 0 {
			b.Fatal("no records extracted")
		}
		ReleaseEvents(records)
	}
}
