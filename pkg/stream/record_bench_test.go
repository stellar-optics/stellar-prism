package stream

import (
	"fmt"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

func BenchmarkEventPipelineAllocations(b *testing.B) {
	events := make([]protocol.EventInfo, DefaultPageLimit)
	for i := range events {
		events[i] = protocol.EventInfo{
			ID:              fmt.Sprintf("%019d-0000000001", i+1),
			Ledger:          int32(i + 1),
			LedgerClosedAt:  "2024-01-01T00:00:00Z",
			EventType:       "contract",
			ContractID:      "CBGSB5J4Z3R6Y5YQ5J4Z3R6Y5YQ5J4Z3R6Y5YQ5J4Z3R6Y5YQ5J4Z3R",
			TransactionHash: "0123456789abcdef",
			TopicXDR:        []string{"AAAAAQ=="},
			ValueXDR:        "AAAAAgAAAAE=",
		}
	}

	b.Run("unpooled", func(b *testing.B) {
		benchmarkEventPage(b, events, false)
	})
	b.Run("pooled", func(b *testing.B) {
		benchmarkEventPage(b, events, true)
	})
}

func benchmarkEventPage(b *testing.B, events []protocol.EventInfo, pooled bool) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var records []Record
		if pooled {
			records = getRecordBuffer(len(events))
		} else {
			records = make([]Record, 0, len(events))
		}
		var seen map[string]struct{}
		if pooled {
			seen = getEventIDSet()
		} else {
			seen = make(map[string]struct{})
		}
		for _, event := range events {
			record := eventRecord(event)
			if _, exists := seen[record.ID]; !exists {
				seen[record.ID] = struct{}{}
				records = append(records, record)
			}
		}
		for _, record := range records {
			if record.Event == nil || record.Event.ContractID == "" {
				b.Fatal("event page contains an invalid record")
			}
		}
		if pooled {
			putRecordBuffer(records)
			putEventIDSet(seen)
		}
	}
}
