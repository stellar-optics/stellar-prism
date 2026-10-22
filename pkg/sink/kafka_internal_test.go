package sink

import (
	"testing"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// TestKafkaKeyAlwaysReturnsAKey pins the fallback chain, including the
// degenerate record with no payload, which must still key by record ID rather
// than falling through to an empty key.
func TestKafkaKeyAlwaysReturnsAKey(t *testing.T) {
	t.Parallel()

	bare := stream.Record{ID: "0000000001"}
	for _, by := range []KafkaPartition{PartitionContract, PartitionTx, PartitionRecord} {
		if got := string(kafkaKey(bare, by)); got != bare.ID {
			t.Errorf("kafkaKey(bare, %q) = %q, want the record ID %q", by, got, bare.ID)
		}
	}

	closeOnly := stream.Record{ID: "ledger", Close: &stream.LedgerClose{}}
	if got := string(kafkaKey(closeOnly, PartitionContract)); got != closeOnly.ID {
		t.Errorf("kafkaKey(ledger without hash, contract) = %q, want %q", got, closeOnly.ID)
	}
}
