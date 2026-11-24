package sink_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
)

func TestCursorAuditLoggerHashChainAndTamperDetection(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	now := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	logger, err := sink.NewCursorAuditLogger(&buf, sink.CursorAuditOptions{
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewCursorAuditLogger() error = %v", err)
	}
	for _, ledger := range []uint32{100, 101, 102} {
		if err := logger.Record(ledger); err != nil {
			t.Fatalf("Record(%d) error = %v", ledger, err)
		}
		now = now.Add(time.Second)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if _, err := sink.VerifyCursorAuditLog(strings.NewReader(strings.Join(lines, "\n"))); err != nil {
		t.Fatalf("VerifyCursorAuditLog() error = %v", err)
	}
	if len(lines) != 3 {
		t.Fatalf("got %d entries, want 3", len(lines))
	}
	lines[1] = strings.Replace(lines[1], `"ledger_seq":101`, `"ledger_seq":999`, 1)
	if _, err := sink.VerifyCursorAuditLog(strings.NewReader(strings.Join(lines, "\n"))); err == nil {
		t.Fatal("VerifyCursorAuditLog() accepted a tampered log")
	}
}

func TestCursorAuditLoggerRejectsOutOfOrderLedgers(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, err := sink.NewCursorAuditLogger(&buf)
	if err != nil {
		t.Fatalf("NewCursorAuditLogger() error = %v", err)
	}
	if err := logger.Record(10); err != nil {
		t.Fatal(err)
	}
	if err := logger.Record(9); err == nil {
		t.Fatal("Record() accepted a ledger going backwards")
	}
}
