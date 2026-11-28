package sink

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestStatusBarRendersANSIFooterAndForwards(t *testing.T) {
	var display bytes.Buffer
	var received int
	bar := NewStatusBar(stream.SinkFunc(func(stream.Record) error { received++; return nil }), &display, StatusBarOptions{Enabled: true})

	if err := bar.Emit(stream.Record{Kind: stream.KindLedger, Sequence: 50201020}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := bar.Emit(stream.Record{Kind: stream.KindEvent, Sequence: 50201020}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if received != 2 {
		t.Fatalf("forwarded records = %d, want 2", received)
	}

	out := display.String()
	for _, want := range []string{
		ansiSaveCursor,
		ansiBottomRow,
		ansiClearLine,
		ansiRestoreCursor,
		"[Ledger: 50201020 | Events: 1 | Lag: 0s]",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("status output %q does not contain %q", out, want)
		}
	}
}

func TestStatusBarUpdateRefreshesOnEveryPoll(t *testing.T) {
	var display bytes.Buffer
	bar := NewStatusBar(stream.SinkFunc(func(stream.Record) error { return nil }), &display, StatusBarOptions{Enabled: true})

	bar.Update(stream.Progress{CurrentLedger: 120, LatestLedger: 123})
	out := display.String()
	if !strings.Contains(out, "[Ledger: 120 | Events: 0 | Lag: 15s]") {
		t.Errorf("status output %q does not report the poll lag", out)
	}
	if count := strings.Count(out, ansiSaveCursor); count != 1 {
		t.Errorf("saved the cursor %d times, want 1", count)
	}
}

func TestStatusBarDisabledIsTransparent(t *testing.T) {
	var display bytes.Buffer
	var received int
	bar := NewStatusBar(stream.SinkFunc(func(stream.Record) error { received++; return nil }), &display, StatusBarOptions{Enabled: false})

	if err := bar.Emit(stream.Record{Kind: stream.KindEvent, Sequence: 7}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	bar.Update(stream.Progress{CurrentLedger: 7, LatestLedger: 9})
	if err := bar.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if received != 1 {
		t.Fatalf("forwarded records = %d, want 1", received)
	}
	if display.Len() != 0 {
		t.Fatalf("disabled status bar wrote %q", display.String())
	}
}

func TestStatusBarFlushClearsFooter(t *testing.T) {
	var display bytes.Buffer
	bar := NewStatusBar(stream.SinkFunc(func(stream.Record) error { return nil }), &display, StatusBarOptions{Enabled: true})

	if err := bar.Emit(stream.Record{Kind: stream.KindLedger, Sequence: 11}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := bar.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	// Every render saves and restores the cursor, so a balanced footer leaves
	// both halves present in the output.
	if saves, restores := strings.Count(display.String(), ansiSaveCursor), strings.Count(display.String(), ansiRestoreCursor); saves != restores {
		t.Errorf("cursor saves = %d, restores = %d", saves, restores)
	}
}
