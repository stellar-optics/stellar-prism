package sink

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// The status bar is drawn below the scrolled event log without disturbing it:
// the cursor is saved, moved to the bottom row, the line is cleared and
// rewritten, and the cursor is restored. The save/restore pair is what keeps a
// concurrent log line from being overwritten.
const (
	ansiSaveCursor    = "\x1b7"
	ansiRestoreCursor = "\x1b8"
	ansiClearLine     = "\x1b[2K"
	ansiBottomRow     = "\x1b[999;1H"
)

// StatusBar renders a compact live stream summary to diagnostics while
// forwarding records unchanged. Disabled bars are transparent.
type StatusBar struct {
	next        stream.Sink
	diagnostics io.Writer
	enabled     bool
	now         func() time.Time

	mu     sync.Mutex
	ledger uint32
	lag    uint32
	events uint64
	drawn  bool
}

// StatusBarOptions configures a StatusBar.
type StatusBarOptions struct {
	// Enabled turns rendering on. The caller disables it when stdout is not a
	// terminal or the stream is not following.
	Enabled bool
	// Now supplies the clock used for the fallback lag. Tests set it; nil
	// means time.Now.
	Now func() time.Time
}

// NewStatusBar wraps next. The status line is suppressed unless Enabled.
func NewStatusBar(next stream.Sink, diagnostics io.Writer, opts StatusBarOptions) *StatusBar {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &StatusBar{next: next, diagnostics: diagnostics, enabled: opts.Enabled, now: now}
}

// Emit forwards r, then refreshes the footer with what the record says about
// the stream. A failed delivery is returned unchanged and does not claim
// progress.
func (s *StatusBar) Emit(r stream.Record) error {
	if err := s.next.Emit(r); err != nil {
		return err
	}
	if !s.enabled {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Kind == stream.KindEvent {
		s.events++
	}
	if r.Sequence > s.ledger {
		s.ledger = r.Sequence
	}
	s.renderLocked()
	return nil
}

// Update refreshes the footer from a poll result, even when the poll emitted
// nothing. This is what tells an operator watching a quiet contract that prism
// is alive and current rather than hung.
func (s *StatusBar) Update(progress stream.Progress) {
	if !s.enabled {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if progress.CurrentLedger > s.ledger {
		s.ledger = progress.CurrentLedger
	}
	s.lag = progress.Lag()
	s.renderLocked()
}

// Flush forwards Flush and clears the footer, leaving the terminal as it was
// found.
func (s *StatusBar) Flush() error {
	if s.enabled {
		s.mu.Lock()
		if s.drawn {
			fmt.Fprintf(s.diagnostics, "%s%s%s%s", ansiSaveCursor, ansiBottomRow, ansiClearLine, ansiRestoreCursor)
			s.drawn = false
		}
		s.mu.Unlock()
	}
	return s.next.Flush()
}

// renderLocked writes the bottom line. The caller holds s.mu.
func (s *StatusBar) renderLocked() {
	lag := time.Duration(s.lag) * stream.RoughLedgerCloseInterval
	line := fmt.Sprintf("[Ledger: %d | Events: %d | Lag: %s]",
		s.ledger, s.events, lag.Round(time.Second))

	fmt.Fprintf(s.diagnostics, "%s%s%s%s%s",
		ansiSaveCursor, ansiBottomRow, ansiClearLine, line, ansiRestoreCursor)
	s.drawn = true
}
