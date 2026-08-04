package sink

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/stellar-optics/stellar-xdr-lens/pkg/lens"
	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// Human renders records for a terminal: a one-line header locating the
// record, followed by its decoded payload as a lens tree.
//
// The payload is rendered by lens's own TreeFormatter, so an ScVal shown by
// prism is identical to the same ScVal shown by `lens decode`. Only the
// header is prism's own.
type Human struct {
	w       *bufio.Writer
	palette lensformat.Palette
	tree    *lensformat.TreeFormatter
	compact bool
	first   bool
}

// NewHuman returns a sink writing human-readable output to w.
//
// compact suppresses the decoded payload tree, leaving only the header lines,
// which is what you want when watching for activity rather than reading
// values.
func NewHuman(w io.Writer, palette lensformat.Palette, compact bool) *Human {
	return &Human{
		w:       bufio.NewWriter(w),
		palette: palette,
		tree:    lensformat.NewTreeFormatter(lensformat.WithPalette(palette)),
		compact: compact,
		first:   true,
	}
}

// Emit implements stream.Sink.
func (h *Human) Emit(r stream.Record) error {
	// Blank line between records, but not before the first, so the stream
	// starts flush against the prompt.
	if !h.first {
		if _, err := h.w.WriteString("\n"); err != nil {
			return fmt.Errorf("human: write: %w", err)
		}
	}
	h.first = false

	switch r.Kind {
	case stream.KindEvent:
		if err := h.emitEvent(r); err != nil {
			return err
		}
	case stream.KindLedger:
		if err := h.emitLedger(r); err != nil {
			return err
		}
	default:
		return fmt.Errorf("human: unknown record kind %q", r.Kind)
	}

	// Flush per record: a tail that buffers looks like a tail that has hung.
	if err := h.w.Flush(); err != nil {
		return fmt.Errorf("human: flush: %w", err)
	}
	return nil
}

func (h *Human) emitEvent(r stream.Record) error {
	p := h.palette
	e := r.Event
	if e == nil {
		return fmt.Errorf("human: event record %s has no payload", r.ID)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s%s%s  %sledger %d%s  %s%s%s",
		p.Note, formatTime(r.Time), p.Reset,
		p.Dim, r.Sequence, p.Reset,
		p.Type, e.Type, p.Reset,
	)
	if e.ContractID != "" {
		fmt.Fprintf(&b, "  %s%s%s", p.Key, shorten(e.ContractID), p.Reset)
	}
	if e.TxHash != "" {
		fmt.Fprintf(&b, "  %stx %s%s", p.Dim, shorten(e.TxHash), p.Reset)
	}
	b.WriteString("\n")

	if _, err := h.w.WriteString(b.String()); err != nil {
		return fmt.Errorf("human: write: %w", err)
	}
	if h.compact {
		return nil
	}

	for i, t := range e.TopicXDR {
		if err := h.writePayload(fmt.Sprintf("topic[%d]", i), t); err != nil {
			return err
		}
	}
	if e.ValueXDR != "" {
		if err := h.writePayload("value", e.ValueXDR); err != nil {
			return err
		}
	}
	return nil
}

func (h *Human) emitLedger(r stream.Record) error {
	p := h.palette
	c := r.Close
	if c == nil {
		return fmt.Errorf("human: ledger record %s has no payload", r.ID)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s%s%s  %sledger %d%s",
		p.Note, formatTime(r.Time), p.Reset,
		p.Key, r.Sequence, p.Reset,
	)
	if c.Hash != "" {
		fmt.Fprintf(&b, "  %s%s%s", p.Dim, shorten(c.Hash), p.Reset)
	}
	b.WriteString("\n")

	if _, err := h.w.WriteString(b.String()); err != nil {
		return fmt.Errorf("human: write: %w", err)
	}
	if h.compact || c.MetadataXDR == "" {
		return nil
	}
	return h.writePayload("meta", c.MetadataXDR)
}

// writePayload renders one base64 XDR value through lens, indented under its
// label.
//
// A value that will not decode is reported inline rather than aborting: one
// malformed payload must not kill a long-running tail.
func (h *Human) writePayload(label, payload string) error {
	p := h.palette

	if _, err := fmt.Fprintf(h.w, "  %s%s%s\n", p.Key, label, p.Reset); err != nil {
		return fmt.Errorf("human: write: %w", err)
	}

	value, err := lens.Decode(payload)
	if err != nil {
		if _, wErr := fmt.Fprintf(h.w, "    %s<undecodable: %v>%s\n", p.Remove, err, p.Reset); wErr != nil {
			return fmt.Errorf("human: write: %w", wErr)
		}
		return nil
	}

	var buf bytes.Buffer
	if err := h.tree.Format(&buf, value); err != nil {
		return fmt.Errorf("human: formatting payload: %w", err)
	}
	return h.writeIndented(buf.String(), "    ")
}

func (h *Human) writeIndented(s, indent string) error {
	for line := range strings.SplitSeq(strings.TrimRight(s, "\n"), "\n") {
		if _, err := fmt.Fprintf(h.w, "%s%s\n", indent, line); err != nil {
			return fmt.Errorf("human: write: %w", err)
		}
	}
	return nil
}

// Flush implements stream.Sink.
func (h *Human) Flush() error {
	if err := h.w.Flush(); err != nil {
		return fmt.Errorf("human: flush: %w", err)
	}
	return nil
}

// shorten abbreviates a long identifier for inline display, keeping enough of
// both ends to stay recognisable. It matches the abbreviation lens uses in
// its explain output.
func shorten(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[:5] + "…" + s[len(s)-4:]
}
