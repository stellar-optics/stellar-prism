package sink

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
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
	if err := h.emitRecord(r); err != nil {
		return err
	}
	return h.Flush()
}

// EmitBatch implements stream.BatchSink, writing a whole batch before a
// single flush.
func (h *Human) EmitBatch(records []stream.Record) error {
	for _, r := range records {
		if err := h.emitRecord(r); err != nil {
			return err
		}
	}
	return h.Flush()
}

func (h *Human) emitRecord(r stream.Record) error {
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
	case stream.KindTransaction:
		if err := h.emitTransaction(r); err != nil {
			return err
		}
	case stream.KindStorageChange:
		if err := h.emitStorageChange(r); err != nil {
			return err
		}
	case stream.KindWASMDeployment:
		if err := h.emitWASMDeployment(r); err != nil {
			return err
		}
	case stream.KindFeeStats:
		if r.FeeStats == nil {
			return fmt.Errorf("human: fee stats record %s has no payload", r.ID)
		}
		f := r.FeeStats
		if _, err := fmt.Fprintf(h.w, "%s  ledger %d  base fee %d stroops  base reserve %d stroops  fee pool %d  coins %d  avg fee 10/50 %.2f/%.2f  tx %d  Soroban resource/bid %d/%d", formatTime(r.Time), r.Sequence, f.BaseFee, f.BaseReserve, f.FeePool, f.CoinsInCirculation, f.AverageFee10, f.AverageFee50, f.Transactions, f.SorobanResourceFee, f.SorobanFeeBid); err != nil {
			return fmt.Errorf("human: write: %w", err)
		}
		if d := f.InclusionFeeCharged; d != nil {
			if _, err := fmt.Fprintf(h.w, "  inclusion fee/op min/mode/p10/p50/p95 %d/%d/%d/%d/%d", d.Min, d.Mode, d.P10, d.P50, d.P95); err != nil {
				return fmt.Errorf("human: write: %w", err)
			}
		}
		surge := "no"
		if f.SurgePricing {
			surge = "yes (" + f.SurgeReason + ")"
		}
		if _, err := fmt.Fprintf(h.w, "  capacity %.0f%%  surge %s\n", f.CapacityUsage*100, surge); err != nil {
			return fmt.Errorf("human: write: %w", err)
		}
	case stream.KindTrade:
		if r.Trade == nil {
			return fmt.Errorf("human: trade record %s has no payload", r.ID)
		}
		t := r.Trade
		if _, err := fmt.Fprintf(h.w, "%s  ledger %d  %s %s  %s %s -> %s %s  price %s  trader %s", formatTime(r.Time), r.Sequence, t.Protocol, t.Operation, t.AmountIn, t.AssetIn, t.AmountOut, t.AssetOut, t.Price, t.Trader); err != nil {
			return fmt.Errorf("human: write: %w", err)
		}
		if t.Maker != "" {
			if _, err := fmt.Fprintf(h.w, "  maker %s", t.Maker); err != nil {
				return fmt.Errorf("human: write: %w", err)
			}
		}
		if t.AmountUnit != "" {
			if _, err := fmt.Fprintf(h.w, "  (%s)", t.AmountUnit); err != nil {
				return fmt.Errorf("human: write: %w", err)
			}
		}
		if _, err := h.w.WriteString("\n"); err != nil {
			return fmt.Errorf("human: write: %w", err)
		}
	case stream.KindOperation:
		if err := h.emitOperation(r); err != nil {
			return err
		}
	case stream.KindSimulation:
		if err := h.emitSimulation(r); err != nil {
			return err
		}
	default:
		return fmt.Errorf("human: unknown record kind %q", r.Kind)
	}

	return nil
}

func (h *Human) emitOperation(r stream.Record) error {
	op := r.Operation
	if op == nil {
		return fmt.Errorf("human: operation record %s has no payload", r.ID)
	}
	p := h.palette
	if _, err := fmt.Fprintf(h.w, "%s%s%s  %sledger %d%s  %soperation %s%s  %s%s%s\n",
		p.Note, formatTime(r.Time), p.Reset, p.Dim, r.Sequence, p.Reset,
		p.Type, op.Type, p.Reset, p.Key, op.SourceAccount, p.Reset); err != nil {
		return fmt.Errorf("human: write: %w", err)
	}
	if h.compact {
		return nil
	}
	if op.ParametersXDR != "" {
		if err := h.writePayload("parameters", op.ParametersXDR); err != nil {
			return err
		}
	}
	if op.ResultCode != "" {
		if _, err := fmt.Fprintf(h.w, "  %sresult %s%s\n", p.Key, op.ResultCode, p.Reset); err != nil {
			return fmt.Errorf("human: write: %w", err)
		}
	}
	if op.ResultXDR != "" {
		return h.writePayload("result", op.ResultXDR)
	}
	return nil
}

func (h *Human) emitSimulation(r stream.Record) error {
	sim := r.Simulation
	if sim == nil {
		return fmt.Errorf("human: simulation record %s has no payload", r.ID)
	}
	p := h.palette
	if _, err := fmt.Fprintf(h.w, "%sledger %d%s  %ssimulation%s  instructions %d  disk-read %d  write %d  min-fee %d",
		p.Note, sim.Ledger, p.Reset, p.Type, p.Reset, sim.Instructions, sim.DiskReadBytes, sim.WriteBytes, sim.MinResourceFee); err != nil {
		return fmt.Errorf("human: write: %w", err)
	}
	if sim.Error != "" {
		if _, err := fmt.Fprintf(h.w, "  %s%s%s", p.Remove, sim.Error, p.Reset); err != nil {
			return fmt.Errorf("human: write: %w", err)
		}
	}
	if len(sim.Changed) > 0 {
		if _, err := fmt.Fprintf(h.w, "  %schanged %s%s", p.Key, strings.Join(sim.Changed, ","), p.Reset); err != nil {
			return fmt.Errorf("human: write: %w", err)
		}
	}
	if _, err := h.w.WriteString("\n"); err != nil {
		return fmt.Errorf("human: write: %w", err)
	}
	if h.compact || sim.ReturnValueXDR == "" {
		return nil
	}
	return h.writePayload("return", sim.ReturnValueXDR)
}

func (h *Human) emitEvent(r stream.Record) error {
	p := h.palette
	e := r.Event
	if e == nil {
		return fmt.Errorf("human: event record %s has no payload", r.ID)
	}

	var b strings.Builder
	indent := ""
	if e.CallDepth > 0 {
		indent = strings.Repeat("   ", e.CallDepth-1)
		fmt.Fprintf(&b, "%s└─ [call %d] ", indent, e.CallDepth)
		indent += "   "
	}
	fmt.Fprintf(&b, "%s%s%s  %sledger %d%s  %s%s%s",
		p.Note, formatTime(r.Time), p.Reset,
		p.Dim, r.Sequence, p.Reset,
		p.Type, e.Type, p.Reset,
	)
	if e.ContractID != "" {
		fmt.Fprintf(&b, "  %s%s%s", p.Key, shorten(e.ContractID), p.Reset)
	}
	if e.ParentContractID != "" {
		fmt.Fprintf(&b, "  %sfrom %s%s", p.Dim, shorten(e.ParentContractID), p.Reset)
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
		if err := h.writePayload(fmt.Sprintf("%stopic[%d]", indent, i), t); err != nil {
			return err
		}
	}
	if e.ValueXDR != "" {
		if err := h.writePayload(indent+"value", e.ValueXDR); err != nil {
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
	if c.Consensus != nil {
		m := c.Consensus
		if _, err := fmt.Fprintf(h.w, "  consensus: signers=%v signature=%t quorum-data=%t\n", m.ValueSigners, m.SignaturePresent, m.QuorumDataAvailable); err != nil {
			return err
		}
		if m.CloseDeltaSeconds != nil {
			if _, err := fmt.Fprintf(h.w, "  close delta: %ds\n", *m.CloseDeltaSeconds); err != nil {
				return err
			}
		}
		for _, warning := range m.Warnings {
			if _, err := fmt.Fprintf(h.w, "  warning: %s\n", stream.SanitizeText(warning)); err != nil {
				return err
			}
		}
		if m.Error != "" {
			if _, err := fmt.Fprintf(h.w, "  consensus error: %s\n", stream.SanitizeText(m.Error)); err != nil {
				return err
			}
		}
	}
	if h.compact || c.MetadataXDR == "" {
		return nil
	}
	return h.writePayload("meta", c.MetadataXDR)
}

func (h *Human) emitTransaction(r stream.Record) error {
	p := h.palette
	t := r.Transaction
	if t == nil {
		return fmt.Errorf("human: transaction record %s has no payload", r.ID)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s%s%s  %sledger %d%s  %stx %s%s",
		p.Note, formatTime(r.Time), p.Reset,
		p.Dim, r.Sequence, p.Reset,
		p.Key, shorten(t.Hash), p.Reset,
	)
	if !t.Successful {
		fmt.Fprintf(&b, "  %sfailed%s", p.Remove, p.Reset)
		if t.FailedOpIndex != nil {
			fmt.Fprintf(&b, " at op %d", *t.FailedOpIndex)
			if t.FailedOpType != "" {
				fmt.Fprintf(&b, " (%s)", t.FailedOpType)
			}
		}
		if t.ErrorCode != "" {
			fmt.Fprintf(&b, ": %s", t.ErrorCode)
		} else if t.FailureReason != "" {
			fmt.Fprintf(&b, ": %s", t.FailureReason)
		}
	}
	if t.Failure.Trapped() {
		fmt.Fprintf(&b, "  %strapped%s", p.Remove, p.Reset)
	}
	if t.FeeBump != nil {
		fmt.Fprintf(&b, "  %sfee-bump by %s%s", p.Dim, shorten(t.FeeBump.FeeAccount), p.Reset)
	}
	b.WriteString("\n")

	if _, err := h.w.WriteString(b.String()); err != nil {
		return fmt.Errorf("human: write: %w", err)
	}
	for _, conflict := range t.Conflicts {
		if _, err := fmt.Fprintf(h.w, "  %sconflict%s  key %s  transactions %s\n",
			p.Remove, p.Reset, shorten(conflict.KeyXDR), strings.Join(conflict.Transactions, ", ")); err != nil {
			return fmt.Errorf("human: write: %w", err)
		}
	}
	memo, memoErr := stream.DecodeMemo(t.EnvelopeXDR)
	if memoErr != nil && t.Memo != nil {
		memo = *t.Memo
	}
	if memo.Type != "" {
		if _, err := fmt.Fprintf(h.w, "  memo %s: %s\n", memo.Type, stream.HumanMemo(memo)); err != nil {
			return err
		}
	}
	if h.compact {
		return nil
	}

	if t.Failure != nil {
		if err := h.emitFailure(t.Failure); err != nil {
			return err
		}
	}
	if t.FeeBump != nil {
		if err := h.emitFeeBump(t.FeeBump); err != nil {
			return err
		}
	}

	if t.EnvelopeXDR != "" {
		if err := h.writePayload("envelope", t.EnvelopeXDR); err != nil {
			return err
		}
	}
	if t.ResultXDR != "" {
		if err := h.writePayload("result", t.ResultXDR); err != nil {
			return err
		}
	}
	if t.ResultMetaXDR != "" {
		if err := h.writePayload("meta", t.ResultMetaXDR); err != nil {
			return err
		}
	}
	return nil
}

// emitFailure renders the decoded reason(s) for a failed transaction, naming
// the most specific level available: failing operation, fee-bump inner
// transaction, then the transaction-level code.
func (h *Human) emitFailure(f *stream.Failure) error {
	p := h.palette

	if f.Operation != nil {
		label := f.Operation.Type
		if label == "" {
			label = "operation"
		}
		if _, err := fmt.Fprintf(h.w, "  %sfailed operation %d (%s): %s: %s%s\n",
			p.Key, f.Operation.Index, label, f.Operation.Detail.Code,
			f.Operation.Detail.Summary, p.Reset); err != nil {
			return fmt.Errorf("human: write: %w", err)
		}
		if f.Operation.Detail.Hint != "" {
			if _, err := fmt.Fprintf(h.w, "  %shint: %s%s\n",
				p.Dim, f.Operation.Detail.Hint, p.Reset); err != nil {
				return fmt.Errorf("human: write: %w", err)
			}
		}
		if f.Operation.Trapped {
			if _, err := fmt.Fprintf(h.w, "  %scontract execution trapped or panicked%s\n",
				p.Remove, p.Reset); err != nil {
				return fmt.Errorf("human: write: %w", err)
			}
		}
	} else if f.Inner != nil {
		if _, err := fmt.Fprintf(h.w, "  %sinner reason: %s: %s%s\n",
			p.Key, f.Inner.Code, f.Inner.Summary, p.Reset); err != nil {
			return fmt.Errorf("human: write: %w", err)
		}
	} else {
		if _, err := fmt.Fprintf(h.w, "  %sreason: %s: %s%s\n",
			p.Key, f.Detail.Code, f.Detail.Summary, p.Reset); err != nil {
			return fmt.Errorf("human: write: %w", err)
		}
	}

	if f.Detail.Hint != "" && f.Operation == nil && f.Inner == nil {
		if _, err := fmt.Fprintf(h.w, "  %shint: %s%s\n", p.Dim, f.Detail.Hint, p.Reset); err != nil {
			return fmt.Errorf("human: write: %w", err)
		}
	}
	return nil
}

// emitFeeBump renders the wrapper and inner transaction details side by side,
// so the effective payer is never confused with the transaction's author.
func (h *Human) emitFeeBump(info *stream.FeeBump) error {
	p := h.palette
	if _, err := fmt.Fprintf(h.w, "  %sfee account %s, inner source %s%s\n",
		p.Key, info.FeeAccount, info.InnerSource, p.Reset); err != nil {
		return fmt.Errorf("human: write: %w", err)
	}
	if _, err := fmt.Fprintf(h.w, "  %sfees: max %d stroops, inner %d, charged %d%s\n",
		p.Dim, info.MaxFee, info.InnerFee, info.FeeCharged, p.Reset); err != nil {
		return fmt.Errorf("human: write: %w", err)
	}
	if info.InnerHash != "" {
		if _, err := fmt.Fprintf(h.w, "  %sinner tx %s%s\n", p.Dim, info.InnerHash, p.Reset); err != nil {
			return fmt.Errorf("human: write: %w", err)
		}
	}
	return nil
}

func (h *Human) emitStorageChange(r stream.Record) error {
	p := h.palette
	c := r.StorageChange
	if c == nil {
		return fmt.Errorf("human: storage change record %s has no payload", r.ID)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s%s%s  %sledger %d%s  %s%s%s  %s%s%s",
		p.Note, formatTime(r.Time), p.Reset,
		p.Dim, r.Sequence, p.Reset,
		p.Type, c.ChangeType, p.Reset,
		p.Key, c.EntryType, p.Reset,
	)
	if c.ContractID != "" {
		fmt.Fprintf(&b, "  %s%s%s", p.Key, shorten(c.ContractID), p.Reset)
	}
	if c.Durability != "" {
		fmt.Fprintf(&b, "  %s%s%s", p.Dim, c.Durability, p.Reset)
	}
	if c.WASMHash != "" {
		fmt.Fprintf(&b, "  %swasm %s%s", p.Dim, shorten(c.WASMHash), p.Reset)
	}
	if c.KeyHash != "" {
		fmt.Fprintf(&b, "  %skey %s%s", p.Dim, shorten(c.KeyHash), p.Reset)
	}
	if c.EntryType == "ttl" && c.LiveUntilLedger > 0 {
		fmt.Fprintf(&b, "  live until %d (%d ledgers)", c.LiveUntilLedger, c.RemainingTTL)
		if c.TTLWarning {
			fmt.Fprintf(&b, "  %sTTL WARNING%s", p.Remove, p.Reset)
		}
	}
	b.WriteString("\n")

	if _, err := h.w.WriteString(b.String()); err != nil {
		return fmt.Errorf("human: write: %w", err)
	}
	if h.compact {
		return nil
	}

	for _, payload := range []struct {
		label string
		xdr   string
	}{
		{"key", c.KeyXDR},
		{"entry", c.EntryXDR},
		{"change", c.ChangeXDR},
	} {
		if payload.xdr == "" {
			continue
		}
		if err := h.writePayload(payload.label, payload.xdr); err != nil {
			return err
		}
	}
	return nil
}

func (h *Human) emitWASMDeployment(r stream.Record) error {
	p := h.palette
	w := r.WASMDeployment
	if w == nil {
		return fmt.Errorf("human: wasm record %s has no payload", r.ID)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s%s%s  %sledger %d%s  %s%s%s",
		p.Note, formatTime(r.Time), p.Reset,
		p.Dim, r.Sequence, p.Reset,
		p.Type, w.Action, p.Reset,
	)
	if w.ContractID != "" {
		fmt.Fprintf(&b, "  %s%s%s", p.Key, shorten(w.ContractID), p.Reset)
	}
	if w.TxHash != "" {
		fmt.Fprintf(&b, "  %stx %s%s", p.Dim, shorten(w.TxHash), p.Reset)
	}
	if !w.Successful {
		fmt.Fprintf(&b, "  %sfailed%s", p.Remove, p.Reset)
	}
	b.WriteString("\n")

	if _, err := h.w.WriteString(b.String()); err != nil {
		return fmt.Errorf("human: write: %w", err)
	}
	if h.compact {
		return nil
	}

	if w.WASMHash != "" {
		if _, err := fmt.Fprintf(h.w, "  %swasm %s (%d bytes)%s\n",
			p.Key, w.WASMHash, w.WASMSize, p.Reset); err != nil {
			return fmt.Errorf("human: write: %w", err)
		}
	}
	if w.Executable != "" {
		if _, err := fmt.Fprintf(h.w, "  %sexecutable %s%s\n",
			p.Key, w.Executable, p.Reset); err != nil {
			return fmt.Errorf("human: write: %w", err)
		}
	}
	return nil
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

	sanitizeHumanNode(value.Node)
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

// Sanitize untrusted node values before the formatter adds its own trusted
// terminal colors and tree glyphs.
func sanitizeHumanNode(n *lens.Node) {
	if n == nil {
		return
	}
	terminalText := func(s string) string {
		var b strings.Builder
		for _, r := range stream.SanitizeText(s) {
			if r > 126 {
				q := strconv.QuoteRuneToASCII(r)
				b.WriteString(q[1 : len(q)-1])
			} else {
				b.WriteRune(r)
			}
		}
		return b.String()
	}
	if s, ok := n.Value.(string); ok {
		n.Value = terminalText(s)
	}
	n.Note = terminalText(n.Note)
	for _, child := range n.Children {
		sanitizeHumanNode(child)
	}
}
