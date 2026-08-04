// Package sink renders stream records to an output.
//
// Sinks decode XDR through stellar-xdr-lens rather than reimplementing it, so
// a value shown by prism is byte-for-byte what `lens decode` would show for
// the same payload.
package sink

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/stellar-optics/stellar-xdr-lens/pkg/lens"
	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// NDJSON writes one JSON object per line, so a stream pipes directly into jq
// or a log collector without any framing of its own.
//
// The line shape is stable and documented in the README:
//
//	{"kind","id","ledger","time", ...kind-specific fields}
//
// Decoded XDR is embedded under "topics" and "value" using the same
// {type, value} envelope lens emits, so a jq expression written against
// `lens decode --json` works unchanged here.
type NDJSON struct {
	w       *bufio.Writer
	enc     *json.Encoder
	decode  bool
	lensFmt *lensformat.JSONFormatter
}

// NewNDJSON returns a sink writing NDJSON to w.
//
// When decode is false the XDR is passed through in its base64 form, which is
// faster and keeps the output small for archival; when true, each payload is
// decoded through lens.
func NewNDJSON(w io.Writer, decode bool) *NDJSON {
	bw := bufio.NewWriter(w)
	enc := json.NewEncoder(bw)
	enc.SetEscapeHTML(false)
	return &NDJSON{
		w:      bw,
		enc:    enc,
		decode: decode,
		// Compact, because each record must occupy exactly one line.
		lensFmt: lensformat.NewJSONFormatter(lensformat.WithIndent("")),
	}
}

// eventLine is the documented NDJSON shape for an event.
type eventLine struct {
	Kind       string            `json:"kind"`
	ID         string            `json:"id"`
	Ledger     uint32            `json:"ledger"`
	Time       string            `json:"time,omitempty"`
	Type       string            `json:"type,omitempty"`
	ContractID string            `json:"contract,omitempty"`
	TxHash     string            `json:"tx,omitempty"`
	TxIndex    uint32            `json:"txIndex"`
	OpIndex    uint32            `json:"opIndex"`
	Topics     []json.RawMessage `json:"topics,omitempty"`
	Value      json.RawMessage   `json:"value,omitempty"`
	TopicsXDR  []string          `json:"topicsXdr,omitempty"`
	ValueXDR   string            `json:"valueXdr,omitempty"`
}

// ledgerLine is the documented NDJSON shape for a ledger close.
type ledgerLine struct {
	Kind        string          `json:"kind"`
	ID          string          `json:"id"`
	Ledger      uint32          `json:"ledger"`
	Time        string          `json:"time,omitempty"`
	Hash        string          `json:"hash,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
	MetadataXDR string          `json:"metadataXdr,omitempty"`
	HeaderXDR   string          `json:"headerXdr,omitempty"`
}

// Emit implements stream.Sink.
func (n *NDJSON) Emit(r stream.Record) error {
	var payload any

	switch r.Kind {
	case stream.KindEvent:
		line := eventLine{
			Kind:    string(r.Kind),
			ID:      r.ID,
			Ledger:  r.Sequence,
			Time:    formatTime(r.Time),
			TxIndex: r.Event.TxIndex,
			OpIndex: r.Event.OpIndex,
		}
		if r.Event != nil {
			line.Type = r.Event.Type
			line.ContractID = r.Event.ContractID
			line.TxHash = r.Event.TxHash

			if n.decode {
				for _, t := range r.Event.TopicXDR {
					raw, err := n.decodeXDR(t)
					if err != nil {
						return err
					}
					line.Topics = append(line.Topics, raw)
				}
				if r.Event.ValueXDR != "" {
					raw, err := n.decodeXDR(r.Event.ValueXDR)
					if err != nil {
						return err
					}
					line.Value = raw
				}
			} else {
				line.TopicsXDR = r.Event.TopicXDR
				line.ValueXDR = r.Event.ValueXDR
			}
		}
		payload = line

	case stream.KindLedger:
		line := ledgerLine{
			Kind:   string(r.Kind),
			ID:     r.ID,
			Ledger: r.Sequence,
			Time:   formatTime(r.Time),
		}
		if r.Close != nil {
			line.Hash = r.Close.Hash
			if n.decode && r.Close.MetadataXDR != "" {
				raw, err := n.decodeXDR(r.Close.MetadataXDR)
				if err != nil {
					return err
				}
				line.Metadata = raw
			} else {
				line.MetadataXDR = r.Close.MetadataXDR
				line.HeaderXDR = r.Close.HeaderXDR
			}
		}
		payload = line

	default:
		return fmt.Errorf("ndjson: unknown record kind %q", r.Kind)
	}

	if err := n.enc.Encode(payload); err != nil {
		return fmt.Errorf("ndjson: encoding record %s: %w", r.ID, err)
	}
	// Flush per record: a tail that buffers is a tail that appears to hang,
	// and anything downstream is waiting on these lines in real time.
	if err := n.w.Flush(); err != nil {
		return fmt.Errorf("ndjson: flushing record %s: %w", r.ID, err)
	}
	return nil
}

// decodeXDR renders a base64 payload through lens, producing the same
// {type, value} envelope that `lens decode --json` emits.
//
// A payload that will not decode is not fatal to a stream: the raw base64 is
// preserved under "xdr" alongside the error, so a single malformed value
// cannot terminate a long-running tail.
func (n *NDJSON) decodeXDR(payload string) (json.RawMessage, error) {
	value, err := lens.Decode(payload)
	if err != nil {
		fallback, mErr := json.Marshal(map[string]string{
			"xdr":   payload,
			"error": err.Error(),
		})
		if mErr != nil {
			return nil, fmt.Errorf("ndjson: encoding undecodable payload: %w", mErr)
		}
		return fallback, nil
	}

	var buf bytes.Buffer
	if err := n.lensFmt.Format(&buf, value); err != nil {
		return nil, fmt.Errorf("ndjson: formatting decoded payload: %w", err)
	}
	return json.RawMessage(bytes.TrimSpace(buf.Bytes())), nil
}

// Flush implements stream.Sink.
func (n *NDJSON) Flush() error {
	if err := n.w.Flush(); err != nil {
		return fmt.Errorf("ndjson: flush: %w", err)
	}
	return nil
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
