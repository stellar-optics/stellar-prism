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
	Kind             string            `json:"kind"`
	ID               string            `json:"id"`
	Ledger           uint32            `json:"ledger"`
	Time             string            `json:"time,omitempty"`
	Type             string            `json:"type,omitempty"`
	ContractID       string            `json:"contract,omitempty"`
	TxHash           string            `json:"tx,omitempty"`
	TxIndex          uint32            `json:"txIndex"`
	OpIndex          uint32            `json:"opIndex"`
	CallDepth        int               `json:"callDepth,omitempty"`
	ParentContractID string            `json:"parentContract,omitempty"`
	InvocationPath   []string          `json:"invocationPath,omitempty"`
	Topics           []json.RawMessage `json:"topics,omitempty"`
	Value            json.RawMessage   `json:"value,omitempty"`
	TopicsXDR        []string          `json:"topicsXdr,omitempty"`
	ValueXDR         string            `json:"valueXdr,omitempty"`
}

// ledgerLine is the documented NDJSON shape for a ledger close.
type ledgerLine struct {
	Consensus   *stream.ConsensusMetrics `json:"consensus,omitempty"`
	Kind        string                   `json:"kind"`
	ID          string                   `json:"id"`
	Ledger      uint32                   `json:"ledger"`
	Time        string                   `json:"time,omitempty"`
	Hash        string                   `json:"hash,omitempty"`
	Metadata    json.RawMessage          `json:"metadata,omitempty"`
	MetadataXDR string                   `json:"metadataXdr,omitempty"`
	HeaderXDR   string                   `json:"headerXdr,omitempty"`
}

// transactionLine is the documented NDJSON shape for a transaction.
type transactionLine struct {
	Memo          *stream.SanitizedMemo `json:"memo,omitempty"`
	MemoError     string                `json:"memoError,omitempty"`
	Kind          string                `json:"kind"`
	ID            string                `json:"id"`
	Ledger        uint32                `json:"ledger"`
	Time          string                `json:"time,omitempty"`
	Hash          string                `json:"hash,omitempty"`
	Index         uint32                `json:"index"`
	Successful    bool                  `json:"successful"`
	Status        string                `json:"status,omitempty"`
	FeeAccount    string                `json:"feeAccount,omitempty"`
	FeeBump       *feeBumpLine          `json:"feeBump,omitempty"`
	Failure       *failureLine          `json:"failure,omitempty"`
	ErrorCode     string                `json:"error_code,omitempty"`
	ErrorDetail   string                `json:"error_detail,omitempty"`
	FailedOpIndex *uint32               `json:"failedOpIndex,omitempty"`
	FailedOpType  string                `json:"failedOpType,omitempty"`
	Envelope      json.RawMessage       `json:"envelope,omitempty"`
	Result        json.RawMessage       `json:"result,omitempty"`
	ResultMeta    json.RawMessage       `json:"resultMeta,omitempty"`
	EnvelopeXDR   string                `json:"envelopeXdr,omitempty"`
	ResultXDR     string                `json:"resultXdr,omitempty"`
	ResultMetaXDR string                `json:"resultMetaXdr,omitempty"`
	Conflicts     []conflictLine        `json:"conflicts,omitempty"`
}

type conflictLine struct {
	KeyXDR       string   `json:"keyXdr"`
	ContractID   string   `json:"contract,omitempty"`
	Transactions []string `json:"transactions"`
}

type operationLine struct {
	Kind          string          `json:"kind"`
	ID            string          `json:"id"`
	Ledger        uint32          `json:"ledger"`
	Time          string          `json:"time,omitempty"`
	Index         uint32          `json:"index"`
	Type          string          `json:"type"`
	SourceAccount string          `json:"sourceAccount,omitempty"`
	Parameters    json.RawMessage `json:"parameters,omitempty"`
	ParametersXDR string          `json:"parametersXdr,omitempty"`
	ResultCode    string          `json:"resultCode,omitempty"`
	Result        json.RawMessage `json:"result,omitempty"`
	ResultXDR     string          `json:"resultXdr,omitempty"`
	TxHash        string          `json:"tx,omitempty"`
}

type simulationLine struct {
	Kind           string                     `json:"kind"`
	ID             string                     `json:"id"`
	Ledger         uint32                     `json:"ledger"`
	Time           string                     `json:"time,omitempty"`
	Error          string                     `json:"error,omitempty"`
	ReturnValue    json.RawMessage            `json:"returnValue,omitempty"`
	ReturnValueXDR string                     `json:"returnValueXdr,omitempty"`
	Instructions   uint32                     `json:"instructions"`
	DiskReadBytes  uint32                     `json:"diskReadBytes"`
	WriteBytes     uint32                     `json:"writeBytes"`
	MinResourceFee int64                      `json:"minResourceFee"`
	Changed        []string                   `json:"changed,omitempty"`
	Previous       *stream.SimulationSnapshot `json:"previous,omitempty"`
}

// failureLine is the documented NDJSON shape for a decoded failure.
//
// It mirrors stream.Failure: the transaction-level code, the inner code a
// fee-bump wrapper reported, and the first failing operation each render as
// their own object, so a jq expression can select whichever level matters.
type failureLine struct {
	Code      string                `json:"code,omitempty"`
	Constant  string                `json:"constant,omitempty"`
	Summary   string                `json:"summary,omitempty"`
	Hint      string                `json:"hint,omitempty"`
	Headline  string                `json:"headline,omitempty"`
	Inner     *failureLine          `json:"inner,omitempty"`
	Operation *operationFailureLine `json:"operation,omitempty"`
}

// operationFailureLine is the documented NDJSON shape for the first failing
// operation of a transaction.
type operationFailureLine struct {
	Index    int    `json:"index"`
	Type     string `json:"type,omitempty"`
	Trapped  bool   `json:"trapped,omitempty"`
	Code     string `json:"code,omitempty"`
	Constant string `json:"constant,omitempty"`
	Summary  string `json:"summary,omitempty"`
	Hint     string `json:"hint,omitempty"`
}

// feeBumpLine is the documented NDJSON shape for fee-bump classification.
type feeBumpLine struct {
	FeeAccount      string `json:"feeAccount,omitempty"`
	InnerSource     string `json:"innerSource,omitempty"`
	InnerHash       string `json:"innerHash,omitempty"`
	MaxFee          int64  `json:"maxFee,omitempty"`
	InnerFee        int64  `json:"innerFee,omitempty"`
	FeeCharged      int64  `json:"feeCharged,omitempty"`
	InnerOperations int    `json:"innerOperations,omitempty"`
	OuterSignatures int    `json:"outerSignatures,omitempty"`
	InnerSignatures int    `json:"innerSignatures,omitempty"`
}

// storageChangeLine is the documented NDJSON shape for a storage mutation.
type storageChangeLine struct {
	Kind            string          `json:"kind"`
	ID              string          `json:"id"`
	Ledger          uint32          `json:"ledger"`
	Time            string          `json:"time,omitempty"`
	ChangeType      string          `json:"changeType,omitempty"`
	EntryType       string          `json:"entryType,omitempty"`
	ContractID      string          `json:"contract,omitempty"`
	Durability      string          `json:"durability,omitempty"`
	KeyHash         string          `json:"keyHash,omitempty"`
	WASMHash        string          `json:"wasmHash,omitempty"`
	Key             json.RawMessage `json:"key,omitempty"`
	Entry           json.RawMessage `json:"entry,omitempty"`
	Change          json.RawMessage `json:"change,omitempty"`
	KeyXDR          string          `json:"keyXdr,omitempty"`
	EntryXDR        string          `json:"entryXdr,omitempty"`
	ChangeXDR       string          `json:"changeXdr,omitempty"`
	LiveUntilLedger uint32          `json:"liveUntilLedger,omitempty"`
	RemainingTTL    uint32          `json:"remainingTtl,omitempty"`
	TTLWarning      bool            `json:"ttlWarning,omitempty"`
}

// wasmLine is the documented NDJSON shape for a deployment host function.
type wasmLine struct {
	Kind       string `json:"kind"`
	ID         string `json:"id"`
	Ledger     uint32 `json:"ledger"`
	Time       string `json:"time,omitempty"`
	Action     string `json:"action,omitempty"`
	ContractID string `json:"contract,omitempty"`
	WASMHash   string `json:"wasmHash,omitempty"`
	WASMSize   int    `json:"wasmSize,omitempty"`
	Executable string `json:"executable,omitempty"`
	TxHash     string `json:"tx,omitempty"`
	OpIndex    uint32 `json:"opIndex"`
	Successful bool   `json:"successful"`
}

// Emit implements stream.Sink.
func (n *NDJSON) Emit(r stream.Record) error {
	if err := n.encode(r); err != nil {
		return err
	}
	return n.Flush()
}

// EmitBatch implements stream.BatchSink, writing a whole batch before a
// single flush.
func (n *NDJSON) EmitBatch(records []stream.Record) error {
	for _, r := range records {
		if err := n.encode(r); err != nil {
			return err
		}
	}
	return n.Flush()
}

func (n *NDJSON) encode(r stream.Record) error {
	payload, err := n.document(r)
	if err != nil {
		return err
	}
	if err := n.enc.Encode(payload); err != nil {
		return fmt.Errorf("ndjson: encoding record %s: %w", r.ID, err)
	}
	return nil
}

// document builds the JSON shape for one record.
func (n *NDJSON) document(r stream.Record) (any, error) {
	var payload any

	switch r.Kind {
	case stream.KindEvent:
		line := eventLine{
			Kind:   string(r.Kind),
			ID:     r.ID,
			Ledger: r.Sequence,
			Time:   formatTime(r.Time),
		}
		if r.Event != nil {
			line.TxIndex = r.Event.TxIndex
			line.OpIndex = r.Event.OpIndex
			line.Type = r.Event.Type
			line.ContractID = r.Event.ContractID
			line.TxHash = r.Event.TxHash
			line.CallDepth = r.Event.CallDepth
			line.ParentContractID = r.Event.ParentContractID
			line.InvocationPath = r.Event.InvocationPath

			if n.decode {
				for _, t := range r.Event.TopicXDR {
					raw, err := n.decodeXDR(t)
					if err != nil {
						return nil, err
					}
					line.Topics = append(line.Topics, raw)
				}
				if r.Event.ValueXDR != "" {
					raw, err := n.decodeXDR(r.Event.ValueXDR)
					if err != nil {
						return nil, err
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
			line.Consensus = r.Close.Consensus
			if n.decode && r.Close.MetadataXDR != "" {
				raw, err := n.decodeXDR(r.Close.MetadataXDR)
				if err != nil {
					return nil, err
				}
				line.Metadata = raw
			} else {
				line.MetadataXDR = r.Close.MetadataXDR
				line.HeaderXDR = r.Close.HeaderXDR
			}
		}
		payload = line

	case stream.KindTransaction:
		line := transactionLine{
			Kind:   string(r.Kind),
			ID:     r.ID,
			Ledger: r.Sequence,
			Time:   formatTime(r.Time),
		}
		if r.Transaction != nil {
			line.Hash = r.Transaction.Hash
			memo, memoErr := stream.DecodeMemo(r.Transaction.EnvelopeXDR)
			if memoErr == nil {
				line.Memo = &memo
			} else {
				line.Memo = r.Transaction.Memo
				line.MemoError = r.Transaction.MemoError
			}
			line.Index = r.Transaction.Index
			line.Successful = r.Transaction.Successful
			line.ErrorCode = r.Transaction.ErrorCode
			line.ErrorDetail = r.Transaction.FailureReason
			line.FailedOpIndex = r.Transaction.FailedOpIndex
			line.FailedOpType = r.Transaction.FailedOpType
			line.Status = r.Transaction.Status
			line.FeeAccount = r.Transaction.FeeAccount
			line.FeeBump = feeBumpLineOf(r.Transaction.FeeBump)
			line.Failure = failureLineOf(r.Transaction.Failure)
			if n.decode {
				if r.Transaction.EnvelopeXDR != "" {
					raw, err := n.decodeXDR(r.Transaction.EnvelopeXDR)
					if err != nil {
						return nil, err
					}
					line.Envelope = raw
				}
				if r.Transaction.ResultXDR != "" {
					raw, err := n.decodeXDR(r.Transaction.ResultXDR)
					if err != nil {
						return nil, err
					}
					line.Result = raw
				}
				if r.Transaction.ResultMetaXDR != "" {
					raw, err := n.decodeXDR(r.Transaction.ResultMetaXDR)
					if err != nil {
						return nil, err
					}
					line.ResultMeta = raw
				}
			} else {
				line.EnvelopeXDR = r.Transaction.EnvelopeXDR
				line.ResultXDR = r.Transaction.ResultXDR
				line.ResultMetaXDR = r.Transaction.ResultMetaXDR
			}
			for _, conflict := range r.Transaction.Conflicts {
				line.Conflicts = append(line.Conflicts, conflictLine{
					KeyXDR: conflict.KeyXDR, ContractID: conflict.ContractID, Transactions: conflict.Transactions,
				})
			}
		}
		payload = line

	case stream.KindOperation:
		line := operationLine{Kind: string(r.Kind), ID: r.ID, Ledger: r.Sequence, Time: formatTime(r.Time)}
		if op := r.Operation; op != nil {
			line.Index, line.Type, line.SourceAccount, line.ResultCode, line.TxHash = op.Index, op.Type, op.SourceAccount, op.ResultCode, op.TransactionHash
			if n.decode {
				if op.ParametersXDR != "" {
					raw, err := n.decodeXDR(op.ParametersXDR)
					if err != nil {
						return err
					}
					line.Parameters = raw
				}
				if op.ResultXDR != "" {
					raw, err := n.decodeXDR(op.ResultXDR)
					if err != nil {
						return err
					}
					line.Result = raw
				}
			} else {
				line.ParametersXDR, line.ResultXDR = op.ParametersXDR, op.ResultXDR
			}
		}
		payload = line

	case stream.KindSimulation:
		line := simulationLine{Kind: string(r.Kind), ID: r.ID, Ledger: r.Sequence, Time: formatTime(r.Time)}
		if sim := r.Simulation; sim != nil {
			line.Ledger, line.Error = sim.Ledger, sim.Error
			line.Instructions, line.DiskReadBytes, line.WriteBytes, line.MinResourceFee = sim.Instructions, sim.DiskReadBytes, sim.WriteBytes, sim.MinResourceFee
			line.Changed, line.Previous = sim.Changed, sim.Previous
			if n.decode && sim.ReturnValueXDR != "" {
				raw, err := n.decodeXDR(sim.ReturnValueXDR)
				if err != nil {
					return err
				}
				line.ReturnValue = raw
			} else {
				line.ReturnValueXDR = sim.ReturnValueXDR
			}
		}
		payload = line

	case stream.KindStorageChange:
		line := storageChangeLine{
			Kind:   string(r.Kind),
			ID:     r.ID,
			Ledger: r.Sequence,
			Time:   formatTime(r.Time),
		}
		if c := r.StorageChange; c != nil {
			line.ChangeType = c.ChangeType
			line.EntryType = c.EntryType
			line.ContractID = c.ContractID
			line.Durability = c.Durability
			line.KeyHash = c.KeyHash
			line.WASMHash = c.WASMHash
			line.LiveUntilLedger = c.LiveUntilLedger
			line.RemainingTTL = c.RemainingTTL
			line.TTLWarning = c.TTLWarning
			if n.decode {
				if c.KeyXDR != "" {
					raw, err := n.decodeXDR(c.KeyXDR)
					if err != nil {
						return err
					}
					line.Key = raw
				}
				if c.EntryXDR != "" {
					raw, err := n.decodeXDR(c.EntryXDR)
					if err != nil {
						return err
					}
					line.Entry = raw
				}
				if c.ChangeXDR != "" {
					raw, err := n.decodeXDR(c.ChangeXDR)
					if err != nil {
						return err
					}
					line.Change = raw
				}
			} else {
				line.KeyXDR = c.KeyXDR
				line.EntryXDR = c.EntryXDR
				line.ChangeXDR = c.ChangeXDR
			}
		}
		payload = line

	case stream.KindWASMDeployment:
		line := wasmLine{
			Kind:   string(r.Kind),
			ID:     r.ID,
			Ledger: r.Sequence,
			Time:   formatTime(r.Time),
		}
		if w := r.WASMDeployment; w != nil {
			line.Action = w.Action
			line.ContractID = w.ContractID
			line.WASMHash = w.WASMHash
			line.WASMSize = w.WASMSize
			line.Executable = w.Executable
			line.TxHash = w.TxHash
			line.OpIndex = w.OpIndex
			line.Successful = w.Successful
		}
		payload = line

	case stream.KindFeeStats:
		if r.FeeStats == nil {
			return fmt.Errorf("ndjson: fee stats record %s has no payload", r.ID)
		}
		line := struct {
			Kind   string `json:"kind"`
			ID     string `json:"id"`
			Ledger uint32 `json:"ledger"`
			Time   string `json:"time,omitempty"`
			*stream.FeeStats
		}{Kind: string(r.Kind), ID: r.ID, Ledger: r.Sequence, Time: formatTime(r.Time), FeeStats: r.FeeStats}
		payload = line

	case stream.KindTrade:
		if r.Trade == nil {
			return fmt.Errorf("ndjson: trade record %s has no payload", r.ID)
		}
		line := struct {
			Kind   string `json:"kind"`
			ID     string `json:"id"`
			Ledger uint32 `json:"ledger"`
			Time   string `json:"time,omitempty"`
			*stream.Trade
		}{Kind: string(r.Kind), ID: r.ID, Ledger: r.Sequence, Time: formatTime(r.Time), Trade: r.Trade}
		payload = line

	default:
		return nil, fmt.Errorf("ndjson: unknown record kind %q", r.Kind)
	}

	return payload, nil
}

// EncodeRecord renders one record as the same JSON document NDJSON emits,
// without the newline framing. Broker sinks use it as the message body, so a
// Kafka or AMQP consumer sees exactly what `prism --json` would have printed.
func EncodeRecord(r stream.Record, decode bool) ([]byte, error) {
	n := &NDJSON{
		decode:  decode,
		lensFmt: lensformat.NewJSONFormatter(lensformat.WithIndent("")),
	}
	payload, err := n.document(r)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(payload); err != nil {
		return nil, fmt.Errorf("ndjson: encoding record %s: %w", r.ID, err)
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// feeBumpLineOf converts a classification to its wire shape, or returns nil
// when the transaction was not fee-bumped.
func feeBumpLineOf(info *stream.FeeBump) *feeBumpLine {
	if info == nil {
		return nil
	}
	return &feeBumpLine{
		FeeAccount:      info.FeeAccount,
		InnerSource:     info.InnerSource,
		InnerHash:       info.InnerHash,
		MaxFee:          info.MaxFee,
		InnerFee:        info.InnerFee,
		FeeCharged:      info.FeeCharged,
		InnerOperations: info.InnerOperations,
		OuterSignatures: info.OuterSignatures,
		InnerSignatures: info.InnerSignatures,
	}
}

// failureLineOf converts a decoded failure to its wire shape, preserving the
// level at which each code was reported.
func failureLineOf(failure *stream.Failure) *failureLine {
	if failure == nil {
		return nil
	}
	line := &failureLine{
		Code:     failure.Detail.Code,
		Constant: failure.Detail.Constant,
		Summary:  failure.Detail.Summary,
		Hint:     failure.Detail.Hint,
		Headline: failure.Headline,
	}
	if failure.Inner != nil {
		line.Inner = &failureLine{
			Code:     failure.Inner.Code,
			Constant: failure.Inner.Constant,
			Summary:  failure.Inner.Summary,
			Hint:     failure.Inner.Hint,
		}
	}
	if failure.Operation != nil {
		line.Operation = &operationFailureLine{
			Index:    failure.Operation.Index,
			Type:     failure.Operation.Type,
			Trapped:  failure.Operation.Trapped,
			Code:     failure.Operation.Detail.Code,
			Constant: failure.Operation.Detail.Constant,
			Summary:  failure.Operation.Detail.Summary,
			Hint:     failure.Operation.Detail.Hint,
		}
	}
	return line
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

	sanitizeJSONMemoNodes(value.Node, false)
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

// Preserve the lens envelope while sanitizing its decoded memo fields too.
func sanitizeJSONMemoNodes(n *lens.Node, inMemo bool) {
	if n == nil {
		return
	}
	inMemo = inMemo || n.TypeName == "Memo"
	if inMemo {
		if s, ok := n.Value.(string); ok {
			n.Value = stream.SanitizeText(s)
		}
	}
	for _, child := range n.Children {
		sanitizeJSONMemoNodes(child, inMemo)
	}
}
