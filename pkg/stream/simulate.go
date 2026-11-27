package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/stellar-optics/stellar-xdr-lens/pkg/lens"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// SimulationQuery describes a template transaction to re-simulate at every
// newly observed ledger head.
type SimulationQuery struct {
	TransactionEnvelope string
	StartLedger         uint32
}

// SimulationSource polls the live head and simulates the template once per
// newly closed ledger. The previous estimate is retained only for computing
// the next record's Changed fields.
type SimulationSource struct {
	client   rpc.Client
	query    SimulationQuery
	lastHead uint32
	previous *Simulation
}

// NewSimulationSource returns a source for a base64 TransactionEnvelope.
func NewSimulationSource(client rpc.Client, q SimulationQuery) (*SimulationSource, error) {
	if client == nil {
		return nil, fmt.Errorf("simulation source: rpc client is required")
	}
	if q.TransactionEnvelope == "" {
		return nil, fmt.Errorf("simulation source: transaction envelope is required")
	}
	return &SimulationSource{client: client, query: q}, nil
}

func (s *SimulationSource) Name() string { return "simulate" }

func (s *SimulationSource) CursorLedger(cursor string) (uint32, bool) {
	if cursor == "" {
		return 0, false
	}
	value, err := strconv.ParseUint(cursor, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(value), true
}

func (s *SimulationSource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	head, err := s.client.GetLatestLedger(ctx)
	if err != nil {
		return Batch{}, err
	}
	if head.Sequence <= s.lastHead {
		return Batch{NextCursor: strconv.FormatUint(uint64(s.lastHead), 10), Latest: head.Sequence}, nil
	}
	if s.lastHead == 0 && s.query.StartLedger > 0 && head.Sequence < s.query.StartLedger {
		return Batch{NextCursor: strconv.FormatUint(uint64(head.Sequence), 10), Latest: head.Sequence}, nil
	}

	resp, err := s.client.SimulateTransaction(ctx, protocol.SimulateTransactionRequest{
		Transaction: s.query.TransactionEnvelope,
	})
	if err != nil {
		return Batch{}, err
	}
	current, err := simulationFromResponse(head.Sequence, resp)
	if err != nil {
		return Batch{}, err
	}
	s.lastHead = head.Sequence
	if s.previous != nil {
		current.Changed = simulationChanges(s.previous, current)
		current.Previous = &SimulationSnapshot{
			ReturnValueXDR:     s.previous.ReturnValueXDR,
			Instructions:       s.previous.Instructions,
			DiskReadBytes:      s.previous.DiskReadBytes,
			WriteBytes:         s.previous.WriteBytes,
			MinResourceFee:     s.previous.MinResourceFee,
			ReadOnlyFootprint:  append([]string(nil), s.previous.ReadOnlyFootprint...),
			ReadWriteFootprint: append([]string(nil), s.previous.ReadWriteFootprint...),
			AuthXDR:            append([]string(nil), s.previous.AuthXDR...),
			EventsXDR:          append([]string(nil), s.previous.EventsXDR...),
		}
	}
	s.previous = current
	return Batch{
		Records: []Record{{
			ID:         fmt.Sprintf("%010d", head.Sequence),
			Sequence:   head.Sequence,
			Kind:       KindSimulation,
			Simulation: current,
		}},
		NextCursor: strconv.FormatUint(uint64(head.Sequence), 10),
		Latest:     head.Sequence,
	}, nil
}

func simulationFromResponse(ledger uint32, response protocol.SimulateTransactionResponse) (*Simulation, error) {
	simulation := &Simulation{
		Ledger:         ledger,
		Error:          response.Error,
		MinResourceFee: response.MinResourceFee,
		EventsXDR:      append([]string(nil), response.EventsXDR...),
		EventsJSON:     cloneRawMessages(response.EventsJSON),
	}
	for _, result := range response.Results {
		if simulation.ReturnValueXDR == "" && len(simulation.ReturnValueJSON) == 0 {
			if result.ReturnValueXDR != nil {
				simulation.ReturnValueXDR = *result.ReturnValueXDR
			}
			simulation.ReturnValueJSON = append(json.RawMessage(nil), result.ReturnValueJSON...)
		}
		if result.AuthXDR != nil {
			simulation.AuthXDR = append(simulation.AuthXDR, (*result.AuthXDR)...)
		}
		simulation.AuthJSON = append(simulation.AuthJSON, cloneRawMessages(result.AuthJSON)...)
	}
	if response.TransactionDataXDR != "" {
		var data xdr.SorobanTransactionData
		if err := xdr.SafeUnmarshalBase64(response.TransactionDataXDR, &data); err != nil {
			return nil, fmt.Errorf("decoding simulation transaction data: %w", err)
		}
		simulation.Instructions = uint32(data.Resources.Instructions)
		simulation.DiskReadBytes = uint32(data.Resources.DiskReadBytes)
		simulation.WriteBytes = uint32(data.Resources.WriteBytes)
		for _, key := range data.Resources.Footprint.ReadOnly {
			encoded, err := xdr.MarshalBase64(key)
			if err != nil {
				return nil, fmt.Errorf("encoding read-only footprint key: %w", err)
			}
			simulation.ReadOnlyFootprint = append(simulation.ReadOnlyFootprint, encoded)
		}
		for _, key := range data.Resources.Footprint.ReadWrite {
			encoded, err := xdr.MarshalBase64(key)
			if err != nil {
				return nil, fmt.Errorf("encoding read-write footprint key: %w", err)
			}
			simulation.ReadWriteFootprint = append(simulation.ReadWriteFootprint, encoded)
		}
	}
	return simulation, nil
}

func cloneRawMessages(messages []json.RawMessage) []json.RawMessage {
	if len(messages) == 0 {
		return nil
	}
	cloned := make([]json.RawMessage, len(messages))
	for i := range messages {
		cloned[i] = append(json.RawMessage(nil), messages[i]...)
	}
	return cloned
}

func simulationChanges(previous, current *Simulation) []string {
	var changed []string
	if !sameScVal(previous.ReturnValueXDR, current.ReturnValueXDR) ||
		!reflect.DeepEqual(previous.ReturnValueJSON, current.ReturnValueJSON) {
		changed = append(changed, "return_value")
	}
	if previous.Instructions != current.Instructions {
		changed = append(changed, "instructions")
	}
	if previous.DiskReadBytes != current.DiskReadBytes || previous.WriteBytes != current.WriteBytes {
		changed = append(changed, "memory")
	}
	if previous.MinResourceFee != current.MinResourceFee {
		changed = append(changed, "min_resource_fee")
	}
	if previous.Error != current.Error {
		changed = append(changed, "error")
	}
	if !reflect.DeepEqual(previous.ReadOnlyFootprint, current.ReadOnlyFootprint) ||
		!reflect.DeepEqual(previous.ReadWriteFootprint, current.ReadWriteFootprint) {
		changed = append(changed, "footprint")
	}
	if !reflect.DeepEqual(previous.AuthXDR, current.AuthXDR) ||
		!reflect.DeepEqual(previous.AuthJSON, current.AuthJSON) {
		changed = append(changed, "auth")
	}
	if !reflect.DeepEqual(previous.EventsXDR, current.EventsXDR) ||
		!reflect.DeepEqual(previous.EventsJSON, current.EventsJSON) {
		changed = append(changed, "events")
	}
	return changed
}

// sameScVal compares decoded ScVals when possible. This avoids treating two
// equivalent XDR spellings as a state change and keeps comparison semantics
// aligned with the lens decoder used by prism's sinks.
func sameScVal(left, right string) bool {
	if left == right {
		return true
	}
	if left == "" || right == "" {
		return false
	}
	a, errA := lens.DecodeAs(left, "ScVal")
	b, errB := lens.DecodeAs(right, "ScVal")
	if errA != nil || errB != nil {
		return false
	}
	return reflect.DeepEqual(a.Raw, b.Raw)
}
