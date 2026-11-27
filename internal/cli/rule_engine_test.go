package cli

import (
	"context"
	"fmt"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestNewRuleEngine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filters []stream.ExpressionFilter
		wantLen int
	}{
		{
			name:    "empty input yields empty engine",
			filters: nil,
			wantLen: 0,
		},
		{
			name:    "empty input yields empty engine with zero length",
			filters: []stream.ExpressionFilter{},
			wantLen: 0,
		},
		{
			name: "engine holds compiled filters",
			filters: []stream.ExpressionFilter{
				mustCompile(t, `value.amount > 100`),
				mustCompile(t, `topics[0].symbol == "transfer"`),
			},
			wantLen: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			engine := NewRuleEngine(tc.filters...)
			if tc.wantLen > 0 && len(engine.filters) != tc.wantLen {
				t.Fatalf("NewRuleEngine: got %d filters, want %d", len(engine.filters), tc.wantLen)
			}
			if tc.filters == nil {
				// nil input should still create a usable engine
				ok, err := engine.Match(stream.Record{})
				if err != nil {
					t.Fatalf("NewRuleEngine(nil): unexpected error: %v", err)
				}
				if !ok {
					t.Fatalf("NewRuleEngine(nil): expected match on empty record")
				}
			}
		})
	}
}

func TestNewRuleEngineNilFilter(t *testing.T) {
	t.Parallel()

	// When called with no arguments, the engine should be empty
	engine := NewRuleEngine()
	if len(engine.filters) != 0 {
		t.Fatalf("NewRuleEngine(): expected 0 filters, got %d", len(engine.filters))
	}

	// Matching with empty engine should pass
	ok, err := engine.Match(stream.Record{})
	if !ok {
		t.Fatalf("NewRuleEngine().Match: expected true, got false")
	}
	if err != nil {
		t.Fatalf("NewRuleEngine().Match: unexpected error: %v", err)
	}
}

func TestRuleEngineMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filters   []string
		record    stream.Record
		wantMatch bool
	}{
		{
			name:      "empty engine matches everything",
			filters:   nil,
			record:    stream.Record{Sequence: 1},
			wantMatch: true,
		},
		{
			name:      "single matching filter - numeric comparison",
			filters:   []string{`value.amount > 100`},
			record:    makeEventRecord(t, map[string]any{"amount": int64(150)}),
			wantMatch: true,
		},
		{
			name:      "single non-matching filter - numeric comparison",
			filters:   []string{`value.amount > 100`},
			record:    makeEventRecord(t, map[string]any{"amount": int64(50)}),
			wantMatch: false,
		},
		{
			name:      "multiple filters ANDed - all match",
			filters:   []string{`value.amount > 100`, `value.asset.code == "USD"`},
			record:    makeEventRecord(t, map[string]any{"amount": int64(150), "asset": map[string]any{"code": "USD"}}),
			wantMatch: true,
		},
		{
			name:      "multiple filters ANDed - one fails",
			filters:   []string{`value.amount > 100`, `value.asset.code == "USD"`},
			record:    makeEventRecord(t, map[string]any{"amount": int64(150), "asset": map[string]any{"code": "EUR"}}),
			wantMatch: false,
		},
		{
			name:      "non-event record does not match",
			filters:   []string{`value.amount > 100`},
			record:    stream.Record{Sequence: 1},
			wantMatch: false,
		},
		{
			name:      "expression with topic index - matches",
			filters:   []string{`topics[0].symbol == "transfer"`},
			record:    makeEventWithTopics(t, []map[string]any{{"symbol": "transfer"}}),
			wantMatch: true,
		},
		{
			name:      "expression with topic index - no match",
			filters:   []string{`topics[0].symbol == "transfer"`},
			record:    makeEventWithTopics(t, []map[string]any{{"symbol": "payment"}}),
			wantMatch: false,
		},
		{
			name:      "expression with contract field - matches",
			filters:   []string{`contract == "C123"`},
			record:    stream.Record{Kind: stream.KindEvent, Event: &stream.Event{ContractID: "C123"}},
			wantMatch: true,
		},
		{
			name:      "expression with contract field - no match",
			filters:   []string{`contract == "C123"`},
			record:    stream.Record{Kind: stream.KindEvent, Event: &stream.Event{ContractID: "C456"}},
			wantMatch: false,
		}, {
			name:      "expression with opIndex field - no match",
			filters:   []string{`opIndex > 0`},
			record:    stream.Record{Kind: stream.KindEvent, Event: &stream.Event{OpIndex: 0}},
			wantMatch: false,
		},
		{
			name:      "expression with tx field",
			filters:   []string{`tx == "abc123"`},
			record:    stream.Record{Kind: stream.KindEvent, Event: &stream.Event{TxHash: "abc123"}},
			wantMatch: true,
		},
		{
			name:      "expression with tx field - no match",
			filters:   []string{`tx == "abc123"`},
			record:    stream.Record{Kind: stream.KindEvent, Event: &stream.Event{TxHash: "def456"}},
			wantMatch: false,
		},
		{
			name:      "expression with ledger field - greater than",
			filters:   []string{`ledger > 1000`},
			record:    stream.Record{Sequence: 1001, Kind: stream.KindEvent, Event: &stream.Event{}},
			wantMatch: true,
		},
		{
			name:      "expression with ledger field - less than",
			filters:   []string{`ledger < 1000`},
			record:    stream.Record{Sequence: 999, Kind: stream.KindEvent, Event: &stream.Event{}},
			wantMatch: true,
		},
		{
			name:      "expression with txIndex field",
			filters:   []string{`txIndex > 0`},
			record:    stream.Record{Kind: stream.KindEvent, Event: &stream.Event{TxIndex: 1}},
			wantMatch: true,
		},
		{
			name:      "expression with opIndex field",
			filters:   []string{`opIndex > 0`},
			record:    stream.Record{Kind: stream.KindEvent, Event: &stream.Event{OpIndex: 1}},
			wantMatch: true,
		},
		{
			name:    "complex boolean expression - all conditions true",
			filters: []string{`value.amount > 100 && topics[0].symbol == "transfer" && contract != "C456"`},
			record: stream.Record{
				Kind:  stream.KindEvent,
				Event: &stream.Event{ValueXDR: encodeMap(t, map[string]any{"amount": int64(150)}), TopicXDR: []string{encodeSymbol(t, "transfer")}, ContractID: "C123"},
			},
			wantMatch: true,
		},
		{
			name:    "complex boolean expression - amount fails",
			filters: []string{`value.amount > 100 && topics[0].symbol == "transfer" && contract != "C456"`},
			record: stream.Record{
				Kind:  stream.KindEvent,
				Event: &stream.Event{ValueXDR: encodeMap(t, map[string]any{"amount": int64(50)}), TopicXDR: []string{encodeSymbol(t, "transfer")}, ContractID: "C123"},
			},
			wantMatch: false,
		},
		{
			name:    "complex boolean expression - topic fails",
			filters: []string{`value.amount > 100 && topics[0].symbol == "transfer" && contract != "C456"`},
			record: stream.Record{
				Kind:  stream.KindEvent,
				Event: &stream.Event{ValueXDR: encodeMap(t, map[string]any{"amount": int64(150)}), TopicXDR: []string{encodeSymbol(t, "payment")}, ContractID: "C123"},
			},
			wantMatch: false,
		},
		{
			name:    "complex boolean expression - contract fails",
			filters: []string{`value.amount > 100 && topics[0].symbol == "transfer" && contract != "C456"`},
			record: stream.Record{
				Kind:  stream.KindEvent,
				Event: &stream.Event{ValueXDR: encodeMap(t, map[string]any{"amount": int64(150)}), TopicXDR: []string{encodeSymbol(t, "transfer")}, ContractID: "C456"},
			},
			wantMatch: false,
		},
		{
			name:      "OR expression - left matches",
			filters:   []string{`value.amount > 100 || value.asset.code == "EUR"`},
			record:    makeEventRecord(t, map[string]any{"amount": int64(150), "asset": map[string]any{"code": "USD"}}),
			wantMatch: true,
		},
		{
			name:      "OR expression - right matches",
			filters:   []string{`value.amount > 100 || value.asset.code == "EUR"`},
			record:    makeEventRecord(t, map[string]any{"amount": int64(50), "asset": map[string]any{"code": "EUR"}}),
			wantMatch: true,
		},
		{
			name:      "OR expression - neither matches",
			filters:   []string{`value.amount > 100 || value.asset.code == "EUR"`},
			record:    makeEventRecord(t, map[string]any{"amount": int64(50), "asset": map[string]any{"code": "USD"}}),
			wantMatch: false,
		}, {
			name:      "NOT expression - true",
			filters:   []string{`!(value.asset.code == "EUR")`},
			record:    makeEventRecord(t, map[string]any{"amount": int64(150), "asset": map[string]any{"code": "USD"}}),
			wantMatch: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			filters := make([]stream.ExpressionFilter, 0, len(tc.filters))
			for _, expr := range tc.filters {
				f, err := stream.CompileExpression(expr)
				if err != nil {
					t.Fatalf("CompileExpression(%q): %v", expr, err)
				}
				filters = append(filters, f)
			}

			engine := NewRuleEngine(filters...)
			gotMatch, err := engine.Match(tc.record)

			if err != nil {
				t.Fatalf("Match: unexpected error: %v", err)
			}
			if gotMatch != tc.wantMatch {
				t.Fatalf("Match: got %v, want %v", gotMatch, tc.wantMatch)
			}
		})
	}
}

func TestRuleEngineExpressions(t *testing.T) {
	t.Parallel()

	engine := NewRuleEngine(
		mustCompile(t, `value.amount > 100`),
		mustCompile(t, `topics[0].symbol == "transfer"`),
	)

	exprs := engine.Expressions()
	if len(exprs) != 2 {
		t.Fatalf("Expressions: got %d expressions, want 2", len(exprs))
	}
	if exprs[0] != `value.amount > 100` {
		t.Fatalf("Expressions[0]: got %q, want %q", exprs[0], `value.amount > 100`)
	}
	if exprs[1] != `topics[0].symbol == "transfer"` {
		t.Fatalf("Expressions[1]: got %q, want %q", exprs[1], `topics[0].symbol == "transfer"`)
	}
}

func TestRuleEngineExpressionsWithNil(t *testing.T) {
	t.Parallel()

	// Test that NewRuleEngine() with no args produces empty engine
	engine := NewRuleEngine()
	exprs := engine.Expressions()
	if len(exprs) != 0 {
		t.Fatalf("Expressions with empty engine: got %d expressions, want 0", len(exprs))
	}
}

func TestCompileRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		exprs       []string
		wantErr     bool
		wantLen     int
		wantFilters []string
	}{
		{
			name:    "empty input returns empty engine",
			exprs:   nil,
			wantLen: 0,
		},
		{
			name:        "empty strings are skipped",
			exprs:       []string{"", "  ", `value.amount > 100`},
			wantLen:     1,
			wantFilters: []string{`value.amount > 100`},
		},
		{
			name:        "single valid expression",
			exprs:       []string{`value.amount > 100`},
			wantLen:     1,
			wantFilters: []string{`value.amount > 100`},
		},
		{
			name:        "multiple valid expressions",
			exprs:       []string{`value.amount > 100`, `topics[0].symbol == "transfer"`},
			wantLen:     2,
			wantFilters: []string{`value.amount > 100`, `topics[0].symbol == "transfer"`},
		},
		{
			name:    "invalid expression returns error",
			exprs:   []string{`invalid syntax !!!`},
			wantErr: true,
		},
		{
			name:    "mixed valid and invalid returns error",
			exprs:   []string{`value.amount > 100`, `invalid !!!`},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			engine, err := CompileRules(tc.exprs...)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("CompileRules: expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("CompileRules: unexpected error: %v", err)
			}

			if len(engine.filters) != tc.wantLen {
				t.Fatalf("CompileRules: got %d filters, want %d", len(engine.filters), tc.wantLen)
			}

			for i, want := range tc.wantFilters {
				got := engine.Expressions()[i]
				if got != want {
					t.Fatalf("CompileRules: filter[%d]: got %q, want %q", i, got, want)
				}
			}
		})
	}
}

func TestCompileRulesPreservesOrder(t *testing.T) {
	t.Parallel()

	expressions := []string{
		`value.amount > 100`,
		`topics[0].symbol == "transfer"`,
		`contract == "C123"`,
		`tx == "abc123"`,
	}

	engine, err := CompileRules(expressions...)
	if err != nil {
		t.Fatalf("CompileRules: %v", err)
	}

	got := engine.Expressions()
	if len(got) != len(expressions) {
		t.Fatalf("CompileRules: got %d expressions, want %d", len(got), len(expressions))
	}

	for i, want := range expressions {
		if got[i] != want {
			t.Fatalf("CompileRules: order[%d]: got %q, want %q", i, got[i], want)
		}
	}
}

func TestRuleEngineMatchRecordWithoutEvent(t *testing.T) {
	t.Parallel()

	// Rule engine should not match records without events
	engine := NewRuleEngine(mustCompile(t, `value.amount > 100`))

	// Record with no Kind set
	ok, err := engine.Match(stream.Record{Sequence: 1})
	if ok {
		t.Fatalf("expected no match for record without event")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Record with non-event kind
	ok, err = engine.Match(stream.Record{Kind: stream.KindLedger, Sequence: 1})
	if ok {
		t.Fatalf("expected no match for non-event record")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCompileRulesTrimSpace(t *testing.T) {
	t.Parallel()

	// Expressions with surrounding whitespace should be trimmed
	engine, err := CompileRules("  value.amount > 100  ")
	if err != nil {
		t.Fatalf("CompileRules: %v", err)
	}

	exprs := engine.Expressions()
	if len(exprs) != 1 {
		t.Fatalf("CompileRules: got %d expressions, want 1", len(exprs))
	}
	if exprs[0] != "value.amount > 100" {
		t.Fatalf("CompileRules: got %q, want %q (whitespace should be trimmed)", exprs[0], "value.amount > 100")
	}
}

func TestRuleEngineRoundTrip(t *testing.T) {
	t.Parallel()

	// Test that a compiled rule engine can be used to filter records correctly
	// This is an integration-style test that exercises the full flow
	engine, err := CompileRules(
		`value.amount > 1000`,
		`topics[0].symbol == "transfer"`,
		`contract == "C123"`,
	)
	if err != nil {
		t.Fatalf("CompileRules: %v", err)
	}

	// Create a record that should match
	record := stream.Record{
		Kind: stream.KindEvent,
		Event: &stream.Event{
			ValueXDR:   encodeMap(t, map[string]any{"amount": int64(2000)}),
			TopicXDR:   []string{encodeSymbol(t, "transfer")},
			ContractID: "C123",
			TxHash:     "abc123",
			TxIndex:    0,
			OpIndex:    0,
		},
		Sequence: 1000,
	}

	ok, err := engine.Match(record)
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if !ok {
		t.Fatalf("expected record to match all rules")
	}

	// Verify the expressions are correctly stored
	exprs := engine.Expressions()
	if len(exprs) != 3 {
		t.Fatalf("expected 3 expressions, got %d", len(exprs))
	}
}

// Helper functions

func mustCompile(t *testing.T, expr string) stream.ExpressionFilter {
	t.Helper()
	f, err := stream.CompileExpression(expr)
	if err != nil {
		t.Fatalf("CompileExpression(%q): %v", expr, err)
	}
	return f
}

func makeEventRecord(t *testing.T, value map[string]any) stream.Record {
	t.Helper()
	return stream.Record{
		Kind:  stream.KindEvent,
		Event: &stream.Event{ValueXDR: encodeMap(t, value)},
	}
}

func makeEventWithTopics(t *testing.T, topics []map[string]any) stream.Record {
	t.Helper()
	topicXDR := make([]string, len(topics))
	for i, topic := range topics {
		topicXDR[i] = encodeMap(t, topic)
	}
	return stream.Record{
		Kind:     stream.KindEvent,
		Event:    &stream.Event{TopicXDR: topicXDR},
		Sequence: 1000,
	}
}

func encodeMap(t *testing.T, value map[string]any) string {
	t.Helper()
	// Create a ScVal Map from the value map
	entries := make(xdr.ScMap, 0, len(value))
	for k, v := range value {
		key, err := xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol(k))
		if err != nil {
			t.Fatalf("encoding key %q: %v", k, err)
		}
		var val xdr.ScVal
		switch v := v.(type) {
		case int64:
			val, err = xdr.NewScVal(xdr.ScValTypeScvI128, xdr.Int128Parts{Hi: 0, Lo: xdr.Uint64(uint64(v))})
		case string:
			val, err = xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol(v))
		case map[string]any:
			val, err = encodeNestedMap(t, v)
		default:
			t.Fatalf("unsupported value type: %T", v)
		}
		if err != nil {
			t.Fatalf("encoding value %q: %v", k, err)
		}
		entries = append(entries, xdr.ScMapEntry{Key: key, Val: val})
	}
	payload, err := xdr.NewScVal(xdr.ScValTypeScvMap, &entries)
	if err != nil {
		t.Fatalf("creating map ScVal: %v", err)
	}
	encoded, err := xdr.MarshalBase64(payload)
	if err != nil {
		t.Fatalf("marshalling: %v", err)
	}
	return encoded
}

func encodeNestedMap(t *testing.T, value map[string]any) (xdr.ScVal, error) {
	t.Helper()
	entries := make(xdr.ScMap, 0, len(value))
	for k, v := range value {
		key, err := xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol(k))
		if err != nil {
			return xdr.ScVal{}, err
		}
		var val xdr.ScVal
		switch v := v.(type) {
		case int64:
			val, err = xdr.NewScVal(xdr.ScValTypeScvI128, xdr.Int128Parts{Hi: 0, Lo: xdr.Uint64(uint64(v))})
		case string:
			val, err = xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol(v))
		default:
			return xdr.ScVal{}, fmt.Errorf("unsupported nested value type: %T", v)
		}
		if err != nil {
			return xdr.ScVal{}, err
		}
		entries = append(entries, xdr.ScMapEntry{Key: key, Val: val})
	}
	return xdr.NewScVal(xdr.ScValTypeScvMap, &entries)
}

func encodeSymbol(t *testing.T, symbol string) string {
	t.Helper()
	val, err := xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol(symbol))
	if err != nil {
		t.Fatalf("encoding symbol %q: %v", symbol, err)
	}
	encoded, err := xdr.MarshalBase64(val)
	if err != nil {
		t.Fatalf("marshalling symbol: %v", err)
	}
	return encoded
}

type mockAlertSink struct {
	dispatched []sink.AlertNotification
}

func (m *mockAlertSink) Dispatch(ctx context.Context, alert sink.AlertNotification) error {
	m.dispatched = append(m.dispatched, alert)
	return nil
}

func (m *mockAlertSink) DispatchAsync(alert sink.AlertNotification) bool {
	m.dispatched = append(m.dispatched, alert)
	return true
}

func (m *mockAlertSink) Close() error {
	return nil
}

func TestRuleEngineAlertDispatch(t *testing.T) {
	engine := NewRuleEngine()
	mockSink := &mockAlertSink{}
	engine.AddAlertSink(mockSink)

	err := engine.AddAlertRule(AlertRule{
		Name:        "HighSeqAlert",
		Expression:  "ledger > 100",
		Severity:    "critical",
		Description: "Ledger sequence is high",
	})
	if err != nil {
		t.Fatalf("AddAlertRule() error = %v", err)
	}

	// Record with sequence 50 (should not trigger)
	recLow := stream.Record{
		Kind:     stream.KindEvent,
		Sequence: 50,
		Event: &stream.Event{
			TxHash:     "hash123",
			ContractID: "contract456",
		},
	}
	triggeredLow := engine.EvaluateAndAlert(context.Background(), recLow)
	if len(triggeredLow) != 0 {
		t.Errorf("expected 0 triggered alerts, got %d", len(triggeredLow))
	}
	if len(mockSink.dispatched) != 0 {
		t.Errorf("expected 0 dispatched alerts, got %d", len(mockSink.dispatched))
	}

	// Record with sequence 150 (should trigger)
	recHigh := stream.Record{
		Kind:     stream.KindEvent,
		Sequence: 150,
		Event: &stream.Event{
			TxHash:     "hash123",
			ContractID: "contract456",
		},
	}
	triggeredHigh := engine.EvaluateAndAlert(context.Background(), recHigh)
	if len(triggeredHigh) != 1 {
		t.Fatalf("expected 1 triggered alert, got %d", len(triggeredHigh))
	}
	if len(mockSink.dispatched) != 1 {
		t.Fatalf("expected 1 dispatched alert, got %d", len(mockSink.dispatched))
	}

	alert := mockSink.dispatched[0]
	if alert.RuleName != "HighSeqAlert" {
		t.Errorf("rule name = %q, want HighSeqAlert", alert.RuleName)
	}
	if alert.Severity != "critical" {
		t.Errorf("severity = %q, want critical", alert.Severity)
	}
	if alert.TxHash != "hash123" {
		t.Errorf("txHash = %q, want hash123", alert.TxHash)
	}

	if err := engine.Close(); err != nil {
		t.Errorf("engine.Close() error = %v", err)
	}
}
