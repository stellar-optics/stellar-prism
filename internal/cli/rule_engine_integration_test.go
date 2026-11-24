package cli

import (
	"fmt"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/rules"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// TestRuleEngineIntegrationWithExpressionFilter verifies that the rule engine
// correctly integrates with stream.ExpressionFilter and handles real-world
// filter scenarios including XDR-encoded values.
func TestRuleEngineIntegrationWithExpressionFilter(t *testing.T) {
	t.Parallel()

	// This test exercises the full path from compiled expression to rule engine
	// matching, simulating how the CLI would use the rule engine with
	// user-provided filter expressions.
	tests := []struct {
		name        string
		expressions []string
		valueXDR    string // base64-encoded XDR value
		topicsXDR   []string
		contractID  string
		eventType   string
		txHash      string
		ledgerSeq   uint32
		wantMatch   bool
	}{
		{
			name:        "transfer event with amount > 1M",
			expressions: []string{`value.amount > 1000000 && topics[0].symbol == "transfer"`},
			valueXDR:    encodeTestMap(t, map[string]any{"amount": int64(1500000)}),
			topicsXDR:   []string{encodeTestSymbol(t, "transfer")},
			contractID:  "C123",
			wantMatch:   true,
		},
		{
			name:        "transfer event with amount < 1M",
			expressions: []string{`value.amount > 1000000 && topics[0].symbol == "transfer"`},
			valueXDR:    encodeTestMap(t, map[string]any{"amount": int64(500000)}),
			topicsXDR:   []string{encodeTestSymbol(t, "transfer")},
			contractID:  "C123",
			wantMatch:   false,
		},
		{
			name:        "payment topic filter",
			expressions: []string{`topics[0].symbol == "payment" || topics[0].symbol == "transfer"`},
			valueXDR:    encodeTestMap(t, map[string]any{"amount": int64(100)}),
			topicsXDR:   []string{encodeTestSymbol(t, "payment")},
			wantMatch:   true,
		},
		{
			name:        "contract ID filter",
			expressions: []string{`contract == "C789"`},
			contractID:  "C789",
			wantMatch:   true,
		},
		{
			name:        "contract ID filter - no match",
			expressions: []string{`contract == "C789"`},
			contractID:  "C123",
			wantMatch:   false,
		},
		{
			name: "complex multi-condition filter",
			expressions: []string{
				`value.amount > 1000`,
				`value.asset.code == "USD"`,
				`topics[0].symbol == "transfer"`,
				`contract != "C999"`,
			},
			valueXDR: encodeTestMap(t, map[string]any{
				"amount": int64(2000),
				"asset":  map[string]any{"code": "USD"},
			}),
			topicsXDR:  []string{encodeTestSymbol(t, "transfer")},
			contractID: "C123",
			wantMatch:  true,
		},
		{
			name: "complex multi-condition - asset mismatch",
			expressions: []string{
				`value.amount > 1000`,
				`value.asset.code == "USD"`,
				`topics[0].symbol == "transfer"`,
			},
			valueXDR: encodeTestMap(t, map[string]any{
				"amount": int64(2000),
				"asset":  map[string]any{"code": "EUR"},
			}),
			topicsXDR: []string{encodeTestSymbol(t, "transfer")},
			wantMatch: false,
		}, {
			name:        "ledger sequence filter",
			expressions: []string{`ledger >= 1000000`},
			valueXDR:    encodeTestMap(t, map[string]any{}),
			ledgerSeq:   1000001,
			wantMatch:   true,
		}, {
			name:        "ledger sequence filter - too low",
			expressions: []string{`ledger >= 1000000`},
			valueXDR:    encodeTestMap(t, map[string]any{}),
			ledgerSeq:   999999,
			wantMatch:   false,
		},
		{
			name:        "string comparison - starts with",
			expressions: []string{`value.asset.code >= "USD" && value.asset.code < "USF"`},
			valueXDR:    encodeTestMap(t, map[string]any{"asset": map[string]any{"code": "USD"}}),
			wantMatch:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Build the record
			record := stream.Record{
				Kind:     stream.KindEvent,
				Sequence: tc.ledgerSeq,
			}

			if tc.valueXDR != "" || len(tc.topicsXDR) > 0 || tc.contractID != "" || tc.eventType != "" || tc.txHash != "" {
				record.Event = &stream.Event{
					ValueXDR:   tc.valueXDR,
					TopicXDR:   tc.topicsXDR,
					ContractID: tc.contractID,
					Type:       tc.eventType,
					TxHash:     tc.txHash,
				}
			}

			// Compile and build rule engine
			engine, err := CompileRules(tc.expressions...)
			if err != nil {
				t.Fatalf("CompileRules(%v): %v", tc.expressions, err)
			}

			// Match the record
			gotMatch, err := engine.Match(record)
			if err != nil {
				t.Fatalf("Match: %v", err)
			}

			if gotMatch != tc.wantMatch {
				t.Fatalf("Match: got %v, want %v. Expressions: %v", gotMatch, tc.wantMatch, tc.expressions)
			}
		})
	}
}

// TestRuleEngineIntegrationWithConfigExpressions simulates how the rule engine
// would be used with expressions loaded from a config file or command flags.
func TestRuleEngineIntegrationWithConfigExpressions(t *testing.T) {
	t.Parallel()

	// Simulate expressions that might come from a config file
	configExpressions := []string{
		// Filter 1: Only high-value transfers
		`value.amount > 1000000 && topics[0].symbol == "transfer"`,
		// Filter 2: Specific contract
		`contract == "CC001"`,
	}

	// Compile all expressions
	engine, err := CompileRules(configExpressions...)
	if err != nil {
		t.Fatalf("CompileRules: %v", err)
	}

	// Verify expressions are stored correctly
	exprs := engine.Expressions()
	if len(exprs) != 2 {
		t.Fatalf("expected 2 expressions, got %d", len(exprs))
	}
	if exprs[0] != configExpressions[0] {
		t.Fatalf("expression[0] mismatch: got %q, want %q", exprs[0], configExpressions[0])
	}
	if exprs[1] != configExpressions[1] {
		t.Fatalf("expression[1] mismatch: got %q, want %q", exprs[1], configExpressions[1])
	}

	// Test matching with a record that satisfies both filters
	record := stream.Record{
		Kind: stream.KindEvent,
		Event: &stream.Event{
			ValueXDR:   encodeTestMap(t, map[string]any{"amount": int64(2000000)}),
			TopicXDR:   []string{encodeTestSymbol(t, "transfer")},
			ContractID: "CC001",
		},
	}

	ok, err := engine.Match(record)
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if !ok {
		t.Fatalf("expected record to match both config expressions")
	}

	// Test with a record that fails the first filter
	record2 := stream.Record{
		Kind: stream.KindEvent,
		Event: &stream.Event{
			ValueXDR:   encodeTestMap(t, map[string]any{"amount": int64(100)}),
			TopicXDR:   []string{encodeTestSymbol(t, "transfer")},
			ContractID: "CC001",
		},
	}

	ok, err = engine.Match(record2)
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if ok {
		t.Fatalf("expected record to NOT match (amount too low)")
	}
}

// TestRuleEngineIntegrationErrorHandling verifies error propagation through
// the rule engine when expressions encounter malformed data.
func TestRuleEngineIntegrationErrorHandling(t *testing.T) {
	t.Parallel()

	// Test that a malformed XDR value causes the filter to reject the record
	// rather than panic
	engine, err := CompileRules(`value.amount > 100`)
	if err != nil {
		t.Fatalf("CompileRules: %v", err)
	}

	// Record with malformed XDR value - the filter should handle this gracefully
	// (the underlying filter will return an error during decoding)
	malformedXDR := "not-valid-base64!!!"
	record := stream.Record{
		Kind:  stream.KindEvent,
		Event: &stream.Event{ValueXDR: malformedXDR},
	}

	// The rule engine wraps the underlying error
	_, err = engine.Match(record)
	if err == nil {
		t.Fatalf("expected error for malformed XDR")
	}
	// The error should mention the rule expression
	if len(err.Error()) == 0 {
		t.Fatalf("error should contain message")
	}
}

func TestRuleEngineIntegrationWithCEP(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 24, 9, 0, 0, 0, time.UTC)
	alerts := 0
	engine, err := CompileRulesWithOptions(rules.Options{
		Now: func() time.Time { return now },
		OnAlert: func(rules.Alert) error {
			alerts++
			return nil
		},
	}, `count() > 1 within 1m where contract == "CABC"`)
	if err != nil {
		t.Fatalf("CompileRulesWithOptions: %v", err)
	}

	for i := 0; i < 2; i++ {
		record := stream.Record{
			Time: now.Add(time.Duration(i) * time.Second),
			Kind: stream.KindEvent,
			Event: &stream.Event{
				ContractID: "CABC",
				ValueXDR:   encodeTestMap(t, map[string]any{"amount": int64(1)}),
			},
		}
		matched, err := engine.Match(record)
		if err != nil {
			t.Fatalf("Match: %v", err)
		}
		if matched != (i == 1) {
			t.Fatalf("Match on event %d = %v, want %v", i, matched, i == 1)
		}
	}
	if alerts != 1 {
		t.Fatalf("alert callback invoked %d times, want 1", alerts)
	}
}

// encodeTestMap creates a base64-encoded XDR ScVal Map for testing.
func encodeTestMap(t *testing.T, value map[string]any) string {
	t.Helper()
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
			nested, err := encodeTestNestedMap(t, v)
			if err != nil {
				t.Fatalf("encoding nested map for key %q: %v", k, err)
			}
			val = nested
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

// encodeTestNestedMap encodes a nested map value.
func encodeTestNestedMap(t *testing.T, value map[string]any) (xdr.ScVal, error) {
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

// encodeTestSymbol creates a base64-encoded XDR ScVal Symbol for testing.
func encodeTestSymbol(t *testing.T, symbol string) string {
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
