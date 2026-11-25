package stream

import (
	"go/parser"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestExpressionFilterArithmeticStringsAndNestedFields(t *testing.T) {
	t.Parallel()
	context := map[string]any{
		"value":    map[string]any{"amount": int64(150), "asset": map[string]any{"code": "USD"}},
		"topics":   []any{map[string]any{"symbol": "transfer"}},
		"topic":    []any{"transfer"},
		"contract": "C123",
	}
	cases := []struct {
		expr string
		want bool
	}{
		{`value.amount * 2 > 100 && value.asset.code == "USD"`, true},
		{`topics[0].symbol == "transfer" && contract != "C456"`, true},
		{`topic[0] == "transfer" && contains(topic[0], "fer")`, true},
		{`value.amount + 3 < 100`, false},
	}
	for _, tc := range cases {
		t.Run(tc.expr, func(t *testing.T) {
			parsed, err := parser.ParseExpr(tc.expr)
			if err != nil {
				t.Fatal(err)
			}
			if err := validateFilterAST(parsed); err != nil {
				t.Fatal(err)
			}
			got, err := evalFilterExpr(parsed, context)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestExpressionFilterReadsScValTopicsAndValue(t *testing.T) {
	t.Parallel()

	symbol, err := xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol("transfer"))
	if err != nil {
		t.Fatal(err)
	}
	topic, err := xdr.MarshalBase64(symbol)
	if err != nil {
		t.Fatal(err)
	}
	value, err := xdr.NewScVal(xdr.ScValTypeScvI128, xdr.Int128Parts{Hi: 0, Lo: xdr.Uint64(150)})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := xdr.MarshalBase64(value)
	if err != nil {
		t.Fatal(err)
	}

	filter, err := CompileExpression(`topic[0] == "transfer" && value > 100`)
	if err != nil {
		t.Fatal(err)
	}
	matched, err := filter.Match(Record{
		Kind: KindEvent,
		Event: &Event{
			TopicXDR: []string{topic},
			ValueXDR: payload,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Fatal("filter did not match the decoded topic and value")
	}
}

func TestExpressionFilterReadsNestedScValMapFields(t *testing.T) {
	key, err := xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol("amount"))
	if err != nil {
		t.Fatal(err)
	}
	amount, err := xdr.NewScVal(xdr.ScValTypeScvI128, xdr.Int128Parts{Hi: 0, Lo: xdr.Uint64(1_500_000_000)})
	if err != nil {
		t.Fatal(err)
	}
	entries := xdr.ScMap{{Key: key, Val: amount}}
	payload, err := xdr.NewScVal(xdr.ScValTypeScvMap, &entries)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := xdr.MarshalBase64(payload)
	if err != nil {
		t.Fatal(err)
	}
	filter, err := CompileExpression(`value.amount > 100`)
	if err != nil {
		t.Fatal(err)
	}
	matched, err := filter.Match(Record{Sequence: 7, Kind: KindEvent, Event: &Event{ValueXDR: encoded}})
	if err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Fatal("nested amount expression did not match")
	}
}

func TestCompileExpressionRejectsUnknownAndNonBoolean(t *testing.T) {
	t.Parallel()
	if _, err := CompileExpression(`os.Exit(0)`); err == nil {
		t.Fatal("CompileExpression accepted an unsupported call")
	}
	filter, err := CompileExpression(`value.amount + 1`)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := filter.(*expressionFilter); !ok {
		t.Fatalf("filter type %T", filter)
	}
}
