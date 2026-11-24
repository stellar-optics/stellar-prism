package cli

import (
	"fmt"
	"strings"

	"github.com/stellar-optics/stellar-prism/pkg/rules"
	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// ruleEngine wraps compiled expression filters behind a small name-based
// dispatch. CEP filters retain their own sliding-window state.
type ruleEngine struct {
	filters []stream.ExpressionFilter
}

// NewRuleEngine builds an engine from already-compiled expressions. A nil or
// empty input yields an engine that passes every event through.
func NewRuleEngine(filters ...stream.ExpressionFilter) *ruleEngine {

	return &ruleEngine{filters: append([]stream.ExpressionFilter(nil), filters...)}
}

// Match returns true when the record satisfies every compiled rule. A record
// that any rule rejects is dropped; a rule that errors on a particular record
// rejects it and reports the underlying reason through the returned error.
func (e *ruleEngine) Match(record stream.Record) (bool, error) {
	if len(e.filters) == 0 {
		return true, nil
	}
	for _, f := range e.filters {
		if f == nil {
			continue
		}
		ok, err := f.Match(record)
		if err != nil {
			return false, fmt.Errorf("rule %q: %w", f.Expression(), err)
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// Expressions returns the source expressions this engine was built from, in
// the order they were given to NewRuleEngine.
func (e *ruleEngine) Expressions() []string {
	out := make([]string, len(e.filters))
	for i, f := range e.filters {
		if f == nil {
			out[i] = ""
			continue
		}
		out[i] = f.Expression()
	}
	return out
}

// CompileRules parses each expression once and returns an engine holding the
// compiled filters. CEP expressions are compiled as stateful sliding windows.
func CompileRules(exprs ...string) (*ruleEngine, error) {
	return CompileRulesWithOptions(rules.Options{}, exprs...)
}

// CompileRulesWithOptions compiles expressions and applies options to any CEP
// rules in the input.
func CompileRulesWithOptions(options rules.Options, exprs ...string) (*ruleEngine, error) {
	if len(exprs) == 0 {
		return NewRuleEngine(), nil
	}
	compiled := make([]stream.ExpressionFilter, 0, len(exprs))
	for _, expr := range exprs {
		expr = strings.TrimSpace(expr)
		if expr == "" {
			continue
		}
		var (
			f   stream.ExpressionFilter
			err error
		)
		if rules.IsCEPExpression(expr) {
			f, err = rules.Compile(expr, options)
		} else {
			f, err = stream.CompileExpression(expr)
		}
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, f)
	}
	return NewRuleEngine(compiled...), nil
}

// CompileRulesWithWebhook configures the supplied webhook to receive each CEP
// alert as a JSON payload. Ordinary expression filters do not emit alerts.
func CompileRulesWithWebhook(webhook *sink.Webhook, exprs ...string) (*ruleEngine, error) {
	if webhook == nil {
		return nil, fmt.Errorf("rule engine: webhook is required")
	}
	return CompileRulesWithOptions(rules.Options{
		OnAlert: func(alert rules.Alert) error {
			return webhook.EmitJSON(alert)
		},
	}, exprs...)
}
