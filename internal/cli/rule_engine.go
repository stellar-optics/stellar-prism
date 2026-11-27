package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// AlertSink is the interface implemented by alert notification destinations (e.g. Telegram, PagerDuty).
type AlertSink interface {
	Dispatch(ctx context.Context, alert sink.AlertNotification) error
	DispatchAsync(alert sink.AlertNotification) bool
	Close() error
}

// AlertRule pairs a filter expression with alert metadata such as name and severity.
type AlertRule struct {
	Name        string
	Expression  string
	Filter      stream.ExpressionFilter
	Severity    string
	Description string
}

// ruleEngine wraps compiled stream.ExpressionFilter predicates and optional alert dispatchers.
type ruleEngine struct {
	filters    []stream.ExpressionFilter
	alertRules []AlertRule
	alertSinks []AlertSink
}

// NewRuleEngine builds an engine from already-compiled expressions. A nil or
// empty input yields an engine that passes every event through.
func NewRuleEngine(filters ...stream.ExpressionFilter) *ruleEngine {
	return &ruleEngine{
		filters: append([]stream.ExpressionFilter(nil), filters...),
	}
}

// AddAlertSink registers an alert notification destination (e.g. Telegram or PagerDuty).
func (e *ruleEngine) AddAlertSink(s AlertSink) {
	if s != nil {
		e.alertSinks = append(e.alertSinks, s)
	}
}

// AddAlertRule registers a named rule with specific severity for alert dispatching.
func (e *ruleEngine) AddAlertRule(rule AlertRule) error {
	if rule.Filter == nil && rule.Expression != "" {
		f, err := stream.CompileExpression(rule.Expression)
		if err != nil {
			return fmt.Errorf("compiling alert rule %q: %w", rule.Name, err)
		}
		rule.Filter = f
	}
	if rule.Severity == "" {
		rule.Severity = "info"
	}
	e.alertRules = append(e.alertRules, rule)
	return nil
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

// EvaluateAndAlert tests the record against all registered alert rules and dispatches
// alerts asynchronously to all configured sinks for any matching rules.
func (e *ruleEngine) EvaluateAndAlert(ctx context.Context, record stream.Record) []sink.AlertNotification {
	if len(e.alertRules) == 0 || len(e.alertSinks) == 0 {
		return nil
	}

	var triggered []sink.AlertNotification

	for _, r := range e.alertRules {
		if r.Filter == nil {
			continue
		}
		matched, err := r.Filter.Match(record)
		if err == nil && matched {
			alert := sink.AlertNotification{
				RuleName:  r.Name,
				Severity:  r.Severity,
				LedgerSeq: record.Sequence,
				Timestamp: record.Time,
				Message:   r.Description,
			}
			if alert.Message == "" {
				alert.Message = fmt.Sprintf("Rule %q triggered", r.Name)
			}
			if !record.Time.IsZero() {
				alert.Timestamp = record.Time
			} else {
				alert.Timestamp = time.Now()
			}
			if record.Event != nil {
				alert.TxHash = record.Event.TxHash
				alert.ContractID = record.Event.ContractID
				alert.EventIndex = record.Event.EventIndex
			}

			triggered = append(triggered, alert)

			// Non-blocking dispatch to all sinks
			for _, s := range e.alertSinks {
				s.DispatchAsync(alert)
			}
		}
	}

	return triggered
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

// CompileRules is a thin wrapper around stream.CompileExpression for
// consumer code that prefers an explicit compile step. It parses the
// expression once and returns a rule engine holding the compiled form.
func CompileRules(exprs ...string) (*ruleEngine, error) {
	if len(exprs) == 0 {
		return NewRuleEngine(), nil
	}
	compiled := make([]stream.ExpressionFilter, 0, len(exprs))
	for _, expr := range exprs {
		expr = strings.TrimSpace(expr)
		if expr == "" {
			continue
		}
		f, err := stream.CompileExpression(expr)
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, f)
	}
	return NewRuleEngine(compiled...), nil
}

// Close gracefully closes any registered alert sinks.
func (e *ruleEngine) Close() error {
	var errs []string
	for _, s := range e.alertSinks {
		if err := s.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("closing alert sinks: %s", strings.Join(errs, "; "))
	}
	return nil
}
