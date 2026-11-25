package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// ruleEngine wraps compiled stream.ExpressionFilter predicates behind a small
// name-based dispatch the CLI can wire from flags or config files without
// re-parsing the expression language itself.
//
// The engine is deliberately stateless: compilation is done once by the caller,
// and Match is a pure function of the record. That keeps the evaluator
// offline-first and trivially testable without a network or a process.
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

type Rule struct {
	Name      string
	Condition string
	Action    RuleAction
}

type RuleAction struct {
	Type    string
	URL     string
	Webhook string
	Slack   string
}

type RuleMatch struct {
	Rule   Rule
	Record stream.Record
}

type RulesEngine struct {
	rules  []Rule
	bound  []stream.ExpressionFilter
	client *http.Client
	queue  chan RuleMatch
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	stderr func(string, ...any)
}

func NewRulesEngine(path string) (*RulesEngine, error) {
	return LoadRulesEngine(path)
}

func LoadRules(path string) (*RulesEngine, error) {
	return LoadRulesEngine(path)
}

func LoadRulesEngine(path string) (*RulesEngine, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading rules file %q: %w", path, err)
	}
	rules, err := parseRulesYAML(data)
	if err != nil {
		return nil, fmt.Errorf("parsing rules file %q: %w", path, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	engine := &RulesEngine{
		rules: rules, client: &http.Client{Timeout: 10 * time.Second},
		queue: make(chan RuleMatch, 256), ctx: ctx, cancel: cancel,
		stderr: func(string, ...any) {},
	}
	engine.bound = make([]stream.ExpressionFilter, 0, len(rules))
	for _, rule := range rules {
		filter, err := stream.CompileExpression(rule.Condition)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("rule %q: %w", rule.Name, err)
		}
		engine.bound = append(engine.bound, filter)
	}
	engine.wg.Add(1)
	go engine.runDispatcher()
	return engine, nil
}

func parseRulesYAML(data []byte) ([]Rule, error) {
	type rawRule struct {
		Name      string    `yaml:"name"`
		Condition string    `yaml:"condition"`
		Action    yaml.Node `yaml:"action"`
		URL       string    `yaml:"url"`
		Webhook   string    `yaml:"webhook"`
		Slack     string    `yaml:"slack"`
	}
	var wrapped struct {
		Rules []rawRule `yaml:"rules"`
	}
	if err := yaml.Unmarshal(data, &wrapped); err != nil {
		return nil, err
	}
	raw := wrapped.Rules
	if raw == nil {
		if err := yaml.Unmarshal(data, &raw); err != nil {
			return nil, err
		}
	}
	rules := make([]Rule, 0, len(raw))
	for i, r := range raw {
		r.Name = strings.TrimSpace(r.Name)
		r.Condition = strings.TrimSpace(r.Condition)
		if r.Name == "" {
			return nil, fmt.Errorf("rule %d: name is required", i+1)
		}
		if r.Condition == "" {
			return nil, fmt.Errorf("rule %q: condition is required", r.Name)
		}
		if _, err := stream.CompileExpression(r.Condition); err != nil {
			return nil, fmt.Errorf("rule %q: %w", r.Name, err)
		}
		action := RuleAction{URL: r.URL, Webhook: r.Webhook, Slack: r.Slack}
		if r.Action.Kind != 0 {
			if r.Action.Kind == yaml.ScalarNode {
				action.Type = strings.ToLower(strings.TrimSpace(r.Action.Value))
			} else {
				var object struct {
					Type    string `yaml:"type"`
					URL     string `yaml:"url"`
					Webhook string `yaml:"webhook"`
					Slack   string `yaml:"slack"`
				}
				if err := r.Action.Decode(&object); err != nil {
					return nil, fmt.Errorf("rule %q: invalid action: %w", r.Name, err)
				}
				action.Type = strings.ToLower(strings.TrimSpace(object.Type))
				action.URL = firstRuleValue(strings.TrimSpace(object.URL), action.URL)
				action.Webhook = firstRuleValue(strings.TrimSpace(object.Webhook), action.Webhook)
				action.Slack = firstRuleValue(strings.TrimSpace(object.Slack), action.Slack)
			}
		}
		if action.Type == "" {
			switch {
			case action.Webhook != "" || action.URL != "" && action.Type == "":
				action.Type = "webhook"
			case action.Slack != "":
				action.Type = "slack"
			default:
				action.Type = "log"
			}
		}
		rules = append(rules, Rule{Name: r.Name, Condition: r.Condition, Action: action})
	}
	return rules, nil
}

func firstRuleValue(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func (e *RulesEngine) Evaluate(record stream.Record) ([]RuleMatch, error) {
	if e == nil {
		return nil, nil
	}
	matches := make([]RuleMatch, 0)
	for i, filter := range e.bound {
		matched, err := filter.Match(record)
		if err != nil {
			return nil, fmt.Errorf("rule %q: %w", e.rules[i].Name, err)
		}
		if matched {
			matches = append(matches, RuleMatch{Rule: e.rules[i], Record: record})
		}
	}
	return matches, nil
}

func (e *RulesEngine) Match(record stream.Record) (bool, error) {
	matches, err := e.Evaluate(record)
	return len(matches) > 0, err
}

func (e *RulesEngine) Dispatch(record stream.Record) {
	matches, err := e.Evaluate(record)
	if err != nil {
		e.stderr("rule engine: %v", err)
		return
	}
	for _, match := range matches {
		select {
		case e.queue <- match:
		default:
			e.stderr("rule engine: alert queue is full; dropping rule %q", match.Rule.Name)
		}
	}
}

func (e *RulesEngine) runDispatcher() {
	defer e.wg.Done()
	for {
		select {
		case <-e.ctx.Done():
			return
		case match := <-e.queue:
			if err := e.dispatch(match); err != nil {
				e.stderr("rule engine: dispatching rule %q: %v", match.Rule.Name, err)
			}
		}
	}
}

func (e *RulesEngine) dispatch(match RuleMatch) error {
	action := match.Rule.Action
	url := firstRuleValue(action.URL, action.Webhook, action.Slack)
	switch action.Type {
	case "log":
		e.stderr("rule engine: rule %q matched record %s", match.Rule.Name, match.Record.ID)
		return nil
	case "webhook", "slack":
		if url == "" {
			return fmt.Errorf("action %q requires a URL", action.Type)
		}
		body, err := json.Marshal(match.Record)
		if err != nil {
			return err
		}
		if action.Type == "slack" {
			body, err = json.Marshal(map[string]string{
				"text": fmt.Sprintf("Prism rule %q matched record %s", match.Rule.Name, match.Record.ID),
			})
			if err != nil {
				return err
			}
		}
		req, err := http.NewRequestWithContext(e.ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := e.client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("action returned HTTP %s", resp.Status)
		}
		return nil
	default:
		return fmt.Errorf("unsupported action type %q", action.Type)
	}
}

func (e *RulesEngine) SetStderr(stderr func(string, ...any)) {
	if e != nil && stderr != nil {
		e.stderr = stderr
	}
}

func (e *RulesEngine) Close() {
	if e == nil {
		return
	}
	e.cancel()
	e.wg.Wait()
}

type RulesSink struct {
	next   stream.Sink
	engine *RulesEngine
}

func NewRulesSink(next stream.Sink, engine *RulesEngine) *RulesSink {
	return &RulesSink{next: next, engine: engine}
}

func (s *RulesSink) Emit(record stream.Record) error {
	if s.engine != nil {
		s.engine.Dispatch(record)
	}
	return s.next.Emit(record)
}

func (s *RulesSink) Flush() error {
	return s.next.Flush()
}
