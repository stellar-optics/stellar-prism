// Package rules implements stateful complex-event rules for stream records.
package rules

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/stellar-optics/stellar-xdr-lens/pkg/lens"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

var (
	cepExpressionPattern = regexp.MustCompile(
		`^\s*(count\s*\(\s*\)|sum\s*\(\s*[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*\s*\))\s*(>=|<=|==|!=|>|<)\s*((?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?)\s+within\s+(\S+)(?:\s+where\s+(.+))?\s*$`,
	)
	cepExpressionPrefix = regexp.MustCompile(`^\s*(?:count|sum)\s*\(`)
)

// Alert describes a CEP threshold crossing and the records contributing to it.
type Alert struct {
	Expression string `json:"expression"`
	// TriggeredAt is the event time of the record that crossed the threshold.
	TriggeredAt time.Time `json:"triggeredAt"`
	// WindowStart and WindowEnd bound the records included in the aggregate.
	WindowStart time.Time `json:"windowStart"`
	WindowEnd   time.Time `json:"windowEnd"`
	// Count is the number of records included in the aggregate.
	Count int `json:"count"`
	// Sum is the aggregate value for sum rules.
	Sum float64 `json:"sum,omitempty"`
	// Records contains the records included in the aggregate.
	Records []stream.Record `json:"records"`
}

// Options configures a CEP rule. OnAlert is called after each matching
// record that satisfies the window aggregation comparison.
type Options struct {
	// Now supplies the current time when a record has no timestamp.
	Now func() time.Time
	// OnAlert receives a matching alert. A webhook can be wired with
	// CompileRulesWithWebhook.
	OnAlert func(Alert) error
}

type aggregateKind uint8

const (
	countAggregate aggregateKind = iota
	sumAggregate
)

type bufferedEvent struct {
	at     time.Time
	record stream.Record
	value  float64
}

// Engine evaluates one compiled CEP expression and keeps its sliding window
// between calls to Match. It implements stream.ExpressionFilter so it can be
// combined with ordinary rule expressions.
type Engine struct {
	expression string
	aggregate  aggregateKind
	field      string
	operator   string
	threshold  float64
	window     time.Duration
	where      stream.ExpressionFilter
	options    Options
	now        func() time.Time

	mu     sync.Mutex
	events []bufferedEvent
	sum    float64
}

// IsCEPExpression reports whether expression starts with a supported CEP
// aggregate. It allows callers to route CEP syntax to Compile while keeping
// ordinary expression compilation unchanged.
func IsCEPExpression(expression string) bool {
	return cepExpressionPrefix.MatchString(expression)
}

// Compile parses a windowed count or sum expression.
//
// Supported forms are:
//
//	count() > 5 within 1m
//	sum(amount) > 50000 within 5m where contract == "CABC..."
//
// A sum field is read from the decoded event value. A leading "value." is
// optional. The optional where clause uses stream's existing expression
// language.
func Compile(expression string, options Options) (*Engine, error) {
	expression = strings.TrimSpace(expression)
	matches := cepExpressionPattern.FindStringSubmatch(expression)
	if matches == nil {
		return nil, fmt.Errorf("invalid CEP expression %q", expression)
	}

	aggregateText := strings.TrimSpace(matches[1])
	kind := countAggregate
	field := ""
	if strings.HasPrefix(aggregateText, "sum") {
		kind = sumAggregate
		openParen := strings.IndexByte(aggregateText, '(')
		field = strings.TrimSpace(strings.TrimSuffix(aggregateText[openParen+1:], ")"))
		field = strings.TrimPrefix(field, "value.")
		if field == "" {
			return nil, fmt.Errorf("CEP expression %q has an empty sum field", expression)
		}
	}

	window, err := time.ParseDuration(matches[4])
	if err != nil {
		return nil, fmt.Errorf("CEP expression %q has invalid window: %w", expression, err)
	}
	if window <= 0 {
		return nil, fmt.Errorf("CEP expression %q window must be positive", expression)
	}

	threshold, err := strconv.ParseFloat(matches[3], 64)
	if err != nil || math.IsNaN(threshold) || math.IsInf(threshold, 0) {
		return nil, fmt.Errorf("CEP expression %q has an invalid threshold", expression)
	}

	var where stream.ExpressionFilter
	if strings.TrimSpace(matches[5]) != "" {
		where, err = stream.CompileExpression(strings.TrimSpace(matches[5]))
		if err != nil {
			return nil, fmt.Errorf("CEP expression %q has invalid where clause: %w", expression, err)
		}
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}

	return &Engine{
		expression: expression,
		aggregate:  kind,
		field:      field,
		operator:   matches[2],
		threshold:  threshold,
		window:     window,
		where:      where,
		options:    options,
		now:        now,
	}, nil
}

// Expression returns the source expression used to build the engine.
func (e *Engine) Expression() string { return e.expression }

// Match adds a matching event to the sliding window, evicts expired events,
// and reports whether the aggregate currently satisfies the threshold.
func (e *Engine) Match(record stream.Record) (bool, error) {
	if record.Kind != stream.KindEvent || record.Event == nil {
		return false, nil
	}

	at := record.Time
	if at.IsZero() {
		at = e.now()
	}

	e.mu.Lock()
	e.evict(at.Add(-e.window))
	if e.where != nil {
		matches, err := e.where.Match(record)
		if err != nil {
			e.mu.Unlock()
			return false, fmt.Errorf("CEP where clause: %w", err)
		}
		if !matches {
			e.mu.Unlock()
			return false, nil
		}
	}

	value := float64(0)
	if e.aggregate == sumAggregate {
		var err error
		value, err = eventNumber(record.Event.ValueXDR, e.field)
		if err != nil {
			e.mu.Unlock()
			return false, fmt.Errorf("CEP sum field %q: %w", e.field, err)
		}
	}

	event := bufferedEvent{at: at, record: record, value: value}
	index := len(e.events)
	for index > 0 && e.events[index-1].at.After(at) {
		index--
	}
	e.events = append(e.events, bufferedEvent{})
	copy(e.events[index+1:], e.events[index:])
	e.events[index] = event
	e.sum += value
	e.evict(at.Add(-e.window))

	count := len(e.events)
	total := e.sum
	matched := compare(total, e.threshold, e.operator)
	if e.aggregate == countAggregate {
		matched = compare(float64(count), e.threshold, e.operator)
	}

	var alert *Alert
	if matched && e.options.OnAlert != nil {
		records := make([]stream.Record, len(e.events))
		for i, buffered := range e.events {
			records[i] = buffered.record
		}
		alert = &Alert{
			Expression:  e.expression,
			TriggeredAt: at,
			WindowStart: e.events[0].at,
			WindowEnd:   at,
			Count:       count,
			Sum:         total,
			Records:     records,
		}
	}
	e.mu.Unlock()

	if alert != nil {
		if err := e.options.OnAlert(*alert); err != nil {
			return false, fmt.Errorf("emitting CEP alert for %q: %w", e.expression, err)
		}
	}
	return matched, nil
}

func (e *Engine) evict(cutoff time.Time) {
	expired := 0
	for expired < len(e.events) && !e.events[expired].at.After(cutoff) {
		e.sum -= e.events[expired].value
		expired++
	}
	if expired > 0 {
		copy(e.events, e.events[expired:])
		e.events = e.events[:len(e.events)-expired]
	}
}

func compare(value, threshold float64, operator string) bool {
	switch operator {
	case ">":
		return value > threshold
	case ">=":
		return value >= threshold
	case "<":
		return value < threshold
	case "<=":
		return value <= threshold
	case "==":
		return value == threshold
	case "!=":
		return value != threshold
	default:
		return false
	}
}

func eventNumber(encoded, field string) (float64, error) {
	if encoded == "" {
		return 0, fmt.Errorf("event has no value")
	}
	decoded, err := lens.Decode(encoded)
	if err != nil {
		return 0, fmt.Errorf("decoding event value: %w", err)
	}

	node := decoded.Node
	for _, part := range strings.Split(field, ".") {
		node = unwrapScVal(node)
		if node == nil {
			return 0, fmt.Errorf("field %q does not exist", field)
		}
		if entries := mapEntries(node); entries != nil {
			var found *lens.Node
			for _, entry := range entries.Children {
				if scalarText(entry.Child("Key")) == part {
					found = entry.Child("Val")
					break
				}
			}
			node = found
		} else {
			node = node.Child(part)
		}
	}
	if node == nil {
		return 0, fmt.Errorf("field %q does not exist", field)
	}

	value, ok := nodeNumber(node)
	if !ok {
		return 0, fmt.Errorf("field %q is not numeric", field)
	}
	return value, nil
}

func unwrapScVal(node *lens.Node) *lens.Node {
	for node != nil && node.TypeName == "ScVal" {
		var active *lens.Node
		for _, child := range node.Children {
			if child.Name != "Type" {
				active = child
				break
			}
		}
		node = active
	}
	return node
}

func mapEntries(node *lens.Node) *lens.Node {
	node = unwrapScVal(node)
	if node == nil {
		return nil
	}
	if node.TypeName == "ScMap" {
		return node
	}
	if entries := node.Child("Map"); entries != nil {
		return entries
	}
	return nil
}

func scalarText(node *lens.Node) string {
	node = unwrapScVal(node)
	if node == nil {
		return ""
	}
	if node.Kind == lens.KindScalar && node.Value != nil {
		return fmt.Sprint(node.Value)
	}
	for _, child := range node.Children {
		if text := scalarText(child); text != "" {
			return text
		}
	}
	return ""
}

func nodeNumber(node *lens.Node) (float64, bool) {
	node = unwrapScVal(node)
	if node == nil {
		return 0, false
	}
	if node.TypeName == "Int128Parts" || node.TypeName == "UInt128Parts" {
		hi, hiOK := scalarNumber(node.Child("Hi"))
		lo, loOK := scalarNumber(node.Child("Lo"))
		if hiOK && loOK {
			return math.Ldexp(hi, 64) + lo, true
		}
		return 0, false
	}
	if value, ok := scalarNumber(node); ok {
		return value, true
	}
	for _, child := range node.Children {
		if value, ok := nodeNumber(child); ok {
			return value, true
		}
	}
	return 0, false
}

func scalarNumber(node *lens.Node) (float64, bool) {
	if node == nil || node.Kind != lens.KindScalar {
		return 0, false
	}
	switch value := node.Value.(type) {
	case int64:
		return float64(value), true
	case uint64:
		return float64(value), true
	case float64:
		return value, true
	default:
		return 0, false
	}
}
