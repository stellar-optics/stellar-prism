package stream

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"strconv"
	"strings"

	"github.com/stellar-optics/stellar-xdr-lens/pkg/lens"
)

// RecordFilter decides whether a record should be sent to the sink.
type RecordFilter interface {
	Match(Record) (bool, error)
}

// ExpressionFilter is a compiled --filter-expr predicate.
type ExpressionFilter interface {
	RecordFilter
	Expression() string
}

// FilterSet applies every child filter; a record must pass all of them.
type FilterSet []RecordFilter

// Match implements RecordFilter.
func (filters FilterSet) Match(record Record) (bool, error) {
	for _, filter := range filters {
		if filter == nil {
			continue
		}
		matched, err := filter.Match(record)
		if err != nil || !matched {
			return matched, err
		}
	}
	return true, nil
}

type expressionFilter struct {
	source string
	root   ast.Expr
}

// CompileExpression parses an expression once, before a stream starts.
// Supported values are value fields, topics (or topic), contract, type, tx,
// ledger, txIndex, and opIndex. Operators follow Go syntax for arithmetic,
// comparison, boolean logic, indexing, and parentheses. contains(container,
// value) checks strings, lists, and map keys.
func CompileExpression(source string) (ExpressionFilter, error) {
	root, err := parser.ParseExpr(source)
	if err != nil {
		return nil, fmt.Errorf("invalid filter expression: %w", err)
	}
	if err := validateFilterAST(root); err != nil {
		return nil, err
	}
	return &expressionFilter{source: source, root: root}, nil
}

func (f *expressionFilter) Expression() string { return f.source }

func (f *expressionFilter) Match(record Record) (bool, error) {
	if record.Kind != KindEvent || record.Event == nil {
		return false, nil
	}
	context, err := eventExpressionContext(record)
	if err != nil {
		return false, err
	}
	value, err := evalFilterExpr(f.root, context)
	if err != nil {
		return false, fmt.Errorf("filter expression %q: %w", f.source, err)
	}
	matched, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("filter expression %q must evaluate to a boolean", f.source)
	}
	return matched, nil
}

func validateFilterAST(expr ast.Expr) error {
	switch n := expr.(type) {
	case *ast.BasicLit:
		if n.Kind != token.INT && n.Kind != token.FLOAT && n.Kind != token.STRING {
			return fmt.Errorf("filter expression only supports numeric and string literals")
		}
	case *ast.Ident:
		switch n.Name {
		case "true", "false", "value", "topics", "topic", "contract", "type", "tx", "ledger", "txIndex", "opIndex":
		default:
			return fmt.Errorf("unknown filter expression name %q", n.Name)
		}
	case *ast.SelectorExpr:
		return validateFilterAST(n.X)
	case *ast.IndexExpr:
		if err := validateFilterAST(n.X); err != nil {
			return err
		}
		return validateFilterAST(n.Index)
	case *ast.ParenExpr:
		return validateFilterAST(n.X)
	case *ast.UnaryExpr:
		if n.Op != token.NOT && n.Op != token.ADD && n.Op != token.SUB {
			return fmt.Errorf("unsupported unary operator %s", n.Op)
		}
		return validateFilterAST(n.X)
	case *ast.BinaryExpr:
		switch n.Op {
		case token.LAND, token.LOR, token.EQL, token.NEQ, token.LSS, token.LEQ, token.GTR, token.GEQ,
			token.ADD, token.SUB, token.MUL, token.QUO, token.REM:
		default:
			return fmt.Errorf("unsupported filter operator %s", n.Op)
		}
		if err := validateFilterAST(n.X); err != nil {
			return err
		}
		return validateFilterAST(n.Y)
	case *ast.CallExpr:
		name, ok := n.Fun.(*ast.Ident)
		if !ok || name.Name != "contains" || len(n.Args) != 2 || n.Ellipsis.IsValid() {
			return fmt.Errorf("only contains(container, value) is supported")
		}
		for _, arg := range n.Args {
			if err := validateFilterAST(arg); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported expression construct %T", expr)
	}
	return nil
}

func eventExpressionContext(record Record) (map[string]any, error) {
	e := record.Event
	topics := make([]any, len(e.TopicXDR))
	topic := make([]any, len(e.TopicXDR))
	for i, encoded := range e.TopicXDR {
		decoded, err := lens.Decode(encoded)
		if err != nil {
			return nil, fmt.Errorf("decoding topic[%d] for filter: %w", i, err)
		}
		topics[i] = filterTopicValue(decoded.Node)
		topic[i] = filterTopicScalar(decoded.Node)
	}
	var value any
	if e.ValueXDR != "" {
		decoded, err := lens.Decode(e.ValueXDR)
		if err != nil {
			return nil, fmt.Errorf("decoding value for filter: %w", err)
		}
		value = filterNodeValue(decoded.Node)
	}
	return map[string]any{
		"value": value, "topics": topics, "topic": topic, "contract": e.ContractID,
		"type": e.Type, "tx": e.TxHash, "ledger": int64(record.Sequence),
		"txIndex": int64(e.TxIndex), "opIndex": int64(e.OpIndex),
	}, nil
}

func filterTopicScalar(node *lens.Node) any {
	if node != nil {
		for _, child := range node.Children {
			if child.Name == "Sym" || child.Name == "Str" {
				return filterNodeValue(child)
			}
		}
	}
	return filterNodeValue(node)
}

func filterTopicValue(node *lens.Node) any {
	if node == nil {
		return nil
	}
	for _, child := range node.Children {
		switch child.Name {
		case "Sym":
			return map[string]any{"symbol": filterNodeValue(child)}
		case "Str":
			return map[string]any{"string": filterNodeValue(child)}
		}
	}
	return filterNodeValue(node)
}

func filterNodeValue(node *lens.Node) any {
	if node == nil {
		return nil
	}
	switch node.Kind {
	case lens.KindList:
		out := make([]any, len(node.Children))
		for i, child := range node.Children {
			out[i] = filterNodeValue(child)
		}
		return out
	case lens.KindStruct:
		for _, child := range node.Children {
			if child.Name == "Map" && child.Kind == lens.KindList {
				mapped := make(map[string]any, len(child.Children))
				for _, entry := range child.Children {
					keyNode, valueNode := entry.Child("Key"), entry.Child("Val")
					if keyNode == nil || valueNode == nil {
						continue
					}
					key := firstFilterScalar(keyNode)
					if key == nil {
						continue
					}
					mapped[fmt.Sprint(key)] = filterNodeValue(valueNode)
				}
				return mapped
			}
		}
		if node.TypeName == "ScVal" {
			for _, child := range node.Children {
				if child.Name != "Type" {
					return filterNodeValue(child)
				}
			}
		}
		if node.TypeName == "Int128Parts" || node.TypeName == "UInt128Parts" {
			hi, _ := asInt(filterNodeValue(node.Child("Hi")))
			lo, _ := asInt(filterNodeValue(node.Child("Lo")))
			if hi == 0 {
				return uint64(lo)
			}
			if node.TypeName == "Int128Parts" && uint64(hi) == math.MaxUint64 {
				return lo
			}
		}
		out := make(map[string]any, len(node.Children))
		for _, child := range node.Children {
			if child.Name != "" {
				out[child.Name] = filterNodeValue(child)
			}
		}
		return out
	default:
		return node.Value
	}
}

func firstFilterScalar(node *lens.Node) any {
	if node == nil {
		return nil
	}
	if node.Kind == lens.KindScalar {
		return node.Value
	}
	for _, child := range node.Children {
		if child.Name == "Sym" || child.Name == "Str" {
			if value := firstFilterScalar(child); value != nil {
				return value
			}
		}
	}
	for _, child := range node.Children {
		if value := firstFilterScalar(child); value != nil {
			return value
		}
	}
	return nil
}

func evalFilterExpr(expr ast.Expr, context map[string]any) (any, error) {
	switch n := expr.(type) {
	case *ast.BasicLit:
		switch n.Kind {
		case token.INT:
			return strconv.ParseInt(n.Value, 0, 64)
		case token.FLOAT:
			return strconv.ParseFloat(n.Value, 64)
		case token.STRING:
			return strconv.Unquote(n.Value)
		}
	case *ast.Ident:
		if n.Name == "true" {
			return true, nil
		}
		if n.Name == "false" {
			return false, nil
		}
		return context[n.Name], nil
	case *ast.SelectorExpr:
		base, err := evalFilterExpr(n.X, context)
		if err != nil {
			return nil, err
		}
		return lookupFilterField(base, n.Sel.Name)
	case *ast.IndexExpr:
		base, err := evalFilterExpr(n.X, context)
		if err != nil {
			return nil, err
		}
		index, err := evalFilterExpr(n.Index, context)
		if err != nil {
			return nil, err
		}
		return lookupFilterIndex(base, index)
	case *ast.ParenExpr:
		return evalFilterExpr(n.X, context)
	case *ast.UnaryExpr:
		value, err := evalFilterExpr(n.X, context)
		if err != nil {
			return nil, err
		}
		switch n.Op {
		case token.NOT:
			b, ok := value.(bool)
			if !ok {
				return nil, fmt.Errorf("operator ! requires a boolean")
			}
			return !b, nil
		case token.ADD:
			if _, ok := asFloat(value); ok {
				return value, nil
			}
		case token.SUB:
			if i, ok := asInt(value); ok {
				return -i, nil
			}
			if f, ok := asFloat(value); ok {
				return -f, nil
			}
		}
		return nil, fmt.Errorf("invalid operand for %s", n.Op)
	case *ast.BinaryExpr:
		left, err := evalFilterExpr(n.X, context)
		if err != nil {
			return nil, err
		}
		if n.Op == token.LAND {
			b, ok := left.(bool)
			if !ok {
				return nil, fmt.Errorf("&& requires booleans")
			}
			if !b {
				return false, nil
			}
		}
		if n.Op == token.LOR {
			b, ok := left.(bool)
			if !ok {
				return nil, fmt.Errorf("|| requires booleans")
			}
			if b {
				return true, nil
			}
		}
		right, err := evalFilterExpr(n.Y, context)
		if err != nil {
			return nil, err
		}
		return applyFilterOperator(n.Op, left, right)
	case *ast.CallExpr:
		args := make([]any, len(n.Args))
		for i, arg := range n.Args {
			value, err := evalFilterExpr(arg, context)
			if err != nil {
				return nil, err
			}
			args[i] = value
		}
		return filterContains(args[0], args[1])
	}
	return nil, fmt.Errorf("unsupported expression %T", expr)
}

func filterContains(container, value any) (bool, error) {
	switch items := container.(type) {
	case string:
		needle, ok := value.(string)
		if !ok {
			return false, fmt.Errorf("contains on a string requires a string value")
		}
		return strings.Contains(items, needle), nil
	case []any:
		for _, item := range items {
			if filterValuesEqual(item, value) {
				return true, nil
			}
		}
		return false, nil
	case map[string]any:
		key, ok := value.(string)
		if !ok {
			return false, fmt.Errorf("contains on an object requires a string key")
		}
		_, found := items[key]
		return found, nil
	default:
		return false, fmt.Errorf("contains requires a string, list, or object; got %T", container)
	}
}

func filterValuesEqual(left, right any) bool {
	return fmt.Sprint(left) == fmt.Sprint(right) && fmt.Sprintf("%T", left) == fmt.Sprintf("%T", right)
}

func lookupFilterField(value any, name string) (any, error) {
	if object, ok := value.(map[string]any); ok {
		if v, found := object[name]; found {
			return v, nil
		}
		// ScVal nodes wrap their active arm (for example Symbol) around the
		// useful payload. Permit direct access through that one-arm wrapper.
		if len(object) == 1 {
			for _, child := range object {
				if nested, ok := child.(map[string]any); ok {
					if v, found := nested[name]; found {
						return v, nil
					}
				}
			}
		}
		return nil, fmt.Errorf("field %q does not exist", name)
	}
	return nil, fmt.Errorf("cannot select field %q from %T", name, value)
}

func lookupFilterIndex(value, index any) (any, error) {
	if items, ok := value.([]any); ok {
		i, ok := asInt(index)
		if !ok || i < 0 || i >= int64(len(items)) {
			return nil, fmt.Errorf("index %v is out of range", index)
		}
		return items[i], nil
	}
	if object, ok := value.(map[string]any); ok {
		key, ok := index.(string)
		if !ok {
			return nil, fmt.Errorf("map index must be a string")
		}
		return lookupFilterField(object, key)
	}
	return nil, fmt.Errorf("cannot index %T", value)
}

func applyFilterOperator(op token.Token, left, right any) (any, error) {
	if op == token.LAND || op == token.LOR {
		l, lok := left.(bool)
		r, rok := right.(bool)
		if !lok || !rok {
			return nil, fmt.Errorf("%s requires booleans", op)
		}
		if op == token.LAND {
			return l && r, nil
		}
		return l || r, nil
	}
	if op == token.EQL || op == token.NEQ {
		equal := filterValuesEqual(left, right)
		if op == token.NEQ {
			return !equal, nil
		}
		return equal, nil
	}
	if a, ok := left.(string); ok && op == token.ADD {
		b, ok := right.(string)
		if !ok {
			return nil, fmt.Errorf("string concatenation requires two strings")
		}
		return a + b, nil
	}
	if a, ok := left.(string); ok && (op == token.LSS || op == token.LEQ || op == token.GTR || op == token.GEQ) {
		b, ok := right.(string)
		if !ok {
			return nil, fmt.Errorf("string comparison requires two strings")
		}
		cmp := strings.Compare(a, b)
		return compareFilter(cmp, op), nil
	}
	a, aok := asFloat(left)
	b, bok := asFloat(right)
	if !aok || !bok {
		return nil, fmt.Errorf("operator %s requires numeric operands", op)
	}
	if op == token.LSS || op == token.LEQ || op == token.GTR || op == token.GEQ {
		cmp := 0
		if a < b {
			cmp = -1
		}
		if a > b {
			cmp = 1
		}
		return compareFilter(cmp, op), nil
	}
	if ai, ok1 := asInt(left); ok1 {
		if bi, ok2 := asInt(right); ok2 {
			switch op {
			case token.ADD:
				return ai + bi, nil
			case token.SUB:
				return ai - bi, nil
			case token.MUL:
				return ai * bi, nil
			case token.REM:
				if bi == 0 {
					return nil, fmt.Errorf("division by zero")
				}
				return ai % bi, nil
			}
		}
	}
	switch op {
	case token.ADD:
		return a + b, nil
	case token.SUB:
		return a - b, nil
	case token.MUL:
		return a * b, nil
	case token.QUO:
		if b == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return a / b, nil
	}
	return nil, fmt.Errorf("unsupported operator %s", op)
}

func compareFilter(cmp int, op token.Token) bool {
	switch op {
	case token.LSS:
		return cmp < 0
	case token.LEQ:
		return cmp <= 0
	case token.GTR:
		return cmp > 0
	case token.GEQ:
		return cmp >= 0
	}
	return false
}

func asInt(value any) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint32:
		return int64(v), true
	case uint64:
		return int64(v), true
	}
	return 0, false
}

func asFloat(value any) (float64, bool) {
	if i, ok := asInt(value); ok {
		return float64(i), true
	}
	switch v := value.(type) {
	case float32:
		return float64(v), true
	case float64:
		return v, true
	}
	return 0, false
}
