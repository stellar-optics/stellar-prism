package stream

import (
	"context"

	"github.com/stellar-optics/stellar-xdr-lens/pkg/lens"
)

// ExcludeFilter wraps a Source to exclude events matching topic or contract filters.
type ExcludeFilter struct {
	source   Source
	matcher  *excludeMatcher
	excluded int
}

type excludeMatcher struct {
	excludePatterns  []ExcludeTopicPattern
	excludeContracts map[string]bool
}

type ExcludeTopicPattern struct {
	isWildcard bool
	isBase64   bool
	pattern    string
	symbol     string
	topicXDR   string
}

func NewExcludeFilter(source Source, excludeTopics []string, excludeContracts []string) (*ExcludeFilter, error) {
	matcher, err := newExcludeMatcher(excludeTopics, excludeContracts)
	if err != nil {
		return nil, err
	}
	return &ExcludeFilter{source: source, matcher: matcher}, nil
}

func newExcludeMatcher(excludeTopics, excludeContracts []string) (*excludeMatcher, error) {
	matcher := &excludeMatcher{excludeContracts: make(map[string]bool)}
	for _, c := range excludeContracts {
		matcher.excludeContracts[c] = true
	}
	for _, t := range excludeTopics {
		pattern, err := parseExcludeTopicPattern(t)
		if err != nil {
			return nil, err
		}
		matcher.excludePatterns = append(matcher.excludePatterns, pattern)
	}
	return matcher, nil
}

func parseExcludeTopicPattern(pattern string) (ExcludeTopicPattern, error) {
	p := ExcludeTopicPattern{pattern: pattern}
	if pattern == "*" || pattern == "**" {
		p.isWildcard = true
		return p, nil
	}
	// Try to decode as base64 ScVal
	decoded, err := lens.Decode(pattern)
	if err == nil {
		// Check if it's a symbol
		if decoded.Node != nil {
			for _, child := range decoded.Node.Children {
				if child.Name == "Sym" && child.Value != nil {
					if sym, ok := child.Value.(string); ok {
						p.isBase64 = true
						p.symbol = sym
						p.topicXDR = pattern
						return p, nil
					}
				}
			}
		}
		// It's a valid ScVal but not a symbol - treat as base64
		p.isBase64 = true
		p.topicXDR = pattern
		return p, nil
	}
	// Not a valid base64 ScVal, treat as symbol string
	p.isBase64 = false
	p.symbol = pattern
	return p, nil
}

func (f *ExcludeFilter) Name() string {
	return f.source.Name() + " (excluded)"
}

func (f *ExcludeFilter) Fetch(ctx context.Context, cursor string) (Batch, error) {
	batch, err := f.source.Fetch(ctx, cursor)
	if err != nil {
		return batch, err
	}
	if len(f.matcher.excludePatterns) == 0 && len(f.matcher.excludeContracts) == 0 {
		return batch, nil
	}
	filtered := make([]Record, 0, len(batch.Records))
	for _, r := range batch.Records {
		if f.matcher.shouldExclude(r) {
			f.excluded++
			continue
		}
		filtered = append(filtered, r)
	}
	batch.Records = filtered
	return batch, nil
}

func (f *excludeMatcher) shouldExclude(r Record) bool {
	if r.Kind != KindEvent || r.Event == nil {
		return false
	}
	// Check contract exclusion
	if f.excludeContracts[r.Event.ContractID] {
		return true
	}
	// Check topic exclusion
	for _, topicXDR := range r.Event.TopicXDR {
		for _, pattern := range f.excludePatterns {
			if pattern.isWildcard {
				// Wildcard matches everything
				return true
			}
			if pattern.isBase64 {
				if pattern.topicXDR == topicXDR {
					return true
				}
				if pattern.symbol != "" {
					// Compare symbol
					decoded, err := lens.Decode(topicXDR)
					if err == nil && decoded.Node != nil {
						for _, child := range decoded.Node.Children {
							if child.Name == "Sym" && child.Value != nil {
								if sym, ok := child.Value.(string); ok && sym == pattern.symbol {
									return true
								}
							}
						}
					}
				}
			} else {
				// Plain symbol string match
				decoded, err := lens.Decode(topicXDR)
				if err == nil && decoded.Node != nil {
					for _, child := range decoded.Node.Children {
						if child.Name == "Sym" && child.Value != nil {
							if sym, ok := child.Value.(string); ok && sym == pattern.symbol {
								return true
							}
						}
					}
				}
			}
		}
	}
	return false
}

func (f *ExcludeFilter) CursorLedger(cursor string) (uint32, bool) {
	return f.source.CursorLedger(cursor)
}

func (f *ExcludeFilter) Excluded() int { return f.excluded }
