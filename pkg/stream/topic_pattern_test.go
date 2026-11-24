package stream_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"testing"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// ScVal switch constants for building topic fixtures, mirroring the wire
// values the real XDR decoder accepts: U32 is switch 3, Symbol is switch 15.
const (
	scvU32Switch    = 3
	scvSymbolSwitch = 15
)

// xdrTopicBase64 encodes hand-built XDR into the base64 form the stream API
// uses, so the fixtures are genuine ScVals rather than opaque strings.
func xdrTopicBase64(parts ...any) string {
	var buf bytes.Buffer
	var word [4]byte
	for _, p := range parts {
		switch v := p.(type) {
		case uint32:
			binary.BigEndian.PutUint32(word[:], v)
			buf.Write(word[:])
		case string:
			binary.BigEndian.PutUint32(word[:], uint32(len(v))) //nolint:gosec // fixture data
			buf.Write(word[:])
			buf.WriteString(v)
			// XDR pads strings to a multiple of four.
			if rem := len(v) % 4; rem != 0 {
				buf.Write(make([]byte, 4-rem))
			}
		}
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func symbolTopic(s string) string {
	return xdrTopicBase64(uint32(scvSymbolSwitch), s)
}

func u32Topic(v uint32) string {
	return xdrTopicBase64(uint32(scvU32Switch), v)
}

// TestTopicPatternMatching covers the segment grammar against real decoded
// ScVals. Every case runs the pattern through lens the same way the sink
// does, so the tests exercise the envelope path, not pattern internals.
func TestTopicPatternMatching(t *testing.T) {
	t.Parallel()

	fee := symbolTopic("fee")
	transfer := symbolTopic("transfer")
	u7 := u32Topic(7)

	tests := []struct {
		name   string
		spec   string
		topics []string
		want   bool
	}{
		{name: "single wildcard matches", spec: "*", topics: []string{fee}, want: true},
		{name: "wildcard needs a topic", spec: "*", topics: nil, want: false},
		{name: "double wildcard matches nothing", spec: "**", topics: nil, want: true},
		{name: "double wildcard matches one", spec: "**", topics: []string{transfer, u7}, want: true},
		{name: "wildcard then double", spec: "fee,**", topics: []string{fee}, want: true},
		{name: "wildcard then double with tail", spec: "fee,**", topics: []string{fee, u7}, want: true},
		{name: "literal symbol text", spec: "fee", topics: []string{fee}, want: true},
		{name: "literal mismatch", spec: "fee", topics: []string{transfer}, want: false},
		{name: "exact count, two segments", spec: "fee,transfer", topics: []string{fee, transfer}, want: true},
		{name: "extra topics do not match", spec: "fee,transfer", topics: []string{fee, transfer, u7}, want: false},
		{name: "single wildcard is exact count", spec: "*", topics: []string{fee, transfer}, want: false},
		{name: "numeric leaf matches", spec: "7", topics: []string{u7}, want: true},
		{name: "raw base64 is always a candidate", spec: u7, topics: []string{u7}, want: true},
		{name: "prefix matches", spec: "prefix:tr", topics: []string{transfer}, want: true},
		{name: "prefix anchors", spec: "prefix:ans", topics: []string{transfer}, want: false},
		{name: "glob question mark", spec: "f?e", topics: []string{fee}, want: true},
		{name: "glob star", spec: "f*", topics: []string{fee}, want: true},
		{name: "glob does not sink", spec: "s*r", topics: []string{transfer}, want: false},
		{name: "regex unanchored", spec: "re:ans", topics: []string{transfer}, want: true},
		{name: "regex anchored", spec: "re:\\Afee\\z", topics: []string{fee}, want: true},
		{name: "regex anchored no match", spec: "re:\\Afee\\z", topics: []string{transfer}, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p, err := stream.CompileTopicPattern(tc.spec)
			if err != nil {
				t.Fatalf("CompileTopicPattern(%q) error = %v", tc.spec, err)
			}
			if got := p.Matches(tc.topics); got != tc.want {
				t.Errorf("Matches(%v) = %v, want %v", tc.topics, got, tc.want)
			}
			if p.String() != tc.spec {
				t.Errorf("String() = %q, want the original spec %q", p.String(), tc.spec)
			}
		})
	}
}

// TestTopicPatternCompileErrors covers every malformed pattern class.
func TestTopicPatternCompileErrors(t *testing.T) {
	t.Parallel()

	sevenTopics := "a,a,a,a,a,a,a" // one past the server's segment limit plus "**"
	tests := []struct {
		name string
		spec string
		want string
	}{
		{name: "empty", spec: "", want: "empty topic pattern"},
		{name: "empty segment", spec: "a,,b", want: "empty segment"},
		{name: "double wildcard not last", spec: "a,**,b", want: "must be the last segment"},
		{name: "too many segments", spec: sevenTopics,
			want: fmt.Sprintf("at most %d", stream.MaxTopicSegments)},
		{name: "empty regex body", spec: "re:", want: "empty regular expression"},
		{name: "invalid regex", spec: "re:[", want: "not a valid regular expression"},
		{name: "empty prefix", spec: "prefix:", want: "empty prefix"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := stream.CompileTopicPattern(tc.spec)
			if err == nil {
				t.Fatalf("CompileTopicPattern(%q) error = nil, want failure", tc.spec)
			}
			if !contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err.Error(), tc.want)
			}
		})
	}
}

// TestCompileTopicPatterns checks the list-level compiler and fail-fast
// behaviour on the first malformed pattern.
func TestCompileTopicPatterns(t *testing.T) {
	t.Parallel()

	ok, err := stream.CompileTopicPatterns([]string{"fee", "*", "re:f.e"})
	if err != nil {
		t.Fatalf("CompileTopicPatterns() error = %v", err)
	}
	if len(ok) != 3 {
		t.Errorf("compiled %d patterns, want 3", len(ok))
	}

	empty, err := stream.CompileTopicPatterns(nil)
	if err != nil {
		t.Fatalf("CompileTopicPatterns(nil) error = %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("nil input compiled %d patterns, want 0", len(empty))
	}

	many := make([]string, 1000)
	for i := range many {
		many[i] = fmt.Sprintf("topic-%04d", i)
	}
	compiled, err := stream.CompileTopicPatterns(many)
	if err != nil {
		t.Fatalf("CompileTopicPatterns(1000 patterns) error = %v", err)
	}
	if len(compiled) != len(many) {
		t.Errorf("compiled %d patterns, want %d", len(compiled), len(many))
	}

	if _, err := stream.CompileTopicPatterns([]string{"*", "a,,b"}); err == nil {
		t.Error("CompileTopicPatterns(bad second) error = nil, want fail-fast on the bad pattern")
	}
}

func TestTopicPatternMatcher(t *testing.T) {
	matcher, err := stream.CompileTopicPatternMatcher([]string{
		"transfer",
		"stellar*",
		"[mint, *]",
		"re:burn.*",
	})
	if err != nil {
		t.Fatalf("CompileTopicPatternMatcher() error = %v", err)
	}

	for _, tc := range []struct {
		name   string
		topics []string
		want   bool
	}{
		{name: "exact", topics: []string{"transfer"}, want: true},
		{name: "exact rejects substring", topics: []string{"pretransfer"}, want: false},
		{name: "prefix wildcard", topics: []string{"stellar-token"}, want: true},
		{name: "prefix anchored", topics: []string{"prestellar-token"}, want: false},
		{name: "tuple and wildcard", topics: []string{"mint", "7"}, want: true},
		{name: "tuple requires exact topic count", topics: []string{"mint", "7", "extra"}, want: false},
		{name: "regex fallback", topics: []string{"burn.amount"}, want: true},
		{name: "no tuple matches", topics: []string{"unrelated"}, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := matcher.Matches(tc.topics); got != tc.want {
				t.Errorf("Matches(%v) = %v, want %v", tc.topics, got, tc.want)
			}
		})
	}

	allocations := testing.AllocsPerRun(100, func() {
		matcher.Matches([]string{"not-a-match"})
	})
	if allocations != 0 {
		t.Errorf("Matches() allocated %v times per call, want zero", allocations)
	}
}

func BenchmarkTopicPatternMatcher1000(b *testing.B) {
	specs := make([]string, 1000)
	patterns := make([]*regexp.Regexp, len(specs))
	for i := range specs {
		specs[i] = fmt.Sprintf("topic-%04d", i)
		patterns[i] = regexp.MustCompile("^" + regexp.QuoteMeta(specs[i]) + "$")
	}
	matcher, err := stream.CompileTopicPatternMatcher(specs)
	if err != nil {
		b.Fatal(err)
	}
	text := "topic-0999"

	b.Run("aho-corasick", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			matcher.Matches([]string{text})
		}
	})
	b.Run("linear-regexp", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, pattern := range patterns {
				if pattern.MatchString(text) {
					break
				}
			}
		}
	})
}

// TestPatternSinkFiltersEvents checks that only matching event records reach
// the wrapped sink, and that non-event records pass through untouched.
func TestPatternSinkFiltersEvents(t *testing.T) {
	t.Parallel()

	fee := symbolTopic("fee")
	inner := &collector{}
	p, err := stream.CompileTopicPattern("fee")
	if err != nil {
		t.Fatalf("CompileTopicPattern() error = %v", err)
	}
	s := stream.NewPatternSink(inner, []stream.TopicPattern{p})

	if err := s.Emit(stream.Record{ID: "0001", Kind: stream.KindEvent,
		Event: &stream.Event{TopicXDR: []string{fee}}}); err != nil {
		t.Fatalf("Emit(matching event) error = %v", err)
	}
	if err := s.Emit(stream.Record{ID: "0002", Kind: stream.KindEvent,
		Event: &stream.Event{TopicXDR: []string{symbolTopic("transfer")}}}); err != nil {
		t.Fatalf("Emit(filtered event) error = %v", err)
	}
	if err := s.Emit(stream.Record{ID: "0003", Kind: stream.KindLedger,
		Close: &stream.LedgerClose{}}); err != nil {
		t.Fatalf("Emit(ledger) error = %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if got, want := len(inner.ids), 2; got != want {
		t.Fatalf("emitted %d records, want %d: %v", got, want, inner.ids)
	}
	if got, want := inner.ids[0], "0001"; got != want {
		t.Errorf("first emitted ID = %q, want %q", got, want)
	}
	if got, want := inner.ids[1], "0003"; got != want {
		t.Errorf("second emitted ID = %q, want the ledger record %q", got, want)
	}
}

// TestPatternSinkPassesThroughWhenUnconfigured — a sink with no patterns
// emits everything it is handed.
func TestPatternSinkPassesThroughWhenUnconfigured(t *testing.T) {
	t.Parallel()

	inner := &collector{}
	s := stream.NewPatternSink(inner, nil)
	for i := range 3 {
		r := stream.Record{ID: fmt.Sprintf("%04d", i), Kind: stream.KindEvent,
			Event: &stream.Event{}}
		if err := s.Emit(r); err != nil {
			t.Fatalf("Emit(%d) error = %v", i, err)
		}
	}
	if len(inner.ids) != 3 {
		t.Errorf("emitted %d records, want 3", len(inner.ids))
	}
}

// TestTailAppliesPatternSink covers the integration seam: a real tailer run
// through a pattern-filtered sink against a fake server.
func TestTailAppliesPatternSink(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(500, 30)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(7))
	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 500, PageLimit: 7})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	inner := &collector{}
	patterns, err := stream.CompileTopicPatterns([]string{"*"})
	if err != nil {
		t.Fatalf("CompileTopicPatterns() error = %v", err)
	}
	tl := stream.NewTailer(src, stream.NewPatternSink(inner, patterns), stream.Options{Clock: newFakeClock()})
	if err := tl.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertStreamInvariants(t, inner.ids, idsOf(events))

	// A pattern that matches the fixture's u32(7) topic numerically must let
	// every record through; a bogus pattern must let none through. Both use
	// the decoded text path, not the server-side base64 filters.
	for _, tc := range []struct {
		spec string
		want int
	}{
		{spec: "7", want: len(events)},
		{spec: "nope", want: 0},
	} {
		filtered := &collector{}
		patterns, err := stream.CompileTopicPatterns([]string{tc.spec})
		if err != nil {
			t.Fatalf("CompileTopicPatterns(%q) error = %v", tc.spec, err)
		}
		tl := stream.NewTailer(src, stream.NewPatternSink(filtered, patterns), stream.Options{Clock: newFakeClock()})
		if err := tl.Run(context.Background()); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		if len(filtered.ids) != tc.want {
			t.Errorf("pattern %q emitted %d records, want %d", tc.spec, len(filtered.ids), tc.want)
		}
	}
}

// TestPatternSinkErrorPropagates checks a wrapped sink's error stops the
// stream rather than being swallowed.
func TestPatternSinkErrorPropagates(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(600, 10)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(5))
	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 600, PageLimit: 5})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	wantErr := errors.New("downstream broke")
	inner := &collector{failAt: 2, err: wantErr}
	patterns, err := stream.CompileTopicPatterns([]string{"*"})
	if err != nil {
		t.Fatalf("CompileTopicPatterns() error = %v", err)
	}
	tl := stream.NewTailer(src, stream.NewPatternSink(inner, patterns), stream.Options{Clock: newFakeClock()})
	if got := tl.Run(context.Background()); !errors.Is(got, wantErr) {
		t.Fatalf("Run() error = %v, want it to wrap %v", got, wantErr)
	}
}
