package sink

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// DryRunSink validates the stream path without delivering records to an
// external destination. It writes compact diagnostics to diagnostics.
type DryRunSink struct {
	diagnostics io.Writer
	mu          sync.Mutex
	matched     uint64
}

// NewDryRunSink returns a side-effect-free sink.
func NewDryRunSink(diagnostics io.Writer) *DryRunSink { return &DryRunSink{diagnostics: diagnostics} }

// Emit records a successful filter match without forwarding it.
func (s *DryRunSink) Emit(r stream.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.matched++
	_, err := fmt.Fprintf(s.diagnostics, "dry-run: match id=%s ledger=%d kind=%s\n", r.ID, r.Sequence, r.Kind)
	return err
}

// Flush implements stream.Sink.
func (*DryRunSink) Flush() error { return nil }

// Matched returns the number of records that reached the sink.
func (s *DryRunSink) Matched() uint64 { s.mu.Lock(); defer s.mu.Unlock(); return s.matched }

// ReadinessCheck is one line of the dry-run readiness checklist.
type ReadinessCheck struct {
	Name   string
	Detail string
	Err    error
}

// OK reports whether the check passed.
func (c ReadinessCheck) OK() bool { return c.Err == nil }

// ReadinessValidator collects preflight checks and renders their checklist.
//
// It is deliberately independent of RPC and CLI types: callers supply one
// check per dependency, which keeps the output format testable in isolation.
type ReadinessValidator struct {
	checks []ReadinessCheck
}

// Add appends a check to the validator.
func (v *ReadinessValidator) Add(name, detail string, err error) {
	v.checks = append(v.checks, ReadinessCheck{Name: name, Detail: detail, Err: err})
}

// Run executes fn and records its result under name. fn returns the detail
// shown on the checklist alongside the check result.
func (v *ReadinessValidator) Run(name string, fn func() (string, error)) {
	detail, err := fn()
	v.Add(name, detail, err)
}

// Checks returns the checks recorded so far.
func (v *ReadinessValidator) Checks() []ReadinessCheck {
	return append([]ReadinessCheck(nil), v.checks...)
}

// Err joins the failures from every check, so one report lists them all.
func (v *ReadinessValidator) Err() error {
	errs := make([]error, 0, len(v.checks))
	for _, c := range v.checks {
		if c.Err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", c.Name, c.Err))
		}
	}
	return errors.Join(errs...)
}

// Write prints the readiness checklist to w.
func (v *ReadinessValidator) Write(w io.Writer) error {
	if _, err := fmt.Fprintln(w, "dry-run readiness:"); err != nil {
		return err
	}
	for _, c := range v.checks {
		status := "ok"
		if !c.OK() {
			status = "FAIL"
		}
		line := fmt.Sprintf("  [%s] %s", status, c.Name)
		if c.Detail != "" {
			line += ": " + c.Detail
		}
		if c.Err != nil {
			line += " (" + c.Err.Error() + ")"
		}
		if _, err := fmt.Fprintln(w, line); err != nil {
			return err
		}
	}
	return nil
}

// String renders the checklist, which is useful in tests and error messages.
func (v *ReadinessValidator) String() string {
	var b strings.Builder
	if err := v.Write(&b); err != nil {
		return ""
	}
	return b.String()
}
