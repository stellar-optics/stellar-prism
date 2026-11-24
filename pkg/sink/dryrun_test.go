package sink

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestDryRunSinkDoesNotDeliverAndReportsMatch(t *testing.T) {
	var diagnostics bytes.Buffer
	sink := NewDryRunSink(&diagnostics)
	if err := sink.Emit(stream.Record{ID: "0001", Sequence: 7, Kind: stream.KindEvent}); err != nil {
		t.Fatal(err)
	}
	if sink.Matched() != 1 {
		t.Fatalf("Matched() = %d, want 1", sink.Matched())
	}
	if got := diagnostics.String(); !strings.Contains(got, "id=0001") {
		t.Fatalf("diagnostic %q", got)
	}
}

func TestReadinessValidatorChecklist(t *testing.T) {
	var validator ReadinessValidator
	validator.Add("filters", "2 filters compiled", nil)
	validator.Run("rpc", func() (string, error) {
		return "ledger 42", nil
	})
	validator.Add("sink", "unreachable", errors.New("connection refused"))

	var out bytes.Buffer
	if err := validator.Write(&out); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"dry-run readiness:",
		"[ok] filters: 2 filters compiled",
		"[ok] rpc: ledger 42",
		"[FAIL] sink: unreachable (connection refused)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("checklist missing %q\n%s", want, got)
		}
	}
	if validator.Err() == nil {
		t.Fatal("Err() = nil, want the failed check")
	}
	if !strings.Contains(validator.Err().Error(), "sink: connection refused") {
		t.Fatalf("Err() = %v, want the sink failure", validator.Err())
	}
	if len(validator.Checks()) != 3 {
		t.Fatalf("Checks() = %d, want 3", len(validator.Checks()))
	}
}
