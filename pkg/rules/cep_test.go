package rules

import (
	"errors"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestCountThresholdUsesSlidingWindow(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 11, 24, 9, 0, 0, 0, time.UTC)
	engine, err := Compile(`count() > 1 within 1m`, Options{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	for _, at := range []time.Time{start, start.Add(30 * time.Second)} {
		matched, err := engine.Match(eventRecord(at, "CABC", 1))
		if err != nil {
			t.Fatalf("Match: %v", err)
		}
		if at.Equal(start) && matched {
			t.Fatal("first event unexpectedly met count threshold")
		}
		if at.After(start) && !matched {
			t.Fatal("second event did not meet count threshold")
		}
	}

	matched, err := engine.Match(eventRecord(start.Add(2*time.Minute), "CABC", 1))
	if err != nil {
		t.Fatalf("Match after window expiry: %v", err)
	}
	if matched {
		t.Fatal("expired events still met count threshold")
	}
	if len(engine.events) != 1 {
		t.Fatalf("window contains %d events after eviction, want 1", len(engine.events))
	}
}

func TestSumThresholdAndWhereEmitAlert(t *testing.T) {
	t.Parallel()

	var got Alert
	engine, err := Compile(`sum(amount) > 5000 within 5m where contract == "CABC"`, Options{
		OnAlert: func(alert Alert) error {
			got = alert
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	start := time.Date(2026, 11, 24, 9, 0, 0, 0, time.UTC)

	matched, err := engine.Match(eventRecord(start, "OTHER", 3000))
	if err != nil {
		t.Fatalf("Match excluded contract: %v", err)
	}
	if matched {
		t.Fatal("event for a different contract was included")
	}
	matched, err = engine.Match(eventRecord(start.Add(time.Minute), "CABC", 3000))
	if err != nil {
		t.Fatalf("Match first matching event: %v", err)
	}
	if matched {
		t.Fatal("one matching event unexpectedly met the sum threshold")
	}
	matched, err = engine.Match(eventRecord(start.Add(2*time.Minute), "CABC", 2501))
	if err != nil {
		t.Fatalf("Match threshold event: %v", err)
	}
	if !matched {
		t.Fatal("sum threshold was not met")
	}
	if got.Count != 2 || got.Sum != 5501 || len(got.Records) != 2 {
		t.Fatalf("alert aggregate = count %d, sum %v, records %d; want 2, 5501, 2",
			got.Count, got.Sum, len(got.Records))
	}
	if got.Expression != `sum(amount) > 5000 within 5m where contract == "CABC"` {
		t.Errorf("alert expression = %q", got.Expression)
	}
}

func TestCompileRejectsInvalidCEPExpressions(t *testing.T) {
	t.Parallel()

	tests := []string{
		`count() > 5 within 0s`,
		`sum() > 5 within 1m`,
		`sum(amount) > 5 within invalid`,
		`sum(amount) > 5 within 1m where unknown == 1`,
		`count() > NaN within 1m`,
	}
	for _, expression := range tests {
		t.Run(expression, func(t *testing.T) {
			t.Parallel()
			if _, err := Compile(expression, Options{}); err == nil {
				t.Fatalf("Compile(%q) succeeded; want error", expression)
			}
		})
	}
}

func TestAlertHandlerErrorIsReturned(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("webhook unavailable")
	engine, err := Compile(`count() > 0 within 1m`, Options{
		OnAlert: func(Alert) error { return wantErr },
	})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	if _, err := engine.Match(eventRecord(time.Now(), "CABC", 1)); !errors.Is(err, wantErr) {
		t.Fatalf("Match error = %v, want wrapped webhook error", err)
	}
}

func eventRecord(at time.Time, contract string, amount int64) stream.Record {
	amountValue, err := xdr.NewScVal(
		xdr.ScValTypeScvI128,
		xdr.Int128Parts{Hi: 0, Lo: xdr.Uint64(amount)},
	)
	if err != nil {
		panic(err)
	}
	key, err := xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol("amount"))
	if err != nil {
		panic(err)
	}
	entries := xdr.ScMap{{Key: key, Val: amountValue}}
	value, err := xdr.NewScVal(xdr.ScValTypeScvMap, &entries)
	if err != nil {
		panic(err)
	}
	encoded, err := xdr.MarshalBase64(value)
	if err != nil {
		panic(err)
	}
	return stream.Record{
		Time: at,
		Kind: stream.KindEvent,
		Event: &stream.Event{
			ContractID: contract,
			ValueXDR:   encoded,
		},
	}
}
