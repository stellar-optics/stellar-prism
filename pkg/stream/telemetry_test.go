package stream

import (
	"context"
	"math"
	"testing"

	"go.opentelemetry.io/otel"
)

func TestInitTelemetryWithoutEndpointIsNoop(t *testing.T) {
	shutdown, err := InitTelemetry(context.Background(), "  ", 0.5)
	if err != nil {
		t.Fatalf("InitTelemetry() error = %v", err)
	}
	if shutdown == nil {
		t.Fatal("InitTelemetry() returned a nil shutdown function")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown() error = %v", err)
	}
}

// TestInitTelemetryRejectsInvalidSampleRate checks validation happens even when
// tracing is disabled: a sample rate outside 0 to 1 is a configuration error,
// not something to discover only once a collector is configured.
func TestInitTelemetryRejectsInvalidSampleRate(t *testing.T) {
	tests := []struct {
		name string
		rate float64
	}{
		{name: "negative", rate: -0.1},
		{name: "above one", rate: 1.1},
		{name: "not a number", rate: math.NaN()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := InitTelemetry(context.Background(), "", tc.rate); err == nil {
				t.Fatalf("InitTelemetry() with sample rate %v returned no error", tc.rate)
			}
			if _, err := InitTelemetry(context.Background(), "http://localhost:4318", tc.rate); err == nil {
				t.Fatalf("InitTelemetry() with endpoint and sample rate %v returned no error", tc.rate)
			}
		})
	}
}

func TestInitTelemetryRejectsUnsupportedEndpointScheme(t *testing.T) {
	if _, err := InitTelemetry(context.Background(), "ftp://collector:4318", 1); err == nil {
		t.Fatal("InitTelemetry() accepted an unsupported endpoint scheme")
	}
}

// TestNewOTLPTraceExporterAcceptsEndpointForms covers the shapes an operator is
// likely to pass: a bare host:port for a local collector, an http URL, an
// https URL, and a URL with its own path.
func TestNewOTLPTraceExporterAcceptsEndpointForms(t *testing.T) {
	endpoints := []string{
		"localhost:4318",
		"http://localhost:4318",
		"http://localhost:4318/v1/traces",
		"https://collector.example/v1/traces",
		"https://collector.example/custom/path?tenant=prism",
	}
	for _, endpoint := range endpoints {
		t.Run(endpoint, func(t *testing.T) {
			exporter, err := newOTLPTraceExporter(context.Background(), endpoint)
			if err != nil {
				t.Fatalf("newOTLPTraceExporter(%q) error = %v", endpoint, err)
			}
			// No spans were recorded, so this only releases the exporter's
			// background machinery; it must not block on a connection.
			_ = exporter.Shutdown(context.Background())
		})
	}
}

func TestRecordAttributesIdentifyTheRecord(t *testing.T) {
	record := Record{
		ID:       "00000000000000000042",
		Sequence: 1234,
		Kind:     KindEvent,
		Event: &Event{
			Type:       "contract",
			ContractID: "CABC",
			TxHash:     "deadbeef",
		},
	}
	attrs := make(map[string]string, len(RecordAttributes(record)))
	for _, kv := range RecordAttributes(record) {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}

	want := map[string]string{
		"ledger.sequence":  "1234",
		"record.kind":      "event",
		"record.id":        "00000000000000000042",
		"event.type":       "contract",
		"contract.id":      "CABC",
		"transaction.hash": "deadbeef",
	}
	for key, value := range want {
		if attrs[key] != value {
			t.Errorf("attribute %q = %q, want %q", key, attrs[key], value)
		}
	}
}

// TestRecordAttributesFallBackToTransactionHash covers non-event records, which
// carry their transaction hash on the transaction payload instead.
func TestRecordAttributesFallBackToTransactionHash(t *testing.T) {
	attrs := make(map[string]string)
	for _, kv := range RecordAttributes(Record{Sequence: 7, Kind: KindTransaction, Transaction: &Transaction{Hash: "abc123"}}) {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}
	if got, want := attrs["transaction.hash"], "abc123"; got != want {
		t.Errorf("transaction.hash = %q, want %q", got, want)
	}
	if _, ok := attrs["contract.id"]; ok {
		t.Error("a transaction record should not carry a contract.id attribute")
	}
}

// restoreTracerProvider puts the process-wide provider back after a test that
// installs one, so tests in the same package cannot observe each other's
// provider.
func restoreTracerProvider(t *testing.T) {
	t.Helper()
	previous := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
}
