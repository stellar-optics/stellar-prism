package stream

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	// TracerName is the instrumentation scope every prism span is created
	// under, so a collector can filter prism's spans from a shared pipeline.
	TracerName = "github.com/stellar-optics/stellar-prism"
	// ServiceName is the resource service.name prism reports with its spans.
	ServiceName = "stellar-prism"
	// defaultOTLPPath is the OTLP/HTTP traces path used when an endpoint
	// carries no path of its own.
	defaultOTLPPath = "/v1/traces"
)

// Tracer returns the tracer prism's spans are created with. Calling it is
// cheap, and it deliberately reads the global provider on every call so a
// provider installed after package initialisation is honoured.
func Tracer() trace.Tracer { return otel.Tracer(TracerName) }

// InitTelemetry installs an OTLP/HTTP trace provider and returns the function
// that flushes and shuts it down.
//
// An empty endpoint disables tracing and returns a no-op shutdown function, so
// a run without --otel-endpoint pays nothing beyond a few attribute lookups.
// The sample rate is always validated: a nonsensical value is a configuration
// mistake whether or not tracing is switched on.
//
// endpoint may be a full URL (for example
// http://collector:4318/v1/traces, or an https collector URL) or a bare
// host:port, which is treated as plaintext HTTP.
func InitTelemetry(ctx context.Context, endpoint string, sampleRate float64) (func(context.Context) error, error) {
	if err := validateSampleRate(sampleRate); err != nil {
		return nil, err
	}
	if strings.TrimSpace(endpoint) == "" {
		return func(context.Context) error { return nil }, nil
	}
	exporter, err := newOTLPTraceExporter(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	return InstallTelemetry(exporter, sampleRate)
}

// InstallTelemetry makes a tracer provider around exporter the process-wide
// provider.
//
// It is exported so tests and embedders can install an in-memory exporter, or
// one of their own, instead of an OTLP one. The returned function flushes
// buffered spans before shutting the provider down, so spans produced shortly
// before exit are not lost.
func InstallTelemetry(exporter sdktrace.SpanExporter, sampleRate float64) (func(context.Context) error, error) {
	if err := validateSampleRate(sampleRate); err != nil {
		return nil, err
	}
	if exporter == nil {
		return nil, fmt.Errorf("OpenTelemetry exporter is nil")
	}
	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(attribute.String("service.name", ServiceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("building OpenTelemetry resource: %w", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRate))),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

func validateSampleRate(sampleRate float64) error {
	if math.IsNaN(sampleRate) || sampleRate < 0 || sampleRate > 1 {
		return fmt.Errorf("OpenTelemetry sample rate must be between 0 and 1, got %v", sampleRate)
	}
	return nil
}

// newOTLPTraceExporter builds the OTLP/HTTP exporter for endpoint. It does not
// connect: the exporter batches spans and opens connections lazily, so an
// unreachable collector degrades tracing rather than the stream.
func newOTLPTraceExporter(ctx context.Context, endpoint string) (sdktrace.SpanExporter, error) {
	raw := strings.TrimSpace(endpoint)
	if raw == "" {
		return nil, fmt.Errorf("OpenTelemetry endpoint is empty")
	}

	// A bare host:port is the common local-collector form. url.Parse reads
	// "localhost:4318" as a scheme-qualified opaque URL, so it is handled
	// before parsing.
	if !strings.Contains(raw, "://") {
		return otlptracehttp.New(ctx,
			otlptracehttp.WithEndpoint(raw),
			otlptracehttp.WithInsecure(),
		)
	}

	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parsing OpenTelemetry endpoint %q: %w", raw, err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("OpenTelemetry endpoint %q has no host", raw)
	}

	path := u.Path
	if path == "" || path == "/" {
		path = defaultOTLPPath
	}
	if u.RawQuery != "" {
		path += "?" + u.RawQuery
	}

	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(u.Host),
		otlptracehttp.WithURLPath(path),
	}
	switch u.Scheme {
	case "http":
		opts = append(opts, otlptracehttp.WithInsecure())
	case "https":
		// The exporter's default transport is TLS, which is what an https
		// collector needs.
	default:
		return nil, fmt.Errorf("unsupported OpenTelemetry endpoint scheme %q, want http or https", u.Scheme)
	}
	return otlptracehttp.New(ctx, opts...)
}

// RecordSpanError marks a span as failed. RecordError alone only attaches an
// exception event in the Go SDK; setting the status as well is what lets a
// collector filter for failed sink writes or failed polls.
func RecordSpanError(span trace.Span, err error) {
	if span == nil || err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// RecordAttributes returns the span attributes that identify a record: the
// ledger it belongs to, the contract and event type when it is an event, and
// the transaction hash when one is known.
//
// Empty values are omitted rather than recorded as "", so a collector query on
// contract.id selects only spans for contract events.
func RecordAttributes(r Record) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.Int64("ledger.sequence", int64(r.Sequence)),
		attribute.String("record.kind", string(r.Kind)),
	}
	if r.ID != "" {
		attrs = append(attrs, attribute.String("record.id", r.ID))
	}

	var contractID, eventType, txHash string
	if r.Event != nil {
		contractID, eventType, txHash = r.Event.ContractID, r.Event.Type, r.Event.TxHash
	}
	if r.Transaction != nil && txHash == "" {
		txHash = r.Transaction.Hash
	}
	if eventType != "" {
		attrs = append(attrs, attribute.String("event.type", eventType))
	}
	if contractID != "" {
		attrs = append(attrs, attribute.String("contract.id", contractID))
	}
	if txHash != "" {
		attrs = append(attrs, attribute.String("transaction.hash", txHash))
	}
	return attrs
}
