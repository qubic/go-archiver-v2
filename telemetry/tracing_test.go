package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSetup_Disabled_NoopAndNoError(t *testing.T) {
	shutdown, err := Setup(context.Background(), Config{Enabled: false, ServiceName: "test"})
	if err != nil {
		t.Fatalf("Setup returned error: %v", err)
	}
	// the global (no-op) tracer must still produce a usable, non-recording span
	_, span := otel.Tracer("test").Start(context.Background(), "x")
	span.End()
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown returned error: %v", err)
	}
}

func TestSetup_UnknownExporter_Errors(t *testing.T) {
	_, err := Setup(context.Background(), Config{Enabled: true, Exporter: "bogus", ServiceName: "test"})
	if err == nil {
		t.Fatal("expected error for unknown exporter, got nil")
	}
}

func TestSetup_StdoutExporter_RecordsSampledSpanTree(t *testing.T) {
	shutdown, err := Setup(context.Background(), Config{
		Enabled: true, Exporter: "stdout", SamplingRatio: 1.0, ServiceName: "smoke",
	})
	if err != nil {
		t.Fatalf("Setup returned error: %v", err)
	}
	defer func() {
		if err := shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown returned error: %v", err)
		}
	}()

	// Setup installed the global provider; obtain the tracer the way packages do.
	tracer := otel.Tracer("smoke")
	ctx, root := tracer.Start(context.Background(), "process_tick")
	if !root.IsRecording() {
		t.Error("root span should be recording with ratio 1.0 sampling")
	}
	if !root.SpanContext().IsValid() {
		t.Error("root span context should be valid (provider wired)")
	}
	rootTraceID := root.SpanContext().TraceID()

	childCtx, child := tracer.Start(ctx, "store_phase")
	if child.SpanContext().TraceID() != rootTraceID {
		t.Error("child span should share the root's trace ID (parent propagation)")
	}
	if child.SpanContext().SpanID() == root.SpanContext().SpanID() {
		t.Error("child span should have its own span ID")
	}
	child.End()
	root.End()
	_ = childCtx
}

// TestNewTracerProvider_ExportsEachSpanOnce guards against the SDK setup being a
// source of duplicate spans: with a single BatchSpanProcessor each emitted span
// must be exported exactly once. If Jaeger shows doubled spans while this passes,
// the duplication is on the transport/ingest side (e.g. OTLP at-least-once retry),
// not in our provider.
func TestNewTracerProvider_ExportsEachSpanOnce(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp, err := newTracerProvider(exporter, Config{SamplingRatio: 1.0, ServiceName: "dedup-test"})
	if err != nil {
		t.Fatalf("newTracerProvider returned error: %v", err)
	}
	tracer := tp.Tracer("dedup-test")

	ctx, root := tracer.Start(context.Background(), "process_tick")
	_, a := tracer.Start(ctx, "fetch")
	a.End()
	_, b := tracer.Start(ctx, "store_phase")
	b.End()
	root.End()

	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush returned error: %v", err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 3 {
		t.Fatalf("expected 3 exported spans, got %d (duplicate export in provider setup?)", len(spans))
	}
	seen := map[string]int{}
	for _, s := range spans {
		seen[s.SpanContext.SpanID().String()]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("span %s exported %d times, want 1", id, n)
		}
	}
}
