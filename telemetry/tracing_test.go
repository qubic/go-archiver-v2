package telemetry

import (
	"context"
	"testing"
)

func TestSetup_Disabled_NoopAndNoError(t *testing.T) {
	tracer, shutdown, err := Setup(context.Background(), Config{Enabled: false, ServiceName: "test"})
	if err != nil {
		t.Fatalf("Setup returned error: %v", err)
	}
	// noop tracer must still produce a usable (non-recording) span
	_, span := tracer.Start(context.Background(), "x")
	span.End()
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown returned error: %v", err)
	}
}

func TestSetup_UnknownExporter_Errors(t *testing.T) {
	_, _, err := Setup(context.Background(), Config{Enabled: true, Exporter: "bogus", ServiceName: "test"})
	if err == nil {
		t.Fatal("expected error for unknown exporter, got nil")
	}
}

func TestSetup_StdoutExporter_RecordsSampledSpanTree(t *testing.T) {
	tracer, shutdown, err := Setup(context.Background(), Config{
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
