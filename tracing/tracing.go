package tracing

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// scopeName is the OpenTelemetry instrumentation scope for all archiver spans.
const scopeName = "archiver-v2"

// Tracer returns the archiver's tracer from the global TracerProvider configured
// by telemetry.Setup (a no-op provider until then), so packages can create spans
// without holding or injecting a tracer.
func Tracer() trace.Tracer {
	return otel.Tracer(scopeName)
}
