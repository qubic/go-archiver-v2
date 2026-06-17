package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
)

// Config holds the tracing configuration. It mirrors the Tracing config
// sub-struct in main.go.
type Config struct {
	Enabled       bool
	Exporter      string // stdout | otlp-grpc | otlp-http | none
	OTLPEndpoint  string
	OTLPInsecure  bool
	SamplingRatio float64
	ServiceName   string
}

func noopShutdown(context.Context) error { return nil }

// Setup builds the global TracerProvider and returns a tracer plus a shutdown
// func that flushes the batch span processor. When tracing is disabled it
// installs a no-op tracer (spans are dropped) so callers stay free of nil checks.
func Setup(ctx context.Context, cfg Config) (trace.Tracer, func(context.Context) error, error) {
	if !cfg.Enabled || cfg.Exporter == "none" {
		return otel.Tracer(cfg.ServiceName), noopShutdown, nil
	}

	exporter, err := newExporter(ctx, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("creating span exporter: %w", err)
	}

	tp, err := newTracerProvider(exporter, cfg)
	if err != nil {
		return nil, nil, err
	}
	otel.SetTracerProvider(tp)

	return tp.Tracer(cfg.ServiceName), tp.Shutdown, nil
}

// newTracerProvider builds the provider used by Setup. It is kept separate so
// tests can drive it with an in-memory exporter and assert each span is exported
// exactly once (a single BatchSpanProcessor, no duplicate processors).
func newTracerProvider(exporter sdktrace.SpanExporter, cfg Config) (*sdktrace.TracerProvider, error) {
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.ServiceName),
	))
	if err != nil {
		return nil, fmt.Errorf("building resource: %w", err)
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter), // async export, no hot-path latency
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SamplingRatio))),
	), nil
}

func newExporter(ctx context.Context, cfg Config) (sdktrace.SpanExporter, error) {
	switch cfg.Exporter {
	case "stdout":
		return stdouttrace.New(stdouttrace.WithPrettyPrint())
	case "otlp-grpc":
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint)}
		if cfg.OTLPInsecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		return otlptracegrpc.New(ctx, opts...)
	case "otlp-http":
		opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(cfg.OTLPEndpoint)}
		if cfg.OTLPInsecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		return otlptracehttp.New(ctx, opts...)
	default:
		return nil, fmt.Errorf("unknown tracing exporter: %q", cfg.Exporter)
	}
}
