package network

import (
	"context"

	"github.com/qubic/go-archiver-v2/tracing"
	"github.com/qubic/go-node-connector/v2/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// tracingQubicClient wraps a QubicClient and emits one span per hot-path network
// call. It embeds QubicClient so the ~13 non-instrumented methods pass through
// unchanged.
type tracingQubicClient struct {
	QubicClient
}

// NewTracingQubicClient returns a QubicClient that traces the hot-path network
// methods used by the tick-syncing loop.
func NewTracingQubicClient(inner QubicClient) QubicClient {
	return &tracingQubicClient{QubicClient: inner}
}

func recordErr(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

func (c *tracingQubicClient) GetTickInfo(ctx context.Context) (types.TickInfo, error) {
	ctx, span := tracing.Tracer().Start(ctx, "qubic.GetTickInfo", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	res, err := c.QubicClient.GetTickInfo(ctx)
	recordErr(span, err)
	return res, err
}

func (c *tracingQubicClient) GetSystemInfo(ctx context.Context) (types.SystemInfo, error) {
	ctx, span := tracing.Tracer().Start(ctx, "qubic.GetSystemInfo", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	res, err := c.QubicClient.GetSystemInfo(ctx)
	recordErr(span, err)
	return res, err
}

func (c *tracingQubicClient) GetComputors(ctx context.Context) (types.Computors, error) {
	ctx, span := tracing.Tracer().Start(ctx, "qubic.GetComputors", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	res, err := c.QubicClient.GetComputors(ctx)
	recordErr(span, err)
	return res, err
}

func (c *tracingQubicClient) GetQuorumVotes(ctx context.Context, tickNumber uint32) (types.QuorumVotes, error) {
	ctx, span := tracing.Tracer().Start(ctx, "qubic.GetQuorumVotes",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.Int64("qubic.tick_number", int64(tickNumber))))
	defer span.End()
	res, err := c.QubicClient.GetQuorumVotes(ctx, tickNumber)
	recordErr(span, err)
	return res, err
}

func (c *tracingQubicClient) GetTickData(ctx context.Context, tickNumber uint32) (types.TickData, error) {
	ctx, span := tracing.Tracer().Start(ctx, "qubic.GetTickData",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.Int64("qubic.tick_number", int64(tickNumber))))
	defer span.End()
	res, err := c.QubicClient.GetTickData(ctx, tickNumber)
	recordErr(span, err)
	return res, err
}

func (c *tracingQubicClient) GetTickTransactions(ctx context.Context, tickNumber uint32) (types.Transactions, error) {
	ctx, span := tracing.Tracer().Start(ctx, "qubic.GetTickTransactions",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.Int64("qubic.tick_number", int64(tickNumber))))
	defer span.End()
	res, err := c.QubicClient.GetTickTransactions(ctx, tickNumber)
	recordErr(span, err)
	return res, err
}

func (c *tracingQubicClient) GetTxStatus(ctx context.Context, tick uint32) (types.TransactionStatus, error) {
	ctx, span := tracing.Tracer().Start(ctx, "qubic.GetTxStatus",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.Int64("qubic.tick_number", int64(tick))))
	defer span.End()
	res, err := c.QubicClient.GetTxStatus(ctx, tick)
	recordErr(span, err)
	return res, err
}
