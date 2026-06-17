package bob

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Client is an HTTP client for bob's JSON-RPC 2.0 API.
type Client struct {
	baseURL    string
	httpClient *http.Client
	tracer     trace.Tracer
}

// NewClient creates a new bob HTTP client. A nil tracer is replaced with a no-op
// tracer so callers (and tests) never need to pass one.
func NewClient(baseURL string, tracer trace.Tracer) *Client {
	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("bob")
	}
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		tracer: tracer,
	}
}

type jsonRPCRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
	ID      int         `json:"id"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result"`
	Error   *jsonRPCError   `json:"error,omitempty"`
	ID      int             `json:"id"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *jsonRPCError) Error() string {
	return fmt.Sprintf("RPC error %d: %s", e.Code, e.Message)
}

// RPCCall makes a JSON-RPC 2.0 call to bob.
func (c *Client) RPCCall(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	ctx, span := c.tracer.Start(ctx, "bob.RPCCall",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("bob.rpc_method", method)))
	defer span.End()

	res, err := c.rpcCall(ctx, method, params)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return res, err
}

func (c *Client) rpcCall(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	reqBody := jsonRPCRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      1,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshalling RPC request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/qubic", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating RPC request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing RPC request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading RPC response: %w", err)
	}

	var rpcResp jsonRPCResponse
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return nil, fmt.Errorf("unmarshalling RPC response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, rpcResp.Error
	}

	return rpcResp.Result, nil
}
