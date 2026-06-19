package bob

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/qubic/go-archiver-v2/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Client is an HTTP client for bob's JSON-RPC 2.0 API.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a new bob HTTP client.
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Status is bob's REST /status payload (subset). currentIndexingTick is bob's indexing
// frontier: ticks at or below it have final tx execution flags, while ticks above it
// (fetched but not yet indexed) may report a premature executed=false.
type Status struct {
	CurrentFetchingTick uint32 `json:"currentFetchingTick"`
	CurrentIndexingTick uint32 `json:"currentIndexingTick"`
}

// GetStatus fetches bob's indexing frontier via REST GET /status (not the /qubic
// JSON-RPC endpoint).
func (c *Client) GetStatus(ctx context.Context) (Status, error) {
	ctx, span := tracing.Tracer().Start(ctx, "bob.GetStatus", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	st, err := c.getStatus(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return st, err
}

func (c *Client) getStatus(ctx context.Context) (Status, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/status", nil)
	if err != nil {
		return Status{}, fmt.Errorf("creating status request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Status{}, fmt.Errorf("executing status request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Status{}, fmt.Errorf("reading status response: %w", err)
	}

	var st Status
	if err := json.Unmarshal(body, &st); err != nil {
		return Status{}, fmt.Errorf("unmarshalling status response: %w", err)
	}
	return st, nil
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
	ctx, span := tracing.Tracer().Start(ctx, "bob.RPCCall",
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
