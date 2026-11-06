// Package mcpclient is a minimal MCP client for tests and benchmarks. It
// sends each request as a fresh HTTP POST with a new JSON-RPC id, as a
// conforming 2026-07-28 client must, and accepts JSON or SSE responses.
package mcpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcp"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/opkey"
)

// Client talks to one MCP endpoint.
type Client struct {
	URL   string
	HTTP  *http.Client
	Token string // sent as a bearer token when set
	// Capabilities is sent as io.modelcontextprotocol/clientCapabilities.
	Capabilities map[string]any

	nextID atomic.Int64
}

// New returns a client for the endpoint at url.
func New(url string) *Client {
	return &Client{URL: url, HTTP: &http.Client{}, Capabilities: map[string]any{}}
}

// CallOptions are optional parts of a tools/call request.
type CallOptions struct {
	OperationKey   string
	RunID          string
	StepID         string
	ProgressToken  any
	InputResponses any
	RequestState   json.RawMessage
	Capabilities   map[string]any
	Header         http.Header
}

// Response is what came back for one request.
type Response struct {
	Status int
	Header http.Header
	// Message is the final JSON-RPC response.
	Message *mcp.Message
	// Notifications arrived on the SSE stream before the final response.
	Notifications []*mcp.Message
}

// ErrTransport wraps failures where no complete response arrived, such as a
// dropped connection. The request may or may not have taken effect.
var ErrTransport = errors.New("mcpclient: no complete response")

// Call sends tools/call.
func (c *Client) Call(ctx context.Context, tool string, args any, o CallOptions) (*Response, error) {
	meta := map[string]any{}
	if o.OperationKey != "" {
		meta[opkey.MetaOperationKey] = o.OperationKey
	}
	if o.RunID != "" {
		meta[opkey.MetaRunID] = o.RunID
	}
	if o.StepID != "" {
		meta[opkey.MetaStepID] = o.StepID
	}
	if o.ProgressToken != nil {
		meta["progressToken"] = o.ProgressToken
	}
	params := map[string]any{"name": tool, "arguments": args}
	if o.InputResponses != nil {
		params["inputResponses"] = o.InputResponses
	}
	if o.RequestState != nil {
		params["requestState"] = o.RequestState
	}
	return c.request(ctx, mcp.MethodToolsCall, tool, params, meta, o.Capabilities, o.Header)
}

// Request sends any other method with the given params.
func (c *Client) Request(ctx context.Context, method string, params map[string]any) (*Response, error) {
	name := ""
	if n, ok := params["name"].(string); ok {
		name = n
	} else if u, ok := params["uri"].(string); ok {
		name = u
	}
	return c.request(ctx, method, name, params, map[string]any{}, nil, nil)
}

func (c *Client) request(ctx context.Context, method, name string, params, meta, caps map[string]any, extra http.Header) (*Response, error) {
	if params == nil {
		params = map[string]any{}
	}
	if caps == nil {
		caps = c.Capabilities
	}
	meta[mcp.MetaProtocolVersion] = mcp.ProtocolVersion
	meta[mcp.MetaClientInfo] = map[string]any{"name": "crashsafe-testclient", "version": "0.1.0"}
	meta[mcp.MetaClientCapabilities] = caps
	params["_meta"] = meta

	id := c.nextID.Add(1)
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	mcp.SetRequestHeaders(req.Header, method, name)
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	// Extra headers replace the defaults, so tests can send a mismatch.
	for k, vs := range extra {
		req.Header.Del(k)
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTransport, err)
	}
	defer resp.Body.Close()
	out := &Response{Status: resp.StatusCode, Header: resp.Header}

	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		r := mcp.NewSSEReader(resp.Body)
		for {
			ev, err := r.Next()
			if err != nil {
				return out, fmt.Errorf("%w: stream ended before the final response: %v", ErrTransport, err)
			}
			m, err := mcp.ParseMessage(ev.Data)
			if err != nil {
				return out, fmt.Errorf("mcpclient: bad event: %w", err)
			}
			if m.IsResponse() {
				out.Message = m
				return out, nil
			}
			out.Notifications = append(out.Notifications, m)
		}
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return out, fmt.Errorf("%w: %v", ErrTransport, err)
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return out, nil
	}
	m, err := mcp.ParseMessage(raw)
	if err != nil {
		return out, fmt.Errorf("mcpclient: status %d, body %q: %w", resp.StatusCode, raw, err)
	}
	out.Message = m
	return out, nil
}

// RPCError returns the response's JSON-RPC error, if any.
func (r *Response) RPCError() *mcp.RPCError {
	if r == nil || r.Message == nil || len(r.Message.Error) == 0 {
		return nil
	}
	var e mcp.RPCError
	if json.Unmarshal(r.Message.Error, &e) != nil {
		return &mcp.RPCError{Code: mcp.CodeInternalError, Message: string(r.Message.Error)}
	}
	return &e
}

// ToolResult is a decoded tools/call result.
type ToolResult struct {
	ResultType        string          `json:"resultType"`
	IsError           bool            `json:"isError"`
	Content           []mcp.Content   `json:"content"`
	StructuredContent map[string]any  `json:"structuredContent"`
	InputRequests     json.RawMessage `json:"inputRequests"`
	RequestState      json.RawMessage `json:"requestState"`
	TaskID            string          `json:"taskId"`
	Raw               json.RawMessage `json:"-"`
}

// Text joins the result's text content.
func (t *ToolResult) Text() string {
	var parts []string
	for _, c := range t.Content {
		parts = append(parts, c.Text)
	}
	return strings.Join(parts, "\n")
}

// GatewayStatus returns structuredContent.gatewayStatus, set on results the
// gateway produced itself.
func (t *ToolResult) GatewayStatus() string {
	s, _ := t.StructuredContent["gatewayStatus"].(string)
	return s
}

// ToolResult decodes the response's result.
func (r *Response) ToolResult() (*ToolResult, error) {
	if r == nil || r.Message == nil {
		return nil, errors.New("mcpclient: no response")
	}
	if e := r.RPCError(); e != nil {
		return nil, e
	}
	var t ToolResult
	if err := json.Unmarshal(r.Message.Result, &t); err != nil {
		return nil, fmt.Errorf("mcpclient: decode result: %w", err)
	}
	if t.ResultType == "" {
		t.ResultType = mcp.ResultComplete
	}
	t.Raw = r.Message.Result
	return &t, nil
}
