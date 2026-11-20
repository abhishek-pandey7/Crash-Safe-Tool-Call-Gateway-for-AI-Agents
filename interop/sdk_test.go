// Package interop runs the gateway between the official MCP Go SDK's
// client and server, so the protocol handling is checked against an
// implementation written by someone else, not only against this
// repository's own fake server and test client.
package interop

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/config"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/gateway"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/opkey"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store/storetest"
)

// ledger counts effects that really happened inside the SDK server.
type ledger struct {
	mu      sync.Mutex
	effects map[string]int
}

func (l *ledger) add(kind, ref string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.effects == nil {
		l.effects = map[string]int{}
	}
	l.effects[kind+":"+ref]++
	n := 0
	for _, c := range l.effects {
		n += c
	}
	return n
}

func (l *ledger) count(kind, ref string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.effects[kind+":"+ref]
}

type refundIn struct {
	Reference string `json:"reference"`
	Amount    int    `json:"amount"`
}

type refundOut struct {
	RefundID  string `json:"refundId"`
	Reference string `json:"reference"`
}

type lookupIn struct {
	Reference string `json:"reference"`
}

type lookupOut struct {
	Found     bool   `json:"found"`
	Reference string `json:"reference"`
}

type emailIn struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

type emailOut struct {
	MessageID int `json:"messageId"`
}

// newSDKServer builds a server with the official SDK whose tools record
// their effects in l.
func newSDKServer(l *ledger) *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{Name: "sdk-refunds", Version: "0.0.1"}, nil)
	mcp.AddTool(s, &mcp.Tool{Name: "create_refund", Description: "Refund an order."},
		func(_ context.Context, _ *mcp.CallToolRequest, in refundIn) (*mcp.CallToolResult, refundOut, error) {
			n := l.add("refund", in.Reference)
			return nil, refundOut{RefundID: fmt.Sprintf("rf_%d", n), Reference: in.Reference}, nil
		})
	mcp.AddTool(s, &mcp.Tool{Name: "find_refund_by_reference", Description: "Look up a refund. Read-only.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true}},
		func(_ context.Context, _ *mcp.CallToolRequest, in lookupIn) (*mcp.CallToolResult, lookupOut, error) {
			return nil, lookupOut{Found: l.count("refund", in.Reference) > 0, Reference: in.Reference}, nil
		})
	mcp.AddTool(s, &mcp.Tool{Name: "send_email", Description: "Send an email."},
		func(_ context.Context, _ *mcp.CallToolRequest, in emailIn) (*mcp.CallToolResult, emailOut, error) {
			return nil, emailOut{MessageID: l.add("email", in.To)}, nil
		})
	return s
}

// dropper loses the response to the next tools/call of an armed tool after
// the SDK has run it: the effect happens and the answer never arrives.
type dropper struct {
	next http.Handler
	mu   sync.Mutex
	arm  map[string]int
}

func (d *dropper) lose(tool string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.arm == nil {
		d.arm = map[string]int{}
	}
	d.arm[tool]++
}

func (d *dropper) take(tool string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.arm[tool] > 0 {
		d.arm[tool]--
		return true
	}
	return false
}

func (d *dropper) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		d.next.ServeHTTP(w, r)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	var msg struct {
		Method string `json:"method"`
		Params struct {
			Name string `json:"name"`
		} `json:"params"`
	}
	_ = json.Unmarshal(body, &msg)
	if msg.Method != "tools/call" || !d.take(msg.Params.Name) {
		d.next.ServeHTTP(w, r)
		return
	}
	d.next.ServeHTTP(&discard{h: http.Header{}}, r) // the tool runs
	if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
		conn.Close() // and its answer is lost
	}
}

// discard is a ResponseWriter that throws the response away.
type discard struct{ h http.Header }

func (d *discard) Header() http.Header         { return d.h }
func (d *discard) Write(p []byte) (int, error) { return len(p), nil }
func (d *discard) WriteHeader(int)             {}
func (d *discard) Flush()                      {}

// upstream serves an SDK server in stateless mode, as the 2026-07-28
// protocol requires, with JSON or SSE responses.
func upstream(t *testing.T, l *ledger, jsonResponses bool) (*dropper, string) {
	t.Helper()
	srv := newSDKServer(l)
	h := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv },
		&mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: jsonResponses})
	d := &dropper{next: h}
	ts := httptest.NewServer(d)
	t.Cleanup(ts.Close)
	return d, ts.URL + "/mcp"
}

const gatewayConfig = `
upstream: %s
lease: 1s
wait_timeout: 2s
upstream_timeout: 5s
store_timeout: 2s
reconcile_interval: 500ms
tools:
  find_refund_by_reference: {policy: read_only}
  create_refund:
    policy: reconcilable
    reconcile: {tool: find_refund_by_reference, match_argument: reference, settle_after: 2s}
  send_email: {policy: at_most_once}
`

// startGateway runs the gateway in front of upstreamURL and returns its URL.
func startGateway(t *testing.T, upstreamURL string) string {
	t.Helper()
	cfg, err := config.Parse(fmt.Appendf(nil, gatewayConfig, upstreamURL))
	if err != nil {
		t.Fatal(err)
	}
	st := storetest.New(t)
	gw, err := gateway.New(cfg, st, gateway.Options{Instance: "interop"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go gw.Run(ctx)
	ts := httptest.NewServer(gw)
	t.Cleanup(ts.Close)
	return ts.URL + "/mcp"
}

// connect opens an official SDK client session.
func connect(t *testing.T, url string) *mcp.ClientSession {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := mcp.NewClient(&mcp.Implementation{Name: "sdk-client", Version: "0.0.1"}, nil)
	cs, err := c.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: url, DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { cs.Close() })
	return cs
}

func callTool(t *testing.T, cs *mcp.ClientSession, name string, args any, key string) (*mcp.CallToolResult, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p := &mcp.CallToolParams{Name: name, Arguments: args}
	if key != "" {
		p.Meta = mcp.Meta{opkey.MetaOperationKey: key}
	}
	return cs.CallTool(ctx, p)
}

func structured(t *testing.T, r *mcp.CallToolResult) map[string]any {
	t.Helper()
	raw, err := json.Marshal(r.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return m
}

func modes(t *testing.T, run func(t *testing.T, jsonResponses bool)) {
	t.Run("sse", func(t *testing.T) { run(t, false) })
	t.Run("json", func(t *testing.T) { run(t, true) })
}

// What the official SDK client does after a lost response, with no
// gateway. This documents the problem; the other tests check the fix.
func TestSDKLostResponseWithoutGateway(t *testing.T) {
	modes(t, func(t *testing.T, jsonResponses bool) {
		l := &ledger{}
		d, url := upstream(t, l, jsonResponses)
		cs := connect(t, url)
		d.lose("create_refund")
		res, err := callTool(t, cs, "create_refund", refundIn{Reference: "order-1", Amount: 20}, "")
		t.Logf("first attempt: result %v, error %v; refunds so far: %d", res != nil, err, l.count("refund", "order-1"))
		if err == nil {
			t.Fatal("the lost response reached the client; the fault did not apply")
		}
		// An agent retries, as the SDK requires after a broken stream.
		if _, err := callTool(t, cs, "create_refund", refundIn{Reference: "order-1", Amount: 20}, ""); err != nil {
			t.Fatalf("retry: %v", err)
		}
		if n := l.count("refund", "order-1"); n != 2 {
			t.Fatalf("expected the duplicate this project exists to prevent, got %d refunds", n)
		}
	})
}

// Ordinary traffic passes through the gateway between the SDK's client and
// server, and a retried call is answered from the record.
func TestSDKThroughGateway(t *testing.T) {
	modes(t, func(t *testing.T, jsonResponses bool) {
		l := &ledger{}
		_, url := upstream(t, l, jsonResponses)
		cs := connect(t, startGateway(t, url))

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		tools, err := cs.ListTools(ctx, nil)
		if err != nil {
			t.Fatalf("tools/list through the gateway: %v", err)
		}
		if len(tools.Tools) != 3 {
			t.Fatalf("listed %d tools", len(tools.Tools))
		}

		first, err := callTool(t, cs, "create_refund", refundIn{Reference: "order-2", Amount: 20}, "refund-order-2")
		if err != nil || first.IsError {
			t.Fatalf("first call: %v %+v", err, first)
		}
		again, err := callTool(t, cs, "create_refund", refundIn{Reference: "order-2", Amount: 20}, "refund-order-2")
		if err != nil || again.IsError {
			t.Fatalf("retry: %v %+v", err, again)
		}
		if a, b := structured(t, first), structured(t, again); a["refundId"] != b["refundId"] || a["refundId"] == nil {
			t.Fatalf("retry was not the same result: %v vs %v", a, b)
		}
		if n := l.count("refund", "order-2"); n != 1 {
			t.Fatalf("I1: %d refunds", n)
		}
	})
}

// A lost response to a tool with no way to check: the SDK client is told
// the outcome is unknown, and retrying does not send it again.
func TestSDKLostResponseIsUnknown(t *testing.T) {
	modes(t, func(t *testing.T, jsonResponses bool) {
		l := &ledger{}
		d, url := upstream(t, l, jsonResponses)
		cs := connect(t, startGateway(t, url))
		d.lose("send_email")

		for i := range 2 {
			res, err := callTool(t, cs, "send_email", emailIn{To: "a@example.com", Subject: "hi"}, "email-1")
			if err != nil {
				t.Fatalf("attempt %d: %v", i+1, err)
			}
			if !res.IsError || structured(t, res)["gatewayStatus"] != gateway.StatusOutcomeUnknown {
				t.Fatalf("attempt %d: want outcome unknown, got %+v", i+1, res)
			}
		}
		if n := l.count("email", "a@example.com"); n != 1 {
			t.Fatalf("I1: %d emails", n)
		}
	})
}

// A lost response to a reconcilable tool: the gateway's lookup, also made
// to the SDK server, confirms the refund, and the SDK client gets a result.
func TestSDKLostResponseIsReconciled(t *testing.T) {
	modes(t, func(t *testing.T, jsonResponses bool) {
		l := &ledger{}
		d, url := upstream(t, l, jsonResponses)
		cs := connect(t, startGateway(t, url))
		d.lose("create_refund")

		res, err := callTool(t, cs, "create_refund", refundIn{Reference: "order-3", Amount: 20}, "refund-order-3")
		if err != nil {
			t.Fatal(err)
		}
		if res.IsError || structured(t, res)["gatewayStatus"] != gateway.StatusReconciled {
			t.Fatalf("want a reconciled result, got %+v", res)
		}
		if n := l.count("refund", "order-3"); n != 1 {
			t.Fatalf("I1: %d refunds", n)
		}
	})
}
