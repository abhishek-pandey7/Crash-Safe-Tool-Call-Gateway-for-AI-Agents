package faketool_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/faketool"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcp"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcpclient"
)

var ctx = context.Background()

func start(t *testing.T) (*faketool.Server, *mcpclient.Client) {
	t.Helper()
	fake := faketool.New()
	srv := httptest.NewServer(fake.Handler())
	t.Cleanup(srv.Close)
	return fake, mcpclient.New(srv.URL + "/mcp")
}

func call(t *testing.T, c *mcpclient.Client, tool string, args any, o mcpclient.CallOptions) *mcpclient.ToolResult {
	t.Helper()
	r, err := c.Call(ctx, tool, args, o)
	if err != nil {
		t.Fatal(err)
	}
	res, err := r.ToolResult()
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestRefundAndLookup(t *testing.T) {
	fake, c := start(t)
	res := call(t, c, "find_refund_by_reference", map[string]any{"reference": "r1"}, mcpclient.CallOptions{})
	if res.StructuredContent["found"] != false {
		t.Fatalf("found before refund: %+v", res)
	}
	res = call(t, c, "create_refund", map[string]any{"reference": "r1", "amount": 20}, mcpclient.CallOptions{})
	if res.IsError || res.StructuredContent["refundId"] != "rf_1" {
		t.Fatalf("refund: %+v", res)
	}
	res = call(t, c, "find_refund_by_reference", map[string]any{"reference": "r1"}, mcpclient.CallOptions{})
	if res.StructuredContent["found"] != true {
		t.Fatalf("not found after refund: %+v", res)
	}
	if fake.Ledger.CountReference("r1") != 1 {
		t.Fatal("ledger disagrees")
	}

	res = call(t, c, "create_refund", map[string]any{"reference": "r2", "amount": 0}, mcpclient.CallOptions{})
	if !res.IsError || fake.Ledger.CountReference("r2") != 0 {
		t.Fatalf("invalid amount should fail without effect: %+v", res)
	}
}

func TestRejectsMismatchedHeaders(t *testing.T) {
	fake, c := start(t)
	h := http.Header{}
	h.Set(mcp.HeaderName, "send_email")
	r, err := c.Call(ctx, "create_refund", map[string]any{"reference": "x", "amount": 1}, mcpclient.CallOptions{Header: h})
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != http.StatusBadRequest || r.RPCError() == nil || r.RPCError().Code != mcp.CodeHeaderMismatch {
		t.Fatalf("expected 400 HeaderMismatch, got %d %v", r.Status, r.RPCError())
	}
	if fake.Ledger.CountReference("x") != 0 || fake.Requests("create_refund") != 0 {
		t.Fatal("a rejected request reached the tool")
	}
}

func TestDropResponseLosesTheReplyNotTheEffect(t *testing.T) {
	fake, c := start(t)
	fake.AddFault(faketool.Fault{Tool: "create_refund", DropResponse: true})
	_, err := c.Call(ctx, "create_refund", map[string]any{"reference": "r1", "amount": 20}, mcpclient.CallOptions{})
	if !errors.Is(err, mcpclient.ErrTransport) {
		t.Fatalf("expected a transport error, got %v", err)
	}
	if fake.Ledger.CountReference("r1") != 1 {
		t.Fatal("the effect should have happened")
	}
}

func TestCancelBeforeEffect(t *testing.T) {
	fake, c := start(t)
	fake.AddFault(faketool.Fault{Tool: "send_email", DelayBeforeMs: 500})
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := c.Call(short, "send_email", map[string]any{"to": "a@x", "subject": "hi"}, mcpclient.CallOptions{}); err == nil {
		t.Fatal("expected timeout")
	}
	time.Sleep(600 * time.Millisecond)
	if n := fake.Ledger.Count(func(e faketool.Effect) bool { return e.Tool == "send_email" }); n != 0 {
		t.Fatalf("cancelled call took effect %d times", n)
	}

	fake.AddFault(faketool.Fault{Tool: "send_email", DelayBeforeMs: 300, IgnoreCancel: true})
	short2, cancel2 := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel2()
	_, _ = c.Call(short2, "send_email", map[string]any{"to": "a@x", "subject": "hi"}, mcpclient.CallOptions{})
	time.Sleep(400 * time.Millisecond)
	if n := fake.Ledger.Count(func(e faketool.Effect) bool { return e.Tool == "send_email" }); n != 1 {
		t.Fatalf("tool ignoring cancellation took effect %d times", n)
	}
}

func TestNativeKeyDeduplicates(t *testing.T) {
	fake, c := start(t)
	args := map[string]any{"amount": 5, "idempotency_key": "k1"}
	a := call(t, c, "create_charge", args, mcpclient.CallOptions{})
	b := call(t, c, "create_charge", args, mcpclient.CallOptions{})
	if string(a.Raw) != string(b.Raw) || fake.Ledger.CountReference("k1") != 1 {
		t.Fatalf("key not honoured: %s vs %s", a.Raw, b.Raw)
	}
}

func TestMultiRoundTrip(t *testing.T) {
	fake, c := start(t)
	args := map[string]any{"to": "bob", "amount": 10}
	first := call(t, c, "confirm_transfer", args, mcpclient.CallOptions{})
	if first.ResultType != mcp.ResultInputRequired || len(first.RequestState) == 0 {
		t.Fatalf("expected input_required: %s", first.Raw)
	}
	done := call(t, c, "confirm_transfer", args, mcpclient.CallOptions{
		InputResponses: map[string]any{"confirm": map[string]any{"action": "accept", "content": map[string]any{}}},
		RequestState:   first.RequestState,
	})
	if done.StructuredContent["transferred"] != true || fake.Ledger.CountReference("bob") != 1 {
		t.Fatalf("continuation: %s", done.Raw)
	}
}

func TestTaskHandle(t *testing.T) {
	_, c := start(t)
	caps := map[string]any{"extensions": map[string]any{faketool.TasksExtension: map[string]any{}}}
	res := call(t, c, "start_export", map[string]any{"dataset": "d"}, mcpclient.CallOptions{Capabilities: caps})
	if res.ResultType != mcp.ResultTask || res.TaskID == "" {
		t.Fatalf("expected a task: %s", res.Raw)
	}
	r, err := c.Request(ctx, "tasks/get", map[string]any{"taskId": res.TaskID})
	if err != nil || r.RPCError() != nil {
		t.Fatalf("tasks/get: %v %v", err, r.RPCError())
	}
}

func TestProgressStream(t *testing.T) {
	fake, c := start(t)
	fake.AddFault(faketool.Fault{Tool: "send_email", Progress: 3})
	r, err := c.Call(ctx, "send_email", map[string]any{"to": "a", "subject": "s"}, mcpclient.CallOptions{ProgressToken: "p1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Notifications) != 3 || r.Message == nil {
		t.Fatalf("got %d notifications, final %v", len(r.Notifications), r.Message)
	}
}
