package e2e

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/faketool"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/gateway"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcp"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcpclient"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
)

var refund = map[string]any{"reference": "order-1001", "amount": 20}

// The response from the gateway to the client is lost: the client times
// out, the gateway still records the result, and the retry gets it.
func TestLostResponseToClientIsReplayed(t *testing.T) {
	e := newEnv(t, envOptions{})
	e.fake.AddFault(faketool.Fault{Tool: "create_refund", DelayAfterMs: 400})

	short, cancel := context.WithTimeout(bg, 100*time.Millisecond)
	defer cancel()
	if _, err := e.client.Call(short, "create_refund", refund, keyed("k1")); err == nil {
		t.Fatal("expected the client to time out")
	}
	time.Sleep(600 * time.Millisecond)

	r, res := call(t, e.client, "create_refund", refund, keyed("k1"))
	if res.IsError || res.StructuredContent["refundId"] != "rf_1" || r.Header.Get("X-Gateway-Replayed") != "true" {
		t.Fatalf("retry did not get the original result: %s", res.Raw)
	}
	if n := e.fake.Ledger.CountReference("order-1001"); n != 1 {
		t.Fatalf("I1: %d effects, want 1", n)
	}
}

// The upstream response is lost after the effect: the gateway cannot know
// what happened, says so, and never resends.
func TestLostUpstreamResponseIsUnknown(t *testing.T) {
	e := newEnv(t, envOptions{})
	e.fake.AddFault(faketool.Fault{Tool: "create_refund", DropResponse: true})

	for i := range 3 {
		_, res := call(t, e.client, "create_refund", refund, keyed("k1"))
		if !res.IsError || res.GatewayStatus() != gateway.StatusOutcomeUnknown {
			t.Fatalf("attempt %d: want outcome unknown, got %s", i+1, res.Raw)
		}
	}
	if n := e.fake.Ledger.CountReference("order-1001"); n != 1 {
		t.Fatalf("I1: %d effects, want 1", n)
	}
	if n := e.fake.Requests("create_refund"); n != 1 {
		t.Fatalf("gateway resent an at_most_once call: %d requests", n)
	}
}

func TestReplayMatchesOriginal(t *testing.T) {
	e := newEnv(t, envOptions{})
	_, first := call(t, e.client, "create_refund", refund, keyed("k1"))
	r, again := call(t, e.client, "create_refund", refund, keyed("k1"))
	if string(first.Raw) != string(again.Raw) {
		t.Fatalf("I5: replay differs:\n%s\n%s", first.Raw, again.Raw)
	}
	if r.Header.Get("X-Gateway-Replayed") != "true" || r.Header.Get("X-Gateway-Operation-Key") == "" {
		t.Fatal("replay headers missing")
	}
	if e.fake.Ledger.CountReference("order-1001") != 1 {
		t.Fatal("I1 violated")
	}
}

func TestToolErrorsAreStoredAndReplayed(t *testing.T) {
	e := newEnv(t, envOptions{})
	bad := map[string]any{"reference": "order-1", "amount": 0}
	_, first := call(t, e.client, "create_refund", bad, keyed("k1"))
	_, again := call(t, e.client, "create_refund", bad, keyed("k1"))
	if !first.IsError || string(first.Raw) != string(again.Raw) || e.fake.Requests("create_refund") != 1 {
		t.Fatalf("isError result not replayed: %s / %s", first.Raw, again.Raw)
	}
}

func TestKeyConflict(t *testing.T) {
	e := newEnv(t, envOptions{})
	call(t, e.client, "create_refund", refund, keyed("k1"))
	r, _ := call(t, e.client, "create_refund", map[string]any{"reference": "order-1001", "amount": 99}, keyed("k1"))
	if err := r.RPCError(); err == nil || err.Code != mcp.CodeKeyConflict {
		t.Fatalf("I6: want key conflict, got %+v", err)
	}
	if e.fake.Requests("create_refund") != 1 {
		t.Fatal("conflicting call reached the tool")
	}
}

func TestRequireKey(t *testing.T) {
	e := newEnv(t, envOptions{extra: "  wire_money: {policy: at_most_once, require_key: true}"})
	r, _ := call(t, e.client, "wire_money", map[string]any{}, mcpclient.CallOptions{})
	if err := r.RPCError(); err == nil || err.Code != mcp.CodeKeyRequired {
		t.Fatalf("want key required, got %+v", err)
	}
}

func TestReadOnlyPassesThrough(t *testing.T) {
	e := newEnv(t, envOptions{})
	for range 2 {
		r, res := call(t, e.client, "get_invoice", map[string]any{"id": "inv-1"}, keyed("k1"))
		if res.IsError || r.Header.Get("X-Gateway-Operation-Key") != "" {
			t.Fatalf("read-only call was intercepted: %s", res.Raw)
		}
	}
	if e.fake.Requests("get_invoice") != 2 {
		t.Fatal("read-only calls should always reach the tool")
	}
	ops, _ := e.store.List(bg, store.ListFilter{})
	if len(ops) != 0 {
		t.Fatalf("read-only call was recorded: %d operations", len(ops))
	}
}

func TestKeylessCallsGroupWithinWindow(t *testing.T) {
	e := newEnv(t, envOptions{})
	email := map[string]any{"to": "a@example.com", "subject": "Your refund"}
	call(t, e.client, "send_email", email, mcpclient.CallOptions{})
	call(t, e.client, "send_email", map[string]any{"subject": "Your refund", "to": "a@example.com"}, mcpclient.CallOptions{})
	if n := e.fake.Requests("send_email"); n != 1 {
		t.Fatalf("identical keyless calls inside the window ran %d times", n)
	}
	time.Sleep(2200 * time.Millisecond) // fallback_window is 2s
	call(t, e.client, "send_email", email, mcpclient.CallOptions{})
	if n := e.fake.Requests("send_email"); n != 2 {
		t.Fatalf("a call after the window should be a new operation; ran %d times", n)
	}
}

func TestRunAndStepIdentifyOperation(t *testing.T) {
	e := newEnv(t, envOptions{})
	o := mcpclient.CallOptions{RunID: "run-7", StepID: "step-3"}
	call(t, e.client, "create_refund", refund, o)
	call(t, e.client, "create_refund", refund, o)
	o.StepID = "step-4"
	call(t, e.client, "create_refund", refund, o)
	if n := e.fake.Ledger.CountReference("order-1001"); n != 2 {
		t.Fatalf("want one effect per step, got %d", n)
	}
}

func TestConcurrentDuplicatesRunOnce(t *testing.T) {
	e := newEnv(t, envOptions{})
	e.fake.AddFault(faketool.Fault{Tool: "create_refund", DelayBeforeMs: 300})
	var wg sync.WaitGroup
	var replays atomic.Int32
	for range 10 {
		wg.Go(func() {
			r, res := call(t, e.client, "create_refund", refund, keyed("k1"))
			if res == nil || res.IsError {
				t.Errorf("duplicate got an error: %+v", r.RPCError())
				return
			}
			if r.Header.Get("X-Gateway-Replayed") == "true" {
				replays.Add(1)
			}
		})
	}
	wg.Wait()
	if n := e.fake.Ledger.CountReference("order-1001"); n != 1 {
		t.Fatalf("I1: %d effects, want 1", n)
	}
	if replays.Load() != 9 {
		t.Fatalf("want 9 replays, got %d", replays.Load())
	}
}

func TestSlowCallReportsInProgress(t *testing.T) {
	e := newEnv(t, envOptions{})
	e.fake.AddFault(faketool.Fault{Tool: "create_refund", DelayBeforeMs: 3000})
	done := make(chan struct{})
	go func() {
		defer close(done)
		call(t, e.client, "create_refund", refund, keyed("k1"))
	}()
	time.Sleep(200 * time.Millisecond)
	_, res := call(t, e.client, "create_refund", refund, keyed("k1")) // wait_timeout is 2s
	if res.GatewayStatus() != gateway.StatusInProgress {
		t.Fatalf("want in progress, got %s", res.Raw)
	}
	<-done
	_, res = call(t, e.client, "create_refund", refund, keyed("k1"))
	if res.IsError || e.fake.Ledger.CountReference("order-1001") != 1 {
		t.Fatalf("after completion: %s", res.Raw)
	}
}

func TestUpstreamDownIsNotExecuted(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := "http://" + ln.Addr().String() + "/mcp"
	ln.Close()
	e := newEnv(t, envOptions{upstream: dead})

	r, res := call(t, e.client, "create_refund", refund, keyed("k1"))
	if res.GatewayStatus() != gateway.StatusNotExecuted {
		t.Fatalf("want not executed, got %s", res.Raw)
	}
	if got := e.opState(t, r.Header.Get("X-Gateway-Operation-Key")); got != store.NotExecuted {
		t.Fatalf("state %s, want not_executed", got)
	}
	// Proven unsent, so a retry may try again.
	_, res = call(t, e.client, "create_refund", refund, keyed("k1"))
	if res.GatewayStatus() != gateway.StatusNotExecuted {
		t.Fatalf("retry: %s", res.Raw)
	}
}

func TestPrincipalsAreSeparated(t *testing.T) {
	e := newEnv(t, envOptions{extra: "auth:\n  tokens:\n    alice-token: alice\n    bob-token: bob"})
	alice, bob := mcpclient.New(e.url), mcpclient.New(e.url)
	alice.Token, bob.Token = "alice-token", "bob-token"

	_, a := call(t, alice, "create_refund", refund, keyed("shared"))
	_, b := call(t, bob, "create_refund", refund, keyed("shared"))
	if a.StructuredContent["refundId"] == b.StructuredContent["refundId"] {
		t.Fatal("I9: bob received alice's stored result")
	}

	anon := mcpclient.New(e.url)
	r, err := anon.Call(bg, "create_refund", refund, keyed("shared"))
	if err != nil || r.Status != http.StatusUnauthorized {
		t.Fatalf("unauthenticated call: %v %d", err, r.Status)
	}
	revoked := mcpclient.New(e.url)
	revoked.Token = "old-token"
	if r, _ := revoked.Call(bg, "create_refund", refund, keyed("shared")); r.Status != http.StatusUnauthorized {
		t.Fatal("an unknown token got a replay")
	}
}

func TestHeaderMismatchRejectedAtGateway(t *testing.T) {
	e := newEnv(t, envOptions{})
	h := http.Header{mcp.HeaderName: {"get_invoice"}}
	r, err := e.client.Call(bg, "create_refund", refund, mcpclient.CallOptions{Header: h})
	if err != nil || r.Status != http.StatusBadRequest || r.RPCError().Code != mcp.CodeHeaderMismatch {
		t.Fatalf("want 400 header mismatch: %v %+v", err, r)
	}
	if e.fake.Requests("create_refund")+e.fake.Requests("get_invoice") != 0 {
		t.Fatal("a mismatched request reached the tool")
	}
}

func TestNativeKeyResendsSafely(t *testing.T) {
	e := newEnv(t, envOptions{})
	e.fake.AddFault(faketool.Fault{Tool: "create_charge", DropResponse: true})
	_, res := call(t, e.client, "create_charge", map[string]any{"amount": 5}, keyed("charge-1"))
	if res.IsError {
		t.Fatalf("resend with the same key should succeed: %s", res.Raw)
	}
	effects := e.fake.Ledger.Effects()
	if len(effects) != 1 || e.fake.Requests("create_charge") != 2 {
		t.Fatalf("I2: %d effects from %d requests, want 1 from 2", len(effects), e.fake.Requests("create_charge"))
	}
	if effects[0].Key == "" {
		t.Fatal("the gateway did not inject an idempotency key")
	}
}

func TestProgressIsStreamedAndFinalIsReplayed(t *testing.T) {
	e := newEnv(t, envOptions{})
	e.fake.AddFault(faketool.Fault{Tool: "send_email", Progress: 3})
	o := mcpclient.CallOptions{OperationKey: "k1", ProgressToken: "p1"}
	r, err := e.client.Call(bg, "send_email", map[string]any{"to": "a", "subject": "s"}, o)
	if err != nil || len(r.Notifications) != 3 {
		t.Fatalf("progress not forwarded: %v %+v", err, r)
	}
	first, _ := r.ToolResult()
	_, again := call(t, e.client, "send_email", map[string]any{"to": "a", "subject": "s"}, o)
	if string(first.Raw) != string(again.Raw) {
		t.Fatal("replay of a streamed call differs")
	}
}

func TestMultiRoundTripThroughGateway(t *testing.T) {
	e := newEnv(t, envOptions{})
	args := map[string]any{"to": "bob", "amount": 10}
	_, first := call(t, e.client, "confirm_transfer", args, keyed("t1"))
	if first.ResultType != mcp.ResultInputRequired {
		t.Fatalf("want input_required, got %s", first.Raw)
	}
	cont := mcpclient.CallOptions{
		OperationKey:   "t1",
		InputResponses: map[string]any{"confirm": map[string]any{"action": "accept", "content": map[string]any{}}},
		RequestState:   first.RequestState,
	}
	_, done := call(t, e.client, "confirm_transfer", args, cont)
	if done.StructuredContent["transferred"] != true {
		t.Fatalf("continuation: %s", done.Raw)
	}
	r, again := call(t, e.client, "confirm_transfer", args, cont)
	if string(again.Raw) != string(done.Raw) || r.Header.Get("X-Gateway-Replayed") != "true" {
		t.Fatal("completed continuation not replayed")
	}
	if e.fake.Ledger.CountReference("bob") != 1 {
		t.Fatal("transfer ran more than once")
	}
}

func TestTaskHandleIsReplayed(t *testing.T) {
	e := newEnv(t, envOptions{})
	caps := map[string]any{"extensions": map[string]any{faketool.TasksExtension: map[string]any{}}}
	o := mcpclient.CallOptions{OperationKey: "x1", Capabilities: caps}
	_, first := call(t, e.client, "start_export", map[string]any{"dataset": "d"}, o)
	_, again := call(t, e.client, "start_export", map[string]any{"dataset": "d"}, o)
	if first.TaskID == "" || first.TaskID != again.TaskID || e.fake.Ledger.CountReference("d") != 1 {
		t.Fatalf("task handle not replayed: %s / %s", first.Raw, again.Raw)
	}
	r, err := e.client.Request(bg, "tasks/get", map[string]any{"taskId": first.TaskID})
	if err != nil || r.RPCError() != nil {
		t.Fatalf("tasks/get should pass through: %v %+v", err, r.RPCError())
	}
}

func TestClientDisconnectDoesNotLoseOutcome(t *testing.T) {
	e := newEnv(t, envOptions{})
	e.fake.AddFault(faketool.Fault{Tool: "create_refund", DelayBeforeMs: 300})
	short, cancel := context.WithTimeout(bg, 100*time.Millisecond)
	defer cancel()
	_, _ = e.client.Call(short, "create_refund", refund, keyed("k1"))
	time.Sleep(500 * time.Millisecond)

	r, res := call(t, e.client, "create_refund", refund, keyed("k1"))
	if res.IsError || r.Header.Get("X-Gateway-Replayed") != "true" {
		t.Fatalf("outcome lost after the client left: %s", res.Raw)
	}
	if e.fake.Ledger.CountReference("order-1001") != 1 {
		t.Fatal("I1 violated")
	}
}

// Go's HTTP transport resends a request after a failure on a reused
// connection when it thinks the request is replayable, which includes a
// POST carrying an Idempotency-Key header. The gateway must never let that
// happen to a dispatched call.
func TestUpstreamClientNeverResends(t *testing.T) {
	var requests atomic.Int32
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	fake := faketool.New()
	var onReusedConn atomic.Bool
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				br := bufio.NewReader(conn)
				for n := 0; ; n++ {
					req, err := http.ReadRequest(br)
					if err != nil {
						return
					}
					if req.Header.Get(mcp.HeaderName) == "create_refund" {
						if requests.Add(1) == 1 && n > 0 {
							onReusedConn.Store(true)
						}
						// Read the whole call, then close cleanly without a
						// response. Closing with unread data would send a
						// reset, which the transport never retries.
						_, _ = io.Copy(io.Discard, req.Body)
						return
					}
					// Serve anything else normally, keeping the connection alive.
					rec := httptest.NewRecorder()
					fake.Handler().ServeHTTP(rec, req)
					resp := rec.Result()
					resp.ContentLength = int64(rec.Body.Len())
					_ = resp.Write(conn)
				}
			}(conn)
		}
	}()

	e := newEnv(t, envOptions{upstream: "http://" + ln.Addr().String() + "/mcp"})
	// Warm a keep-alive connection so the call goes out on a reused one.
	// The transport returns a finished connection to its idle pool
	// asynchronously, so give it a moment.
	call(t, e.client, "send_email", map[string]any{"to": "a", "subject": "s"}, keyed("warm"))
	time.Sleep(100 * time.Millisecond)

	h := http.Header{"Idempotency-Key": {"client-supplied"}}
	_, res := call(t, e.client, "create_refund", refund, mcpclient.CallOptions{OperationKey: "k1", Header: h})
	if res.GatewayStatus() != gateway.StatusOutcomeUnknown {
		t.Fatalf("want outcome unknown, got %s", res.Raw)
	}
	time.Sleep(100 * time.Millisecond)
	if !onReusedConn.Load() {
		t.Fatal("precondition: the call did not go out on a reused connection, so this test proves nothing")
	}
	if n := requests.Load(); n != 1 {
		t.Fatalf("the upstream received the call %d times", n)
	}
}
