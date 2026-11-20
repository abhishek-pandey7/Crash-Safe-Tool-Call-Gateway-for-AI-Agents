package e2e

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/faketool"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/faultproxy"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/gateway"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcpclient"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
)

// outageConfig keeps store calls short, so an outage is noticed quickly.
const outageConfig = "store_timeout: 1s"

// Both ways Postgres can fail: refusing connections, or a partition that
// swallows traffic and answers nothing.
var outageModes = []faultproxy.Mode{faultproxy.Down, faultproxy.Hang}

// untilAnswered retries a call until the gateway answers with something
// other than store_unavailable or a transport error, as an agent would.
func untilAnswered(t *testing.T, c *mcpclient.Client, tool string, args any, o mcpclient.CallOptions) (*mcpclient.Response, *mcpclient.ToolResult) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(bg, 10*time.Second)
		r, err := c.Call(ctx, tool, args, o)
		cancel()
		if err != nil {
			if !errors.Is(err, mcpclient.ErrTransport) {
				t.Fatalf("%s: %v", tool, err)
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		res, err := r.ToolResult()
		if err != nil {
			t.Fatalf("%s: %v", tool, err)
		}
		if res.GatewayStatus() == gateway.StatusStoreUnavailable || res.GatewayStatus() == gateway.StatusInProgress {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return r, res
	}
	t.Fatalf("%s: no answer within 30s", tool)
	return nil, nil
}

// waitState polls the store until an operation reaches want.
func waitState(t *testing.T, st *store.Store, key string, want store.State, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last store.State
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(bg, time.Second)
		op, err := st.Get(ctx, key)
		cancel()
		if err == nil {
			last = op.State
			if last == want {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("operation %s stayed %q, want %q", key, last, want)
}

// With the store unreachable before a call starts, nothing is sent, the
// caller is told so promptly, and read-only tools keep working.
func TestStoreOutageRefusesBeforeSending(t *testing.T) {
	for _, mode := range outageModes {
		t.Run(mode.String(), func(t *testing.T) {
			e := newEnv(t, envOptions{proxyStore: true, extra: outageConfig})
			e.db.Set(mode)

			start := time.Now()
			_, res := call(t, e.client, "create_refund", refund, keyed("k1"))
			if res.GatewayStatus() != gateway.StatusStoreUnavailable {
				t.Fatalf("want store unavailable, got %s", res.Raw)
			}
			if took := time.Since(start); took > 5*time.Second {
				t.Fatalf("answer took %v; a hung store must not hang the caller", took)
			}
			if n := e.fake.Requests("create_refund"); n != 0 {
				t.Fatalf("sent %d requests with no store", n)
			}

			_, inv := call(t, e.client, "get_invoice", map[string]any{"id": "inv-1"}, mcpclient.CallOptions{})
			if inv == nil || inv.IsError {
				t.Fatalf("read-only call failed during the outage: %v", inv)
			}

			e.db.Set(faultproxy.Pass)
			_, res = untilAnswered(t, e.client, "create_refund", refund, keyed("k1"))
			if res.IsError || e.fake.Ledger.CountReference("order-1001") != 1 {
				t.Fatalf("after recovery: %s, %d effects", res.Raw, e.fake.Ledger.CountReference("order-1001"))
			}
		})
	}
}

// The store fails while the call is with the tool. The caller still gets
// the real answer, the record catches up once the store is back, and a
// retry replays it without running the tool again.
func TestStoreOutageDuringDispatchKeepsTheAnswer(t *testing.T) {
	for _, mode := range outageModes {
		t.Run(mode.String(), func(t *testing.T) {
			var e *env
			var once sync.Once
			e = newEnv(t, envOptions{proxyStore: true, extra: outageConfig, hooks: gateway.Hooks{
				At: func(p gateway.Point, _ string) {
					if p == gateway.PointAfterSend {
						once.Do(func() { e.db.Set(mode) })
					}
				},
			}})

			r, res := call(t, e.client, "create_refund", refund, keyed("k1"))
			if res == nil || res.IsError || res.StructuredContent["reference"] != "order-1001" {
				t.Fatalf("want the tool's result, got %v", res)
			}
			key := r.Header.Get("X-Gateway-Operation-Key")

			e.db.Set(faultproxy.Pass)
			waitState(t, e.store, key, store.Completed, 30*time.Second)

			r2, res2 := untilAnswered(t, e.client, "create_refund", refund, keyed("k1"))
			if r2.Header.Get("X-Gateway-Replayed") != "true" || res2.IsError {
				t.Fatalf("retry was not a replay: %s", res2.Raw)
			}
			if n := e.fake.Ledger.CountReference("order-1001"); n != 1 {
				t.Fatalf("I1: %d effects", n)
			}
			if n := e.fake.Requests("create_refund"); n != 1 {
				t.Fatalf("I8: %d requests", n)
			}
		})
	}
}

// The owner's lease runs out while the store is down and the tool is still
// working. A second instance must not resend, and the owner's late result
// still becomes the recorded outcome.
func TestLeaseLapsesDuringStoreOutage(t *testing.T) {
	var e *env
	var once sync.Once
	e = newEnv(t, envOptions{proxyStore: true, extra: outageConfig, hooks: gateway.Hooks{
		At: func(p gateway.Point, _ string) {
			if p == gateway.PointAfterSend {
				once.Do(func() {
					e.db.Set(faultproxy.Down)
					time.AfterFunc(2500*time.Millisecond, func() { e.db.Set(faultproxy.Pass) })
				})
			}
		},
	}})
	// Below the 5s upstream timeout, so the owner gets the real answer.
	e.fake.AddFault(faketool.Fault{Tool: "create_refund", DelayBeforeMs: 4000, IgnoreCancel: true})
	other := e.addGateway(t, "other", gateway.Hooks{}, true)
	otherClient := serve(t, other)

	first := make(chan *mcpclient.ToolResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(bg, 20*time.Second)
		defer cancel()
		r, err := e.client.Call(ctx, "create_refund", refund, keyed("k1"))
		if err != nil {
			first <- nil
			return
		}
		res, _ := r.ToolResult()
		first <- res
	}()

	// After the store is back, but while the tool is still working.
	time.Sleep(3500 * time.Millisecond)
	_, mid := untilAnswered(t, otherClient, "create_refund", refund, keyed("k1"))
	switch mid.GatewayStatus() {
	case gateway.StatusOutcomeUnknown, "":
	default:
		t.Fatalf("second instance answered %s", mid.Raw)
	}

	if res := <-first; res == nil || res.IsError {
		t.Fatalf("owner lost its answer: %v", res)
	}
	_, final := untilAnswered(t, otherClient, "create_refund", refund, keyed("k1"))
	if final.IsError || final.StructuredContent["reference"] != "order-1001" {
		t.Fatalf("late result was not recorded: %s", final.Raw)
	}
	if n := e.fake.Ledger.CountReference("order-1001"); n != 1 {
		t.Fatalf("I1: %d effects", n)
	}
	if n := e.fake.Requests("create_refund"); n != 1 {
		t.Fatalf("I8: %d requests", n)
	}
}

// A gateway process loses the store mid-dispatch and is then killed, so
// its pending write is lost. The next instance must report the outcome as
// unknown, never resend, and /-/readyz must report the outage meanwhile.
func TestStoreOutageThenCrashIsUnknownNotResent(t *testing.T) {
	fake, fakeURL := startFake(t)
	fake.AddFault(faketool.Fault{Tool: "create_refund", DelayBeforeMs: 1500, IgnoreCancel: true})
	cfg := writeConfig(t, fakeURL, "", outageConfig)

	u, err := url.Parse(processDB(t))
	if err != nil {
		t.Fatal(err)
	}
	db, err := faultproxy.Start(u.Host)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	u.Host = db.Addr()
	g1 := startGateway(t, cfg, u.String(), procOptions{instance: "g1"})

	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(bg, 15*time.Second)
		defer cancel()
		_, _ = mcpclient.New(g1.url).Call(ctx, "create_refund", refund, keyed("k1"))
	}()
	waitFor(t, 10*time.Second, func() bool { return fake.Requests("create_refund") == 1 })
	db.Set(faultproxy.Down)
	if code := readyz(g1); code != http.StatusServiceUnavailable {
		t.Fatalf("readyz during the outage: %d", code)
	}
	if m := fetchMetrics(t, g1); !strings.Contains(m, "\ngateway_store_up 0\n") || strings.Contains(m, "gateway_operations{") {
		t.Fatalf("metrics during the outage:\n%s", m)
	}
	waitFor(t, 10*time.Second, func() bool { return fake.Ledger.CountReference("order-1001") == 1 })
	<-done
	g1.kill()

	db.Set(faultproxy.Pass)
	g2 := startGateway(t, cfg, u.String(), procOptions{instance: "g2"})
	time.Sleep(1500 * time.Millisecond) // past the dead owner's lease
	_, res := untilAnswered(t, mcpclient.New(g2.url), "create_refund", refund, keyed("k1"))
	if res.GatewayStatus() != gateway.StatusOutcomeUnknown {
		t.Fatalf("want outcome unknown, got %s", res.Raw)
	}
	if n := fake.Ledger.CountReference("order-1001"); n != 1 {
		t.Fatalf("I1: %d effects", n)
	}
	if n := fake.Requests("create_refund"); n != 1 {
		t.Fatalf("I8: %d requests", n)
	}
}

func readyz(p *proc) int {
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, p.base+"/-/readyz", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	resp.Body.Close()
	return resp.StatusCode
}

func waitFor(t *testing.T, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", within)
}
