package e2e

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/config"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/faketool"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/gateway"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcpclient"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store/storetest"
)

// baseConfig is the policy set the end-to-end tests run with.
const baseConfig = `
upstream: %s
lease: 1s
wait_timeout: 2s
upstream_timeout: 5s
fallback_window: 2s
tools:
  get_invoice: {policy: read_only}
  find_refund_by_reference: {policy: read_only}
  create_refund: {policy: at_most_once}
  send_email: {policy: at_most_once}
  create_charge: {policy: native_key, key_argument: idempotency_key}
  confirm_transfer: {policy: at_most_once}
  start_export: {policy: at_most_once}
%s
`

// env is one gateway in front of one fake tool server, backed by a fresh
// Postgres schema.
type env struct {
	fake   *faketool.Server
	store  *store.Store
	cfg    *config.Config
	gw     *gateway.Gateway
	url    string
	client *mcpclient.Client
}

type envOptions struct {
	upstream string // overrides the fake server's URL
	extra    string // extra YAML appended to the config
	hooks    gateway.Hooks
	instance string
	noRun    bool // skip background work
}

func testLogger() *slog.Logger {
	if os.Getenv("GATEWAY_TEST_LOGS") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newEnv(t *testing.T, o envOptions) *env {
	t.Helper()
	fake, fakeURL := startFake(t)
	if o.upstream == "" {
		o.upstream = fakeURL
	}
	cfg, err := config.Parse(fmt.Appendf(nil, baseConfig, o.upstream, o.extra))
	if err != nil {
		t.Fatal(err)
	}
	st := storetest.New(t)
	e := &env{fake: fake, store: st, cfg: cfg}
	e.gw = e.addGateway(t, o.instance, o.hooks, !o.noRun)
	srv := httptest.NewServer(e.gw)
	t.Cleanup(srv.Close)
	e.url = srv.URL + "/mcp"
	e.client = mcpclient.New(e.url)
	return e
}

// addGateway starts another gateway instance on the same store and config.
func (e *env) addGateway(t *testing.T, instance string, hooks gateway.Hooks, run bool) *gateway.Gateway {
	t.Helper()
	gw, err := gateway.New(e.cfg, e.store, gateway.Options{Instance: instance, Logger: testLogger(), Hooks: hooks})
	if err != nil {
		t.Fatal(err)
	}
	if run {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go gw.Run(ctx)
	}
	return gw
}

// call makes one tools/call and decodes the result.
func call(t *testing.T, c *mcpclient.Client, tool string, args any, o mcpclient.CallOptions) (*mcpclient.Response, *mcpclient.ToolResult) {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, 15*time.Second)
	defer cancel()
	r, err := c.Call(ctx, tool, args, o)
	if err != nil {
		t.Fatalf("%s: %v", tool, err)
	}
	if e := r.RPCError(); e != nil {
		return r, nil
	}
	res, err := r.ToolResult()
	if err != nil {
		t.Fatalf("%s: %v", tool, err)
	}
	return r, res
}

func keyed(key string) mcpclient.CallOptions { return mcpclient.CallOptions{OperationKey: key} }

// opState reads an operation's state through the store.
func (e *env) opState(t *testing.T, key string) store.State {
	t.Helper()
	op, err := e.store.Get(bg, key)
	if err != nil {
		t.Fatal(err)
	}
	return op.State
}
