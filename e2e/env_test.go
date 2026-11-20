package e2e

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/config"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/faketool"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/faultproxy"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/gateway"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcpclient"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store/storetest"
)

// baseConfig is the policy set the end-to-end tests run with. Its verbs
// are the upstream, the create_refund policy and extra YAML.
const baseConfig = `
upstream: %s
lease: 1s
wait_timeout: 2s
upstream_timeout: 5s
fallback_window: 2s
reconcile_interval: 500ms
tools:
  get_invoice: {policy: read_only}
  find_refund_by_reference: {policy: read_only}
  create_refund: %s
  send_email: {policy: at_most_once}
  create_charge: {policy: native_key, key_argument: idempotency_key}
  confirm_transfer: {policy: at_most_once}
  start_export: {policy: at_most_once}
%s
`

// Policies for create_refund.
const (
	refundAtMostOnce   = "{policy: at_most_once}"
	refundReconcilable = "{policy: reconcilable, reconcile: {tool: find_refund_by_reference, match_argument: reference, settle_after: 2s}}"
)

func configYAML(upstream, refund, extra string) []byte {
	if refund == "" {
		refund = refundAtMostOnce
	}
	return fmt.Appendf(nil, baseConfig, upstream, refund, extra)
}

// env is one gateway in front of one fake tool server, backed by a fresh
// Postgres schema.
type env struct {
	fake   *faketool.Server
	store  *store.Store
	cfg    *config.Config
	gw     *gateway.Gateway
	url    string
	client *mcpclient.Client
	// db sits between the gateway and Postgres when proxyStore is set.
	db *faultproxy.Proxy
}

type envOptions struct {
	upstream   string // overrides the fake server's URL
	refund     string // create_refund policy; at_most_once by default
	extra      string // extra YAML appended to the config
	hooks      gateway.Hooks
	instance   string
	noRun      bool // skip background work
	proxyStore bool // reach Postgres through a fault proxy
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
	cfg, err := config.Parse(configYAML(o.upstream, o.refund, o.extra))
	if err != nil {
		t.Fatal(err)
	}
	e := &env{fake: fake, cfg: cfg}
	if o.proxyStore {
		e.db, e.store = proxiedStore(t)
	} else {
		e.store = storetest.New(t)
	}
	e.store.SetTimeout(cfg.StoreTimeout)
	e.gw = e.addGateway(t, o.instance, o.hooks, !o.noRun)
	srv := httptest.NewServer(e.gw)
	t.Cleanup(srv.Close)
	e.url = srv.URL + "/mcp"
	e.client = mcpclient.New(e.url)
	return e
}

// proxiedStore opens a store in a fresh schema whose connections go through
// a fault proxy, so a test can cut or stall Postgres.
func proxiedStore(t *testing.T) (*faultproxy.Proxy, *store.Store) {
	t.Helper()
	u, err := url.Parse(storetest.URL(t))
	if err != nil {
		t.Fatal(err)
	}
	p, err := faultproxy.Start(u.Host)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	u.Host = p.Addr()
	q := u.Query()
	q.Set("pool_max_conns", "4")
	u.RawQuery = q.Encode()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := store.Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	return p, st
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
