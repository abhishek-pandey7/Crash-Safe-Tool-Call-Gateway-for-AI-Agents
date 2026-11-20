package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/faketool"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcpclient"
)

// gatewaySubject is this repository's gateway as a separate process, over
// Streamable HTTP, in front of the HTTP version of the same fake tool.
type gatewaySubject struct {
	name, bin, dbBase, policy string

	fake    *faketool.Server
	fakeSrv *httptest.Server
	state   string
	cfg     string
	dbURL   string
	drop    func()
	port    string
	gen     int
	cmd     *exec.Cmd
	exited  chan struct{}
}

func newGateway(name, bin, dbBase, policy string) *gatewaySubject {
	return &gatewaySubject{name: name, bin: bin, dbBase: dbBase, policy: policy}
}

func (g *gatewaySubject) Name() string { return g.name }

func (g *gatewaySubject) Start(state string) error {
	if g.dbBase == "" {
		return errors.New("no database: set -db or GATEWAY_DATABASE_URL")
	}
	g.state = state
	g.fake = faketool.New()
	g.fakeSrv = httptest.NewServer(g.fake.Handler())
	dbURL, drop, err := freshSchema(g.dbBase)
	if err != nil {
		return err
	}
	g.dbURL, g.drop = dbURL, drop
	g.cfg = filepath.Join(state, "gateway.yaml")
	cfg := fmt.Sprintf(`upstream: %s/mcp
lease: 5s
wait_timeout: 5s
upstream_timeout: 20s
reconcile_interval: 1s
tools:
  find_refund_by_reference: {policy: read_only}
  create_refund: %s
`, g.fakeSrv.URL, g.policy)
	if err := os.WriteFile(g.cfg, []byte(cfg), 0o644); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	g.port = strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	ln.Close()
	return g.launch()
}

func (g *gatewaySubject) Restart() error { return g.launch() }

func (g *gatewaySubject) launch() error {
	g.gen++
	cmd := exec.Command(g.bin, "-config", g.cfg)
	cmd.Env = append(os.Environ(),
		"GATEWAY_DATABASE_URL="+g.dbURL,
		"GATEWAY_LISTEN=127.0.0.1:"+g.port,
		fmt.Sprintf("GATEWAY_INSTANCE_ID=gateway-%d", g.gen))
	logf, _ := os.OpenFile(filepath.Join(g.state, "gateway.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		return err
	}
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	g.cmd, g.exited = cmd, exited

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-exited:
			return errors.New("gateway exited during startup; see gateway.log")
		default:
		}
		resp, err := http.Get("http://127.0.0.1:" + g.port + "/-/readyz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("gateway not ready")
}

func (g *gatewaySubject) Alive() bool {
	select {
	case <-g.exited:
		return false
	default:
		return true
	}
}

func (g *gatewaySubject) Kill() {
	if g.cmd != nil && g.Alive() {
		_ = g.cmd.Process.Kill()
		<-g.exited
	}
}

func (g *gatewaySubject) Stop() {
	g.Kill()
	if g.fakeSrv != nil {
		g.fakeSrv.Close()
	}
	if g.drop != nil {
		g.drop()
	}
}

func (g *gatewaySubject) Call(ref string, timeout time.Duration) Answer {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	c := mcpclient.New("http://127.0.0.1:" + g.port + "/mcp")
	r, err := c.Call(ctx, "create_refund", map[string]any{"reference": ref, "amount": 20},
		mcpclient.CallOptions{OperationKey: ref})
	secs := time.Since(start).Seconds()
	if err != nil {
		if ctx.Err() != nil {
			return Answer{Kind: "no_answer", Seconds: secs}
		}
		return Answer{Kind: "closed", Seconds: secs}
	}
	if e := r.RPCError(); e != nil {
		return Answer{Kind: "rpc_error", Code: e.Code, Text: e.Message, Seconds: secs}
	}
	res, err := r.ToolResult()
	if err != nil {
		return Answer{Kind: "rpc_error", Text: err.Error(), Seconds: secs}
	}
	kind := "result"
	if res.IsError {
		kind = "tool_error"
	}
	return Answer{Kind: kind, Text: res.Text(), Seconds: secs}
}

func (g *gatewaySubject) AddFault(ref string, f Fault) error {
	g.fake.AddFault(faketool.Fault{Tool: "create_refund", Reference: ref, DropResponse: f.CrashAfterEffect,
		DelayBeforeMs: f.DelayBeforeMs, DelayAfterMs: f.DelayAfterMs, IgnoreCancel: true})
	return nil
}

func (g *gatewaySubject) Effects(ref string) int { return g.fake.Ledger.CountReference(ref) }

// freshSchema creates a schema and returns a URL using it and a drop func.
func freshSchema(base string) (string, func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var b [6]byte
	_, _ = rand.Read(b[:])
	schema := "compare_" + hex.EncodeToString(b[:])
	conn, err := pgx.Connect(ctx, base)
	if err != nil {
		return "", nil, err
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		return "", nil, err
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", nil, err
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	drop := func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if conn, err := pgx.Connect(c, base); err == nil {
			_, _ = conn.Exec(c, "DROP SCHEMA "+schema+" CASCADE")
			_ = conn.Close(c)
		}
	}
	return u.String(), drop, nil
}
