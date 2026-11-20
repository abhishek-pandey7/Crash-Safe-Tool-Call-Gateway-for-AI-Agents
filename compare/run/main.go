// Command run puts each tool-call safety layer through the same failures
// and counts, from the tool's own ledger, how many refunds really happened.
//
// It is meant to run inside the image built from compare/Dockerfile, where
// the other tools are installed at pinned versions. See compare/README.md.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Answer is what the agent was told by one call.
type Answer struct {
	Kind    string  `json:"kind"` // result, tool_error, rpc_error, no_answer, closed
	Code    int     `json:"code,omitempty"`
	Text    string  `json:"text,omitempty"`
	Seconds float64 `json:"seconds"`
}

func (a Answer) String() string {
	t := a.Text
	if len(t) > 90 {
		t = t[:90] + "…"
	}
	switch a.Kind {
	case "result":
		return fmt.Sprintf("result: %q", t)
	case "tool_error":
		return fmt.Sprintf("tool error: %q", t)
	case "rpc_error":
		return fmt.Sprintf("error %d: %q", a.Code, t)
	case "no_answer":
		return "no answer before the timeout"
	case "closed":
		return "connection closed"
	}
	return a.Kind
}

// retryable reports whether an agent would simply try again.
func (a Answer) retryable() bool {
	switch a.Kind {
	case "no_answer", "closed":
		return true
	case "tool_error":
		// This gateway's "still running" and "store unavailable" say to retry.
		return strings.HasPrefix(a.Text, "Still running") || strings.HasPrefix(a.Text, "Not sent")
	}
	return false
}

// Fault is applied to the first call for one reference.
type Fault struct {
	CrashAfterEffect bool `json:"crashAfterEffect,omitempty"`
	DelayBeforeMs    int  `json:"delayBeforeMs,omitempty"`
	DelayAfterMs     int  `json:"delayAfterMs,omitempty"`
}

// Subject is one safety layer, or none, in front of the refund tool.
type Subject interface {
	Name() string
	Start(state string) error // a fresh instance with empty state
	Restart() error           // the same state, after a crash or kill
	Kill()                    // SIGKILL the layer (and, over stdio, its server)
	Alive() bool
	Call(ref string, timeout time.Duration) Answer
	AddFault(ref string, f Fault) error
	Effects(ref string) int
	Stop()
}

// Result is one subject in one scenario.
type Result struct {
	Subject  string  `json:"subject"`
	Scenario string  `json:"scenario"`
	Effects  int     `json:"effects"`
	First    Answer  `json:"first"`
	Final    Answer  `json:"final"`
	Attempts int     `json:"attempts"`
	Seconds  float64 `json:"secondsToFinal"`
	Note     string  `json:"note,omitempty"`
	Order    int     `json:"order"` // the scenario's position, for the report
}

var refSeq atomic.Int64

func newRef(scenario string) string {
	return fmt.Sprintf("%s-%d-%d", scenario, time.Now().Unix(), refSeq.Add(1))
}

// agentRetry retries the way an agent does after an ambiguous failure:
// relaunching a dead stdio server, and trying again while the answer says
// nothing either way.
func agentRetry(s Subject, ref string, budget time.Duration) (Answer, int, time.Duration) {
	start := time.Now()
	var a Answer
	attempts := 0
	for time.Since(start) < budget {
		if !s.Alive() {
			if err := s.Restart(); err != nil {
				return Answer{Kind: "closed", Text: "restart failed: " + err.Error()}, attempts, time.Since(start)
			}
		}
		attempts++
		a = s.Call(ref, 45*time.Second)
		if !a.retryable() {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	return a, attempts, time.Since(start)
}

func scenarioLost(s Subject) Result {
	ref := newRef("lost")
	r := Result{Subject: s.Name(), Scenario: "tool dies after the refund, before answering"}
	if err := s.AddFault(ref, Fault{CrashAfterEffect: true}); err != nil {
		r.Note = err.Error()
		return r
	}
	r.First = s.Call(ref, 10*time.Second)
	var d time.Duration
	r.Final, r.Attempts, d = agentRetry(s, ref, 90*time.Second)
	r.Seconds = d.Seconds()
	time.Sleep(time.Second)
	r.Effects = s.Effects(ref)
	return r
}

func scenarioKilled(s Subject) Result {
	ref := newRef("killed")
	r := Result{Subject: s.Name(), Scenario: "safety layer killed after the refund, before answering"}
	if err := s.AddFault(ref, Fault{DelayAfterMs: 3000}); err != nil {
		r.Note = err.Error()
		return r
	}
	done := make(chan Answer, 1)
	go func() { done <- s.Call(ref, 20*time.Second) }()
	deadline := time.Now().Add(15 * time.Second)
	for s.Effects(ref) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if s.Effects(ref) == 0 {
		r.Note = "the refund never happened before the kill"
	}
	s.Kill()
	r.First = <-done
	var d time.Duration
	r.Final, r.Attempts, d = agentRetry(s, ref, 90*time.Second)
	r.Seconds = d.Seconds()
	time.Sleep(time.Second)
	r.Effects = s.Effects(ref)
	return r
}

func scenarioTimeout(s Subject) Result {
	ref := newRef("timeout")
	r := Result{Subject: s.Name(), Scenario: "agent times out while the tool runs, then retries"}
	if err := s.AddFault(ref, Fault{DelayBeforeMs: 3000}); err != nil {
		r.Note = err.Error()
		return r
	}
	r.First = s.Call(ref, time.Second)
	var d time.Duration
	r.Final, r.Attempts, d = agentRetry(s, ref, 90*time.Second)
	r.Seconds = d.Seconds()
	time.Sleep(4 * time.Second) // let the first call finish
	r.Effects = s.Effects(ref)
	return r
}

func main() {
	var (
		only    = flag.String("only", "", "comma-separated subject names to run (default: all)")
		out     = flag.String("out", "/work/results", "directory for results")
		db      = flag.String("db", os.Getenv("GATEWAY_DATABASE_URL"), "Postgres URL for this repository's gateway")
		gateway = flag.String("gateway-bin", "/usr/local/bin/gateway", "this repository's gateway binary")
	)
	flag.Parse()

	fake := envOr("STDIOFAKE", "/usr/local/bin/stdiofake")
	subjects := []Subject{
		newStdio("no safety layer", nil, nil),
		newStdio("mcp-idempotent, memory store (default)",
			func(string) []string { return []string{"node", "/opt/mcp-idempotent/dist/bin/cli.js", "--"} },
			map[string]any{"idempotency-key": nil}),
		// The CLI's --store redis fails to start in this build; see
		// compare/mcp-idempotent-redis.mjs.
		newStdio("mcp-idempotent, Redis store",
			func(string) []string { return []string{"node", "/opt/mcp-idempotent/redis-proxy.mjs", "--"} },
			map[string]any{"idempotency-key": nil}),
		newStdio("once-gate",
			func(state string) []string {
				return []string{"/opt/oncegate/bin/python", "-m", "oncegate", "proxy", "--db", filepath.Join(state, "claims.db"),
					"--ledger", filepath.Join(state, "actions.jsonl"), "--tools", "create_refund", "--"}
			}, nil),
		newStdio("agentsafe-mcp",
			func(state string) []string {
				return []string{"agentsafe-mcp", "--log", filepath.Join(state, "calls.jsonl"), "--policy", filepath.Join(state, "policy.json"), "--"}
			}, nil),
		newGateway("this gateway, at_most_once", *gateway, *db, "{policy: at_most_once}"),
		newGateway("this gateway, reconcilable", *gateway, *db,
			"{policy: reconcilable, reconcile: {tool: find_refund_by_reference, match_argument: reference, settle_after: 3s}}"),
	}
	for _, s := range subjects {
		if st, ok := s.(*stdioSubject); ok {
			st.fake = fake
		}
	}

	want := map[string]bool{}
	for f := range strings.SplitSeq(*only, ",") {
		if f = strings.TrimSpace(f); f != "" {
			want[f] = true
		}
	}
	scenarios := []func(Subject) Result{scenarioLost, scenarioKilled, scenarioTimeout}

	var results []Result
	for _, s := range subjects {
		if len(want) > 0 && !want[s.Name()] {
			continue
		}
		for i, sc := range scenarios {
			if s.Name() == "no safety layer" && i == 1 {
				continue // nothing to kill
			}
			state := filepath.Join(*out, "state", slug(s.Name()), fmt.Sprint(i))
			_ = os.RemoveAll(state)
			if err := os.MkdirAll(state, 0o755); err != nil {
				fail(err)
			}
			fmt.Fprintf(os.Stderr, "%s: scenario %d...\n", s.Name(), i+1)
			if err := s.Start(state); err != nil {
				results = append(results, Result{Subject: s.Name(), Scenario: fmt.Sprint(i + 1), Note: "start failed: " + err.Error()})
				continue
			}
			r := sc(s)
			r.Order = i
			s.Stop()
			results = append(results, r)
			fmt.Fprintf(os.Stderr, "  effects %d; first: %s; final: %s (%.1fs, %d attempts) %s\n",
				r.Effects, r.First, r.Final, r.Seconds, r.Attempts, r.Note)
		}
	}

	raw, _ := json.MarshalIndent(results, "", "  ")
	_ = os.WriteFile(filepath.Join(*out, "comparison.json"), raw, 0o644)
	fmt.Print(markdown(results))
	if n := checkRedisCLI(fake); n != "" {
		fmt.Printf("\nNote: %s\n", n)
	}
}

// checkRedisCLI starts mcp-idempotent's CLI with --store redis and reports
// how it fails, if it does.
func checkRedisCLI(fake string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "node", "/opt/mcp-idempotent/dist/bin/cli.js",
		"--store", "redis", "--redis-url", "redis://127.0.0.1:6379", "--", fake)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	cmd.Stdin = strings.NewReader("")
	_ = cmd.Run()
	for l := range strings.SplitSeq(stderr.String(), "\n") {
		if strings.Contains(l, "fatal error") {
			return "mcp-idempotent's CLI with `--store redis` exits at startup: `" + strings.TrimSpace(l) +
				"`. The Redis rows use the same proxy and store through the package's library API instead (compare/mcp-idempotent-redis.mjs)."
		}
	}
	return ""
}

func markdown(rs []Result) string {
	rs = slices.Clone(rs)
	slices.SortStableFunc(rs, func(a, b Result) int { return a.Order - b.Order })
	var b strings.Builder
	scenario := ""
	for _, r := range rs {
		if r.Scenario != scenario {
			scenario = r.Scenario
			fmt.Fprintf(&b, "\n**%s**\n\n| Layer | Refunds | First answer | Answer after retrying | Time to that answer |\n|---|---|---|---|---|\n", scenario)
		}
		fmt.Fprintf(&b, "| %s | %d | %s | %s | %.1f s |\n", r.Subject, r.Effects, cell(r.First), cell(r.Final), r.Seconds)
	}
	return b.String()
}

func cell(a Answer) string { return strings.ReplaceAll(a.String(), "|", "/") }

func slug(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	return b.String()
}

func envOr(k, v string) string {
	if s := os.Getenv(k); s != "" {
		return s
	}
	return v
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "run:", err)
	os.Exit(1)
}

// ---- stdio subjects ----

type stdioSubject struct {
	name  string
	wrap  func(state string) []string // the proxy command before the server; nil runs the server alone
	meta  map[string]any              // _meta keys whose value is set to the reference
	fake  string
	state string

	mu      sync.Mutex
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	pending map[int64]chan json.RawMessage
	closed  chan struct{}
	nextID  int64
}

func newStdio(name string, wrap func(string) []string, meta map[string]any) *stdioSubject {
	return &stdioSubject{name: name, wrap: wrap, meta: meta}
}

func (s *stdioSubject) Name() string { return s.name }

func (s *stdioSubject) ledger() string { return filepath.Join(s.state, "ledger.jsonl") }
func (s *stdioSubject) faults() string { return filepath.Join(s.state, "faults") }

func (s *stdioSubject) Start(state string) error {
	s.state = state
	if err := os.MkdirAll(s.faults(), 0o755); err != nil {
		return err
	}
	if strings.Contains(s.name, "Redis") {
		_ = exec.Command("redis-cli", "flushall").Run()
	}
	if s.name == "agentsafe-mcp" {
		policy := `{"tools": {"create_refund": {"identity": ["reference"], "key": "none"},
			"find_refund_by_reference": {"pass": true}}}`
		if err := os.WriteFile(filepath.Join(state, "policy.json"), []byte(policy), 0o644); err != nil {
			return err
		}
	}
	return s.launch()
}

func (s *stdioSubject) Restart() error { return s.launch() }

func (s *stdioSubject) launch() error {
	var argv []string
	if s.wrap != nil {
		argv = s.wrap(s.state)
	}
	argv = append(argv, s.fake, "-ledger", s.ledger(), "-faults", s.faults())
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = append(os.Environ(), "LEDGER="+s.ledger(), "FAULTS="+s.faults())
	cmd.Dir = s.state
	setGroup(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	logf, _ := os.OpenFile(filepath.Join(s.state, "stderr.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	cmd.Stderr = logf
	if err := cmd.Start(); err != nil {
		return err
	}
	closed := make(chan struct{})
	s.mu.Lock()
	s.cmd, s.stdin, s.closed = cmd, stdin, closed
	s.pending = map[int64]chan json.RawMessage{}
	s.mu.Unlock()
	go s.read(stdout, closed, stdin)
	go func() { _ = cmd.Wait(); close(closed) }()

	// Handshake.
	res := s.request("initialize", map[string]any{
		"protocolVersion": "2025-11-25",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "comparison-agent", "version": "0.1.0"},
	}, 30*time.Second)
	if res.Kind != "result" {
		return fmt.Errorf("initialize: %s", res)
	}
	s.notify("notifications/initialized", map[string]any{})
	return nil
}

// read dispatches responses and answers the server's own requests.
func (s *stdioSubject) read(r io.Reader, closed chan struct{}, stdin io.WriteCloser) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		var m struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		line := append([]byte(nil), sc.Bytes()...)
		if json.Unmarshal(line, &m) != nil {
			continue
		}
		if m.Method != "" {
			if len(m.ID) > 0 { // a request from the server
				var resp map[string]any
				if m.Method == "ping" {
					resp = map[string]any{"jsonrpc": "2.0", "id": m.ID, "result": map[string]any{}}
				} else {
					resp = map[string]any{"jsonrpc": "2.0", "id": m.ID, "error": map[string]any{"code": -32601, "message": "not supported"}}
				}
				raw, _ := json.Marshal(resp)
				s.mu.Lock()
				_, _ = stdin.Write(append(raw, '\n'))
				s.mu.Unlock()
			}
			continue
		}
		var id int64
		if json.Unmarshal(m.ID, &id) != nil {
			continue
		}
		s.mu.Lock()
		ch := s.pending[id]
		delete(s.pending, id)
		s.mu.Unlock()
		if ch != nil {
			ch <- line
		}
	}
}

func (s *stdioSubject) notify(method string, params any) {
	raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stdin != nil {
		_, _ = s.stdin.Write(append(raw, '\n'))
	}
}

func (s *stdioSubject) request(method string, params any, timeout time.Duration) Answer {
	start := time.Now()
	s.mu.Lock()
	s.nextID++
	id := s.nextID
	ch := make(chan json.RawMessage, 1)
	s.pending[id] = ch
	closed := s.closed
	raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	_, err := s.stdin.Write(append(raw, '\n'))
	s.mu.Unlock()
	if err != nil {
		return Answer{Kind: "closed", Seconds: time.Since(start).Seconds()}
	}
	select {
	case line := <-ch:
		a := decode(line)
		a.Seconds = time.Since(start).Seconds()
		return a
	case <-closed:
		return Answer{Kind: "closed", Seconds: time.Since(start).Seconds()}
	case <-time.After(timeout):
		// As the SDKs do on a timeout.
		s.notify("notifications/cancelled", map[string]any{"requestId": id, "reason": "timeout"})
		return Answer{Kind: "no_answer", Seconds: time.Since(start).Seconds()}
	}
}

func (s *stdioSubject) Call(ref string, timeout time.Duration) Answer {
	params := map[string]any{"name": "create_refund", "arguments": map[string]any{"reference": ref, "amount": 20}}
	if len(s.meta) > 0 {
		meta := map[string]any{}
		for k := range s.meta {
			meta[k] = ref
		}
		params["_meta"] = meta
	}
	return s.request("tools/call", params, timeout)
}

func (s *stdioSubject) Alive() bool {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	select {
	case <-closed:
		return false
	default:
		return true
	}
}

func (s *stdioSubject) Kill() {
	s.mu.Lock()
	cmd, closed := s.cmd, s.closed
	s.mu.Unlock()
	killGroup(cmd)
	<-closed
}

func (s *stdioSubject) Stop() {
	if s.Alive() {
		s.Kill()
	}
}

func (s *stdioSubject) AddFault(ref string, f Fault) error {
	raw, _ := json.Marshal(f)
	return os.WriteFile(filepath.Join(s.faults(), ref+".json"), raw, 0o644)
}

func (s *stdioSubject) Effects(ref string) int {
	raw, err := os.ReadFile(s.ledger())
	if err != nil {
		return 0
	}
	n := 0
	for l := range strings.SplitSeq(string(raw), "\n") {
		var e struct {
			Reference string `json:"reference"`
		}
		if json.Unmarshal([]byte(l), &e) == nil && e.Reference == ref {
			n++
		}
	}
	return n
}

// decode turns one JSON-RPC response into an Answer.
func decode(line []byte) Answer {
	var m struct {
		Result *struct {
			IsError bool `json:"isError"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(line, &m); err != nil {
		return Answer{Kind: "rpc_error", Text: "unparseable: " + err.Error()}
	}
	if m.Error != nil {
		return Answer{Kind: "rpc_error", Code: m.Error.Code, Text: m.Error.Message}
	}
	if m.Result == nil {
		return Answer{Kind: "rpc_error", Text: "no result"}
	}
	var texts []string
	for _, c := range m.Result.Content {
		texts = append(texts, c.Text)
	}
	kind := "result"
	if m.Result.IsError {
		kind = "tool_error"
	}
	return Answer{Kind: kind, Text: strings.Join(texts, " ")}
}
