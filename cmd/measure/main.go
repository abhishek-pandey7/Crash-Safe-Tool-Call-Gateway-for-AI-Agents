// Command measure runs the fake tool server, the gateway and Postgres
// together and measures what the README reports: duplicate effects and
// unknown outcomes when responses are lost, and the latency and throughput
// the gateway adds.
//
//	go run ./cmd/measure -db "postgres://postgres:postgres@127.0.0.1:55432/gateway?sslmode=disable"
//
// It creates a fresh schema for each scenario and drops it afterwards. The
// numbers depend on the machine; the report says which one.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	mrand "math/rand/v2"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/config"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/faketool"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/gateway"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcpclient"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
)

type options struct {
	db          string
	ops         int
	loss        float64
	seed        uint64
	concurrency int
	latencyOps  int
	out         string
	note        string
}

func main() {
	var o options
	flag.StringVar(&o.db, "db", os.Getenv("GATEWAY_DATABASE_URL"), "Postgres URL; a schema is created per scenario")
	flag.IntVar(&o.ops, "ops", 1000, "logical operations per fault scenario")
	flag.Float64Var(&o.loss, "loss", 0.10, "share of first attempts whose response is lost after the effect")
	flag.Uint64Var(&o.seed, "seed", 1, "random seed for which operations lose their response")
	flag.IntVar(&o.concurrency, "concurrency", 16, "concurrent callers")
	flag.IntVar(&o.latencyOps, "latency-ops", 2000, "calls per latency measurement")
	flag.StringVar(&o.out, "out", "results/measurements.json", "where to write the raw results")
	flag.StringVar(&o.note, "note", "", "free text describing the environment, printed with the results")
	flag.Parse()
	if o.db == "" {
		fmt.Fprintln(os.Stderr, "measure: set -db or GATEWAY_DATABASE_URL")
		os.Exit(2)
	}
	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "measure:", err)
		os.Exit(1)
	}
}

// Report is everything one run measured.
type Report struct {
	Machine   Machine         `json:"machine"`
	Options   map[string]any  `json:"options"`
	Faults    []FaultResult   `json:"faults"`
	Latency   []LatencyResult `json:"latency"`
	StartedAt time.Time       `json:"startedAt"`
}

// Machine describes where the numbers came from.
type Machine struct {
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	CPUs     int    `json:"cpus"`
	Go       string `json:"go"`
	Postgres string `json:"postgres"`
	Note     string `json:"note,omitempty"`
}

// FaultResult is one scenario with lost responses.
type FaultResult struct {
	Scenario      string  `json:"scenario"`
	Policy        string  `json:"policy"`
	Operations    int     `json:"operations"`
	LostResponses int     `json:"lostResponses"`
	Effects       int     `json:"effects"`
	Duplicates    int     `json:"duplicates"`   // effects beyond the first, summed
	NeverRan      int     `json:"neverRan"`     // operations with no effect at all
	Completed     int     `json:"completed"`    // callers told it ran
	Unknown       int     `json:"unknown"`      // callers told the outcome is unknown
	Failed        int     `json:"failed"`       // callers that gave up without an answer
	FalseAnswers  int     `json:"falseAnswers"` // told it ran with no effect, or the reverse
	Attempts      int     `json:"attempts"`
	DuplicatesPer float64 `json:"duplicatesPer1000"`
	UnknownPer    float64 `json:"unknownPer1000"`
}

// LatencyResult is one latency or throughput measurement.
type LatencyResult struct {
	Path        string  `json:"path"`
	Concurrency int     `json:"concurrency"`
	Calls       int     `json:"calls"`
	P50Ms       float64 `json:"p50Ms"`
	P99Ms       float64 `json:"p99Ms"`
	PerSecond   float64 `json:"callsPerSecond"`
}

func run(o options) error {
	ctx := context.Background()
	rep := Report{
		StartedAt: time.Now().UTC(),
		Options: map[string]any{"ops": o.ops, "loss": o.loss, "seed": o.seed,
			"concurrency": o.concurrency, "latencyOps": o.latencyOps},
		Machine: Machine{OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(), Go: runtime.Version(), Note: o.note},
	}
	if v, err := postgresVersion(ctx, o.db); err == nil {
		rep.Machine.Postgres = v
	}

	scenarios := []struct {
		name, tool, policy string
		gateway            bool
	}{
		{"direct, agent retries", "create_refund", "none", false},
		{"through the gateway", "create_refund", "at_most_once", true},
		{"through the gateway", "create_refund", "reconcilable", true},
		{"through the gateway", "create_charge", "native_key", true},
	}
	for _, sc := range scenarios {
		fmt.Fprintf(os.Stderr, "faults: %s, %s...\n", sc.name, sc.policy)
		r, err := faultScenario(ctx, o, sc.name, sc.tool, sc.policy, sc.gateway)
		if err != nil {
			return fmt.Errorf("%s %s: %w", sc.name, sc.policy, err)
		}
		rep.Faults = append(rep.Faults, r)
	}

	fmt.Fprintln(os.Stderr, "latency and throughput...")
	lat, err := latency(ctx, o)
	if err != nil {
		return err
	}
	rep.Latency = lat

	if err := os.MkdirAll(filepath.Dir(o.out), 0o755); err != nil {
		return err
	}
	raw, _ := json.MarshalIndent(rep, "", "  ")
	if err := os.WriteFile(o.out, raw, 0o644); err != nil {
		return err
	}
	fmt.Print(markdown(rep))
	fmt.Fprintf(os.Stderr, "raw results: %s\n", o.out)
	return nil
}

// stack is a fake tool server, optionally behind a gateway, on a fresh schema.
type stack struct {
	fake    *faketool.Server
	fakeURL string
	gwURL   string
	close   func()
}

func newStack(ctx context.Context, dbURL, policyYAML string, withGateway bool) (*stack, error) {
	fake := faketool.New()
	fakeSrv := httptest.NewServer(fake.Handler())
	s := &stack{fake: fake, fakeURL: fakeSrv.URL + "/mcp"}
	closers := []func(){fakeSrv.Close}
	s.close = func() {
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i]()
		}
	}
	if !withGateway {
		return s, nil
	}

	schemaURL, drop, err := freshSchema(ctx, dbURL)
	if err != nil {
		s.close()
		return nil, err
	}
	closers = append(closers, drop)
	st, err := store.Open(ctx, schemaURL)
	if err != nil {
		s.close()
		return nil, err
	}
	closers = append(closers, st.Close)

	cfg, err := config.Parse(fmt.Appendf(nil, `
upstream: %s
lease: 2s
wait_timeout: 2s
upstream_timeout: 10s
reconcile_interval: 1s
tools:
  find_refund_by_reference: {policy: read_only}
  create_refund: %s
  create_charge: {policy: native_key, key_argument: idempotency_key}
`, s.fakeURL, policyYAML))
	if err != nil {
		s.close()
		return nil, err
	}
	st.SetTimeout(cfg.StoreTimeout)
	gw, err := gateway.New(cfg, st, gateway.Options{Instance: "measure",
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		s.close()
		return nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	go gw.Run(runCtx)
	gwSrv := httptest.NewServer(gw)
	closers = append(closers, cancel, gwSrv.Close)
	s.gwURL = gwSrv.URL + "/mcp"
	return s, nil
}

func refundPolicy(policy string) string {
	if policy == "reconcilable" {
		return "{policy: reconcilable, reconcile: {tool: find_refund_by_reference, match_argument: reference, settle_after: 1s}}"
	}
	return "{policy: at_most_once}"
}

func faultScenario(ctx context.Context, o options, name, tool, policy string, withGateway bool) (FaultResult, error) {
	s, err := newStack(ctx, o.db, refundPolicy(policy), withGateway)
	if err != nil {
		return FaultResult{}, err
	}
	defer s.close()

	res := FaultResult{Scenario: name, Policy: policy, Operations: o.ops}
	r := mrand.New(mrand.NewPCG(o.seed, 7))
	lost := make([]bool, o.ops)
	for i := range lost {
		lost[i] = r.Float64() < o.loss
	}

	type outcome struct {
		told     string // completed, unknown or failed
		attempts int
	}
	outcomes := make([]outcome, o.ops)
	var next atomic.Int64
	var wg sync.WaitGroup
	for range o.concurrency {
		wg.Go(func() {
			c := mcpclient.New(s.fakeURL)
			if withGateway {
				c = mcpclient.New(s.gwURL)
			}
			for {
				i := int(next.Add(1)) - 1
				if i >= o.ops {
					return
				}
				ref := fmt.Sprintf("op-%d", i)
				if lost[i] {
					s.fake.AddFault(faketool.Fault{Tool: tool, Reference: ref, DropResponse: true})
				}
				args := map[string]any{"reference": ref, "amount": 10}
				if tool == "create_charge" {
					args = map[string]any{"idempotency_key": ref, "amount": 10}
				}
				told, attempts := settle(c, tool, args, ref, policy, withGateway)
				outcomes[i] = outcome{told, attempts}
			}
		})
	}
	wg.Wait()

	for i, oc := range outcomes {
		ref := fmt.Sprintf("op-%d", i)
		n := s.fake.Ledger.CountReference(ref)
		if lost[i] {
			res.LostResponses++
		}
		res.Effects += n
		res.Duplicates += max(0, n-1)
		res.Attempts += oc.attempts
		if n == 0 {
			res.NeverRan++
		}
		switch oc.told {
		case "completed":
			res.Completed++
			if n == 0 {
				res.FalseAnswers++
			}
		case "unknown":
			res.Unknown++
		default:
			res.Failed++
		}
	}
	res.DuplicatesPer = 1000 * float64(res.Duplicates) / float64(o.ops)
	res.UnknownPer = 1000 * float64(res.Unknown) / float64(o.ops)
	return res, nil
}

// settle makes one logical operation the way an agent would: it retries
// after a dropped connection, and through the gateway it also retries
// whenever the gateway says retrying is safe.
func settle(c *mcpclient.Client, tool string, args map[string]any, key, policy string, withGateway bool) (string, int) {
	maxAttempts := 3 // an agent's usual retry budget without a gateway
	if withGateway {
		maxAttempts = 50
	}
	deadline := time.Now().Add(60 * time.Second)
	opts := mcpclient.CallOptions{}
	if withGateway {
		opts.OperationKey = key
	}
	for attempt := 1; attempt <= maxAttempts && time.Now().Before(deadline); attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		r, err := c.Call(ctx, tool, args, opts)
		cancel()
		if err != nil {
			if errors.Is(err, mcpclient.ErrTransport) || errors.Is(err, context.DeadlineExceeded) {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			return "failed", attempt
		}
		res, err := r.ToolResult()
		if err != nil {
			return "failed", attempt
		}
		switch res.GatewayStatus() {
		case gateway.StatusInProgress, gateway.StatusStoreUnavailable, gateway.StatusNotExecuted:
			time.Sleep(100 * time.Millisecond)
			continue
		case gateway.StatusOutcomeUnknown:
			if policy == "at_most_once" {
				return "unknown", attempt
			}
			time.Sleep(300 * time.Millisecond) // the gateway says retrying is safe
			continue
		}
		if res.IsError {
			return "failed", attempt
		}
		return "completed", attempt
	}
	return "failed", maxAttempts
}

func latency(ctx context.Context, o options) ([]LatencyResult, error) {
	s, err := newStack(ctx, o.db, refundPolicy("at_most_once"), true)
	if err != nil {
		return nil, err
	}
	defer s.close()

	// One durable Postgres commit, for scale: the gateway makes three per
	// effectful call (claim, dispatch gate, outcome).
	commit, err := commitLatency(ctx, o.db, o.latencyOps)
	if err != nil {
		return nil, err
	}
	out := []LatencyResult{commit}
	for _, path := range []struct {
		name string
		url  string
		key  bool
	}{{"direct", s.fakeURL, false}, {"through the gateway", s.gwURL, true}} {
		for _, conc := range []int{1, o.concurrency} {
			r, err := measureCalls(path.url, path.name, path.key, conc, o.latencyOps)
			if err != nil {
				return nil, err
			}
			out = append(out, r)
		}
	}
	return out, nil
}

var latencyRun atomic.Int64

func measureCalls(url, name string, keyed bool, conc, calls int) (LatencyResult, error) {
	run := latencyRun.Add(1)
	durations := make([]time.Duration, calls)
	var next atomic.Int64
	var failed atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for range conc {
		wg.Go(func() {
			c := mcpclient.New(url)
			for {
				i := int(next.Add(1)) - 1
				if i >= calls {
					return
				}
				ref := fmt.Sprintf("lat-%d-%d", run, i)
				opts := mcpclient.CallOptions{}
				if keyed {
					opts.OperationKey = ref
				}
				t0 := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				r, err := c.Call(ctx, "create_refund", map[string]any{"reference": ref, "amount": 1}, opts)
				cancel()
				durations[i] = time.Since(t0)
				if err != nil || r.RPCError() != nil {
					failed.Add(1)
				}
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	if n := failed.Load(); n > 0 {
		return LatencyResult{}, fmt.Errorf("%s: %d of %d latency calls failed", name, n, calls)
	}
	return summarize(name, conc, durations, elapsed), nil
}

// commitLatency times single-row inserts, each its own transaction.
func commitLatency(ctx context.Context, dbURL string, calls int) (LatencyResult, error) {
	schemaURL, drop, err := freshSchema(ctx, dbURL)
	if err != nil {
		return LatencyResult{}, err
	}
	defer drop()
	conn, err := pgx.Connect(ctx, schemaURL)
	if err != nil {
		return LatencyResult{}, err
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "CREATE TABLE commits (id int)"); err != nil {
		return LatencyResult{}, err
	}
	durations := make([]time.Duration, calls)
	start := time.Now()
	for i := range calls {
		t0 := time.Now()
		if _, err := conn.Exec(ctx, "INSERT INTO commits VALUES ($1)", i); err != nil {
			return LatencyResult{}, err
		}
		durations[i] = time.Since(t0)
	}
	return summarize("one Postgres commit (reference)", 1, durations, time.Since(start)), nil
}

func summarize(name string, conc int, durations []time.Duration, elapsed time.Duration) LatencyResult {
	slices.Sort(durations)
	pct := func(p float64) float64 {
		i := min(len(durations)-1, int(math.Ceil(p*float64(len(durations))))-1)
		return float64(durations[max(i, 0)].Microseconds()) / 1000
	}
	return LatencyResult{Path: name, Concurrency: conc, Calls: len(durations), P50Ms: pct(0.50), P99Ms: pct(0.99),
		PerSecond: float64(len(durations)) / elapsed.Seconds()}
}

// freshSchema creates a schema and returns a URL that uses it and a
// function that drops it.
func freshSchema(ctx context.Context, base string) (string, func(), error) {
	var b [6]byte
	_, _ = rand.Read(b[:])
	schema := "measure_" + hex.EncodeToString(b[:])
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

func postgresVersion(ctx context.Context, base string) (string, error) {
	conn, err := pgx.Connect(ctx, base)
	if err != nil {
		return "", err
	}
	defer conn.Close(ctx)
	var v string
	err = conn.QueryRow(ctx, "SHOW server_version").Scan(&v)
	return v, err
}

func markdown(r Report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Measured %s on %s/%s, %d CPUs, %s, Postgres %s.",
		r.StartedAt.Format("2006-01-02"), r.Machine.OS, r.Machine.Arch, r.Machine.CPUs, r.Machine.Go, r.Machine.Postgres)
	if r.Machine.Note != "" {
		fmt.Fprintf(&b, " %s.", strings.TrimSuffix(r.Machine.Note, "."))
	}
	b.WriteString("\n\n")
	fmt.Fprintf(&b, "**Lost responses.** %v operations per row, %d concurrent callers, the response lost after the effect on %.0f%% of first attempts (seed %v).\n\n",
		r.Options["ops"], r.Options["concurrency"], 100*r.Options["loss"].(float64), r.Options["seed"])
	b.WriteString("| Path | Policy | Lost responses | Duplicate effects | Told \"unknown\" | Told it ran | Gave up | Wrong answers |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|\n")
	for _, f := range r.Faults {
		fmt.Fprintf(&b, "| %s | %s | %d | %d | %d | %d | %d | %d |\n",
			f.Scenario, f.Policy, f.LostResponses, f.Duplicates, f.Unknown, f.Completed, f.Failed, f.FalseAnswers)
	}
	b.WriteString("\n**Latency and throughput.** `create_refund` with no faults, a new operation on every call.\n\n")
	b.WriteString("| Path | Concurrent callers | Calls | p50 | p99 | Calls per second |\n")
	b.WriteString("|---|---|---|---|---|---|\n")
	for _, l := range r.Latency {
		fmt.Fprintf(&b, "| %s | %d | %d | %.2f ms | %.2f ms | %.0f |\n", l.Path, l.Concurrency, l.Calls, l.P50Ms, l.P99Ms, l.PerSecond)
	}
	return b.String()
}
