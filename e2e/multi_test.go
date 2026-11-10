package e2e

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/faketool"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/gateway"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcpclient"
)

// serve exposes a gateway over HTTP and returns a client for it.
func serve(t *testing.T, gw *gateway.Gateway) *mcpclient.Client {
	t.Helper()
	srv := httptest.NewServer(gw)
	t.Cleanup(srv.Close)
	return mcpclient.New(srv.URL + "/mcp")
}

// pauser freezes an in-process gateway at chosen points, heartbeats
// included, the way a stop-the-world pause or a frozen VM would.
type pauser struct {
	points  map[gateway.Point]bool
	reached chan gateway.Point
	release chan struct{}
	once    sync.Once
}

func newPauser(points ...gateway.Point) *pauser {
	p := &pauser{points: map[gateway.Point]bool{}, reached: make(chan gateway.Point, 16), release: make(chan struct{})}
	for _, pt := range points {
		p.points[pt] = true
	}
	return p
}

func (p *pauser) hooks() gateway.Hooks {
	return gateway.Hooks{At: func(pt gateway.Point, _ string) {
		if !p.points[pt] {
			return
		}
		select {
		case p.reached <- pt:
		default:
		}
		<-p.release
	}}
}

func (p *pauser) wake() { p.once.Do(func() { close(p.release) }) }

// waitFor blocks until the paused gateway reaches point.
func (p *pauser) waitFor(t *testing.T, point gateway.Point) {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case got := <-p.reached:
			if got == point {
				return
			}
		case <-timeout:
			t.Fatalf("gateway never reached %s", point)
		}
	}
}

// An instance freezes after claiming but before the gate, for longer than
// its lease. Another instance takes over and runs the call. When the first
// wakes up, its fence is stale, the gate refuses it, and it returns the
// other instance's result instead of sending.
func TestPausedBeforeGateIsFencedOut(t *testing.T) {
	e := newEnv(t, envOptions{noRun: true})
	p := newPauser(gateway.PointAfterClaim, gateway.PointHeartbeat)
	t.Cleanup(p.wake)
	a := serve(t, e.addGateway(t, "paused-A", p.hooks(), false))
	b := serve(t, e.addGateway(t, "B", gateway.Hooks{}, false))

	first := make(chan *mcpclient.ToolResult, 1)
	go func() {
		_, res := call(t, a, "create_refund", refund, keyed("k1"))
		first <- res
	}()
	p.waitFor(t, gateway.PointAfterClaim)
	time.Sleep(e.cfg.Lease + 300*time.Millisecond)

	res := retryUntilSettled(t, b, "create_refund", refund, keyed("k1"))
	if res.IsError || e.fake.Requests("create_refund") != 1 {
		t.Fatalf("takeover did not run the call: %s", res.Raw)
	}
	p.wake()
	late := <-first
	if string(late.Raw) != string(res.Raw) {
		t.Fatalf("the woken instance did not return the stored result:\n%s\n%s", late.Raw, res.Raw)
	}
	if n := e.fake.Ledger.CountReference("order-1001"); n != 1 {
		t.Fatalf("I8: %d effects after the paused instance woke", n)
	}
}

// An instance freezes right after the gate. Nobody else may send, so a
// retry is told "unknown". The frozen instance may still send once when it
// wakes, and its late result upgrades the record to completed.
func TestPausedAfterGateSendsOnceLate(t *testing.T) {
	e := newEnv(t, envOptions{noRun: true})
	p := newPauser(gateway.PointAfterGate, gateway.PointHeartbeat)
	t.Cleanup(p.wake)
	a := serve(t, e.addGateway(t, "paused-A", p.hooks(), false))
	b := serve(t, e.addGateway(t, "B", gateway.Hooks{}, false))

	first := make(chan *mcpclient.ToolResult, 1)
	go func() {
		_, res := call(t, a, "create_refund", refund, keyed("k1"))
		first <- res
	}()
	p.waitFor(t, gateway.PointAfterGate)
	time.Sleep(e.cfg.Lease + 300*time.Millisecond)

	res := retryUntilSettled(t, b, "create_refund", refund, keyed("k1"))
	if res.GatewayStatus() != gateway.StatusOutcomeUnknown {
		t.Fatalf("want outcome unknown while the owner is frozen, got %s", res.Raw)
	}
	if n := e.fake.Requests("create_refund"); n != 0 {
		t.Fatalf("I8: another instance sent the call (%d requests)", n)
	}

	p.wake()
	late := <-first
	if late.IsError || e.fake.Ledger.CountReference("order-1001") != 1 {
		t.Fatalf("the woken owner should send once and get the result: %s", late.Raw)
	}
	_, again := call(t, b, "create_refund", refund, keyed("k1"))
	if string(again.Raw) != string(late.Raw) {
		t.Fatalf("the late result was not recorded:\n%s", again.Raw)
	}
	if n := e.fake.Requests("create_refund"); n != 1 {
		t.Fatalf("I8: %d dispatches, want 1", n)
	}
}

func TestDuplicatesRaceAcrossInstances(t *testing.T) {
	e := newEnv(t, envOptions{})
	e.fake.AddFault(faketool.Fault{DelayBeforeMs: 100, Times: 1 << 20})
	clients := []*mcpclient.Client{e.client}
	for i := range 2 {
		clients = append(clients, serve(t, e.addGateway(t, fmt.Sprintf("g%d", i+1), gateway.Hooks{}, true)))
	}

	const keys, copies = 10, 6
	results := make([][]string, keys)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for k := range keys {
		ref := fmt.Sprintf("race-%d", k)
		args := map[string]any{"reference": ref, "amount": 10}
		for c := range copies {
			wg.Go(func() {
				res := retryUntilSettled(t, clients[(k+c)%len(clients)], "create_refund", args, keyed(ref))
				mu.Lock()
				results[k] = append(results[k], string(res.Raw))
				mu.Unlock()
			})
		}
	}
	wg.Wait()

	for k := range keys {
		ref := fmt.Sprintf("race-%d", k)
		if n := e.fake.Ledger.CountReference(ref); n != 1 {
			t.Errorf("%s: I1: %d effects", ref, n)
		}
		if n := e.fake.RequestsForReference(ref); n != 1 {
			t.Errorf("%s: I8: dispatched %d times", ref, n)
		}
		for _, r := range results[k][1:] {
			if r != results[k][0] {
				t.Errorf("%s: I5: duplicates got different answers", ref)
				break
			}
		}
	}
}

// chaosOp is one logical operation in the random-kill test.
type chaosOp struct {
	tool   string
	ref    string
	args   map[string]any
	native bool
	final  *mcpclient.ToolResult
	hits   int // attempts cut off by a killed gateway
}

// TestRandomKillsUnderLoad runs operations through three gateway processes
// while instances are killed and restarted at random, then checks every
// operation against the ledger. GATEWAY_CHAOS_SEEDS runs more seeds, for
// example GATEWAY_CHAOS_SEEDS=1,2,3,4,5.
func TestRandomKillsUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in -short mode")
	}
	seeds := []uint64{1}
	if s := os.Getenv("GATEWAY_CHAOS_SEEDS"); s != "" {
		seeds = nil
		for f := range strings.SplitSeq(s, ",") {
			n, err := strconv.ParseUint(strings.TrimSpace(f), 10, 64)
			if err != nil {
				t.Fatalf("bad seed %q", f)
			}
			seeds = append(seeds, n)
		}
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) { runChaos(t, seed) })
	}
}

func runChaos(t *testing.T, seed uint64) {
	const loadFor = 8 * time.Second
	fake, fakeURL := startFake(t)
	// Latency before every effect widens the windows a kill can hit.
	fake.AddFault(faketool.Fault{DelayBeforeMs: 100, Times: 1 << 30})
	cfg := writeConfig(t, fakeURL, "")
	db := processDB(t)

	var mu sync.Mutex
	slots := make([]*proc, 3)
	for i := range slots {
		slots[i] = startGateway(t, cfg, db, procOptions{instance: fmt.Sprintf("g%d-0", i)})
	}
	endpoint := func(r *rand.Rand) string {
		mu.Lock()
		defer mu.Unlock()
		return slots[r.IntN(len(slots))].url
	}

	stop := make(chan struct{})
	chaosDone := make(chan struct{})
	var kills atomic.Int32
	go func() {
		defer close(chaosDone)
		r := rand.New(rand.NewPCG(seed, 1))
		for gen := 1; ; gen++ {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(300+r.IntN(400)) * time.Millisecond):
			}
			i := r.IntN(len(slots))
			mu.Lock()
			old := slots[i]
			mu.Unlock()
			old.kill()
			kills.Add(1)
			next, err := startGatewayE(t, cfg, db, procOptions{instance: fmt.Sprintf("g%d-%d", i, gen)})
			if err != nil {
				t.Errorf("restart: %v", err)
				return
			}
			mu.Lock()
			slots[i] = next
			mu.Unlock()
		}
	}()

	newOp := func(i int) *chaosOp {
		if i%3 == 2 {
			key := fmt.Sprintf("charge-%d", i)
			return &chaosOp{tool: "create_charge", ref: key, native: true,
				args: map[string]any{"amount": 5, "idempotency_key": key}}
		}
		ref := fmt.Sprintf("refund-%d", i)
		return &chaosOp{tool: "create_refund", ref: ref, args: map[string]any{"reference": ref, "amount": 10}}
	}

	var (
		opsMu sync.Mutex
		ops   []*chaosOp
		next  atomic.Int32
		wg    sync.WaitGroup
	)
	loadEnd := time.Now().Add(loadFor)
	for w := range 12 {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(seed, uint64(100+w)))
			for time.Now().Before(loadEnd) {
				op := newOp(int(next.Add(1)))
				op.final = settleChaos(t, fake, op, func() string { return endpoint(r) })
				opsMu.Lock()
				ops = append(ops, op)
				opsMu.Unlock()
			}
		})
	}
	wg.Wait()
	close(stop)
	<-chaosDone
	time.Sleep(300 * time.Millisecond) // let cancelled tool handlers finish

	unknown, refunds, hit := 0, 0, 0
	for _, op := range ops {
		if op.hits > 0 {
			hit++
		}
		if !op.native {
			refunds++
		}
		if op.final == nil {
			continue // already reported
		}
		effects := fake.Ledger.CountReference(op.ref)
		status := op.final.GatewayStatus()
		if op.native {
			if effects != 1 || status != "" {
				t.Errorf("%s: I2: %d effects, final %q", op.ref, effects, status)
			}
			continue
		}
		if effects > 1 {
			t.Errorf("%s: I1: %d effects", op.ref, effects)
		}
		if n := fake.RequestsForReference(op.ref); n > 1 {
			t.Errorf("%s: I8: dispatched %d times", op.ref, n)
		}
		switch status {
		case "":
			if effects != 1 || op.final.StructuredContent["reference"] != op.ref {
				t.Errorf("%s: I3: told it ran, %d effects, result %s", op.ref, effects, op.final.Raw)
			}
		case gateway.StatusOutcomeUnknown:
			unknown++
		default:
			t.Errorf("%s: unexpected final status %q", op.ref, status)
		}
	}
	t.Logf("seed %d: %d operations, %d kills, %d operations cut off by a kill; %d of %d at_most_once operations ended unknown (%.1f%%)",
		seed, len(ops), kills.Load(), hit, unknown, refunds, 100*float64(unknown)/float64(max(refunds, 1)))
	if hit == 0 {
		t.Error("no operation was cut off by a kill, so this run proved nothing; raise the load or kill rate")
	}
}

// settleChaos retries one operation, as an agent would, until it has a
// final answer: a real result, or "unknown" for a tool that cannot be
// resent. It checks I4 whenever the gateway says a call did not run.
func settleChaos(t *testing.T, fake *faketool.Server, op *chaosOp, endpoint func() string) *mcpclient.ToolResult {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(bg, 10*time.Second)
		r, err := mcpclient.New(endpoint()).Call(ctx, op.tool, op.args, keyed(op.ref))
		cancel()
		if err != nil {
			if !errors.Is(err, mcpclient.ErrTransport) {
				t.Errorf("%s: %v", op.ref, err)
				return nil
			}
			op.hits++
			time.Sleep(50 * time.Millisecond)
			continue
		}
		res, err := r.ToolResult()
		if err != nil {
			t.Errorf("%s: protocol error: %v", op.ref, err)
			return nil
		}
		switch res.GatewayStatus() {
		case gateway.StatusInProgress, gateway.StatusStoreUnavailable:
			time.Sleep(100 * time.Millisecond)
			continue
		case gateway.StatusNotExecuted:
			if n := fake.Ledger.CountReference(op.ref); n != 0 {
				t.Errorf("%s: I4: told it did not run, but %d effects", op.ref, n)
			}
			continue // safe to retry
		case gateway.StatusOutcomeUnknown:
			if op.native {
				continue // resending with the same key is safe
			}
		}
		return res
	}
	t.Errorf("%s: no final answer within 60s", op.ref)
	return nil
}
