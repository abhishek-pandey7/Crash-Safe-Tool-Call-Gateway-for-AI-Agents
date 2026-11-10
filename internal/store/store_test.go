package store_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store/storetest"
)

var ctx = context.Background()

func params(base, owner string, lease time.Duration) store.ClaimParams {
	return store.ClaimParams{
		Base: base, Source: "caller_key", Principal: "alice", Upstream: "http://u/mcp",
		Tool: "create_refund", Policy: "at_most_once", ArgsHash: []byte{1, 2, 3},
		Owner: owner, Lease: lease,
	}
}

func claim(t *testing.T, st *store.Store, p store.ClaimParams) *store.Operation {
	t.Helper()
	op, created, err := st.Claim(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatalf("claim of %s was not created", p.Base)
	}
	return op
}

func mustState(t *testing.T, st *store.Store, key string, want store.State) *store.Operation {
	t.Helper()
	op, err := st.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if op.State != want {
		t.Fatalf("%s: state %s, want %s", key, op.State, want)
	}
	return op
}

func TestClaimIsUniqueUnderRace(t *testing.T) {
	st := storetest.New(t)
	var created atomic.Int32
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, err := st.Claim(ctx, params("op_race", fmt.Sprintf("g%d", i), time.Second))
			if err != nil {
				t.Error(err)
			}
			if ok {
				created.Add(1)
			}
		}()
	}
	wg.Wait()
	if created.Load() != 1 {
		t.Fatalf("%d claims created, want 1", created.Load())
	}
}

func TestGatePassesOnce(t *testing.T) {
	st := storetest.New(t)
	op := claim(t, st, params("op_gate", "a", time.Minute))

	if _, ok, _ := st.Gate(ctx, op.Key, "b", op.Fence, time.Minute); ok {
		t.Fatal("non-owner passed the gate")
	}
	if _, ok, _ := st.Gate(ctx, op.Key, "a", op.Fence+1, time.Minute); ok {
		t.Fatal("wrong fence passed the gate")
	}

	var passed atomic.Int32
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok, err := st.Gate(ctx, op.Key, "a", op.Fence, time.Minute); err != nil {
				t.Error(err)
			} else if ok {
				passed.Add(1)
			}
		}()
	}
	wg.Wait()
	if passed.Load() != 1 {
		t.Fatalf("gate passed %d times, want 1", passed.Load())
	}
	got := mustState(t, st, op.Key, store.Dispatching)
	if got.Attempts != 1 || got.DispatchedAt == nil {
		t.Fatalf("dispatch not recorded: %+v", got)
	}
}

func TestTakeoverFencesOutOldOwner(t *testing.T) {
	st := storetest.New(t)
	op := claim(t, st, params("op_takeover", "a", 150*time.Millisecond))

	if _, ok, _ := st.Acquire(ctx, op.Key, "b", time.Minute); ok {
		t.Fatal("took over a live lease")
	}
	time.Sleep(250 * time.Millisecond)
	taken, ok, err := st.Acquire(ctx, op.Key, "b", time.Minute)
	if err != nil || !ok {
		t.Fatalf("takeover after expiry failed: %v", err)
	}
	if taken.Fence != op.Fence+1 || taken.Owner != "b" {
		t.Fatalf("takeover did not bump the fence: %+v", taken)
	}
	// The paused original owner wakes up and tries the gate.
	if _, ok, _ := st.Gate(ctx, op.Key, "a", op.Fence, time.Minute); ok {
		t.Fatal("stale owner passed the gate")
	}
	if _, ok, _ := st.Gate(ctx, op.Key, "b", taken.Fence, time.Minute); !ok {
		t.Fatal("new owner could not pass the gate")
	}
	if ok, _ := st.Heartbeat(ctx, op.Key, "a", op.Fence, time.Minute); ok {
		t.Fatal("stale owner renewed the lease")
	}
}

func TestCompleteFirstWriterWinsAndUpgradesUnknown(t *testing.T) {
	st := storetest.New(t)
	op := claim(t, st, params("op_done", "a", 150*time.Millisecond))
	if _, ok, _ := st.Gate(ctx, op.Key, "a", op.Fence, 150*time.Millisecond); !ok {
		t.Fatal("gate failed")
	}
	if _, ok, _ := st.ExpireDispatch(ctx, op.Key, "b"); ok {
		t.Fatal("expired a live dispatch")
	}
	time.Sleep(250 * time.Millisecond)
	if _, ok, _ := st.ExpireDispatch(ctx, op.Key, "b"); !ok {
		t.Fatal("did not expire a lapsed dispatch")
	}
	mustState(t, st, op.Key, store.Unknown)

	// A late response from the original owner is still the truth.
	done, ok, err := st.Complete(ctx, op.Key, "a", store.Outcome{Kind: store.KindResult, Body: []byte(`{"ok":1}`)})
	if err != nil || !ok || string(done.Result) != `{"ok":1}` {
		t.Fatalf("late completion: %v %v", ok, err)
	}
	if _, ok, _ := st.Complete(ctx, op.Key, "b", store.Outcome{Kind: store.KindResult, Body: []byte(`{"ok":2}`)}); ok {
		t.Fatal("second completion overwrote the first")
	}
	if _, ok, _ := st.Acquire(ctx, op.Key, "b", time.Minute); ok {
		t.Fatal("re-acquired a completed operation")
	}
}

func TestNotExecutedCanRunAgain(t *testing.T) {
	st := storetest.New(t)
	op := claim(t, st, params("op_retry", "a", time.Minute))
	st.Gate(ctx, op.Key, "a", op.Fence, time.Minute)
	if ok, _ := st.MarkNotExecuted(ctx, op.Key, "b", op.Fence+1, nil); ok {
		t.Fatal("wrong fence marked not executed")
	}
	if ok, _ := st.MarkNotExecuted(ctx, op.Key, "a", op.Fence, map[string]any{"reason": "dial failed"}); !ok {
		t.Fatal("owner could not mark not executed")
	}
	again, ok, _ := st.Acquire(ctx, op.Key, "b", time.Minute)
	if !ok || again.State != store.Claimed || again.Fence != op.Fence+1 {
		t.Fatalf("not reclaimable: %+v", again)
	}
}

func TestRedispatch(t *testing.T) {
	st := storetest.New(t)
	op := claim(t, st, params("op_native", "a", time.Minute))
	st.Gate(ctx, op.Key, "a", op.Fence, time.Minute)

	if _, ok, _ := st.Redispatch(ctx, op.Key, "b", time.Minute, store.FromStale, nil); ok {
		t.Fatal("resent a live dispatch")
	}
	st.MarkUnknown(ctx, op.Key, "a", op.Fence, nil)
	re, ok, err := st.Redispatch(ctx, op.Key, "b", time.Minute, store.FromStale, map[string]any{"reason": "native key resend"})
	if err != nil || !ok || re.Fence != op.Fence+1 || re.Attempts != 2 {
		t.Fatalf("resend from unknown: %+v %v", re, err)
	}

	if ok, _ := st.AwaitInput(ctx, op.Key, "b", re.Fence, []byte(`{"resultType":"input_required"}`)); !ok {
		t.Fatal("await input failed")
	}
	if _, ok, _ := st.Redispatch(ctx, op.Key, "c", time.Minute, store.FromStale, nil); ok {
		t.Fatal("stale resend of an awaiting operation")
	}
	cont, ok, _ := st.Redispatch(ctx, op.Key, "c", time.Minute, store.FromAwaitingInput, nil)
	if !ok || cont.Owner != "c" || cont.Result != nil {
		t.Fatalf("continuation: %+v", cont)
	}
}

func TestWindowedClaimRollsOver(t *testing.T) {
	st := storetest.New(t)
	p := params("op_window", "a", time.Minute)
	p.Window = 300 * time.Millisecond

	first := claim(t, st, p)
	if again, created, _ := st.Claim(ctx, p); created || again.Key != first.Key {
		t.Fatal("duplicate inside the window started a new operation")
	}
	time.Sleep(400 * time.Millisecond)
	if again, created, _ := st.Claim(ctx, p); created || again.Key != first.Key {
		t.Fatal("an active operation rolled over")
	}

	st.Gate(ctx, first.Key, "a", first.Fence, time.Minute)
	st.Complete(ctx, first.Key, "a", store.Outcome{Kind: store.KindResult, Body: []byte(`{}`)})
	if again, created, _ := st.Claim(ctx, p); created || again.Key != first.Key {
		t.Fatal("rolled over right after finishing")
	}
	time.Sleep(400 * time.Millisecond)
	next, created, err := st.Claim(ctx, p)
	if err != nil || !created || next.Generation != 1 {
		t.Fatalf("no new generation after the window: %+v %v", next, err)
	}
}

func TestSweepAndEvents(t *testing.T) {
	st := storetest.New(t)
	claimed := claim(t, st, params("op_sweep_claim", "a", 100*time.Millisecond))
	sent := claim(t, st, params("op_sweep_sent", "a", 100*time.Millisecond))
	st.Gate(ctx, sent.Key, "a", sent.Fence, 100*time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	released, unknown, err := st.Sweep(ctx, "sweeper")
	if err != nil || released != 1 || unknown != 1 {
		t.Fatalf("sweep: %d released, %d unknown, %v", released, unknown, err)
	}
	mustState(t, st, claimed.Key, store.Released)
	mustState(t, st, sent.Key, store.Unknown)

	events, err := st.Events(ctx, sent.Key)
	if err != nil {
		t.Fatal(err)
	}
	want := []struct{ from, to store.State }{{"", store.Claimed}, {store.Claimed, store.Dispatching}, {store.Dispatching, store.Unknown}}
	if len(events) != len(want) {
		t.Fatalf("got %d events: %+v", len(events), events)
	}
	for i, w := range want {
		if events[i].From != w.from || events[i].To != w.to {
			t.Errorf("event %d: %s -> %s, want %s -> %s", i, events[i].From, events[i].To, w.from, w.to)
		}
	}
	if events[2].Instance != "sweeper" || len(events[2].Detail) == 0 {
		t.Errorf("sweep event lacks attribution: %+v", events[2])
	}
}

func TestResolveNeedsEvidence(t *testing.T) {
	st := storetest.New(t)
	op := claim(t, st, params("op_resolve", "a", time.Minute))
	st.Gate(ctx, op.Key, "a", op.Fence, time.Minute)
	st.MarkUnknown(ctx, op.Key, "a", op.Fence, nil)

	if _, _, err := st.Resolve(ctx, op.Key, "operator", store.Resolution{State: store.Completed}); err == nil {
		t.Fatal("resolved without evidence")
	}
	if _, _, err := st.Resolve(ctx, op.Key, "operator", store.Resolution{State: store.Unknown, Evidence: map[string]any{"x": 1}}); err == nil {
		t.Fatal("resolved to unknown")
	}
	res, ok, err := st.Resolve(ctx, op.Key, "operator", store.Resolution{
		State: store.NotExecuted, Evidence: map[string]any{"checked": "payment provider dashboard"},
	})
	if err != nil || !ok || res.State != store.NotExecuted {
		t.Fatalf("resolve: %+v %v", res, err)
	}
}

func TestPurgeKeepsTheKey(t *testing.T) {
	st := storetest.New(t)
	op := claim(t, st, params("op_purge", "a", time.Minute))
	st.Gate(ctx, op.Key, "a", op.Fence, time.Minute)
	st.Complete(ctx, op.Key, "a", store.Outcome{Kind: store.KindResult, Body: []byte(`{"x":1}`), Hash: []byte{9}})
	time.Sleep(50 * time.Millisecond)

	n, err := st.PurgeResults(ctx, 10*time.Millisecond)
	if err != nil || n != 1 {
		t.Fatalf("purged %d, %v", n, err)
	}
	got := mustState(t, st, op.Key, store.Completed)
	if got.Result != nil || !got.ResultPurged || len(got.ResultHash) == 0 {
		t.Fatalf("purge removed too much or too little: %+v", got)
	}
}

func TestReconcilableQueue(t *testing.T) {
	st := storetest.New(t)
	p := params("op_rec", "a", time.Minute)
	p.ReconcileRef = []byte(`"order-7"`)
	op := claim(t, st, p)
	plain := claim(t, st, params("op_plain", "a", time.Minute))
	for _, o := range []*store.Operation{op, plain} {
		st.Gate(ctx, o.Key, "a", o.Fence, time.Minute)
		st.MarkUnknown(ctx, o.Key, "a", o.Fence, nil)
	}
	ops, err := st.Reconcilable(ctx, 10)
	if err != nil || len(ops) != 1 || ops[0].Key != op.Key || string(ops[0].ReconcileRef) != `"order-7"` {
		t.Fatalf("reconcilable: %+v %v", ops, err)
	}
}

func TestList(t *testing.T) {
	st := storetest.New(t)
	a := claim(t, st, params("op_list_a", "a", time.Minute))
	claim(t, st, params("op_list_b", "a", time.Minute))
	st.Gate(ctx, a.Key, "a", a.Fence, time.Minute)
	st.MarkUnknown(ctx, a.Key, "a", a.Fence, nil)

	ops, err := st.List(ctx, store.ListFilter{States: []store.State{store.Unknown}})
	if err != nil || len(ops) != 1 || ops[0].Key != a.Key {
		t.Fatalf("list unknown: %v %v", ops, err)
	}
	all, err := st.List(ctx, store.ListFilter{})
	if err != nil || len(all) != 2 {
		t.Fatalf("list all: %d %v", len(all), err)
	}
}
