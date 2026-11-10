package e2e

import (
	"testing"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/faketool"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/gateway"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
)

// The refund happens but its response is lost. The read finds the refund,
// so the client is told it completed, not "unknown".
func TestReconcileConfirmsLostResponse(t *testing.T) {
	e := newEnv(t, envOptions{refund: refundReconcilable})
	e.fake.AddFault(faketool.Fault{Tool: "create_refund", DropResponse: true})

	r, res := call(t, e.client, "create_refund", refund, keyed("k1"))
	if res.IsError || res.GatewayStatus() != gateway.StatusReconciled {
		t.Fatalf("want a reconciled result, got %s", res.Raw)
	}
	if got := e.opState(t, r.Header.Get("X-Gateway-Operation-Key")); got != store.Completed {
		t.Fatalf("state %s, want completed", got)
	}
	r2, again := call(t, e.client, "create_refund", refund, keyed("k1"))
	if string(again.Raw) != string(res.Raw) || r2.Header.Get("X-Gateway-Replayed") != "true" {
		t.Fatal("the reconciled result is not replayed")
	}
	if n := e.fake.Ledger.CountReference("order-1001"); n != 1 || e.fake.Requests("create_refund") != 1 {
		t.Fatalf("%d effects from %d requests", n, e.fake.Requests("create_refund"))
	}
}

// The call reaches the server but never runs, and the connection drops.
// "Not found" is not trusted until settle_after has passed; after that a
// retry runs the call, once.
func TestReconcileNotFoundRunsOnceAfterSettle(t *testing.T) {
	e := newEnv(t, envOptions{refund: refundReconcilable, noRun: true})
	e.fake.AddFault(faketool.Fault{Tool: "create_refund", DropBeforeEffect: true})

	_, res := call(t, e.client, "create_refund", refund, keyed("k1"))
	if res.GatewayStatus() != gateway.StatusOutcomeUnknown {
		t.Fatalf("before settle_after: want unknown, got %s", res.Raw)
	}
	_, res = call(t, e.client, "create_refund", refund, keyed("k1"))
	if res.GatewayStatus() != gateway.StatusOutcomeUnknown || e.fake.Requests("create_refund") != 1 {
		t.Fatalf("an early 'not found' was trusted: %s", res.Raw)
	}

	time.Sleep(2200 * time.Millisecond) // settle_after is 2s
	_, res = call(t, e.client, "create_refund", refund, keyed("k1"))
	if res.IsError || res.GatewayStatus() != "" {
		t.Fatalf("after settle_after: want the call to run, got %s", res.Raw)
	}
	if n := e.fake.Ledger.CountReference("order-1001"); n != 1 || e.fake.Requests("create_refund") != 2 {
		t.Fatalf("%d effects from %d requests, want 1 from 2", n, e.fake.Requests("create_refund"))
	}
}

// The inline read fails too, so the client hears "unknown"; the background
// loop settles the operation without anyone retrying.
func TestBackgroundReconciliation(t *testing.T) {
	e := newEnv(t, envOptions{refund: refundReconcilable})
	e.fake.AddFault(faketool.Fault{Tool: "create_refund", DropResponse: true})
	e.fake.AddFault(faketool.Fault{Tool: "find_refund_by_reference", DropResponse: true})

	r, res := call(t, e.client, "create_refund", refund, keyed("k1"))
	if res.GatewayStatus() != gateway.StatusOutcomeUnknown {
		t.Fatalf("want unknown while the read is failing, got %s", res.Raw)
	}
	key := r.Header.Get("X-Gateway-Operation-Key")
	deadline := time.Now().Add(5 * time.Second)
	for e.opState(t, key) != store.Completed {
		if time.Now().After(deadline) {
			t.Fatalf("background reconciliation did not settle %s", key)
		}
		time.Sleep(100 * time.Millisecond)
	}
	_, again := call(t, e.client, "create_refund", refund, keyed("k1"))
	if again.GatewayStatus() != gateway.StatusReconciled || e.fake.Requests("create_refund") != 1 {
		t.Fatalf("after background reconciliation: %s", again.Raw)
	}
}

// A read that does not answer the configured question never settles
// anything, even after settle_after.
func TestInconclusiveReadStaysUnknown(t *testing.T) {
	policy := "{policy: reconcilable, reconcile: {tool: find_refund_by_reference, match_argument: reference, found_field: no_such_field, settle_after: 1s}}"
	e := newEnv(t, envOptions{refund: policy})
	e.fake.AddFault(faketool.Fault{Tool: "create_refund", DropBeforeEffect: true})

	call(t, e.client, "create_refund", refund, keyed("k1"))
	time.Sleep(1500 * time.Millisecond)
	_, res := call(t, e.client, "create_refund", refund, keyed("k1"))
	if res.GatewayStatus() != gateway.StatusOutcomeUnknown {
		t.Fatalf("an inconclusive read settled the outcome: %s", res.Raw)
	}
	if e.fake.Requests("create_refund") != 1 {
		t.Fatal("the call was re-run without proof that it never happened")
	}
}

// Past retention the result body is gone, but the key still answers
// "completed" and the tool never runs again.
func TestRetentionKeepsTheKey(t *testing.T) {
	e := newEnv(t, envOptions{extra: "retention: 1s"})
	call(t, e.client, "create_refund", refund, keyed("k1"))
	time.Sleep(2500 * time.Millisecond) // purge runs every second here

	_, res := call(t, e.client, "create_refund", refund, keyed("k1"))
	if res.IsError || res.GatewayStatus() != gateway.StatusResultUnavailable {
		t.Fatalf("want completed without a result, got %s", res.Raw)
	}
	if e.fake.Requests("create_refund") != 1 {
		t.Fatal("an expired result made the key look new")
	}
}

func TestHashOnlyResult(t *testing.T) {
	e := newEnv(t, envOptions{refund: "{policy: at_most_once, store_result: hash}"})
	_, first := call(t, e.client, "create_refund", refund, keyed("k1"))
	if first.IsError || first.GatewayStatus() != "" {
		t.Fatalf("the first caller should get the real result: %s", first.Raw)
	}
	r, again := call(t, e.client, "create_refund", refund, keyed("k1"))
	if again.GatewayStatus() != gateway.StatusResultUnavailable {
		t.Fatalf("a hash-only result was replayed: %s", again.Raw)
	}
	op, err := e.store.Get(bg, r.Header.Get("X-Gateway-Operation-Key"))
	if err != nil || op.Result != nil || len(op.ResultHash) == 0 {
		t.Fatalf("stored more than a hash: %+v %v", op, err)
	}
}
