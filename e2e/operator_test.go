package e2e

import (
	"errors"
	"testing"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/faketool"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/gateway"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/operator"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
)

// An operator confirms an unknown outcome took effect; retries are told so
// and the tool never runs again.
func TestOperatorConfirmsCompletion(t *testing.T) {
	e := newEnv(t, envOptions{})
	e.fake.AddFault(faketool.Fault{Tool: "create_refund", DropResponse: true})
	r, res := call(t, e.client, "create_refund", refund, keyed("k1"))
	if res.GatewayStatus() != gateway.StatusOutcomeUnknown {
		t.Fatalf("want unknown, got %s", res.Raw)
	}
	key := r.Header.Get("X-Gateway-Operation-Key")

	if _, err := operator.Resolve(bg, e.store, key, store.Completed, "refund visible in provider dashboard", "dana"); err != nil {
		t.Fatal(err)
	}
	_, res = call(t, e.client, "create_refund", refund, keyed("k1"))
	if res.IsError || res.GatewayStatus() != operator.StatusResolvedByOperator {
		t.Fatalf("want the operator's confirmation, got %s", res.Raw)
	}
	if e.fake.Requests("create_refund") != 1 {
		t.Fatal("the tool ran again after the operator confirmed it")
	}
}

// An operator establishes that an unknown call never took effect; the next
// retry runs it, once.
func TestOperatorRulesOutEffect(t *testing.T) {
	e := newEnv(t, envOptions{})
	e.fake.AddFault(faketool.Fault{Tool: "create_refund", DropBeforeEffect: true})
	r, _ := call(t, e.client, "create_refund", refund, keyed("k1"))
	key := r.Header.Get("X-Gateway-Operation-Key")

	if _, err := operator.Resolve(bg, e.store, key, store.NotExecuted, "no refund for order-1001 in provider dashboard", "dana"); err != nil {
		t.Fatal(err)
	}
	_, res := call(t, e.client, "create_refund", refund, keyed("k1"))
	if res.IsError || res.GatewayStatus() != "" {
		t.Fatalf("want the call to run, got %s", res.Raw)
	}
	_, again := call(t, e.client, "create_refund", refund, keyed("k1"))
	if string(again.Raw) != string(res.Raw) || e.fake.Ledger.CountReference("order-1001") != 1 {
		t.Fatal("the re-run was not recorded once")
	}
}

func TestOperatorCannotOverrideKnownOutcome(t *testing.T) {
	e := newEnv(t, envOptions{})
	r, _ := call(t, e.client, "create_refund", refund, keyed("k1"))
	key := r.Header.Get("X-Gateway-Operation-Key")
	_, err := operator.Resolve(bg, e.store, key, store.NotExecuted, "looked wrong", "dana")
	if !errors.Is(err, operator.ErrNotResolvable) {
		t.Fatalf("overrode a completed operation: %v", err)
	}
}
