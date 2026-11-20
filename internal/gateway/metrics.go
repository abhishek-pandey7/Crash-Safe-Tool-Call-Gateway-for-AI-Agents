package gateway

import (
	"context"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/config"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/metrics"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
)

// Outcomes of a tools/call, as counted in gateway_tool_calls_total.
const (
	OutcomeCompleted     = "completed"      // sent, and the upstream answered
	OutcomeInputRequired = "input_required" // the upstream asked for more input
	OutcomeReplayed      = "replayed"       // answered from the record, not sent
	OutcomeReadOnly      = "read_only"      // forwarded without a record
	OutcomeKeyConflict   = "key_conflict"
	OutcomeKeyRequired   = "key_required"
	OutcomeRefused       = "refused"     // tool not allowed by the config
	OutcomeInvalid       = "invalid"     // malformed call
	OutcomeClientLeft    = "client_left" // the caller hung up while waiting on another attempt
)

// unlistedTool replaces tool names missing from the config in metric labels.
// Callers choose tool names, so using them as labels would let a caller
// create unbounded series.
const unlistedTool = "_unlisted"

// openStates are the states counted by gateway_operations.
var openStates = []store.State{store.Claimed, store.Dispatching, store.AwaitingInput, store.Unknown}

type gatewayMetrics struct {
	calls            *metrics.Counter
	dispatches       *metrics.Counter
	dispatchSeconds  *metrics.Histogram
	storeErrors      *metrics.Counter
	pendingDropped   *metrics.Counter
	pendingAbandoned *metrics.Counter
	reconciliations  *metrics.Counter
	settled          *metrics.Counter
}

func newGatewayMetrics(r *metrics.Registry, g *Gateway) *gatewayMetrics {
	m := &gatewayMetrics{
		calls: r.NewCounter("gateway_tool_calls_total",
			"tools/call requests by tool, policy and how the gateway answered.", "tool", "policy", "outcome"),
		dispatches: r.NewCounter("gateway_dispatches_total",
			"Sends to the upstream: response (an answer came back), unsent (provably never left), maybe_sent (no answer, may have run).",
			"policy", "result"),
		dispatchSeconds: r.NewHistogram("gateway_dispatch_duration_seconds",
			"Time from sending a call upstream to its final response or failure.",
			[]float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}, "policy"),
		storeErrors: r.NewCounter("gateway_store_errors_total",
			"Failed operation store statements, on the request path or in background work.", "where"),
		pendingDropped: r.NewCounter("gateway_pending_writes_dropped_total",
			"State changes dropped because the retry queue was full. The lease lapses and the operation is settled as unknown."),
		pendingAbandoned: r.NewCounter("gateway_pending_writes_abandoned_total",
			"State changes given up after the retry window."),
		reconciliations: r.NewCounter("gateway_reconciliations_total",
			"Reconciliation reads by answer: found, not_found_settled, not_found_too_early, inconclusive.", "result"),
		settled: r.NewCounter("gateway_abandoned_operations_settled_total",
			"Operations whose owner vanished, settled by the sweep: released (never sent) or unknown (may have run).", "to"),
	}
	r.NewGaugeFunc("gateway_pending_writes",
		"State changes waiting to be retried because the store was unreachable.", nil,
		func() []metrics.Sample { return []metrics.Sample{{Value: float64(len(g.pending))}} })
	r.NewGaugeFunc("gateway_store_up", "1 if the operation store answered a ping at scrape time, else 0.", nil,
		func() []metrics.Sample {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			up := 1.0
			if g.store.Ping(ctx) != nil {
				up = 0
			}
			return []metrics.Sample{{Value: up}}
		})
	r.NewGaugeFunc("gateway_operations",
		"Unfinished operations in the store, across all instances, by state. Absent when the store is unreachable.",
		[]string{"state"},
		func() []metrics.Sample {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			counts, err := g.store.CountOpen(ctx)
			if err != nil {
				return nil
			}
			out := make([]metrics.Sample, 0, len(openStates))
			for _, st := range openStates {
				out = append(out, metrics.Sample{Values: []string{string(st)}, Value: float64(counts[st])})
			}
			return out
		})
	return m
}

// toolLabel bounds tool names used as labels to the configured ones.
func (g *Gateway) toolLabel(name string) string {
	if _, ok := g.cfg.Tools[name]; ok {
		return name
	}
	return unlistedTool
}

func (g *Gateway) countCall(tool, policy, outcome string) {
	g.metrics.calls.Inc(g.toolLabel(tool), policy, outcome)
}

func (g *Gateway) observeDispatch(policy config.Policy, res dispatchResult, took time.Duration) {
	result := "response"
	switch {
	case res.final != nil:
	case res.maybeSent:
		result = "maybe_sent"
	default:
		result = "unsent"
	}
	g.metrics.dispatches.Inc(string(policy), result)
	g.metrics.dispatchSeconds.Observe(took.Seconds(), string(policy))
}
