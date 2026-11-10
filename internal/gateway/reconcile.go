package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/config"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcp"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
)

// StatusReconciled marks a result the gateway recorded after a read
// confirmed that a call whose response was lost took effect. isError is
// false: the call did happen.
const StatusReconciled = "reconciled"

// lookup is the answer of a reconciliation read.
type lookup int

const (
	// inconclusive: the read failed, errored, or did not answer the
	// question. A failed lookup never counts as proof of no effect.
	inconclusive lookup = iota
	found
	notFound
)

// rawArgument returns one argument's raw JSON value, or nil.
func rawArgument(args json.RawMessage, name string) json.RawMessage {
	if name == "" || len(args) == 0 {
		return nil
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(args, &m) != nil {
		return nil
	}
	v := m[name]
	if len(v) == 0 || string(v) == "null" {
		return nil
	}
	return v
}

// lookupOutcome calls the configured read tool. The read goes out with the
// upstream_headers credentials only, never a caller's, because it can run
// in the background long after the caller has gone.
func (g *Gateway) lookupOutcome(ctx context.Context, r *config.Reconcile, ref json.RawMessage) (lookup, map[string]any, error) {
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      "reconcile",
		"method":  mcp.MethodToolsCall,
		"params": map[string]any{
			"name":      r.Tool,
			"arguments": map[string]json.RawMessage{r.LookupArgument: ref},
			"_meta": map[string]any{
				mcp.MetaProtocolVersion:    mcp.ProtocolVersion,
				mcp.MetaClientInfo:         map[string]any{"name": "crashsafe-gateway-reconciler", "version": "0.1.0"},
				mcp.MetaClientCapabilities: map[string]any{},
			},
		},
	})
	if err != nil {
		return inconclusive, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.upstream.String(), bytes.NewReader(body))
	if err != nil {
		return inconclusive, nil, err
	}
	mcp.SetRequestHeaders(req.Header, mcp.MethodToolsCall, r.Tool)
	g.prepareUpstreamHeaders(req.Header)

	resp, err := g.reader.Do(req)
	if err != nil {
		return inconclusive, nil, err
	}
	defer resp.Body.Close()
	msg, err := readFinalResponse(resp)
	if err != nil {
		return inconclusive, nil, err
	}
	if len(msg.Error) > 0 {
		return inconclusive, nil, fmt.Errorf("read returned an error: %s", msg.Error)
	}
	var res struct {
		IsError           bool           `json:"isError"`
		StructuredContent map[string]any `json:"structuredContent"`
	}
	if err := json.Unmarshal(msg.Result, &res); err != nil {
		return inconclusive, nil, err
	}
	if res.IsError {
		return inconclusive, res.StructuredContent, fmt.Errorf("read reported a tool error")
	}
	answer, ok := res.StructuredContent[r.FoundField].(bool)
	if !ok {
		return inconclusive, res.StructuredContent, fmt.Errorf("read has no boolean %q", r.FoundField)
	}
	if answer {
		return found, res.StructuredContent, nil
	}
	return notFound, res.StructuredContent, nil
}

// readFinalResponse reads a JSON or SSE response up to its final message.
func readFinalResponse(resp *http.Response) (*mcp.Message, error) {
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		events := mcp.NewSSEReader(resp.Body)
		for {
			ev, err := events.Next()
			if err != nil {
				return nil, err
			}
			if m, err := mcp.ParseMessage(ev.Data); err == nil && m.IsResponse() {
				return m, nil
			}
		}
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, err
	}
	m, err := mcp.ParseMessage(raw)
	if err != nil || !m.IsResponse() {
		return nil, fmt.Errorf("status %d without a JSON-RPC response", resp.StatusCode)
	}
	return m, nil
}

// reconcile tries to settle an unknown operation of a reconcilable tool. It
// returns the settled operation and true, or false when the outcome stays
// unknown.
//
// "Found" settles at once: the effect exists. "Not found" only settles once
// settle_after has passed since dispatch, measured on the database clock,
// because until then the original call could still be in flight.
func (g *Gateway) reconcile(ctx context.Context, op *store.Operation, pol config.Tool) (*store.Operation, bool) {
	r := pol.Reconcile
	if pol.Policy != config.Reconcilable || r == nil || len(op.ReconcileRef) == 0 || op.State != store.Unknown {
		return op, false
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	answer, evidence, err := g.lookupOutcome(ctx, r, op.ReconcileRef)
	log := g.log.With("op", op.Key, "tool", op.Tool, "read", r.Tool)
	var res store.Resolution
	switch answer {
	case found:
		body := mcp.TextResult(fmt.Sprintf(
			"Completed: the response from %s was lost, but %s confirms that it took effect.", op.Tool, r.Tool),
			false, map[string]any{"gatewayStatus": StatusReconciled, "reconciledWith": r.Tool, "evidence": evidence})
		sum := sha256.Sum256(body)
		res = store.Resolution{
			State: store.Completed, Kind: store.KindResult, Body: body, Hash: sum[:],
			Evidence: map[string]any{"reconciledWith": r.Tool, "found": true, "read": evidence},
		}
	case notFound:
		if op.DispatchedAt == nil || op.Now.Sub(*op.DispatchedAt) < r.SettleAfter {
			log.Info("read found nothing, but the call may still be in flight; outcome stays unknown")
			return op, false
		}
		res = store.Resolution{
			State: store.NotExecuted,
			Evidence: map[string]any{"reconciledWith": r.Tool, "found": false, "read": evidence,
				"settleAfterMs": r.SettleAfter.Milliseconds()},
		}
	default:
		log.Info("reconciliation inconclusive; outcome stays unknown", "err", err)
		return op, false
	}

	settled, ok, err := g.store.Resolve(ctx, op.Key, g.instance, res)
	if err != nil {
		log.Warn("could not record reconciliation", "err", err)
		return op, false
	}
	if !ok {
		// Someone else settled it first; use their answer.
		cur, err := g.store.Get(ctx, op.Key)
		if err != nil {
			return op, false
		}
		return cur, cur.State != store.Unknown
	}
	log.Info("reconciled unknown outcome", "state", settled.State)
	return settled, true
}

// reconcileUnknown checks every unknown reconcilable operation of this
// gateway's upstream.
func (g *Gateway) reconcileUnknown(ctx context.Context) {
	ops, err := g.store.Reconcilable(ctx, 50)
	if err != nil {
		g.log.Warn("listing unknown operations failed", "err", err)
		return
	}
	for _, op := range ops {
		if op.Upstream != g.cfg.Upstream {
			continue
		}
		if pol, ok := g.cfg.ToolPolicy(op.Tool); ok {
			g.reconcile(ctx, op, pol)
		}
	}
}
