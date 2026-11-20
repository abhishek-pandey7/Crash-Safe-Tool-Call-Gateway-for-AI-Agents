package gateway

import (
	"fmt"
	"maps"
	"net/http"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/config"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcp"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
)

// Values of structuredContent.gatewayStatus on results the gateway writes
// itself. They are tool execution errors (isError: true) unless noted, so
// clients pass them to the model, which is often what decides to retry.
const (
	StatusOutcomeUnknown    = "outcome_unknown"
	StatusInProgress        = "in_progress"
	StatusNotExecuted       = "not_executed"
	StatusStoreUnavailable  = "store_unavailable"
	StatusResultUnavailable = "completed_result_unavailable" // isError: false
)

func (g *Gateway) gatewayResult(c *callCtx, status, text string, isError bool, extra map[string]any) {
	structured := map[string]any{"gatewayStatus": status}
	if c.key != "" {
		structured["operationKey"] = c.key
	}
	maps.Copy(structured, extra)
	c.out.w.Header().Set("X-Gateway-Status", status)
	c.outcome = status
	if status == StatusResultUnavailable {
		c.outcome = OutcomeReplayed
	}
	c.out.final(http.StatusOK, mcp.ResultResponse(c.msg.ID, mcp.TextResult(text, isError, structured)))
}

// replay answers with the stored result of a completed operation, under
// the new request's id.
func (g *Gateway) replay(c *callCtx, op *store.Operation) {
	c.out.w.Header().Set("X-Gateway-Replayed", "true")
	c.outcome = OutcomeReplayed
	switch {
	case op.Result != nil && op.ResultKind == store.KindError:
		c.out.final(http.StatusOK, mcp.RawErrorResponse(c.msg.ID, op.Result))
	case op.Result != nil:
		c.out.final(http.StatusOK, mcp.ResultResponse(c.msg.ID, op.Result))
	default:
		reason := "it was not stored"
		if op.ResultPurged {
			reason = "it is older than the retention period"
		}
		extra := map[string]any{}
		if op.FinishedAt != nil {
			extra["finishedAt"] = op.FinishedAt.UTC().Format(time.RFC3339Nano)
		}
		g.gatewayResult(c, StatusResultUnavailable, fmt.Sprintf(
			"Completed: %s already ran for this operation, but its result is not available because %s. "+
				"Do not retry; check the external system for details.", c.params.Name, reason), false, extra)
	}
}

func (g *Gateway) respondUnknown(c *callCtx, op *store.Operation) {
	text := fmt.Sprintf("Outcome unknown: %s was sent, but no result was received. It may or may not have happened. "+
		"Do not retry; retrying this call returns this same message. Check the external system or ask a person.", c.params.Name)
	switch c.pol.Policy {
	case config.NativeKey:
		text = fmt.Sprintf("Outcome unknown: %s was sent, but no result was received. Retrying is safe: the gateway "+
			"resends it with the same idempotency key, so it cannot take effect twice.", c.params.Name)
	case config.Reconcilable:
		text = fmt.Sprintf("Outcome unknown: %s was sent, but no result was received. The gateway is checking with %s "+
			"and will settle the outcome. Retrying later is safe: it returns the confirmed result, and runs the call "+
			"only if the check proves it did not happen.", c.params.Name, c.pol.Reconcile.Tool)
	}
	extra := map[string]any{}
	if op != nil && op.DispatchedAt != nil {
		extra["dispatchedAt"] = op.DispatchedAt.UTC().Format(time.RFC3339Nano)
	}
	g.gatewayResult(c, StatusOutcomeUnknown, text, true, extra)
}

func (g *Gateway) respondInProgress(c *callCtx) {
	g.gatewayResult(c, StatusInProgress, fmt.Sprintf(
		"Still running: %s was started by an earlier attempt and has not finished. Retrying is safe; "+
			"a retry returns the result once it is ready.", c.params.Name), true,
		map[string]any{"retryAfterMs": g.cfg.WaitTimeout.Milliseconds()})
}

// respondNotExecuted is only used when no attempt of the operation can
// have been sent.
func (g *Gateway) respondNotExecuted(c *callCtx, reason string) {
	g.gatewayResult(c, StatusNotExecuted, fmt.Sprintf(
		"Not executed: %s was not sent because %s. Nothing happened. It is safe to retry.", c.params.Name, reason),
		true, nil)
}

// respondStoreDown speaks only for this attempt. An earlier attempt of the
// same operation may still have run.
func (g *Gateway) respondStoreDown(c *callCtx, err error) {
	c.log.Error("operation store unavailable", "err", err)
	g.metrics.storeErrors.Inc("request")
	g.gatewayResult(c, StatusStoreUnavailable, fmt.Sprintf(
		"Not sent: the gateway could not reach its operation store, so it did not send this %s call. "+
			"Retry later.", c.params.Name), true, nil)
}

func (g *Gateway) respondConflict(c *callCtx) {
	c.outcome = OutcomeKeyConflict
	c.out.final(http.StatusOK, mcp.ErrorResponse(c.msg.ID, mcp.NewError(mcp.CodeKeyConflict,
		"Operation key conflict: this key was already used for "+c.params.Name+" with different arguments",
		map[string]any{"operationKey": c.key})))
}
