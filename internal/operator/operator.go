// Package operator records outcomes that a person established outside the
// gateway, such as by checking a payment provider's dashboard.
package operator

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcp"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
)

// StatusResolvedByOperator is the gatewayStatus of a result recorded by an
// operator. isError is false: the call did happen.
const StatusResolvedByOperator = "resolved_by_operator"

// ErrNotResolvable is returned for an operation that is not unknown and
// still has a live owner.
var ErrNotResolvable = errors.New("only unknown operations, or dispatches whose owner is gone, can be resolved")

// Resolve settles an operation as completed or not_executed. Evidence is
// required and kept in the audit trail. A completed operation gets a result
// telling the model that a person confirmed it; a not_executed one runs
// again on the next retry.
func Resolve(ctx context.Context, st *store.Store, key string, outcome store.State, evidence, by string) (from store.State, err error) {
	if strings.TrimSpace(evidence) == "" {
		return "", errors.New("evidence is required: say what you checked and what you found")
	}
	op, err := st.Get(ctx, key)
	if err != nil {
		return "", err
	}
	res := store.Resolution{Evidence: map[string]any{"by": by, "evidence": evidence, "via": "gatewayctl"}}
	switch outcome {
	case store.Completed:
		body := mcp.TextResult(fmt.Sprintf(
			"Completed: the response from %s was lost, and an operator confirmed that it took effect. Evidence: %s",
			op.Tool, evidence), false, map[string]any{
			"gatewayStatus": StatusResolvedByOperator, "resolvedBy": by, "evidence": evidence,
		})
		sum := sha256.Sum256(body)
		res.State, res.Kind, res.Body, res.Hash = store.Completed, store.KindResult, body, sum[:]
	case store.NotExecuted:
		res.State = store.NotExecuted
	default:
		return op.State, fmt.Errorf("outcome must be %s or %s", store.Completed, store.NotExecuted)
	}
	_, ok, err := st.Resolve(ctx, key, by, res)
	if err != nil {
		return op.State, err
	}
	if !ok {
		return op.State, fmt.Errorf("%s is %s: %w", key, op.State, ErrNotResolvable)
	}
	return op.State, nil
}
