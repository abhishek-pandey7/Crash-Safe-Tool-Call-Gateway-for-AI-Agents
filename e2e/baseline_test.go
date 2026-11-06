package e2e

import (
	"testing"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/faketool"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcpclient"
)

// TestLostResponseDuplicatesWithoutGateway reproduces the problem on the
// 2026-07-28 Streamable HTTP transport: the refund succeeds, the response
// is lost, the client retries with a new request id, and the money moves
// twice. The server has no protocol-level way to recognise the retry.
func TestLostResponseDuplicatesWithoutGateway(t *testing.T) {
	fake, url := startFake(t)
	fake.AddFault(faketool.Fault{Tool: "create_refund", DropResponse: true})

	c := mcpclient.New(url)
	args := map[string]any{"reference": "order-1001", "amount": 20}
	r, attempts := retryOnTransportError(t, c, "create_refund", args, mcpclient.CallOptions{}, 3)

	res, err := r.ToolResult()
	if err != nil || res.IsError {
		t.Fatalf("retry should look successful to the client: %v %+v", err, res)
	}
	if attempts != 2 {
		t.Fatalf("expected one retry, took %d attempts", attempts)
	}
	if got := fake.Ledger.CountReference("order-1001"); got != 2 {
		t.Fatalf("expected the duplicate this project exists to prevent (2 effects), got %d", got)
	}
}
