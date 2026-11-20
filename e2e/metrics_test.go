package e2e

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/faketool"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcpclient"
)

func metricsText(t *testing.T, e *env) string {
	t.Helper()
	var b strings.Builder
	if err := e.gw.Metrics().WriteText(&b); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func wantLines(t *testing.T, text string, lines ...string) {
	t.Helper()
	for _, l := range lines {
		if !strings.Contains(text, "\n"+l+"\n") {
			t.Errorf("metrics lack %q", l)
		}
	}
	if t.Failed() {
		t.Logf("metrics:\n%s", text)
	}
}

// Each kind of answer is counted once, under bounded labels.
func TestMetricsCountOutcomes(t *testing.T) {
	e := newEnv(t, envOptions{noRun: true})
	e.fake.AddFault(faketool.Fault{Tool: "send_email", DropResponse: true})

	call(t, e.client, "create_refund", refund, keyed("k1"))                                            // completed
	call(t, e.client, "create_refund", refund, keyed("k1"))                                            // replayed
	call(t, e.client, "create_refund", map[string]any{"reference": "other", "amount": 1}, keyed("k1")) // key conflict
	call(t, e.client, "send_email", map[string]any{"to": "a@example.com", "subject": "hi"}, keyed("m1"))
	call(t, e.client, "get_invoice", map[string]any{"id": "inv-1"}, mcpclient.CallOptions{})
	call(t, e.client, "made_up_tool", map[string]any{}, keyed("u1"))

	text := metricsText(t, e)
	wantLines(t, text,
		`gateway_tool_calls_total{tool="create_refund",policy="at_most_once",outcome="completed"} 1`,
		`gateway_tool_calls_total{tool="create_refund",policy="at_most_once",outcome="replayed"} 1`,
		`gateway_tool_calls_total{tool="create_refund",policy="at_most_once",outcome="key_conflict"} 1`,
		`gateway_tool_calls_total{tool="send_email",policy="at_most_once",outcome="outcome_unknown"} 1`,
		`gateway_tool_calls_total{tool="get_invoice",policy="read_only",outcome="read_only"} 1`,
		`gateway_tool_calls_total{tool="_unlisted",policy="at_most_once",outcome="completed"} 1`,
		`gateway_dispatches_total{policy="at_most_once",result="maybe_sent"} 1`,
		`gateway_dispatches_total{policy="at_most_once",result="response"} 2`,
		`gateway_operations{state="unknown"} 1`,
		`gateway_operations{state="dispatching"} 0`,
		`gateway_store_up 1`,
		`gateway_pending_writes 0`,
	)
	if strings.Contains(text, "made_up_tool") {
		t.Error("a caller-chosen tool name became a label")
	}
	if !strings.Contains(text, `gateway_dispatch_duration_seconds_count{policy="at_most_once"} 3`) {
		t.Errorf("dispatch histogram missing:\n%s", text)
	}
}

// fetchMetrics scrapes a gateway process.
func fetchMetrics(t *testing.T, p *proc) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, p.base+"/-/metrics", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status %d", resp.StatusCode)
	}
	return string(body)
}
