package e2e

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/faketool"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/gateway"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcpclient"
)

// crashSlots caps how many crash scenarios run at once. Each one runs two
// gateway processes.
var crashSlots = make(chan struct{}, 4)

// answer is what the client is finally told.
type answer string

const (
	answerResult     answer = "result"     // the tool's own result
	answerUnknown    answer = "unknown"    // gateway: outcome unknown
	answerReconciled answer = "reconciled" // gateway: confirmed by a read
)

func answerOf(res *mcpclient.ToolResult) answer {
	switch res.GatewayStatus() {
	case "":
		return answerResult
	case gateway.StatusOutcomeUnknown:
		return answerUnknown
	case gateway.StatusReconciled:
		return answerReconciled
	}
	return answer(res.GatewayStatus())
}

// retryUntilSettled keeps retrying, as an agent would, until the answer is
// no longer "in progress" and no longer a transport error.
func retryUntilSettled(t *testing.T, c *mcpclient.Client, tool string, args any, o mcpclient.CallOptions) *mcpclient.ToolResult {
	t.Helper()
	return retryUntil(t, c, tool, args, o, false)
}

// retryUntil is retryUntilSettled that can also keep retrying through
// "outcome unknown", as the reconcilable policy invites.
func retryUntil(t *testing.T, c *mcpclient.Client, tool string, args any, o mcpclient.CallOptions, throughUnknown bool) *mcpclient.ToolResult {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(bg, 10*time.Second)
		r, err := c.Call(ctx, tool, args, o)
		cancel()
		if err != nil {
			if !errors.Is(err, mcpclient.ErrTransport) {
				t.Fatal(err)
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		res, err := r.ToolResult()
		if err != nil {
			t.Fatalf("retry got a protocol error: %v", err)
		}
		if res.GatewayStatus() == gateway.StatusInProgress ||
			(throughUnknown && res.GatewayStatus() == gateway.StatusOutcomeUnknown) {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		return res
	}
	t.Fatal("no settled answer within 20s")
	return nil
}

type crashCase struct {
	point   gateway.Point
	effects []int  // allowed numbers of real effects
	want    answer // what the retry through the surviving instance reports
}

// crashScenario kills gateway A at one point of a call, retries through
// gateway B, and returns what the client was finally told.
func crashScenario(t *testing.T, fakeURL, refundPolicy, tool string, args any, point gateway.Point, throughUnknown bool) *mcpclient.ToolResult {
	t.Helper()
	crashSlots <- struct{}{}
	defer func() { <-crashSlots }()

	cfg := writeConfig(t, fakeURL, refundPolicy, "")
	db := processDB(t)
	a := startGateway(t, cfg, db, procOptions{instance: "A", killAt: string(point)})
	b := startGateway(t, cfg, db, procOptions{instance: "B"})

	ctx, cancel := context.WithTimeout(bg, 10*time.Second)
	_, err := mcpclient.New(a.url).Call(ctx, tool, args, keyed("crash-key"))
	cancel()
	if !a.waitExit(10 * time.Second) {
		t.Fatalf("gateway A was not killed at %s", point)
	}
	if !strings.Contains(a.logs.String(), "killpoint: killing at "+string(point)) {
		t.Fatalf("gateway A exited, but not at kill point %s:\n%s", point, a.logs.String())
	}
	if err == nil {
		t.Fatalf("the client got an answer from a gateway killed at %s", point)
	}
	return retryUntil(t, mcpclient.New(b.url), tool, args, keyed("crash-key"), throughUnknown)
}

// TestCrashMatrixAtMostOnce is the crash table from the design for a tool
// that cannot be checked or deduplicated downstream.
func TestCrashMatrixAtMostOnce(t *testing.T) {
	cases := []crashCase{
		{gateway.PointAfterClaim, []int{1}, answerResult},
		{gateway.PointAfterGate, []int{0}, answerUnknown},
		{gateway.PointWhileSending, []int{0, 1}, answerUnknown},
		{gateway.PointAfterSend, []int{0, 1}, answerUnknown},
		{gateway.PointResponseStarted, []int{1}, answerUnknown},
		{gateway.PointResponseRead, []int{1}, answerUnknown},
		{gateway.PointAfterCommit, []int{1}, answerResult},
	}
	for _, tc := range cases {
		t.Run(string(tc.point), func(t *testing.T) {
			t.Parallel()
			fake, fakeURL := startFake(t)
			ref := "order-" + string(tc.point)
			args := map[string]any{"reference": ref, "amount": 20}

			res := crashScenario(t, fakeURL, refundAtMostOnce, "create_refund", args, tc.point, false)

			effects := fake.Ledger.CountReference(ref)
			if !slices.Contains(tc.effects, effects) {
				t.Errorf("I1: %d effects, allowed %v", effects, tc.effects)
			}
			if got := answerOf(res); got != tc.want {
				t.Errorf("client told %q, want %q: %s", got, tc.want, res.Raw)
			}
			if answerOf(res) == answerResult && effects != 1 {
				t.Errorf("I3: client told it ran, but %d effects", effects)
			}
			if n := fake.RequestsForReference(ref); n > 1 {
				t.Errorf("I8: the call was dispatched %d times", n)
			}
		})
	}
}

// TestCrashMatrixReconcilable runs the same kills for a tool whose outcome
// a read can confirm. A client that keeps retrying should always end with
// exactly one effect: either the read finds it, or, once settle_after has
// passed, the read proves it never happened and the retry runs it once.
func TestCrashMatrixReconcilable(t *testing.T) {
	points := []gateway.Point{
		gateway.PointAfterClaim, gateway.PointAfterGate, gateway.PointWhileSending,
		gateway.PointAfterSend, gateway.PointResponseStarted, gateway.PointResponseRead,
		gateway.PointAfterCommit,
	}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			t.Parallel()
			fake, fakeURL := startFake(t)
			ref := "order-" + string(point)
			args := map[string]any{"reference": ref, "amount": 20}
			res := crashScenario(t, fakeURL, refundReconcilable, "create_refund", args, point, true)

			if n := fake.Ledger.CountReference(ref); n != 1 {
				t.Errorf("%d effects, want exactly 1", n)
			}
			if got := answerOf(res); (got != answerResult && got != answerReconciled) || res.IsError {
				t.Errorf("client told %q: %s", got, res.Raw)
			}
		})
	}
}

// TestCrashMatrixNativeKey runs the same kills for a tool that honours an
// idempotency key. Every row should end with exactly one effect and a real
// result, because the gateway may resend with the same key.
func TestCrashMatrixNativeKey(t *testing.T) {
	points := []gateway.Point{
		gateway.PointAfterClaim, gateway.PointAfterGate, gateway.PointWhileSending,
		gateway.PointAfterSend, gateway.PointResponseStarted, gateway.PointResponseRead,
		gateway.PointAfterCommit,
	}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			t.Parallel()
			fake, fakeURL := startFake(t)
			res := crashScenario(t, fakeURL, "", "create_charge", map[string]any{"amount": 5}, point, false)

			effects := fake.Ledger.Count(func(e faketool.Effect) bool { return e.Tool == "create_charge" })
			if effects != 1 {
				t.Errorf("I2: %d effects, want exactly 1", effects)
			}
			if got := answerOf(res); got != answerResult || res.IsError {
				t.Errorf("client told %q, want the charge result: %s", got, res.Raw)
			}
		})
	}
}
