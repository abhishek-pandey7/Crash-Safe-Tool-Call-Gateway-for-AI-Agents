// Package e2e runs the gateway, the fake tool server and a test client
// together and checks every outcome against the fake server's ledger, the
// ground truth the gateway cannot see.
package e2e

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/faketool"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcpclient"
)

var bg = context.Background()

// startFake serves a fresh fake tool server and returns its MCP endpoint.
func startFake(t *testing.T) (*faketool.Server, string) {
	t.Helper()
	fake := faketool.New()
	srv := httptest.NewServer(fake.Handler())
	t.Cleanup(srv.Close)
	return fake, srv.URL + "/mcp"
}

// retryOnTransportError behaves like an agent that sees a timeout or a
// dropped connection and tries the same call again, with a new request id.
func retryOnTransportError(t *testing.T, c *mcpclient.Client, tool string, args any, o mcpclient.CallOptions, attempts int) (*mcpclient.Response, int) {
	t.Helper()
	for i := 1; i <= attempts; i++ {
		ctx, cancel := context.WithTimeout(bg, 10*time.Second)
		r, err := c.Call(ctx, tool, args, o)
		cancel()
		if err == nil {
			return r, i
		}
		if !errors.Is(err, mcpclient.ErrTransport) {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	t.Fatalf("no response after %d attempts", attempts)
	return nil, attempts
}
