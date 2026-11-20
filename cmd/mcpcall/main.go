// Command mcpcall makes one MCP tools/call and prints what came back. It
// can retry after a dropped connection or timeout, the way an agent does,
// which is how the demo reproduces a duplicate.
//
//	mcpcall -url http://localhost:8080/mcp -tool create_refund \
//	        -args '{"reference":"order-1","amount":20}' -key refund-order-1
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcpclient"
)

func main() {
	var (
		url     = flag.String("url", "http://localhost:8080/mcp", "MCP endpoint")
		tool    = flag.String("tool", "", "tool name")
		args    = flag.String("args", "{}", "tool arguments as JSON")
		key     = flag.String("key", "", "operation key, sent in _meta")
		token   = flag.String("token", "", "bearer token")
		timeout = flag.Duration("timeout", 10*time.Second, "time limit for one attempt")
		retries = flag.Int("retries", 0, "retries after a dropped connection or timeout")
	)
	flag.Parse()
	if *tool == "" {
		fmt.Fprintln(os.Stderr, "mcpcall: -tool is required")
		os.Exit(2)
	}
	var arguments any
	if err := json.Unmarshal([]byte(*args), &arguments); err != nil {
		fmt.Fprintf(os.Stderr, "mcpcall: -args is not JSON: %v\n", err)
		os.Exit(2)
	}

	c := mcpclient.New(*url)
	c.Token = *token
	for attempt := 1; attempt <= *retries+1; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		resp, err := c.Call(ctx, *tool, arguments, mcpclient.CallOptions{OperationKey: *key})
		cancel()
		if err != nil {
			if errors.Is(err, mcpclient.ErrTransport) || errors.Is(err, context.DeadlineExceeded) {
				fmt.Printf("attempt %d: no response (%v)\n", attempt, err)
				if attempt <= *retries {
					fmt.Println("  retrying, as an agent would")
				}
				continue
			}
			fmt.Fprintf(os.Stderr, "mcpcall: %v\n", err)
			os.Exit(1)
		}
		report(attempt, resp)
		return
	}
	os.Exit(1)
}

func report(attempt int, r *mcpclient.Response) {
	res, resErr := r.ToolResult()
	status := r.Header.Get("X-Gateway-Status")
	if status == "" && resErr == nil {
		status = res.GatewayStatus() // set on results the gateway recorded, such as reconciled ones
	}
	fmt.Printf("attempt %d: HTTP %d", attempt, r.Status)
	if status != "" {
		fmt.Printf(", gateway status %s", status)
	}
	if r.Header.Get("X-Gateway-Replayed") == "true" {
		fmt.Print(", answered from the record")
	}
	fmt.Println()
	if e := r.RPCError(); e != nil {
		fmt.Printf("  error %d: %s\n", e.Code, e.Message)
		return
	}
	if resErr != nil {
		fmt.Printf("  %v\n", resErr)
		return
	}
	if res.IsError {
		fmt.Print("  isError: ")
	} else {
		fmt.Print("  ")
	}
	fmt.Println(res.Text())
}
