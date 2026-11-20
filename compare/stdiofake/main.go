// Command stdiofake is the fake refund tool for the comparison, as an MCP
// server over stdio, for the proxies that wrap a server process.
//
// Every refund is appended to the ledger file (-ledger) and synced before
// anything else happens, so the ledger is the ground truth no proxy can
// see. A fault for one reference is a file <faults>/<reference>.json, taken
// by the first call for that reference:
//
//	{"crashAfterEffect": true}  exit after the refund, without answering
//	{"delayBeforeMs": 3000}     wait before the refund
//	{"delayAfterMs": 2000}      wait after the refund, before answering
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type fault struct {
	CrashAfterEffect bool `json:"crashAfterEffect"`
	DelayBeforeMs    int  `json:"delayBeforeMs"`
	DelayAfterMs     int  `json:"delayAfterMs"`
}

// The paths come as flags, not environment variables: some proxies start
// their server with a minimal environment.
var (
	ledgerPath = flag.String("ledger", os.Getenv("LEDGER"), "ledger file")
	faultsDir  = flag.String("faults", os.Getenv("FAULTS"), "directory of fault files")
)

var (
	outMu  sync.Mutex
	out    = bufio.NewWriter(os.Stdout)
	ledger sync.Mutex
)

func send(v any) {
	raw, _ := json.Marshal(v)
	outMu.Lock()
	defer outMu.Unlock()
	out.Write(raw)
	out.WriteByte('\n')
	out.Flush()
}

func reply(id json.RawMessage, result any) {
	send(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func fail(id json.RawMessage, code int, msg string) {
	send(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": msg}})
}

func main() {
	flag.Parse()
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 16<<20)
	var wg sync.WaitGroup
	for in.Scan() {
		var m message
		if json.Unmarshal(in.Bytes(), &m) != nil || len(m.ID) == 0 {
			continue // notifications and responses need no answer
		}
		wg.Add(1)
		go func() { defer wg.Done(); handle(m) }() // calls run concurrently, as in the SDKs
	}
	wg.Wait()
}

func handle(m message) {
	switch m.Method {
	case "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(m.Params, &p)
		version := p.ProtocolVersion
		switch version {
		case "2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05":
		default:
			version = "2025-11-25"
		}
		reply(m.ID, map[string]any{
			"protocolVersion": version,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "stdio-fake-refunds", "version": "0.1.0"},
		})
	case "ping":
		reply(m.ID, map[string]any{})
	case "tools/list":
		reply(m.ID, map[string]any{"tools": []any{
			map[string]any{
				"name": "create_refund", "description": "Refund an order. Has a side effect; no idempotency key.",
				"inputSchema": map[string]any{"type": "object", "required": []string{"reference", "amount"},
					"properties": map[string]any{"reference": map[string]any{"type": "string"}, "amount": map[string]any{"type": "number"}}},
				"annotations": map[string]any{"readOnlyHint": false, "destructiveHint": false, "idempotentHint": false},
			},
			map[string]any{
				"name": "find_refund_by_reference", "description": "Look up a refund. Read-only.",
				"inputSchema": map[string]any{"type": "object", "required": []string{"reference"},
					"properties": map[string]any{"reference": map[string]any{"type": "string"}}},
				"annotations": map[string]any{"readOnlyHint": true},
			},
		}})
	case "tools/call":
		call(m)
	default:
		fail(m.ID, -32601, "Method not found: "+m.Method)
	}
}

func call(m message) {
	var p struct {
		Name      string         `json:"name"`
		Arguments map[string]any `json:"arguments"`
	}
	if err := json.Unmarshal(m.Params, &p); err != nil {
		fail(m.ID, -32602, err.Error())
		return
	}
	ref, _ := p.Arguments["reference"].(string)
	switch p.Name {
	case "find_refund_by_reference":
		n := count(ref)
		reply(m.ID, textResult(fmt.Sprintf("%d refunds for %s", n, ref), map[string]any{"found": n > 0, "reference": ref}))
		return
	case "create_refund":
	default:
		fail(m.ID, -32602, "Unknown tool: "+p.Name)
		return
	}

	f := takeFault(ref)
	time.Sleep(time.Duration(f.DelayBeforeMs) * time.Millisecond)
	n := record(ref, p.Arguments)
	if f.CrashAfterEffect {
		os.Exit(3) // the refund happened; the answer never leaves
	}
	time.Sleep(time.Duration(f.DelayAfterMs) * time.Millisecond)
	reply(m.ID, textResult(fmt.Sprintf("Refund rf_%d created for %s", n, ref),
		map[string]any{"refundId": fmt.Sprintf("rf_%d", n), "reference": ref}))
}

func textResult(text string, structured map[string]any) map[string]any {
	return map[string]any{
		"content":           []any{map[string]any{"type": "text", "text": text}},
		"structuredContent": structured,
		"isError":           false,
	}
}

func takeFault(ref string) fault {
	var f fault
	dir := *faultsDir
	if dir == "" || ref == "" {
		return f
	}
	path := filepath.Join(dir, ref+".json")
	taken := path + ".taken"
	if os.Rename(path, taken) != nil {
		return f // no fault, or another call took it
	}
	raw, err := os.ReadFile(taken)
	if err == nil {
		_ = json.Unmarshal(raw, &f)
	}
	return f
}

// record appends one refund to the ledger and syncs it, returning how many
// refunds the ledger holds in total.
func record(ref string, args map[string]any) int {
	ledger.Lock()
	defer ledger.Unlock()
	path := *ledgerPath
	n := lines(path) + 1
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "stdiofake: ledger:", err)
		os.Exit(1)
	}
	line, _ := json.Marshal(map[string]any{"seq": n, "reference": ref, "args": args, "at": time.Now().UTC()})
	f.Write(append(line, '\n'))
	f.Sync()
	f.Close()
	return n
}

func lines(path string) int {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	return strings.Count(string(raw), "\n")
}

// count is the number of refunds recorded for ref.
func count(ref string) int {
	raw, err := os.ReadFile(*ledgerPath)
	if err != nil {
		return 0
	}
	n := 0
	for _, l := range strings.Split(string(raw), "\n") {
		var e struct {
			Reference string `json:"reference"`
		}
		if json.Unmarshal([]byte(l), &e) == nil && e.Reference == ref && ref != "" {
			n++
		}
	}
	return n
}
