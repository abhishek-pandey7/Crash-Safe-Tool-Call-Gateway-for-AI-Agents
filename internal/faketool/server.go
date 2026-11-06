// Package faketool is a fake MCP tool server for the test suite. It speaks
// the 2026-07-28 Streamable HTTP transport, writes every real effect to its
// own Ledger, and can inject the faults the gateway must survive: a
// response lost after the effect, a slow tool, a tool that ignores
// cancellation, and streamed progress.
package faketool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"sync"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/canonjson"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcp"
)

// TasksExtension is the capability key of the Tasks extension.
const TasksExtension = "io.modelcontextprotocol/tasks"

// Fault changes how the next matching tools/call requests are handled.
type Fault struct {
	// Tool limits the fault to one tool; empty matches any tool.
	Tool string `json:"tool,omitempty"`
	// DropResponse performs the effect, then closes the connection
	// without sending a response: the lost-response case.
	DropResponse bool `json:"dropResponse,omitempty"`
	// DelayBeforeMs waits before the effect. A client disconnect during
	// the wait cancels the call unless IgnoreCancel is set.
	DelayBeforeMs int `json:"delayBeforeMs,omitempty"`
	// DelayAfterMs waits after the effect, before responding.
	DelayAfterMs int `json:"delayAfterMs,omitempty"`
	// IgnoreCancel keeps working after the client disconnects.
	IgnoreCancel bool `json:"ignoreCancel,omitempty"`
	// Progress sends this many progress notifications on an SSE stream
	// before the final response.
	Progress int `json:"progress,omitempty"`
	// Times is how many calls the fault applies to; 0 means 1.
	Times int `json:"times,omitempty"`
}

// Server is the fake tool server. Use Handler to serve it.
type Server struct {
	Ledger Ledger

	mu       sync.Mutex
	faults   []Fault
	requests map[string]int
	charges  map[string]json.RawMessage
	tasks    map[string]json.RawMessage
	nextTask int
}

// New returns a fake server with an empty ledger.
func New() *Server {
	return &Server{
		requests: map[string]int{},
		charges:  map[string]json.RawMessage{},
		tasks:    map[string]json.RawMessage{},
	}
}

// AddFault queues a fault.
func (s *Server) AddFault(f Fault) {
	if f.Times <= 0 {
		f.Times = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults = append(s.faults, f)
}

// Requests returns how many tools/call requests reached the server for a
// tool, whether or not they took effect.
func (s *Server) Requests(tool string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[tool]
}

func (s *Server) takeFault(tool string) Fault {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, f := range s.faults {
		if f.Tool != "" && f.Tool != tool {
			continue
		}
		f.Times--
		if f.Times == 0 {
			s.faults = append(s.faults[:i], s.faults[i+1:]...)
		} else {
			s.faults[i] = f
		}
		return f
	}
	return Fault{}
}

// Handler serves the MCP endpoint at /mcp and test controls at /ledger,
// /faults and /requests.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", s.serveMCP)
	mux.HandleFunc("GET /ledger", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, s.Ledger.Effects())
	})
	mux.HandleFunc("DELETE /ledger", func(w http.ResponseWriter, r *http.Request) {
		s.Ledger.Reset()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /faults", func(w http.ResponseWriter, r *http.Request) {
		var f Fault
		if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.AddFault(f)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /requests", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		counts := maps.Clone(s.requests)
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, counts)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeRPC(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func (s *Server) serveMCP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return
	}
	msg, err := mcp.ParseMessage(body)
	if err != nil {
		writeRPC(w, http.StatusBadRequest, mcp.ErrorResponse(nil, mcp.NewError(mcp.CodeParseError, "parse error", nil)))
		return
	}
	if msg.IsNotification() {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if herr := mcp.ValidateRequestHeaders(r.Header, msg); herr != nil {
		writeRPC(w, http.StatusBadRequest, mcp.ErrorResponse(msg.ID, herr.RPC()))
		return
	}
	switch msg.Method {
	case "server/discover":
		writeRPC(w, http.StatusOK, mcp.ResultResponse(msg.ID, mustJSON(map[string]any{
			"resultType":        mcp.ResultComplete,
			"supportedVersions": []string{mcp.ProtocolVersion},
			"capabilities":      map[string]any{"tools": map[string]any{}, "extensions": map[string]any{TasksExtension: map[string]any{}}},
			"serverInfo":        map[string]any{"name": "faketool", "version": "0.1.0"},
		})))
	case "tools/list":
		writeRPC(w, http.StatusOK, mcp.ResultResponse(msg.ID, mustJSON(map[string]any{
			"resultType": mcp.ResultComplete,
			"tools":      toolList(),
			"ttlMs":      60000,
			"cacheScope": "public",
		})))
	case mcp.MethodToolsCall:
		s.callTool(w, r, msg)
	case "tasks/get":
		s.getTask(w, msg)
	default:
		writeRPC(w, http.StatusNotFound, mcp.ErrorResponse(msg.ID, mcp.NewError(mcp.CodeMethodNotFound, "Method not found", nil)))
	}
}

// call is one tools/call being handled.
type call struct {
	params *mcp.CallParams
	args   map[string]any
}

func (c *call) str(name string) string {
	v, _ := c.args[name].(string)
	return v
}

func (c *call) clientHasTasks() bool {
	var caps struct {
		Extensions map[string]json.RawMessage `json:"extensions"`
	}
	if raw, ok := c.params.Meta[mcp.MetaClientCapabilities]; ok && json.Unmarshal(raw, &caps) == nil {
		_, ok := caps.Extensions[TasksExtension]
		return ok
	}
	return false
}

func (s *Server) callTool(w http.ResponseWriter, r *http.Request, msg *mcp.Message) {
	p, err := mcp.ParseCallParams(msg.Params)
	if err != nil {
		writeRPC(w, http.StatusOK, mcp.ErrorResponse(msg.ID, mcp.NewError(mcp.CodeInvalidParams, err.Error(), nil)))
		return
	}
	c := &call{params: p, args: map[string]any{}}
	if len(p.Arguments) > 0 {
		if err := json.Unmarshal(p.Arguments, &c.args); err != nil {
			writeRPC(w, http.StatusOK, mcp.ErrorResponse(msg.ID, mcp.NewError(mcp.CodeInvalidParams, "arguments must be an object", nil)))
			return
		}
	}

	s.mu.Lock()
	s.requests[p.Name]++
	s.mu.Unlock()

	tool, ok := tools[p.Name]
	if !ok {
		writeRPC(w, http.StatusOK, mcp.ErrorResponse(msg.ID, mcp.NewError(mcp.CodeInvalidParams, "Unknown tool: "+p.Name, nil)))
		return
	}
	fault := s.takeFault(p.Name)

	if fault.DelayBeforeMs > 0 {
		ctx := r.Context()
		if fault.IgnoreCancel {
			ctx = context.Background()
		}
		select {
		case <-time.After(time.Duration(fault.DelayBeforeMs) * time.Millisecond):
		case <-ctx.Done():
			return // cancelled before any effect
		}
	}

	result := tool.run(s, c)

	if fault.DelayAfterMs > 0 {
		time.Sleep(time.Duration(fault.DelayAfterMs) * time.Millisecond)
	}
	if fault.DropResponse {
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
				return
			}
		}
		panic(http.ErrAbortHandler)
	}

	response := mcp.ResultResponse(msg.ID, result)
	if fault.Progress == 0 {
		writeRPC(w, http.StatusOK, response)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	token, hasToken := p.Meta["progressToken"]
	for i := 1; i <= fault.Progress && hasToken; i++ {
		note := fmt.Sprintf(`{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":%s,"progress":%d,"total":%d}}`,
			token, i, fault.Progress)
		_ = mcp.WriteSSE(w, []byte(note))
		if flusher != nil {
			flusher.Flush()
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = mcp.WriteSSE(w, response)
}

func (s *Server) getTask(w http.ResponseWriter, msg *mcp.Message) {
	var p struct {
		TaskID string `json:"taskId"`
	}
	_ = json.Unmarshal(msg.Params, &p)
	s.mu.Lock()
	result, ok := s.tasks[p.TaskID]
	s.mu.Unlock()
	if !ok {
		writeRPC(w, http.StatusOK, mcp.ErrorResponse(msg.ID, mcp.NewError(mcp.CodeInvalidParams, "unknown task", nil)))
		return
	}
	writeRPC(w, http.StatusOK, mcp.ResultResponse(msg.ID, mustJSON(map[string]any{
		"resultType": mcp.ResultComplete,
		"taskId":     p.TaskID,
		"status":     "completed",
		"result":     result,
	})))
}

func mustJSON(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return raw
}

// requestStateFor derives the opaque state the fake server hands out for a
// multi round-trip call, bound to the call's arguments.
func requestStateFor(c *call) string {
	canon, err := canonjson.Canonicalize(c.params.Arguments)
	if err != nil {
		canon = c.params.Arguments
	}
	sum := sha256.Sum256(canon)
	return "confirm:" + hex.EncodeToString(sum[:8])
}
