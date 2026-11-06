package faketool

import (
	"encoding/json"
	"fmt"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcp"
)

type tool struct {
	description string
	schema      map[string]any
	readOnly    bool
	run         func(s *Server, c *call) json.RawMessage
}

func object(props map[string]any, required ...string) map[string]any {
	return map[string]any{"type": "object", "properties": props, "required": required}
}

var (
	str = map[string]any{"type": "string"}
	num = map[string]any{"type": "number"}
)

// tools are the fake server's tools. Effectful tools write to the ledger
// before returning; read-only tools never do.
var tools = map[string]tool{
	"create_refund": {
		description: "Refund money to a customer. Effectful, no idempotency key.",
		schema:      object(map[string]any{"reference": str, "amount": num}, "reference", "amount"),
		run: func(s *Server, c *call) json.RawMessage {
			amount, _ := c.args["amount"].(float64)
			if amount <= 0 {
				// A tool execution error that took no effect.
				return mcp.TextResult("Invalid amount", true, map[string]any{"error": "invalid_amount"})
			}
			e := s.Ledger.Record(Effect{Tool: "create_refund", Reference: c.str("reference"), Args: c.params.Arguments})
			id := fmt.Sprintf("rf_%d", e.Seq)
			return mcp.TextResult(fmt.Sprintf("Refund %s created for %s", id, c.str("reference")), false,
				map[string]any{"refundId": id, "reference": c.str("reference"), "amount": amount})
		},
	},
	"find_refund_by_reference": {
		description: "Look up a refund by its business reference. Read-only.",
		schema:      object(map[string]any{"reference": str}, "reference"),
		readOnly:    true,
		run: func(s *Server, c *call) json.RawMessage {
			ref := c.str("reference")
			for _, e := range s.Ledger.Effects() {
				if e.Tool == "create_refund" && e.Reference == ref {
					id := fmt.Sprintf("rf_%d", e.Seq)
					return mcp.TextResult("Found refund "+id, false,
						map[string]any{"found": true, "refundId": id, "reference": ref})
				}
			}
			return mcp.TextResult("No refund for "+ref, false, map[string]any{"found": false, "reference": ref})
		},
	},
	"send_email": {
		description: "Send an email. Effectful, no way to check or deduplicate.",
		schema:      object(map[string]any{"to": str, "subject": str}, "to", "subject"),
		run: func(s *Server, c *call) json.RawMessage {
			e := s.Ledger.Record(Effect{Tool: "send_email", Reference: c.str("to"), Args: c.params.Arguments})
			return mcp.TextResult(fmt.Sprintf("Email %d sent to %s", e.Seq, c.str("to")), false,
				map[string]any{"messageId": e.Seq})
		},
	},
	"create_charge": {
		description: "Charge a card. Honours idempotency_key like a payment API.",
		schema:      object(map[string]any{"amount": num, "idempotency_key": str}, "amount"),
		run: func(s *Server, c *call) json.RawMessage {
			key := c.str("idempotency_key")
			s.mu.Lock()
			defer s.mu.Unlock()
			if prev, ok := s.charges[key]; ok && key != "" {
				return prev
			}
			e := s.Ledger.Record(Effect{Tool: "create_charge", Reference: key, Key: key, Args: c.params.Arguments})
			result := mcp.TextResult(fmt.Sprintf("Charge ch_%d created", e.Seq), false,
				map[string]any{"chargeId": fmt.Sprintf("ch_%d", e.Seq)})
			if key != "" {
				s.charges[key] = result
			}
			return result
		},
	},
	"get_invoice": {
		description: "Read an invoice. Read-only.",
		schema:      object(map[string]any{"id": str}, "id"),
		readOnly:    true,
		run: func(s *Server, c *call) json.RawMessage {
			return mcp.TextResult("Invoice "+c.str("id"), false, map[string]any{"id": c.str("id"), "total": 42})
		},
	},
	"confirm_transfer": {
		description: "Move money after the user confirms. Uses a multi round-trip request.",
		schema:      object(map[string]any{"to": str, "amount": num}, "to", "amount"),
		run: func(s *Server, c *call) json.RawMessage {
			if len(c.params.InputResponses) == 0 {
				return mustJSON(map[string]any{
					"resultType": mcp.ResultInputRequired,
					"inputRequests": map[string]any{
						"confirm": map[string]any{
							"method": "elicitation/create",
							"params": map[string]any{
								"mode":            "form",
								"message":         fmt.Sprintf("Send %v to %s?", c.args["amount"], c.str("to")),
								"requestedSchema": object(map[string]any{}),
							},
						},
					},
					"requestState": requestStateFor(c),
				})
			}
			var state string
			_ = json.Unmarshal(c.params.RequestState, &state)
			var responses map[string]struct {
				Action string `json:"action"`
			}
			_ = json.Unmarshal(c.params.InputResponses, &responses)
			if state != requestStateFor(c) {
				return mcp.TextResult("Request state does not match this call", true, nil)
			}
			if responses["confirm"].Action != "accept" {
				return mcp.TextResult("Transfer declined", false, map[string]any{"transferred": false})
			}
			e := s.Ledger.Record(Effect{Tool: "confirm_transfer", Reference: c.str("to"), Args: c.params.Arguments})
			return mcp.TextResult(fmt.Sprintf("Transfer %d sent", e.Seq), false,
				map[string]any{"transferred": true, "transferId": e.Seq})
		},
	},
	"start_export": {
		description: "Start a long export. Returns a task handle when the client supports Tasks.",
		schema:      object(map[string]any{"dataset": str}, "dataset"),
		run: func(s *Server, c *call) json.RawMessage {
			e := s.Ledger.Record(Effect{Tool: "start_export", Reference: c.str("dataset"), Args: c.params.Arguments})
			final := mcp.TextResult(fmt.Sprintf("Export %d finished", e.Seq), false, map[string]any{"exportId": e.Seq})
			if !c.clientHasTasks() {
				return final
			}
			s.mu.Lock()
			s.nextTask++
			id := fmt.Sprintf("task_%d", s.nextTask)
			s.tasks[id] = final
			s.mu.Unlock()
			return mustJSON(map[string]any{
				"resultType":     mcp.ResultTask,
				"taskId":         id,
				"status":         "working",
				"ttlMs":          60000,
				"pollIntervalMs": 50,
			})
		},
	},
}

func toolList() []map[string]any {
	names := []string{"confirm_transfer", "create_charge", "create_refund", "find_refund_by_reference",
		"get_invoice", "send_email", "start_export"}
	list := make([]map[string]any, 0, len(names))
	for _, name := range names {
		t := tools[name]
		entry := map[string]any{"name": name, "description": t.description, "inputSchema": t.schema}
		if t.readOnly {
			entry["annotations"] = map[string]any{"readOnlyHint": true}
		}
		list = append(list, entry)
	}
	return list
}
