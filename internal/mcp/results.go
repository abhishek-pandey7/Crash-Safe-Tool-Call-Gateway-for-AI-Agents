package mcp

import "encoding/json"

// Result types defined by the 2026-07-28 revision and the Tasks extension.
const (
	ResultComplete      = "complete"
	ResultInputRequired = "input_required"
	ResultTask          = "task"
)

// ResultType returns a result's resultType. Results that omit it are treated
// as "complete", as the spec requires for earlier servers.
func ResultType(result json.RawMessage) string {
	var r struct {
		ResultType string `json:"resultType"`
	}
	if json.Unmarshal(result, &r) != nil || r.ResultType == "" {
		return ResultComplete
	}
	return r.ResultType
}

// Content is one text content block of a tool result.
type Content struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// ToolResult is a complete tools/call result.
type ToolResult struct {
	ResultType        string    `json:"resultType"`
	IsError           bool      `json:"isError,omitempty"`
	Content           []Content `json:"content"`
	StructuredContent any       `json:"structuredContent,omitempty"`
}

// TextResult builds a complete tool result with one text block.
func TextResult(text string, isError bool, structured any) json.RawMessage {
	raw, err := json.Marshal(ToolResult{
		ResultType:        ResultComplete,
		IsError:           isError,
		Content:           []Content{{Type: "text", Text: text}},
		StructuredContent: structured,
	})
	if err != nil {
		panic(err) // only fails for unmarshalable structured content
	}
	return raw
}
