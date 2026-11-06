package mcp

import (
	"encoding/json"
	"fmt"
)

// CallParams is the subset of tools/call params the gateway reads.
//
// InputResponses and RequestState belong to a multi round-trip retry. They
// are kept apart from Arguments so they never change an operation's identity.
type CallParams struct {
	Name           string                     `json:"name"`
	Arguments      json.RawMessage            `json:"arguments,omitempty"`
	Meta           map[string]json.RawMessage `json:"_meta,omitempty"`
	InputResponses json.RawMessage            `json:"inputResponses,omitempty"`
	RequestState   json.RawMessage            `json:"requestState,omitempty"`
}

// ParseCallParams decodes tools/call params.
func ParseCallParams(raw json.RawMessage) (*CallParams, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("mcp: tools/call without params")
	}
	var p CallParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("mcp: decode tools/call params: %w", err)
	}
	if p.Name == "" {
		return nil, fmt.Errorf("mcp: tools/call without a tool name")
	}
	return &p, nil
}

// IsContinuation reports whether the call is the retry half of a multi
// round-trip request.
func (p *CallParams) IsContinuation() bool {
	return len(p.InputResponses) > 0 || len(p.RequestState) > 0
}

// MetaString returns a string-valued _meta entry, or "" when it is absent or
// not a string.
func (p *CallParams) MetaString(key string) string {
	return metaString(p.Meta, key)
}

// RequestMeta reads the _meta object of any request's params without
// decoding the rest.
func RequestMeta(params json.RawMessage) map[string]json.RawMessage {
	if len(params) == 0 {
		return nil
	}
	var p struct {
		Meta map[string]json.RawMessage `json:"_meta"`
	}
	if json.Unmarshal(params, &p) != nil {
		return nil
	}
	return p.Meta
}

// NameParam returns params.name or params.uri, the body field mirrored by
// the Mcp-Name header.
func NameParam(params json.RawMessage) (string, bool) {
	if len(params) == 0 {
		return "", false
	}
	var p struct {
		Name *string `json:"name"`
		URI  *string `json:"uri"`
	}
	if json.Unmarshal(params, &p) != nil {
		return "", false
	}
	if p.Name != nil {
		return *p.Name, true
	}
	if p.URI != nil {
		return *p.URI, true
	}
	return "", false
}

func metaString(meta map[string]json.RawMessage, key string) string {
	raw, ok := meta[key]
	if !ok {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return ""
	}
	return s
}

// ReplaceArguments returns a copy of a tools/call body whose
// params.arguments is replaced. Every other member is carried over as raw
// JSON, so fields such as requestState are never re-encoded.
func ReplaceArguments(body []byte, args json.RawMessage) ([]byte, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return nil, fmt.Errorf("mcp: decode body: %w", err)
	}
	var params map[string]json.RawMessage
	if err := json.Unmarshal(top["params"], &params); err != nil {
		return nil, fmt.Errorf("mcp: decode params: %w", err)
	}
	params["arguments"] = args
	rawParams, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	top["params"] = rawParams
	return json.Marshal(top)
}
