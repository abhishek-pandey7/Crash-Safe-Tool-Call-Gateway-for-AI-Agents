// Package mcp holds the parts of the Model Context Protocol (2026-07-28
// revision) that the gateway needs at the HTTP and JSON-RPC level: message
// parsing, the request metadata headers, SSE framing and result shapes.
//
// The gateway is a reverse proxy, so this package deliberately models only
// the fields it reads. Everything else is carried as raw JSON and forwarded
// byte for byte.
package mcp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// ProtocolVersion is the only MCP revision the gateway understands.
const ProtocolVersion = "2026-07-28"

// JSON-RPC and MCP error codes.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603

	// Reserved for the MCP specification (-32020 to -32099).
	CodeHeaderMismatch                  = -32020
	CodeMissingRequiredClientCapability = -32021
	CodeUnsupportedProtocolVersion      = -32022

	// Implementation-defined range (-32000 to -32019), used by the gateway.
	CodeKeyRequired  = -32010
	CodeKeyConflict  = -32011
	CodeUnauthorized = -32012
)

// Standard _meta keys from the 2026-07-28 revision.
const (
	MetaProtocolVersion    = "io.modelcontextprotocol/protocolVersion"
	MetaClientInfo         = "io.modelcontextprotocol/clientInfo"
	MetaClientCapabilities = "io.modelcontextprotocol/clientCapabilities"
)

// Method names the gateway treats specially.
const (
	MethodToolsCall     = "tools/call"
	MethodResourcesRead = "resources/read"
	MethodPromptsGet    = "prompts/get"
)

// Message is a single JSON-RPC 2.0 message. Fields the gateway does not
// interpret stay as raw JSON.
type Message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

// ErrNotJSONRPC is returned when a body is valid JSON but not a JSON-RPC 2.0
// message.
var ErrNotJSONRPC = errors.New("mcp: not a json-rpc 2.0 message")

// ParseMessage decodes one JSON-RPC message. Batches are not part of the
// 2026-07-28 revision and are rejected.
func ParseMessage(body []byte) (*Message, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, ErrNotJSONRPC
	}
	var m Message
	if err := json.Unmarshal(trimmed, &m); err != nil {
		return nil, fmt.Errorf("mcp: decode message: %w", err)
	}
	if m.JSONRPC != "2.0" {
		return nil, ErrNotJSONRPC
	}
	return &m, nil
}

// HasID reports whether the message carries a non-null id.
func (m *Message) HasID() bool {
	return len(m.ID) > 0 && !bytes.Equal(bytes.TrimSpace(m.ID), []byte("null"))
}

// IsRequest reports whether the message is a request (method plus id).
func (m *Message) IsRequest() bool { return m.Method != "" && m.HasID() }

// IsNotification reports whether the message is a notification.
func (m *Message) IsNotification() bool { return m.Method != "" && !m.HasID() }

// IsResponse reports whether the message is a response to a request.
func (m *Message) IsResponse() bool {
	return m.Method == "" && m.HasID() && (len(m.Result) > 0 || len(m.Error) > 0)
}

// RPCError is the error member of a JSON-RPC response.
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string { return fmt.Sprintf("json-rpc error %d: %s", e.Code, e.Message) }

// NewError builds an RPCError with optional data, which is marshalled to JSON.
func NewError(code int, message string, data any) *RPCError {
	e := &RPCError{Code: code, Message: message}
	if data != nil {
		if raw, err := json.Marshal(data); err == nil {
			e.Data = raw
		}
	}
	return e
}

// ResultResponse encodes a successful response for id. A missing id is
// encoded as null.
func ResultResponse(id json.RawMessage, result json.RawMessage) []byte {
	return encodeResponse(id, "result", result)
}

// ErrorResponse encodes an error response for id.
func ErrorResponse(id json.RawMessage, e *RPCError) []byte {
	raw, err := json.Marshal(e)
	if err != nil {
		raw = []byte(`{"code":-32603,"message":"internal error"}`)
	}
	return encodeResponse(id, "error", raw)
}

// RawErrorResponse encodes an error response from an already encoded error
// object, such as a stored one.
func RawErrorResponse(id json.RawMessage, errObj json.RawMessage) []byte {
	return encodeResponse(id, "error", errObj)
}

func encodeResponse(id json.RawMessage, member string, value json.RawMessage) []byte {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	var b bytes.Buffer
	b.WriteString(`{"jsonrpc":"2.0","id":`)
	b.Write(id)
	b.WriteString(`,"`)
	b.WriteString(member)
	b.WriteString(`":`)
	b.Write(value)
	b.WriteByte('}')
	return b.Bytes()
}
