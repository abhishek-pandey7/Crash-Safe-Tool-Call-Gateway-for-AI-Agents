package mcp

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
)

// Request metadata headers defined by the Streamable HTTP transport.
const (
	HeaderProtocolVersion = "MCP-Protocol-Version"
	HeaderMethod          = "Mcp-Method"
	HeaderName            = "Mcp-Name"
)

const (
	base64Prefix = "=?base64?"
	base64Suffix = "?="
)

// HeaderError describes a request whose metadata headers are missing,
// malformed or do not match the body. It maps to HTTP 400 with JSON-RPC
// error -32020 (HeaderMismatch), or -32022 for an unsupported version.
type HeaderError struct {
	Code    int
	Message string
	Data    any
}

func (e *HeaderError) Error() string { return e.Message }

// RPC converts the error into a JSON-RPC error object.
func (e *HeaderError) RPC() *RPCError { return NewError(e.Code, e.Message, e.Data) }

func mismatch(format string, args ...any) *HeaderError {
	return &HeaderError{Code: CodeHeaderMismatch, Message: fmt.Sprintf(format, args...)}
}

// namedMethods lists the methods whose requests must carry Mcp-Name.
var namedMethods = map[string]bool{
	MethodToolsCall:     true,
	MethodResourcesRead: true,
	MethodPromptsGet:    true,
}

// ValidateRequestHeaders checks a request's metadata headers against its body
// as the 2026-07-28 transport requires of anything that processes the body.
// It returns nil when they agree.
func ValidateRequestHeaders(h http.Header, m *Message) *HeaderError {
	version := h.Get(HeaderProtocolVersion)
	if version == "" {
		return mismatch("Header mismatch: missing %s header", HeaderProtocolVersion)
	}
	if bodyVersion := metaString(RequestMeta(m.Params), MetaProtocolVersion); bodyVersion != version {
		return mismatch("Header mismatch: %s header value %q does not match body value %q",
			HeaderProtocolVersion, version, bodyVersion)
	}
	if version != ProtocolVersion {
		return &HeaderError{
			Code:    CodeUnsupportedProtocolVersion,
			Message: fmt.Sprintf("Unsupported protocol version: %s", version),
			Data:    map[string]any{"supported": []string{ProtocolVersion}, "requested": version},
		}
	}

	method := h.Get(HeaderMethod)
	if method == "" {
		return mismatch("Header mismatch: missing %s header", HeaderMethod)
	}
	if method != m.Method {
		return mismatch("Header mismatch: %s header value %q does not match body value %q",
			HeaderMethod, method, m.Method)
	}

	if !namedMethods[m.Method] {
		return nil
	}
	rawName := h.Get(HeaderName)
	if rawName == "" {
		return mismatch("Header mismatch: missing %s header", HeaderName)
	}
	name, err := DecodeHeaderValue(rawName)
	if err != nil {
		return mismatch("Header mismatch: %s header: %v", HeaderName, err)
	}
	bodyName, ok := NameParam(m.Params)
	if !ok || name != bodyName {
		return mismatch("Header mismatch: %s header value %q does not match body value %q",
			HeaderName, name, bodyName)
	}
	return nil
}

// DecodeHeaderValue undoes the transport's value encoding: a value wrapped
// as =?base64?...?= is decoded from Base64, and any other value must be
// plain visible ASCII, space or tab.
func DecodeHeaderValue(v string) (string, error) {
	if strings.HasPrefix(v, base64Prefix) && strings.HasSuffix(v, base64Suffix) &&
		len(v) >= len(base64Prefix)+len(base64Suffix) {
		inner := v[len(base64Prefix) : len(v)-len(base64Suffix)]
		dec, err := base64.StdEncoding.DecodeString(inner)
		if err != nil {
			return "", fmt.Errorf("invalid base64 value")
		}
		return string(dec), nil
	}
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c != '\t' && (c < 0x20 || c > 0x7e) {
			return "", fmt.Errorf("invalid character in header value")
		}
	}
	return v, nil
}

// EncodeHeaderValue applies the transport's value encoding, the inverse of
// DecodeHeaderValue.
func EncodeHeaderValue(v string) string {
	safe := v == strings.TrimSpace(v)
	for i := 0; safe && i < len(v); i++ {
		if c := v[i]; c != '\t' && (c < 0x20 || c > 0x7e) {
			safe = false
		}
	}
	if safe && strings.HasPrefix(v, base64Prefix) && strings.HasSuffix(v, base64Suffix) {
		safe = false
	}
	if safe {
		return v
	}
	return base64Prefix + base64.StdEncoding.EncodeToString([]byte(v)) + base64Suffix
}

// SetRequestHeaders sets the metadata headers a conforming client sends for
// a request with the given method and optional name.
func SetRequestHeaders(h http.Header, method, name string) {
	h.Set("Content-Type", "application/json")
	h.Set("Accept", "application/json, text/event-stream")
	h.Set(HeaderProtocolVersion, ProtocolVersion)
	h.Set(HeaderMethod, method)
	if namedMethods[method] {
		h.Set(HeaderName, EncodeHeaderValue(name))
	}
}
