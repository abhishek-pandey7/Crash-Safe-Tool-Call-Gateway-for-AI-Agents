package mcp

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func callBody(name, version string) []byte {
	return []byte(`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"` + name +
		`","arguments":{"a":1},"_meta":{"` + MetaProtocolVersion + `":"` + version + `"}}}`)
}

func TestParseMessageKinds(t *testing.T) {
	cases := []struct {
		body         string
		req, note    bool
		resp, reject bool
	}{
		{body: `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, req: true},
		{body: `{"jsonrpc":"2.0","method":"notifications/progress"}`, note: true},
		{body: `{"jsonrpc":"2.0","id":null,"method":"x"}`, note: true},
		{body: `{"jsonrpc":"2.0","id":"a","result":{}}`, resp: true},
		{body: `[{"jsonrpc":"2.0","id":1,"method":"x"}]`, reject: true},
		{body: `{"jsonrpc":"1.0","id":1,"method":"x"}`, reject: true},
		{body: ``, reject: true},
	}
	for _, c := range cases {
		m, err := ParseMessage([]byte(c.body))
		if c.reject {
			if err == nil {
				t.Errorf("%s: expected rejection", c.body)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", c.body, err)
		}
		if m.IsRequest() != c.req || m.IsNotification() != c.note || m.IsResponse() != c.resp {
			t.Errorf("%s: got req=%v note=%v resp=%v", c.body, m.IsRequest(), m.IsNotification(), m.IsResponse())
		}
	}
}

func TestResponsesKeepID(t *testing.T) {
	got := string(ResultResponse(json.RawMessage(`"abc"`), json.RawMessage(`{"x":1}`)))
	if got != `{"jsonrpc":"2.0","id":"abc","result":{"x":1}}` {
		t.Fatalf("result response: %s", got)
	}
	got = string(ErrorResponse(nil, NewError(CodeKeyConflict, "conflict", nil)))
	if got != `{"jsonrpc":"2.0","id":null,"error":{"code":-32011,"message":"conflict"}}` {
		t.Fatalf("error response: %s", got)
	}
}

func headersFor(method, name, version string) http.Header {
	h := http.Header{}
	SetRequestHeaders(h, method, name)
	h.Set(HeaderProtocolVersion, version)
	return h
}

func TestValidateRequestHeaders(t *testing.T) {
	body := callBody("create_refund", ProtocolVersion)
	m, _ := ParseMessage(body)

	if err := ValidateRequestHeaders(headersFor("tools/call", "create_refund", ProtocolVersion), m); err != nil {
		t.Fatalf("valid headers rejected: %v", err)
	}

	h := headersFor("tools/call", "get_invoice", ProtocolVersion)
	if err := ValidateRequestHeaders(h, m); err == nil || err.Code != CodeHeaderMismatch {
		t.Fatalf("name mismatch: got %v", err)
	}

	h = headersFor("tools/call", "create_refund", ProtocolVersion)
	h.Del(HeaderName)
	if err := ValidateRequestHeaders(h, m); err == nil || err.Code != CodeHeaderMismatch {
		t.Fatalf("missing name: got %v", err)
	}

	h = headersFor("tools/list", "", ProtocolVersion)
	if err := ValidateRequestHeaders(h, m); err == nil || err.Code != CodeHeaderMismatch {
		t.Fatalf("method mismatch: got %v", err)
	}

	h = headersFor("tools/call", "create_refund", "2025-11-25")
	if err := ValidateRequestHeaders(h, m); err == nil || err.Code != CodeHeaderMismatch {
		t.Fatalf("version header differs from body: got %v", err)
	}

	old, _ := ParseMessage(callBody("create_refund", "2025-11-25"))
	h = headersFor("tools/call", "create_refund", "2025-11-25")
	if err := ValidateRequestHeaders(h, old); err == nil || err.Code != CodeUnsupportedProtocolVersion {
		t.Fatalf("old version: got %v", err)
	}
}

func TestBase64EncodedName(t *testing.T) {
	name := "héllo tool"
	body := callBody(name, ProtocolVersion)
	m, _ := ParseMessage(body)
	h := headersFor("tools/call", name, ProtocolVersion)
	if v := h.Get(HeaderName); !strings.HasPrefix(v, "=?base64?") {
		t.Fatalf("expected encoded header, got %q", v)
	}
	if err := ValidateRequestHeaders(h, m); err != nil {
		t.Fatalf("encoded name rejected: %v", err)
	}
}

func TestHeaderValueEncoding(t *testing.T) {
	for in, want := range map[string]string{
		"us-west1":           "us-west1",
		"Hello, 世界":          "=?base64?SGVsbG8sIOS4lueVjA==?=",
		" padded ":           "=?base64?IHBhZGRlZCA=?=",
		"line1\nline2":       "=?base64?bGluZTEKbGluZTI=?=",
		"=?base64?literal?=": "=?base64?PT9iYXNlNjQ/bGl0ZXJhbD89?=",
	} {
		if got := EncodeHeaderValue(in); got != want {
			t.Errorf("encode %q = %q, want %q", in, got, want)
		}
		if back, err := DecodeHeaderValue(want); err != nil || back != in {
			t.Errorf("decode %q = %q, %v", want, back, err)
		}
	}
	if _, err := DecodeHeaderValue("bad\x01value"); err == nil {
		t.Error("control character accepted")
	}
}

func TestCallParams(t *testing.T) {
	raw := json.RawMessage(`{"name":"t","arguments":{"x":1},"inputResponses":{"k":{}},"requestState":"opaque","_meta":{"k":"v"}}`)
	p, err := ParseCallParams(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !p.IsContinuation() || p.MetaString("k") != "v" || string(p.Arguments) != `{"x":1}` {
		t.Fatalf("unexpected params: %+v", p)
	}
	if _, err := ParseCallParams(json.RawMessage(`{"arguments":{}}`)); err == nil {
		t.Fatal("missing name accepted")
	}
}

func TestReplaceArgumentsKeepsOtherFields(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"t","arguments":{"a":1},"requestState":"keep me"}}`)
	out, err := ReplaceArguments(body, json.RawMessage(`{"a":1,"key":"k"}`))
	if err != nil {
		t.Fatal(err)
	}
	m, _ := ParseMessage(out)
	p, _ := ParseCallParams(m.Params)
	if string(p.Arguments) != `{"a":1,"key":"k"}` || string(p.RequestState) != `"keep me"` || string(m.ID) != "1" {
		t.Fatalf("unexpected body: %s", out)
	}
}

func TestResultType(t *testing.T) {
	for raw, want := range map[string]string{
		`{"content":[]}`:                     ResultComplete,
		`{"resultType":"input_required"}`:    ResultInputRequired,
		`{"resultType":"task","taskId":"1"}`: ResultTask,
	} {
		if got := ResultType(json.RawMessage(raw)); got != want {
			t.Errorf("%s: got %s want %s", raw, got, want)
		}
	}
}

func TestSSEReader(t *testing.T) {
	stream := ": keep-alive\n\n" +
		"event: message\ndata: {\"a\":1}\n\n" +
		"data: line1\r\ndata: line2\r\n\r\n"
	r := NewSSEReader(strings.NewReader(stream))
	ev, err := r.Next()
	if err != nil || ev.Name != "message" || string(ev.Data) != `{"a":1}` {
		t.Fatalf("first event: %+v %v", ev, err)
	}
	ev, err = r.Next()
	if err != nil || string(ev.Data) != "line1\nline2" {
		t.Fatalf("second event: %+v %v", ev, err)
	}
	if _, err = r.Next(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}

	r = NewSSEReader(strings.NewReader("data: {\"half\":"))
	if _, err = r.Next(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected unexpected EOF, got %v", err)
	}
}

func TestWriteSSERoundTrip(t *testing.T) {
	var b strings.Builder
	if err := WriteSSE(&b, []byte("x\ny")); err != nil {
		t.Fatal(err)
	}
	ev, err := NewSSEReader(strings.NewReader(b.String())).Next()
	if err != nil || string(ev.Data) != "x\ny" {
		t.Fatalf("round trip: %+v %v", ev, err)
	}
}
