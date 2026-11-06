package opkey

import (
	"bytes"
	"encoding/json"
	"testing"
)

var scope = Scope{Principal: "alice", Upstream: "http://billing/mcp", Tool: "create_refund"}

func mustDerive(t *testing.T, s Scope, in Inputs) Identity {
	t.Helper()
	id, err := Derive(s, in)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestSources(t *testing.T) {
	args := json.RawMessage(`{"amount":20}`)
	if id := mustDerive(t, scope, Inputs{CallerKey: "k1", RunID: "r", StepID: "s", Arguments: args}); id.Source != SourceCallerKey || !id.Keyed() {
		t.Fatalf("caller key not preferred: %+v", id)
	}
	if id := mustDerive(t, scope, Inputs{RunID: "r", StepID: "s", Arguments: args}); id.Source != SourceRunStep || !id.Keyed() {
		t.Fatalf("run and step not used: %+v", id)
	}
	if id := mustDerive(t, scope, Inputs{RunID: "r", Arguments: args}); id.Source != SourceFallback || !id.Windowed() {
		t.Fatalf("run without step should fall back: %+v", id)
	}
}

func TestCallerKeyIgnoresArgumentsForIdentity(t *testing.T) {
	a := mustDerive(t, scope, Inputs{CallerKey: "k1", Arguments: json.RawMessage(`{"amount":20}`)})
	b := mustDerive(t, scope, Inputs{CallerKey: "k1", Arguments: json.RawMessage(`{"amount":30}`)})
	if a.Base != b.Base {
		t.Fatal("same caller key must give the same operation, so the store can flag the conflict")
	}
	if bytes.Equal(a.ArgsHash, b.ArgsHash) {
		t.Fatal("different arguments must hash differently")
	}
}

func TestScopeSeparatesKeys(t *testing.T) {
	in := Inputs{CallerKey: "k1", Arguments: json.RawMessage(`{}`)}
	base := mustDerive(t, scope, in).Base
	for _, s := range []Scope{
		{Principal: "bob", Upstream: scope.Upstream, Tool: scope.Tool},
		{Principal: scope.Principal, Upstream: "http://other/mcp", Tool: scope.Tool},
		{Principal: scope.Principal, Upstream: scope.Upstream, Tool: "send_email"},
	} {
		if mustDerive(t, s, in).Base == base {
			t.Errorf("scope %+v shares a key with %+v", s, scope)
		}
	}
}

func TestFieldBoundariesCannotShift(t *testing.T) {
	a := mustDerive(t, Scope{Principal: "ab", Upstream: "c", Tool: "t"}, Inputs{CallerKey: "k"})
	b := mustDerive(t, Scope{Principal: "a", Upstream: "bc", Tool: "t"}, Inputs{CallerKey: "k"})
	if a.Base == b.Base {
		t.Fatal("moving a boundary between fields produced the same key")
	}
}

func TestEquivalentArgumentsShareFallbackKey(t *testing.T) {
	a := mustDerive(t, scope, Inputs{Arguments: json.RawMessage(`{"b":1.0,"a":"x"}`)})
	b := mustDerive(t, scope, Inputs{Arguments: json.RawMessage(`{"a":"x","b":1}`)})
	if a.Base != b.Base {
		t.Fatal("equivalent arguments produced different fallback keys")
	}
	empty := mustDerive(t, scope, Inputs{})
	null := mustDerive(t, scope, Inputs{Arguments: json.RawMessage(`null`)})
	obj := mustDerive(t, scope, Inputs{Arguments: json.RawMessage(`{}`)})
	if empty.Base != null.Base || null.Base != obj.Base {
		t.Fatal("absent, null and empty arguments should be the same call")
	}
}

func TestBadArgumentsRejected(t *testing.T) {
	if _, err := Derive(scope, Inputs{Arguments: json.RawMessage(`{"a":1,"a":2}`)}); err == nil {
		t.Fatal("duplicate member accepted")
	}
}

func TestKey(t *testing.T) {
	if Key("op_abc", 3) != "op_abc.3" {
		t.Fatal(Key("op_abc", 3))
	}
}
