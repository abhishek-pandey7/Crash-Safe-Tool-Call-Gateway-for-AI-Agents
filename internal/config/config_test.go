package config

import (
	"strings"
	"testing"
	"time"
)

const example = `
upstream: http://billing-mcp:8080/mcp
lease: 10s
tools:
  get_invoice:
    policy: read_only
  find_refund_by_reference:
    policy: read_only
  send_email:
    policy: at_most_once
  create_refund:
    policy: reconcilable
    require_key: true
    reconcile:
      tool: find_refund_by_reference
      match_argument: reference
  create_charge:
    policy: native_key
    require_key: true
    key_argument: idempotency_key
`

func TestParseExample(t *testing.T) {
	c, err := Parse([]byte(example))
	if err != nil {
		t.Fatal(err)
	}
	if c.Lease != 10*time.Second || c.WaitTimeout != DefaultWaitTimeout || c.Listen != DefaultListen {
		t.Fatalf("defaults not applied: %+v", c)
	}
	refund := c.Tools["create_refund"]
	if refund.Reconcile.LookupArgument != "reference" || refund.Reconcile.FoundField != "found" {
		t.Fatalf("reconcile defaults: %+v", refund.Reconcile)
	}
	if refund.Reconcile.SettleAfter != DefaultUpstreamTimeout+10*time.Second {
		t.Fatalf("settle default: %v", refund.Reconcile.SettleAfter)
	}
	if c.Tools["create_charge"].MaxResends != DefaultMaxResends {
		t.Fatal("max_resends default not applied")
	}
	if p, ok := c.ToolPolicy("not_listed"); !ok || p.Policy != AtMostOnce {
		t.Fatalf("unknown tool policy: %+v %v", p, ok)
	}
}

func TestRefuseUnknownTools(t *testing.T) {
	c, err := Parse([]byte("upstream: http://u/mcp\nunknown_tools: refuse\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.ToolPolicy("anything"); ok {
		t.Fatal("unknown tool allowed")
	}
}

func TestRejectsMistakes(t *testing.T) {
	cases := map[string]string{
		"typo in field":        "upstream: http://u/mcp\ntools:\n  t:\n    polcy: read_only\n",
		"unknown policy":       "upstream: http://u/mcp\ntools:\n  t:\n    policy: exactly_once\n",
		"missing upstream":     "lease: 1s\n",
		"bad upstream":         "upstream: ftp://u\n",
		"native without key":   "upstream: http://u/mcp\ntools:\n  t:\n    policy: native_key\n",
		"reconcile no tool":    "upstream: http://u/mcp\ntools:\n  t:\n    policy: reconcilable\n",
		"reconcile not read":   "upstream: http://u/mcp\ntools:\n  r:\n    policy: at_most_once\n  t:\n    policy: reconcilable\n    reconcile:\n      tool: r\n      match_argument: x\n",
		"key arg wrong policy": "upstream: http://u/mcp\ntools:\n  t:\n    policy: at_most_once\n    key_argument: k\n",
		"bad store_result":     "upstream: http://u/mcp\ntools:\n  t:\n    store_result: maybe\n",
		"tiny lease":           "upstream: http://u/mcp\nlease: 1ms\n",
		"both auth modes":      "upstream: http://u/mcp\nauth:\n  tokens: {a: b}\n  principal_header: X-User\n",
	}
	for name, doc := range cases {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("%s: accepted", name)
		} else if strings.TrimSpace(err.Error()) == "" {
			t.Errorf("%s: empty error", name)
		}
	}
}
