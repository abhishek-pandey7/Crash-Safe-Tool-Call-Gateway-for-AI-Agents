package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store/storetest"
)

// unknownOp creates an operation whose outcome is unknown.
func unknownOp(t *testing.T, st *store.Store, base string) string {
	t.Helper()
	ctx := context.Background()
	op, _, err := st.Claim(ctx, store.ClaimParams{
		Base: base, Source: "caller_key", Principal: "alice", Upstream: "http://u/mcp",
		Tool: "create_refund", Policy: "at_most_once", ArgsHash: []byte{1}, Owner: "g1", Lease: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	st.Gate(ctx, op.Key, "g1", op.Fence, time.Minute)
	st.MarkUnknown(ctx, op.Key, "g1", op.Fence, map[string]any{"error": "connection reset"})
	return op.Key
}

func ctl(t *testing.T, db string, args ...string) (int, string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := run(context.Background(), append([]string{"-database", db}, args...), &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestResolveFlow(t *testing.T) {
	db := storetest.URL(t)
	st, err := store.Open(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	key := unknownOp(t, st, "op_ctl")

	if code, out, _ := ctl(t, db, "list", "-state", "unknown"); code != 0 || !strings.Contains(out, key) {
		t.Fatalf("list: %d %q", code, out)
	}
	if code, _, errOut := ctl(t, db, "resolve", key, "-outcome", "completed"); code != 1 || !strings.Contains(errOut, "evidence") {
		t.Fatalf("resolve without evidence: %d %q", code, errOut)
	}
	if code, _, _ := ctl(t, db, "resolve", key, "-outcome", "maybe", "-evidence", "x"); code != 1 {
		t.Fatal("accepted an invalid outcome")
	}
	code, out, errOut := ctl(t, db, "resolve", key, "-outcome", "completed", "-evidence", "refund rf_9 visible in the provider dashboard", "-by", "dana")
	if code != 0 || !strings.Contains(out, "unknown -> completed") {
		t.Fatalf("resolve: %d %q %q", code, out, errOut)
	}
	op, _ := st.Get(context.Background(), key)
	if op.State != store.Completed || !bytes.Contains(op.Result, []byte("resolved_by_operator")) {
		t.Fatalf("not recorded: %+v", op)
	}
	if code, _, errOut := ctl(t, db, "resolve", key, "-outcome", "not_executed", "-evidence", "x"); code != 1 || !strings.Contains(errOut, "completed") {
		t.Fatalf("resolved a completed operation again: %d %q", code, errOut)
	}

	code, out, _ = ctl(t, db, "show", key)
	for _, want := range []string{"state:", "completed", "unknown -> completed", "dana", "provider dashboard"} {
		if code != 0 || !strings.Contains(out, want) {
			t.Fatalf("show lacks %q:\n%s", want, out)
		}
	}
}

func TestHousekeeping(t *testing.T) {
	db := storetest.URL(t)
	if code, out, _ := ctl(t, db, "sweep"); code != 0 || !strings.Contains(out, "released 0") {
		t.Fatalf("sweep: %d %q", code, out)
	}
	if code, _, _ := ctl(t, db, "purge"); code != 1 {
		t.Fatal("purge ran without a retention")
	}
	if code, out, _ := ctl(t, db, "purge", "-retention", "720h"); code != 0 || !strings.Contains(out, "removed 0") {
		t.Fatalf("purge: %d %q", code, out)
	}
}

func TestUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run(context.Background(), nil, &out, &errOut); code != 2 {
		t.Fatalf("no command: exit %d", code)
	}
	if code := run(context.Background(), []string{"-database", "", "list"}, &out, &errOut); code != 2 {
		t.Fatalf("no database: exit %d", code)
	}
	db := storetest.URL(t)
	if code, _, _ := ctl(t, db, "frobnicate"); code != 2 {
		t.Fatalf("unknown command: exit %d", code)
	}
}
