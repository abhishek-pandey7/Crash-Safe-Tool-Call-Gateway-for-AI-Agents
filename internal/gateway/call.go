package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/config"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcp"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/opkey"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
)

// origin records how this request came to own a dispatch, which decides
// what a send that provably never left the gateway means.
type origin int

const (
	// fromClaim: no earlier attempt of the operation was ever sent.
	fromClaim origin = iota
	// fromStale: an earlier dispatch may have been sent (native_key resend).
	fromStale
	// fromAwaiting: an earlier round asked for input (multi round-trip).
	fromAwaiting
)

// callCtx is one effectful tools/call in flight.
type callCtx struct {
	r         *http.Request
	body      []byte
	msg       *mcp.Message
	params    *mcp.CallParams
	pol       config.Tool
	id        opkey.Identity
	principal string
	out       *responder
	key       string
	waitUntil time.Time
	log       *slog.Logger
}

// storeCtx is detached from the client: a client hanging up must not
// abort bookkeeping half-way.
func (c *callCtx) storeCtx() context.Context { return context.WithoutCancel(c.r.Context()) }

func (c *callCtx) bind(key string) {
	c.key = key
	c.out.w.Header().Set("X-Gateway-Operation-Key", key)
	c.log = c.log.With("op", key)
}

func (g *Gateway) handleCall(w http.ResponseWriter, r *http.Request, body []byte, msg *mcp.Message, principal string) {
	p, err := mcp.ParseCallParams(msg.Params)
	if err != nil {
		writeJSONRPC(w, http.StatusBadRequest, mcp.ErrorResponse(msg.ID, mcp.NewError(mcp.CodeInvalidParams, err.Error(), nil)))
		return
	}
	pol, allowed := g.cfg.ToolPolicy(p.Name)
	if !allowed {
		writeJSONRPC(w, http.StatusOK, mcp.ErrorResponse(msg.ID,
			mcp.NewError(mcp.CodeInvalidParams, "Unknown tool: "+p.Name+" is not allowed by the gateway", nil)))
		return
	}
	if pol.Policy == config.ReadOnly {
		g.forward(w, r, body)
		return
	}

	in := opkey.Inputs{
		CallerKey: p.MetaString(opkey.MetaOperationKey),
		RunID:     p.MetaString(opkey.MetaRunID),
		StepID:    p.MetaString(opkey.MetaStepID),
		Arguments: p.Arguments,
	}
	if in.CallerKey == "" && pol.Policy == config.NativeKey {
		// A key the caller already put in the tool's own key argument is
		// a stable identity too.
		if v := stringArgument(p.Arguments, pol.KeyArgument); v != "" {
			in.CallerKey = "native-key:" + v
		}
	}
	id, err := opkey.Derive(opkey.Scope{Principal: principal, Upstream: g.cfg.Upstream, Tool: p.Name}, in)
	if err != nil {
		writeJSONRPC(w, http.StatusOK, mcp.ErrorResponse(msg.ID, mcp.NewError(mcp.CodeInvalidParams, err.Error(), nil)))
		return
	}
	if pol.RequireKey && !id.Keyed() {
		writeJSONRPC(w, http.StatusOK, mcp.ErrorResponse(msg.ID, mcp.NewError(mcp.CodeKeyRequired,
			"Operation key required: "+p.Name+" only runs with a stable key in _meta",
			map[string]any{"metaKeys": []string{opkey.MetaOperationKey, opkey.MetaRunID + " + " + opkey.MetaStepID}})))
		return
	}

	c := &callCtx{
		r: r, body: body, msg: msg, params: p, pol: pol, id: id, principal: principal,
		out:       &responder{w: w},
		waitUntil: time.Now().Add(g.cfg.WaitTimeout),
		log:       g.log.With("tool", p.Name),
	}
	g.run(c)
}

func (g *Gateway) run(c *callCtx) {
	ctx := c.storeCtx()
	var window time.Duration
	if c.id.Windowed() {
		window = g.cfg.FallbackWindow
	}
	op, created, err := g.store.Claim(ctx, store.ClaimParams{
		Base: c.id.Base, Source: string(c.id.Source), Principal: c.principal,
		Upstream: g.cfg.Upstream, Tool: c.params.Name, Policy: string(c.pol.Policy),
		ArgsHash: c.id.ArgsHash, Owner: g.instance, Lease: g.cfg.Lease, Window: window,
	})
	if err != nil {
		g.respondStoreDown(c, err)
		return
	}
	c.bind(op.Key)
	if !created && !bytes.Equal(op.ArgsHash, c.id.ArgsHash) {
		g.respondConflict(c)
		return
	}
	if created && g.runClaimed(c, op) {
		return
	}
	for range 1000 {
		next, done := g.step(c, op)
		if done {
			return
		}
		op = next
	}
	g.respondInProgress(c)
}

// step acts on an operation this request does not own yet. It returns the
// operation to look at next, or done once the client has been answered.
func (g *Gateway) step(c *callCtx, op *store.Operation) (*store.Operation, bool) {
	ctx := c.storeCtx()
	switch op.State {
	case store.Completed:
		g.replay(c, op)
		return nil, true

	case store.Claimed, store.Released, store.NotExecuted:
		if op.State == store.Claimed && !op.LeaseExpired() {
			return g.wait(c, op)
		}
		// Nobody can be sending this operation, so it is safe to own it.
		acquired, ok, err := g.store.Acquire(ctx, op.Key, g.instance, g.cfg.Lease)
		if err != nil {
			g.respondStoreDown(c, err)
			return nil, true
		}
		if ok && g.runClaimed(c, acquired) {
			return nil, true
		}
		return g.reread(c)

	case store.Dispatching:
		if !op.LeaseExpired() {
			return g.wait(c, op)
		}
		if c.pol.Policy == config.NativeKey {
			return g.redispatch(c, store.FromStale)
		}
		// The owner is gone mid-dispatch. Nobody may send again.
		expired, ok, err := g.store.ExpireDispatch(ctx, op.Key, g.instance)
		if err != nil {
			g.respondStoreDown(c, err)
			return nil, true
		}
		if ok {
			return expired, false
		}
		return g.reread(c)

	case store.AwaitingInput:
		return g.redispatch(c, store.FromAwaitingInput)

	case store.Unknown:
		if c.pol.Policy == config.NativeKey {
			return g.redispatch(c, store.FromStale)
		}
		g.respondUnknown(c, op)
		return nil, true
	}
	g.respondInProgress(c)
	return nil, true
}

func (g *Gateway) reread(c *callCtx) (*store.Operation, bool) {
	op, err := g.store.Get(c.storeCtx(), c.key)
	if err != nil {
		g.respondStoreDown(c, err)
		return nil, true
	}
	return op, false
}

// wait polls a running operation until it changes, its owner's lease
// lapses, or the wait timeout passes.
func (g *Gateway) wait(c *callCtx, op *store.Operation) (*store.Operation, bool) {
	poll := 20 * time.Millisecond
	for {
		if time.Now().After(c.waitUntil) {
			g.respondInProgress(c)
			return nil, true
		}
		select {
		case <-c.r.Context().Done():
			return nil, true // the client left; nothing of ours is running
		case <-time.After(poll):
		}
		cur, err := g.store.Get(c.storeCtx(), op.Key)
		if err != nil {
			g.respondStoreDown(c, err)
			return nil, true
		}
		if cur.State != op.State || cur.Fence != op.Fence || cur.LeaseExpired() {
			return cur, false
		}
		poll = min(poll*2, 200*time.Millisecond)
	}
}

// runClaimed takes a claim this request owns through the dispatch gate. It
// returns false if another instance fenced it out first.
func (g *Gateway) runClaimed(c *callCtx, op *store.Operation) bool {
	hb := g.startHeartbeat(op.Key, op.Fence)
	g.at(PointAfterClaim, op.Key)

	if c.r.Context().Err() != nil {
		// Before dispatch, a disconnect really is a cancel.
		hb.stop()
		g.persist("release", op.Key, func(ctx context.Context) (bool, error) {
			return g.store.Release(ctx, op.Key, g.instance, op.Fence, map[string]any{"reason": "client disconnected before dispatch"})
		})
		return true
	}

	gated, ok, err := g.store.Gate(c.storeCtx(), op.Key, g.instance, op.Fence, g.cfg.Lease)
	if err != nil {
		hb.stop()
		// The gate write may or may not have committed, but nothing was
		// sent. If it did commit, record that once the store is back.
		g.persist("not executed after failed gate", op.Key, func(ctx context.Context) (bool, error) {
			return g.store.MarkNotExecuted(ctx, op.Key, g.instance, op.Fence, map[string]any{"reason": "gate write failed; never sent"})
		})
		g.respondStoreDown(c, err)
		return true
	}
	if !ok {
		hb.stop()
		return false
	}
	g.at(PointAfterGate, op.Key)
	g.dispatchOwned(c, gated, hb, fromClaim)
	return true
}

// redispatch restarts an operation for which sending again is safe.
func (g *Gateway) redispatch(c *callCtx, from store.RedispatchFrom) (*store.Operation, bool) {
	reason, o := "native key resend", fromStale
	if from == store.FromAwaitingInput {
		reason, o = "multi round-trip continuation", fromAwaiting
	}
	op, ok, err := g.store.Redispatch(c.storeCtx(), c.key, g.instance, g.cfg.Lease, from, map[string]any{"reason": reason})
	if err != nil {
		g.respondStoreDown(c, err)
		return nil, true
	}
	if !ok {
		return g.reread(c)
	}
	g.dispatchOwned(c, op, nil, o)
	return nil, true
}

// dispatchOwned sends a call that has passed the gate, records the outcome
// and answers the client.
func (g *Gateway) dispatchOwned(c *callCtx, op *store.Operation, hb *heartbeat, o origin) {
	if hb == nil {
		hb = g.startHeartbeat(op.Key, op.Fence)
	}
	defer hb.stop()

	body := c.body
	if c.pol.Policy == config.NativeKey && stringArgument(c.params.Arguments, c.pol.KeyArgument) == "" {
		injected, err := injectArgument(c.body, c.params.Arguments, c.pol.KeyArgument, op.Key)
		if err == nil {
			body = injected
		}
	}

	var res dispatchResult
	for attempt := 0; ; attempt++ {
		res = g.send(c, op, body)
		if res.final != nil || !res.maybeSent || c.pol.Policy != config.NativeKey || attempt >= c.pol.MaxResends {
			break
		}
		c.log.Info("resending with the same idempotency key", "attempt", attempt+1, "err", res.err)
		time.Sleep(time.Duration(100<<attempt) * time.Millisecond)
	}

	if res.final == nil {
		g.settleFailedSend(c, op, res, o)
		return
	}
	g.at(PointResponseRead, op.Key)
	g.commit(c, op, res.final)
	g.at(PointAfterCommit, op.Key)
	status := res.status
	if status == 0 {
		status = http.StatusOK
	}
	c.out.final(status, res.finalRaw)
}

// settleFailedSend records a dispatch that produced no response.
func (g *Gateway) settleFailedSend(c *callCtx, op *store.Operation, res dispatchResult, o origin) {
	detail := map[string]any{"error": errString(res.err)}
	if !res.maybeSent {
		switch o {
		case fromClaim:
			// No attempt of this operation ever left the gateway.
			g.persist("not executed", op.Key, func(ctx context.Context) (bool, error) {
				return g.store.MarkNotExecuted(ctx, op.Key, g.instance, op.Fence, detail)
			})
			g.respondNotExecuted(c, "the upstream server could not be reached")
			return
		case fromAwaiting:
			// Back to waiting for the continuation.
			g.persist("await input", op.Key, func(ctx context.Context) (bool, error) {
				return g.store.AwaitInput(ctx, op.Key, g.instance, op.Fence, nil)
			})
			g.respondNotExecuted(c, "the upstream server could not be reached")
			return
		}
		// fromStale: an earlier dispatch may have taken effect.
	}
	c.log.Warn("outcome unknown", "err", res.err)
	g.persist("unknown", op.Key, func(ctx context.Context) (bool, error) {
		return g.store.MarkUnknown(ctx, op.Key, g.instance, op.Fence, detail)
	})
	op.State = store.Unknown
	g.respondUnknown(c, op)
}

// commit records the final upstream response.
func (g *Gateway) commit(c *callCtx, op *store.Operation, m *mcp.Message) {
	if len(m.Result) > 0 && mcp.ResultType(m.Result) == mcp.ResultInputRequired {
		g.persist("await input", op.Key, func(ctx context.Context) (bool, error) {
			return g.store.AwaitInput(ctx, op.Key, g.instance, op.Fence, m.Result)
		})
		return
	}
	kind, body := store.KindResult, []byte(m.Result)
	if len(m.Error) > 0 {
		kind, body = store.KindError, []byte(m.Error)
	}
	sum := sha256.Sum256(body)
	stored := body
	if c.pol.StoreResult == config.StoreHash || len(body) > g.cfg.MaxResultBytes {
		stored = nil
	}
	out := store.Outcome{Kind: kind, Body: stored, Hash: sum[:]}
	g.persist("complete", op.Key, func(ctx context.Context) (bool, error) {
		_, ok, err := g.store.Complete(ctx, op.Key, g.instance, out)
		return ok, err
	})
}

// heartbeat keeps an owner's lease alive while it works.
type heartbeat struct {
	once sync.Once
	done chan struct{}
	lost atomic.Bool
}

func (h *heartbeat) stop() { h.once.Do(func() { close(h.done) }) }

func (g *Gateway) startHeartbeat(key string, fence int64) *heartbeat {
	hb := &heartbeat{done: make(chan struct{})}
	interval := g.cfg.Lease / 3
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-hb.done:
				return
			case <-t.C:
			}
			g.at(PointHeartbeat, key)
			select {
			case <-hb.done:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), interval)
			ok, err := g.store.Heartbeat(ctx, key, g.instance, fence, g.cfg.Lease)
			cancel()
			if err == nil && !ok {
				hb.lost.Store(true)
				return
			}
		}
	}()
	return hb
}

// stringArgument returns a string-valued argument, or "".
func stringArgument(args json.RawMessage, name string) string {
	if name == "" || len(args) == 0 {
		return ""
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(args, &m) != nil {
		return ""
	}
	var s string
	if json.Unmarshal(m[name], &s) != nil {
		return ""
	}
	return s
}

// injectArgument returns body with one string argument added.
func injectArgument(body []byte, args json.RawMessage, name, value string) ([]byte, error) {
	m := map[string]json.RawMessage{}
	if len(args) > 0 && string(args) != "null" {
		if err := json.Unmarshal(args, &m); err != nil {
			return nil, err
		}
	}
	v, _ := json.Marshal(value)
	m[name] = v
	newArgs, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return mcp.ReplaceArguments(body, newArgs)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
