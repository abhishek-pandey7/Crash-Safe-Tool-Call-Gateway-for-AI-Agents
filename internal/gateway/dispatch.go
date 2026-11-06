package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync/atomic"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcp"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
)

// maxResponseBytes caps an upstream response the gateway will read.
const maxResponseBytes = 32 << 20

// dispatchResult is what one send to the upstream produced.
type dispatchResult struct {
	// maybeSent is false only when the call provably never left the
	// gateway: no connection to the upstream was ever obtained.
	maybeSent bool
	err       error
	status    int
	final     *mcp.Message
	finalRaw  []byte
}

// onceBody is a request body the HTTP transport cannot rewind. Without
// GetBody, Go's transport will not resend the request by itself after a
// failure on a reused connection, even when the client set an
// Idempotency-Key header. It also marks the middle of the body as the
// while_sending point.
type onceBody struct {
	data   []byte
	off    int
	hook   func()
	hooked bool
}

func (b *onceBody) Read(p []byte) (int, error) {
	if b.off >= len(b.data) {
		return 0, io.EOF
	}
	half := len(b.data) / 2
	if b.off >= half && !b.hooked {
		b.hooked = true
		b.hook()
	}
	end := len(b.data)
	if b.off < half {
		end = half
	}
	n := copy(p, b.data[b.off:end])
	b.off += n
	return n, nil
}

func (b *onceBody) Close() error { return nil }

// send forwards one effectful call upstream. Progress notifications on an
// SSE response are relayed to the client as they arrive; the final
// response is returned, not written, so it can be committed first.
func (g *Gateway) send(c *callCtx, op *store.Operation, body []byte) dispatchResult {
	// The client may disconnect, but a dispatched call keeps running so its
	// outcome is recorded and a retry can receive it.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c.r.Context()), g.cfg.UpstreamTimeout)
	defer cancel()

	var gotConn atomic.Bool
	trace := &httptrace.ClientTrace{
		GotConn:              func(httptrace.GotConnInfo) { gotConn.Store(true) },
		WroteRequest:         func(httptrace.WroteRequestInfo) { g.at(PointAfterSend, op.Key) },
		GotFirstResponseByte: func() { g.at(PointResponseStarted, op.Key) },
	}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodPost,
		g.upstream.String(), &onceBody{data: body, hook: func() { g.at(PointWhileSending, op.Key) }})
	if err != nil {
		return dispatchResult{err: err}
	}
	req.ContentLength = int64(len(body))
	req.GetBody = nil
	copyRequestHeaders(req.Header, c.r.Header)
	g.prepareUpstreamHeaders(req.Header)

	resp, err := g.dispatcher.Do(req)
	res := dispatchResult{maybeSent: gotConn.Load()}
	if err != nil {
		res.err = err
		return res
	}
	defer resp.Body.Close()
	res.status = resp.StatusCode

	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		events := mcp.NewSSEReader(resp.Body)
		for {
			ev, err := events.Next()
			if err != nil {
				res.err = fmt.Errorf("stream ended before the final response: %w", err)
				return res
			}
			m, err := mcp.ParseMessage(ev.Data)
			if err == nil && m.IsResponse() {
				res.final, res.finalRaw = m, ev.Data
				return res
			}
			c.out.event(ev.Data)
		}
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		res.err = err
		return res
	}
	if len(raw) > maxResponseBytes {
		res.err = fmt.Errorf("response larger than %d bytes", maxResponseBytes)
		return res
	}
	m, err := mcp.ParseMessage(raw)
	if err != nil || !m.IsResponse() {
		res.err = fmt.Errorf("upstream answered status %d without a JSON-RPC response", resp.StatusCode)
		return res
	}
	res.final, res.finalRaw = m, raw
	return res
}

// copyRequestHeaders copies the client's headers for the upstream request,
// minus connection-scoped ones and those the transport sets itself.
func copyRequestHeaders(dst, src http.Header) {
	for k, vs := range src {
		dst[k] = append([]string(nil), vs...)
	}
	for _, h := range hopHeaders {
		dst.Del(h)
	}
	for f := range strings.SplitSeq(src.Get("Connection"), ",") {
		if f = strings.TrimSpace(f); f != "" {
			dst.Del(f)
		}
	}
	dst.Del("Content-Length")
	dst.Del("Host")
	dst.Del("Accept-Encoding")
}

// responder writes the answer to the client: plain JSON, or SSE once the
// upstream has started streaming progress.
type responder struct {
	w    http.ResponseWriter
	sse  bool
	gone bool
}

func (o *responder) flush() {
	if f, ok := o.w.(http.Flusher); ok {
		f.Flush()
	}
}

// event relays one SSE event, switching the response to SSE first.
func (o *responder) event(data []byte) {
	if o.gone {
		return
	}
	if !o.sse {
		h := o.w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("X-Accel-Buffering", "no")
		o.w.WriteHeader(http.StatusOK)
		o.sse = true
	}
	if err := mcp.WriteSSE(o.w, data); err != nil {
		o.gone = true // client left; the call carries on regardless
		return
	}
	o.flush()
}

// final writes the final JSON-RPC response.
func (o *responder) final(status int, body []byte) {
	if o.sse {
		o.event(body)
		return
	}
	if o.gone {
		return
	}
	writeJSONRPC(o.w, status, body)
}
