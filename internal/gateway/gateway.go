// Package gateway is the crash-safe MCP gateway. It is a reverse proxy at
// the HTTP and JSON-RPC level: effectful tools/call requests go through the
// durable operation pipeline in call.go, and everything else is forwarded
// unchanged.
package gateway

import (
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/config"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/killpoint"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/mcp"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/metrics"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
)

// maxBodyBytes caps a request body.
const maxBodyBytes = 4 << 20

// Point names a step of a tools/call where the crash suite can kill or
// pause the gateway.
type Point string

const (
	PointAfterClaim      Point = "after_claim"      // claimed, before the gate
	PointAfterGate       Point = "after_gate"       // gate passed, nothing sent
	PointWhileSending    Point = "while_sending"    // part of the body written
	PointAfterSend       Point = "after_send"       // request written, tool running
	PointResponseStarted Point = "response_started" // first response byte read
	PointResponseRead    Point = "response_read"    // response read, not committed
	PointAfterCommit     Point = "after_commit"     // committed, client not answered
	PointHeartbeat       Point = "heartbeat"        // before each lease renewal
)

// Hooks lets in-process tests observe or block named points.
type Hooks struct {
	At func(p Point, opKey string)
}

// Options configures a Gateway beyond its config file.
type Options struct {
	// Instance identifies this gateway in leases and the audit trail. It
	// must be unique among running instances.
	Instance string
	Logger   *slog.Logger
	Hooks    Hooks
	// Metrics receives the gateway's metrics. A private registry is used
	// when it is nil.
	Metrics *metrics.Registry
}

// Gateway is an http.Handler.
type Gateway struct {
	cfg      *config.Config
	store    *store.Store
	instance string
	log      *slog.Logger
	hooks    Hooks

	upstream *url.URL
	// dispatcher sends effectful calls. It never resends a request on its
	// own; see newDispatchClient.
	dispatcher *http.Client
	// reader sends read-only reconciliation lookups, which are safe to
	// retry.
	reader  *http.Client
	proxy   *httputil.ReverseProxy
	pending chan pendingWrite

	registry *metrics.Registry
	metrics  *gatewayMetrics
}

// New builds a gateway.
func New(cfg *config.Config, st *store.Store, opts Options) (*Gateway, error) {
	u, err := url.Parse(cfg.Upstream)
	if err != nil {
		return nil, fmt.Errorf("gateway: upstream: %w", err)
	}
	if opts.Instance == "" {
		opts.Instance = DefaultInstanceID()
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.NewRegistry()
	}
	g := &Gateway{
		cfg:        cfg,
		store:      st,
		instance:   opts.Instance,
		log:        opts.Logger.With("instance", opts.Instance),
		hooks:      opts.Hooks,
		upstream:   u,
		dispatcher: newDispatchClient(),
		reader:     &http.Client{Timeout: 15 * time.Second},
		pending:    make(chan pendingWrite, 1024),
		registry:   opts.Metrics,
	}
	g.metrics = newGatewayMetrics(opts.Metrics, g)
	g.proxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			target := *g.upstream
			pr.Out.URL = &target
			pr.Out.Host = target.Host
			g.prepareUpstreamHeaders(pr.Out.Header)
		},
		FlushInterval: -1, // SSE events go out as they arrive
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			g.log.Warn("upstream unreachable", "err", err)
			w.WriteHeader(http.StatusBadGateway)
		},
	}
	return g, nil
}

// Instance returns this gateway's instance ID.
func (g *Gateway) Instance() string { return g.instance }

// Metrics returns the registry holding the gateway's metrics.
func (g *Gateway) Metrics() *metrics.Registry { return g.registry }

// DefaultInstanceID combines the host name, process ID and a random suffix.
func DefaultInstanceID() string {
	host, _ := os.Hostname()
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b[:]))
}

// newDispatchClient returns the HTTP client for effectful calls. Go's
// transport may silently resend a request on a fresh connection when a
// reused one fails, but only if it can rewind the body. Dispatch bodies
// are never rewindable (see onceBody), so a dispatched call is sent at
// most once per dispatch.
func newDispatchClient() *http.Client {
	t := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       60 * time.Second,
		ResponseHeaderTimeout: 0, // bounded by the dispatch context instead
		ForceAttemptHTTP2:     false,
	}
	return &http.Client{
		Transport: t,
		// A redirect would resend the call to another URL.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func (g *Gateway) at(p Point, key string) {
	killpoint.Hit(string(p))
	if g.hooks.At != nil {
		g.hooks.At(p, key)
	}
}

// ServeHTTP routes a request.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	principal, ok := g.authenticate(r)
	if !ok {
		w.Header().Set("WWW-Authenticate", `Bearer realm="mcp-gateway"`)
		writeJSONRPC(w, http.StatusUnauthorized, mcp.ErrorResponse(nil,
			mcp.NewError(mcp.CodeUnauthorized, "Unauthorized: missing or invalid credentials", nil)))
		return
	}
	if r.Method != http.MethodPost {
		g.proxy.ServeHTTP(w, r)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		if _, tooBig := errors.AsType[*http.MaxBytesError](err); tooBig {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		}
		return
	}
	msg, err := mcp.ParseMessage(body)
	if err != nil || msg.Method != mcp.MethodToolsCall || !msg.IsRequest() {
		g.forward(w, r, body)
		return
	}
	if herr := mcp.ValidateRequestHeaders(r.Header, msg); herr != nil {
		writeJSONRPC(w, http.StatusBadRequest, mcp.ErrorResponse(msg.ID, herr.RPC()))
		return
	}
	g.handleCall(w, r, body, msg, principal)
}

// forward proxies a request whose body has already been read.
func (g *Gateway) forward(w http.ResponseWriter, r *http.Request, body []byte) {
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	g.proxy.ServeHTTP(w, r)
}

// authenticate returns the caller's principal. Authentication runs on every
// request, replays included, so a revoked caller never receives a stored
// result.
func (g *Gateway) authenticate(r *http.Request) (string, bool) {
	a := g.cfg.Auth
	switch {
	case len(a.Tokens) > 0:
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || token == "" {
			return "", false
		}
		for known, principal := range a.Tokens {
			if subtle.ConstantTimeCompare([]byte(token), []byte(known)) == 1 {
				return principal, true
			}
		}
		return "", false
	case a.PrincipalHeader != "":
		p := r.Header.Get(a.PrincipalHeader)
		return p, p != ""
	default:
		return "anonymous", true
	}
}

// hopHeaders are connection-scoped and never forwarded.
var hopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"Proxy-Connection", "Te", "Trailer", "Transfer-Encoding", "Upgrade",
}

// prepareUpstreamHeaders removes the gateway's own credentials and adds the
// configured upstream headers.
func (g *Gateway) prepareUpstreamHeaders(h http.Header) {
	if len(g.cfg.Auth.Tokens) > 0 {
		h.Del("Authorization") // the caller's token is for the gateway
	}
	for k, v := range g.cfg.UpstreamHeaders {
		h.Set(k, v)
	}
}

func writeJSONRPC(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
