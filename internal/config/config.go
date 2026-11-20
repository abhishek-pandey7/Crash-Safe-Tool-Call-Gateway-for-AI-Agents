// Package config loads the gateway's YAML configuration.
//
// Tool policies live here, in the operator's hands, because tool
// annotations sent by a server are untrusted hints.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	"go.yaml.in/yaml/v3"
)

// Policy says how the gateway protects a tool.
type Policy string

const (
	// ReadOnly tools have no side effects and pass straight through.
	ReadOnly Policy = "read_only"
	// AtMostOnce tools are dispatched at most once per operation; an
	// interrupted call becomes "outcome unknown".
	AtMostOnce Policy = "at_most_once"
	// Reconcilable tools are at most once, and a configured read tool can
	// resolve an unknown outcome.
	Reconcilable Policy = "reconcilable"
	// NativeKey tools accept an idempotency key, so the gateway may resend
	// with the same key.
	NativeKey Policy = "native_key"
)

// How a stored result is kept.
const (
	StoreFull = "full"
	StoreHash = "hash"
)

// How calls to tools missing from the config are handled.
const (
	UnknownToolsAtMostOnce = "at_most_once"
	UnknownToolsRefuse     = "refuse"
)

// Config is the gateway configuration.
type Config struct {
	Listen      string `yaml:"listen"`
	Upstream    string `yaml:"upstream"`
	DatabaseURL string `yaml:"database_url"`
	InstanceID  string `yaml:"instance_id"`

	// Lease is how long a claim or dispatch stays owned without a
	// heartbeat before another instance may act on it.
	Lease time.Duration `yaml:"lease"`
	// WaitTimeout is how long a duplicate waits for a running operation
	// before getting a retryable "in progress" result.
	WaitTimeout time.Duration `yaml:"wait_timeout"`
	// UpstreamTimeout bounds one dispatch to the upstream server.
	UpstreamTimeout time.Duration `yaml:"upstream_timeout"`
	// FallbackWindow groups keyless calls with identical arguments.
	FallbackWindow time.Duration `yaml:"fallback_window"`
	// Retention is how long result bodies are kept. The key itself is
	// kept forever so an old key never looks new.
	Retention time.Duration `yaml:"retention"`
	// MaxResultBytes caps a stored result; larger ones keep only a hash.
	MaxResultBytes int `yaml:"max_result_bytes"`
	// UnknownTools is at_most_once (the default) or refuse.
	UnknownTools string `yaml:"unknown_tools"`
	// ReconcileInterval is how often unknown outcomes of reconcilable
	// tools are checked in the background.
	ReconcileInterval time.Duration `yaml:"reconcile_interval"`
	// StoreTimeout bounds each statement sent to Postgres, so a database
	// that stops answering cannot hang a caller.
	StoreTimeout time.Duration `yaml:"store_timeout"`

	Auth            Auth              `yaml:"auth"`
	UpstreamHeaders map[string]string `yaml:"upstream_headers"`
	Tools           map[string]Tool   `yaml:"tools"`
}

// Auth decides who the caller is. With neither option set, every caller is
// the principal "anonymous".
type Auth struct {
	// Tokens maps bearer tokens to principal names.
	Tokens map[string]string `yaml:"tokens"`
	// PrincipalHeader trusts a header set by an authenticating proxy in
	// front of the gateway.
	PrincipalHeader string `yaml:"principal_header"`
}

// Enabled reports whether callers must authenticate.
func (a Auth) Enabled() bool { return len(a.Tokens) > 0 || a.PrincipalHeader != "" }

// Tool is the per-tool policy.
type Tool struct {
	Policy Policy `yaml:"policy"`
	// RequireKey refuses calls without a caller key or run and step IDs.
	RequireKey bool `yaml:"require_key"`
	// KeyArgument names the argument a native_key tool reads its
	// idempotency key from.
	KeyArgument string `yaml:"key_argument"`
	// MaxResends bounds in-request resends for native_key tools.
	MaxResends int `yaml:"max_resends"`
	// StoreResult is full (default) or hash, for sensitive results.
	StoreResult string     `yaml:"store_result"`
	Reconcile   *Reconcile `yaml:"reconcile"`
}

// Reconcile describes the read that confirms a reconcilable tool's outcome.
type Reconcile struct {
	// Tool is the read-only tool to call.
	Tool string `yaml:"tool"`
	// MatchArgument names the original call's argument to look up by.
	MatchArgument string `yaml:"match_argument"`
	// LookupArgument names the read tool's argument; it defaults to
	// MatchArgument.
	LookupArgument string `yaml:"lookup_argument"`
	// FoundField is the boolean in the read's structuredContent that
	// answers "did the original call take effect?". Anything else is
	// inconclusive and the outcome stays unknown.
	FoundField string `yaml:"found_field"`
	// SettleAfter is how long after dispatch a "not found" answer is
	// trusted. Before that the original call could still be in flight.
	SettleAfter time.Duration `yaml:"settle_after"`
}

// Defaults.
const (
	DefaultListen          = ":8080"
	DefaultLease           = 30 * time.Second
	DefaultWaitTimeout     = 5 * time.Second
	DefaultUpstreamTimeout = 60 * time.Second
	DefaultFallbackWindow  = 60 * time.Second
	DefaultRetention       = 30 * 24 * time.Hour
	DefaultMaxResultBytes  = 1 << 20
	DefaultMaxResends      = 2
	DefaultFoundField      = "found"
	DefaultReconcileEvery  = 10 * time.Second
	DefaultStoreTimeout    = 5 * time.Second
)

// Load reads and validates a config file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return Parse(raw)
}

// Parse decodes, fills defaults and validates a config document. Unknown
// fields are errors, so a typo in a policy name cannot silently weaken it.
func Parse(raw []byte) (*Config, error) {
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = DefaultListen
	}
	if c.Lease == 0 {
		c.Lease = DefaultLease
	}
	if c.WaitTimeout == 0 {
		c.WaitTimeout = DefaultWaitTimeout
	}
	if c.UpstreamTimeout == 0 {
		c.UpstreamTimeout = DefaultUpstreamTimeout
	}
	if c.FallbackWindow == 0 {
		c.FallbackWindow = DefaultFallbackWindow
	}
	if c.Retention == 0 {
		c.Retention = DefaultRetention
	}
	if c.MaxResultBytes == 0 {
		c.MaxResultBytes = DefaultMaxResultBytes
	}
	if c.UnknownTools == "" {
		c.UnknownTools = UnknownToolsAtMostOnce
	}
	if c.ReconcileInterval == 0 {
		c.ReconcileInterval = DefaultReconcileEvery
	}
	if c.StoreTimeout == 0 {
		c.StoreTimeout = DefaultStoreTimeout
	}
	for name, t := range c.Tools {
		if t.Policy == "" {
			t.Policy = AtMostOnce
		}
		if t.StoreResult == "" {
			t.StoreResult = StoreFull
		}
		if t.Policy == NativeKey && t.MaxResends == 0 {
			t.MaxResends = DefaultMaxResends
		}
		if r := t.Reconcile; r != nil {
			if r.LookupArgument == "" {
				r.LookupArgument = r.MatchArgument
			}
			if r.FoundField == "" {
				r.FoundField = DefaultFoundField
			}
			if r.SettleAfter == 0 {
				r.SettleAfter = c.UpstreamTimeout + c.Lease
			}
		}
		c.Tools[name] = t
	}
}

// Validate checks the config for mistakes that would weaken a guarantee.
func (c *Config) Validate() error {
	var errs []error
	if c.Upstream == "" {
		errs = append(errs, errors.New("upstream is required"))
	} else if u, err := url.Parse(c.Upstream); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		errs = append(errs, fmt.Errorf("upstream %q is not an http(s) URL", c.Upstream))
	}
	if c.Lease < time.Second/10 {
		errs = append(errs, errors.New("lease must be at least 100ms"))
	}
	if c.WaitTimeout < 0 || c.UpstreamTimeout < 0 || c.FallbackWindow < 0 || c.Retention < 0 || c.ReconcileInterval < 0 {
		errs = append(errs, errors.New("durations must not be negative"))
	}
	if c.StoreTimeout < time.Second/10 {
		errs = append(errs, errors.New("store_timeout must be at least 100ms"))
	}
	if c.UnknownTools != UnknownToolsAtMostOnce && c.UnknownTools != UnknownToolsRefuse {
		errs = append(errs, fmt.Errorf("unknown_tools must be %q or %q", UnknownToolsAtMostOnce, UnknownToolsRefuse))
	}
	if len(c.Auth.Tokens) > 0 && c.Auth.PrincipalHeader != "" {
		errs = append(errs, errors.New("auth: use tokens or principal_header, not both"))
	}
	for token, principal := range c.Auth.Tokens {
		if token == "" || principal == "" {
			errs = append(errs, errors.New("auth: tokens and principals must not be empty"))
		}
	}
	for name, t := range c.Tools {
		errs = append(errs, t.validate(name, c)...)
	}
	return errors.Join(errs...)
}

func (t Tool) validate(name string, c *Config) []error {
	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf("tool %s: "+format, append([]any{name}, args...)...))
	}
	switch t.Policy {
	case ReadOnly, AtMostOnce:
		if t.Reconcile != nil {
			fail("reconcile is only valid for policy %s", Reconcilable)
		}
	case Reconcilable:
		r := t.Reconcile
		if r == nil || r.Tool == "" || r.MatchArgument == "" {
			fail("policy %s needs reconcile.tool and reconcile.match_argument", Reconcilable)
		} else if lookup, ok := c.Tools[r.Tool]; !ok || lookup.Policy != ReadOnly {
			fail("reconcile tool %s must be configured as %s", r.Tool, ReadOnly)
		}
	case NativeKey:
		if t.KeyArgument == "" {
			fail("policy %s needs key_argument", NativeKey)
		}
		if t.MaxResends < 0 {
			fail("max_resends must not be negative")
		}
	default:
		fail("unknown policy %q", t.Policy)
	}
	if t.KeyArgument != "" && t.Policy != NativeKey {
		fail("key_argument is only valid for policy %s", NativeKey)
	}
	if t.StoreResult != StoreFull && t.StoreResult != StoreHash {
		fail("store_result must be %q or %q", StoreFull, StoreHash)
	}
	return errs
}

// ToolPolicy returns the policy for a tool, and false when calls to it must
// be refused. Tools missing from the config get at_most_once, the safest
// policy that still lets them run, unless unknown_tools is refuse.
func (c *Config) ToolPolicy(name string) (Tool, bool) {
	if t, ok := c.Tools[name]; ok {
		return t, true
	}
	if c.UnknownTools == UnknownToolsRefuse {
		return Tool{}, false
	}
	return Tool{Policy: AtMostOnce, StoreResult: StoreFull}, true
}
