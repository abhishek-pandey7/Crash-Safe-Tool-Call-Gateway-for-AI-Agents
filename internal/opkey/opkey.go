// Package opkey derives the identity of a logical tool operation.
//
// A retried tools/call arrives with a new JSON-RPC id, so the id says
// nothing about whether two requests are the same operation. The identity
// comes instead from, in order of preference:
//
//  1. a key the caller sends in namespaced _meta,
//  2. the caller's agent run ID and step ID, plus the arguments hash,
//  3. the arguments hash alone, which only groups calls inside a time window.
//
// Every identity is scoped to the authenticated principal, the upstream and
// the tool, so one caller can never collide with, or replay, another's
// operation.
package opkey

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/canonjson"
)

// Default _meta keys a caller can use to identify an operation. There is no
// standard field yet, so these live under this project's own namespace.
const (
	MetaOperationKey = "io.github.abhishek-pandey7/operationKey"
	MetaRunID        = "io.github.abhishek-pandey7/runId"
	MetaStepID       = "io.github.abhishek-pandey7/stepId"
)

// Source says where an operation's identity came from.
type Source string

const (
	SourceCallerKey Source = "caller_key"
	SourceRunStep   Source = "run_step"
	SourceFallback  Source = "args_window"
)

// Scope is what every key is bound to.
type Scope struct {
	Principal string
	Upstream  string
	Tool      string
}

// Identity is a derived operation identity.
type Identity struct {
	// Base identifies the operation within its scope. Fallback identities
	// share a base across time windows; the store adds a generation.
	Base     string
	Source   Source
	ArgsHash []byte
}

// Windowed reports whether identities with this source only group calls
// inside a configured time window.
func (id Identity) Windowed() bool { return id.Source == SourceFallback }

// Keyed reports whether the caller supplied an explicit identity, which is
// what require_key demands.
func (id Identity) Keyed() bool { return id.Source != SourceFallback }

// Inputs are the parts of a call that can identify it.
type Inputs struct {
	CallerKey string
	RunID     string
	StepID    string
	Arguments json.RawMessage
}

// HashArguments returns the SHA-256 of the canonical form of a call's
// arguments. Absent or null arguments hash like an empty object.
func HashArguments(args json.RawMessage) ([]byte, error) {
	if len(args) == 0 || string(args) == "null" {
		args = json.RawMessage("{}")
	}
	canon, err := canonjson.Canonicalize(args)
	if err != nil {
		return nil, fmt.Errorf("opkey: arguments: %w", err)
	}
	sum := sha256.Sum256(canon)
	return sum[:], nil
}

// Derive computes an operation identity.
func Derive(scope Scope, in Inputs) (Identity, error) {
	argsHash, err := HashArguments(in.Arguments)
	if err != nil {
		return Identity{}, err
	}
	var parts []string
	var source Source
	switch {
	case in.CallerKey != "":
		source = SourceCallerKey
		parts = []string{in.CallerKey}
	case in.RunID != "" && in.StepID != "":
		source = SourceRunStep
		parts = []string{in.RunID, in.StepID, string(argsHash)}
	default:
		source = SourceFallback
		parts = []string{string(argsHash)}
	}
	h := sha256.New()
	for _, p := range append([]string{"v1", string(source), scope.Principal, scope.Upstream, scope.Tool}, parts...) {
		// Length-prefix every field so no two field lists share an encoding.
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(p)))
		h.Write(n[:])
		h.Write([]byte(p))
	}
	return Identity{
		Base:     "op_" + hex.EncodeToString(h.Sum(nil))[:40],
		Source:   source,
		ArgsHash: argsHash,
	}, nil
}

// Key joins a base and a generation into the stored operation key.
func Key(base string, generation int) string {
	return fmt.Sprintf("%s.%d", base, generation)
}
