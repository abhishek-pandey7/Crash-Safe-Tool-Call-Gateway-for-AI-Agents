// Package store keeps the durable record of every effectful tool operation
// in Postgres.
//
// Each state change is one conditional UPDATE whose WHERE clause states the
// only situation in which the change is allowed. Two instances racing for
// the same change cannot both succeed: Postgres serialises the row and the
// loser's UPDATE matches nothing. Leases are compared against the database
// clock, so instance clocks never matter.
//
// The central rule is the dispatch gate (Gate and Redispatch): an operation
// moves to dispatching, with a fence that only its owner holds, before any
// byte of the call leaves the gateway.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// State is an operation's position in its lifecycle.
type State string

const (
	Claimed       State = "claimed"
	Dispatching   State = "dispatching"
	AwaitingInput State = "awaiting_input"
	Completed     State = "completed"
	NotExecuted   State = "not_executed"
	Unknown       State = "unknown"
	Released      State = "released"
)

// Result kinds stored with an operation.
const (
	KindResult        = "result"
	KindError         = "error"
	KindInputRequired = "input_required"
)

// ErrNotFound is returned by Get when no operation has the key.
var ErrNotFound = errors.New("store: operation not found")

// Operation is one row of tool_operations, read together with the database
// clock.
type Operation struct {
	Key          string
	KeyBase      string
	Generation   int
	Source       string
	Principal    string
	Upstream     string
	Tool         string
	Policy       string
	ArgsHash     []byte
	State        State
	Owner        string
	Fence        int64
	LeaseUntil   *time.Time
	Attempts     int
	ResultKind   string
	Result       []byte
	ResultHash   []byte
	ResultPurged bool
	Detail       json.RawMessage
	CreatedAt    time.Time
	DispatchedAt *time.Time
	FinishedAt   *time.Time
	// ReconcileRef is the raw JSON value a reconciliation read looks up.
	ReconcileRef json.RawMessage
	// Now is the database clock when the row was read.
	Now time.Time
}

// LeaseExpired reports whether the owner's lease had lapsed when the row
// was read.
func (o *Operation) LeaseExpired() bool {
	return o.LeaseUntil == nil || !o.LeaseUntil.After(o.Now)
}

// Active reports whether some instance may currently be working on the
// operation.
func (o *Operation) Active() bool {
	return o.State == Claimed || o.State == Dispatching
}

// Terminal reports whether the operation has a final outcome.
func (o *Operation) Terminal() bool {
	return o.State == Completed
}

// windowAnchor is the last moment the operation was worked on.
func (o *Operation) windowAnchor() time.Time {
	switch {
	case o.FinishedAt != nil:
		return *o.FinishedAt
	case o.DispatchedAt != nil:
		return *o.DispatchedAt
	default:
		return o.CreatedAt
	}
}

// Store is the Postgres-backed operation store.
type Store struct {
	pool *pgxpool.Pool
}

// New wraps an existing pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Open connects to Postgres and applies migrations.
func Open(ctx context.Context, url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	return OpenConfig(ctx, cfg)
}

// OpenConfig connects with a parsed pool config and applies migrations.
func OpenConfig(ctx context.Context, cfg *pgxpool.Config) (*Store, error) {
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Pool exposes the connection pool.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close closes the pool.
func (s *Store) Close() { s.pool.Close() }

// Ping checks that the database is reachable.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

const columns = `op_key, key_base, generation, key_source, principal, upstream, tool, policy,
	args_hash, state, owner, fence, lease_until, attempts, result_kind, result, result_hash,
	result_purged, last_detail, created_at, dispatched_at, finished_at, reconcile_ref, now()`

func scanOp(row pgx.Row) (*Operation, error) {
	var (
		o          Operation
		state      string
		owner      *string
		resultKind *string
	)
	err := row.Scan(&o.Key, &o.KeyBase, &o.Generation, &o.Source, &o.Principal, &o.Upstream,
		&o.Tool, &o.Policy, &o.ArgsHash, &state, &owner, &o.Fence, &o.LeaseUntil, &o.Attempts,
		&resultKind, &o.Result, &o.ResultHash, &o.ResultPurged, &o.Detail, &o.CreatedAt,
		&o.DispatchedAt, &o.FinishedAt, &o.ReconcileRef, &o.Now)
	if err != nil {
		return nil, err
	}
	o.State = State(state)
	if owner != nil {
		o.Owner = *owner
	}
	if resultKind != nil {
		o.ResultKind = *resultKind
	}
	return &o, nil
}

// one runs a statement returning at most one operation. A statement that
// matched no row reports ok=false and no error.
func (s *Store) one(ctx context.Context, sql string, args ...any) (*Operation, bool, error) {
	op, err := scanOp(s.pool.QueryRow(ctx, sql, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("store: %w", err)
	}
	return op, true, nil
}

// detailJSON encodes an event detail as a jsonb parameter.
func detailJSON(d map[string]any) any {
	if d == nil {
		return nil
	}
	raw, err := json.Marshal(d)
	if err != nil {
		return nil
	}
	return string(raw)
}

func millis(d time.Duration) int64 { return d.Milliseconds() }

// Get reads an operation.
func (s *Store) Get(ctx context.Context, key string) (*Operation, error) {
	op, ok, err := s.one(ctx, `SELECT `+columns+` FROM tool_operations WHERE op_key = $1`, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotFound
	}
	return op, nil
}

// ClaimParams describes a new operation.
type ClaimParams struct {
	Base      string
	Source    string
	Principal string
	Upstream  string
	Tool      string
	Policy    string
	ArgsHash  []byte
	Owner     string
	Lease     time.Duration
	// Window, when non-zero, lets a finished operation's base start a new
	// generation once the window has passed. Keyed operations use 0.
	Window time.Duration
	// ReconcileRef, for reconcilable tools, is the raw JSON value of the
	// argument a reconciliation read looks up.
	ReconcileRef json.RawMessage
}

// Claim records a new operation owned by p.Owner, or returns the existing
// one. created is true only for the instance whose insert won.
func (s *Store) Claim(ctx context.Context, p ClaimParams) (op *Operation, created bool, err error) {
	for range 8 {
		generation := 0
		if p.Window > 0 {
			latest, ok, err := s.one(ctx, `SELECT `+columns+` FROM tool_operations
				WHERE key_base = $1 ORDER BY generation DESC LIMIT 1`, p.Base)
			if err != nil {
				return nil, false, err
			}
			if ok {
				if latest.Active() || latest.Now.Sub(latest.windowAnchor()) < p.Window {
					return latest, false, nil
				}
				generation = latest.Generation + 1
			}
		}
		key := fmt.Sprintf("%s.%d", p.Base, generation)
		var ref any
		if len(p.ReconcileRef) > 0 {
			ref = string(p.ReconcileRef)
		}
		op, created, err := s.one(ctx, `INSERT INTO tool_operations
			(op_key, key_base, generation, key_source, principal, upstream, tool, policy,
			 args_hash, state, owner, lease_until, last_instance, reconcile_ref)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'claimed', $10,
			        now() + $11::bigint * interval '1 millisecond', $10, $12::jsonb)
			ON CONFLICT DO NOTHING
			RETURNING `+columns,
			key, p.Base, generation, p.Source, p.Principal, p.Upstream, p.Tool, p.Policy,
			p.ArgsHash, p.Owner, millis(p.Lease), ref)
		if err != nil {
			return nil, false, err
		}
		if created {
			return op, true, nil
		}
		if p.Window > 0 {
			continue // another instance started this generation; re-read it
		}
		existing, err := s.Get(ctx, key)
		if err != nil {
			return nil, false, err
		}
		return existing, false, nil
	}
	return nil, false, errors.New("store: claim did not settle")
}

// Acquire takes ownership of an operation nobody is working on: a claim
// whose lease lapsed before dispatch, or an operation proven not to have
// run. The fence increases, so the previous owner can no longer pass the
// gate.
func (s *Store) Acquire(ctx context.Context, key, owner string, lease time.Duration) (*Operation, bool, error) {
	return s.one(ctx, `UPDATE tool_operations SET
			state = 'claimed', owner = $2, fence = fence + 1,
			lease_until = now() + $3::bigint * interval '1 millisecond',
			finished_at = NULL, result_kind = NULL, result = NULL, result_hash = NULL,
			last_instance = $2, last_detail = NULL, updated_at = now()
		WHERE op_key = $1
		  AND ((state = 'claimed' AND lease_until < now()) OR state IN ('released', 'not_executed'))
		RETURNING `+columns, key, owner, millis(lease))
}

// Gate is the dispatch gate. It succeeds for exactly one caller: the owner
// holding the current fence of a claimed operation. Nothing may be sent
// upstream unless it returned ok.
func (s *Store) Gate(ctx context.Context, key, owner string, fence int64, lease time.Duration) (*Operation, bool, error) {
	return s.one(ctx, `UPDATE tool_operations SET
			state = 'dispatching', dispatched_at = now(), attempts = attempts + 1,
			lease_until = now() + $4::bigint * interval '1 millisecond',
			last_instance = $2, last_detail = NULL, updated_at = now()
		WHERE op_key = $1 AND state = 'claimed' AND fence = $3 AND owner = $2
		RETURNING `+columns, key, owner, fence, millis(lease))
}

// RedispatchFrom selects which operations Redispatch may restart.
type RedispatchFrom int

const (
	// FromStale restarts an unknown operation, or a dispatch whose owner's
	// lease lapsed. Only valid for tools that honour an idempotency key.
	FromStale RedispatchFrom = iota
	// FromAwaitingInput continues a multi round-trip request.
	FromAwaitingInput
)

// Redispatch claims and gates in one step, with a new fence, for the cases
// where sending again is safe.
func (s *Store) Redispatch(ctx context.Context, key, owner string, lease time.Duration, from RedispatchFrom, detail map[string]any) (*Operation, bool, error) {
	var where string
	switch from {
	case FromStale:
		where = `(state = 'unknown' OR (state = 'dispatching' AND lease_until < now()))`
	case FromAwaitingInput:
		where = `state = 'awaiting_input'`
	default:
		return nil, false, fmt.Errorf("store: unknown redispatch source %d", from)
	}
	return s.one(ctx, `UPDATE tool_operations SET
			state = 'dispatching', owner = $2, fence = fence + 1,
			lease_until = now() + $3::bigint * interval '1 millisecond',
			dispatched_at = now(), attempts = attempts + 1, finished_at = NULL,
			result_kind = NULL, result = NULL, result_hash = NULL,
			last_instance = $2, last_detail = $4::jsonb, updated_at = now()
		WHERE op_key = $1 AND `+where+`
		RETURNING `+columns, key, owner, millis(lease), detailJSON(detail))
}

// Heartbeat extends the owner's lease. It returns false once the owner has
// lost the operation.
func (s *Store) Heartbeat(ctx context.Context, key, owner string, fence int64, lease time.Duration) (bool, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE tool_operations
		SET lease_until = now() + $4::bigint * interval '1 millisecond'
		WHERE op_key = $1 AND owner = $2 AND fence = $3 AND state IN ('claimed', 'dispatching')`,
		key, owner, fence, millis(lease))
	if err != nil {
		return false, fmt.Errorf("store: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// Release gives up a claim that was never dispatched.
func (s *Store) Release(ctx context.Context, key, owner string, fence int64, detail map[string]any) (bool, error) {
	_, ok, err := s.one(ctx, `UPDATE tool_operations SET
			state = 'released', lease_until = NULL, finished_at = now(),
			last_instance = $2, last_detail = $4::jsonb, updated_at = now()
		WHERE op_key = $1 AND state = 'claimed' AND owner = $2 AND fence = $3
		RETURNING `+columns, key, owner, fence, detailJSON(detail))
	return ok, err
}

// Outcome is a final response to record.
type Outcome struct {
	Kind string // KindResult or KindError
	// Body is the raw JSON of the response's result or error member. It is
	// nil when only the hash is kept.
	Body   []byte
	Hash   []byte
	Detail map[string]any
}

// Complete records a final response. A response is the truth whoever
// receives it, so it is accepted from any dispatch of the operation and
// also upgrades an unknown outcome. The first response recorded wins.
func (s *Store) Complete(ctx context.Context, key, instance string, out Outcome) (*Operation, bool, error) {
	return s.one(ctx, `UPDATE tool_operations SET
			state = 'completed', lease_until = NULL, finished_at = now(),
			result_kind = $3, result = $4, result_hash = $5,
			last_instance = $2, last_detail = $6::jsonb, updated_at = now()
		WHERE op_key = $1 AND state IN ('dispatching', 'unknown')
		RETURNING `+columns, key, instance, out.Kind, out.Body, out.Hash, detailJSON(out.Detail))
}

// AwaitInput records an input_required interim result for the dispatch
// holding fence.
func (s *Store) AwaitInput(ctx context.Context, key, instance string, fence int64, interim []byte) (bool, error) {
	_, ok, err := s.one(ctx, `UPDATE tool_operations SET
			state = 'awaiting_input', lease_until = NULL,
			result_kind = 'input_required', result = $4,
			last_instance = $2, last_detail = NULL, updated_at = now()
		WHERE op_key = $1 AND state IN ('dispatching', 'unknown') AND fence = $3
		RETURNING `+columns, key, instance, fence, interim)
	return ok, err
}

// MarkNotExecuted records that the dispatch holding fence provably sent
// nothing upstream.
func (s *Store) MarkNotExecuted(ctx context.Context, key, instance string, fence int64, detail map[string]any) (bool, error) {
	_, ok, err := s.one(ctx, `UPDATE tool_operations SET
			state = 'not_executed', lease_until = NULL, finished_at = now(),
			last_instance = $2, last_detail = $4::jsonb, updated_at = now()
		WHERE op_key = $1 AND state IN ('dispatching', 'unknown') AND fence = $3
		RETURNING `+columns, key, instance, fence, detailJSON(detail))
	return ok, err
}

// MarkUnknown records that the dispatch holding fence may or may not have
// taken effect.
func (s *Store) MarkUnknown(ctx context.Context, key, instance string, fence int64, detail map[string]any) (bool, error) {
	_, ok, err := s.one(ctx, `UPDATE tool_operations SET
			state = 'unknown', lease_until = NULL,
			last_instance = $2, last_detail = $4::jsonb, updated_at = now()
		WHERE op_key = $1 AND state = 'dispatching' AND fence = $3
		RETURNING `+columns, key, instance, fence, detailJSON(detail))
	return ok, err
}

// ExpireDispatch marks a dispatch whose owner's lease lapsed as unknown.
// Any instance may do this; nobody resends.
func (s *Store) ExpireDispatch(ctx context.Context, key, instance string) (*Operation, bool, error) {
	return s.one(ctx, `UPDATE tool_operations SET
			state = 'unknown', lease_until = NULL,
			last_instance = $2, last_detail = '{"reason":"owner lease expired during dispatch"}',
			updated_at = now()
		WHERE op_key = $1 AND state = 'dispatching' AND lease_until < now()
		RETURNING `+columns, key, instance)
}

// Resolution is an outcome established from outside the gateway: by an
// operator, or by a conclusive reconciliation read.
type Resolution struct {
	State    State // Completed or NotExecuted
	Kind     string
	Body     []byte
	Hash     []byte
	Evidence map[string]any
}

// Resolve settles an unknown operation, or a dispatch whose owner is gone.
func (s *Store) Resolve(ctx context.Context, key, by string, r Resolution) (*Operation, bool, error) {
	if r.State != Completed && r.State != NotExecuted {
		return nil, false, fmt.Errorf("store: cannot resolve to %s", r.State)
	}
	if len(r.Evidence) == 0 {
		return nil, false, errors.New("store: a resolution needs evidence")
	}
	var kind any
	if r.Kind != "" {
		kind = r.Kind
	}
	return s.one(ctx, `UPDATE tool_operations SET
			state = $3::op_state, lease_until = NULL, finished_at = now(),
			result_kind = $4, result = $5, result_hash = $6,
			last_instance = $2, last_detail = $7::jsonb, updated_at = now()
		WHERE op_key = $1
		  AND (state = 'unknown' OR (state = 'dispatching' AND lease_until < now()))
		RETURNING `+columns, key, by, string(r.State), kind, r.Body, r.Hash, detailJSON(r.Evidence))
}

// Sweep settles operations whose owners vanished: claims are released,
// because nothing was sent, and dispatches become unknown.
func (s *Store) Sweep(ctx context.Context, instance string) (released, unknown int64, err error) {
	tag, err := s.pool.Exec(ctx, `UPDATE tool_operations SET
			state = 'released', lease_until = NULL, finished_at = now(),
			last_instance = $1, last_detail = '{"reason":"claim lease expired before dispatch"}',
			updated_at = now()
		WHERE state = 'claimed' AND lease_until < now()`, instance)
	if err != nil {
		return 0, 0, fmt.Errorf("store: %w", err)
	}
	released = tag.RowsAffected()
	tag, err = s.pool.Exec(ctx, `UPDATE tool_operations SET
			state = 'unknown', lease_until = NULL,
			last_instance = $1, last_detail = '{"reason":"owner lease expired during dispatch"}',
			updated_at = now()
		WHERE state = 'dispatching' AND lease_until < now()`, instance)
	if err != nil {
		return released, 0, fmt.Errorf("store: %w", err)
	}
	return released, tag.RowsAffected(), nil
}

// PurgeResults drops stored result bodies older than retention. The row and
// the result hash stay, so the key keeps answering "completed".
func (s *Store) PurgeResults(ctx context.Context, retention time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE tool_operations
		SET result = NULL, result_purged = true
		WHERE result IS NOT NULL AND state = 'completed'
		  AND finished_at < now() - $1::bigint * interval '1 millisecond'`, millis(retention))
	if err != nil {
		return 0, fmt.Errorf("store: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ListFilter selects operations for List.
type ListFilter struct {
	States []State
	Tool   string
	Limit  int
}

// List returns the most recently changed operations matching f.
func (s *Store) List(ctx context.Context, f ListFilter) ([]*Operation, error) {
	if f.Limit <= 0 {
		f.Limit = 100
	}
	states := make([]string, len(f.States))
	for i, st := range f.States {
		states[i] = string(st)
	}
	rows, err := s.pool.Query(ctx, `SELECT `+columns+` FROM tool_operations
		WHERE (cardinality($1::text[]) = 0 OR state::text = ANY($1))
		  AND ($2 = '' OR tool = $2)
		ORDER BY updated_at DESC LIMIT $3`, states, f.Tool, f.Limit)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	defer rows.Close()
	var ops []*Operation
	for rows.Next() {
		op, err := scanOp(rows)
		if err != nil {
			return nil, fmt.Errorf("store: %w", err)
		}
		ops = append(ops, op)
	}
	return ops, rows.Err()
}

// Reconcilable returns unknown operations that carry a reconciliation
// reference, least recently changed first.
func (s *Store) Reconcilable(ctx context.Context, limit int) ([]*Operation, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+columns+` FROM tool_operations
		WHERE state = 'unknown' AND reconcile_ref IS NOT NULL
		ORDER BY updated_at LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	defer rows.Close()
	var ops []*Operation
	for rows.Next() {
		op, err := scanOp(rows)
		if err != nil {
			return nil, fmt.Errorf("store: %w", err)
		}
		ops = append(ops, op)
	}
	return ops, rows.Err()
}

// Event is one entry of an operation's audit trail.
type Event struct {
	ID       int64
	At       time.Time
	Instance string
	From     State
	To       State
	Fence    int64
	Detail   json.RawMessage
}

// Events returns an operation's audit trail, oldest first.
func (s *Store) Events(ctx context.Context, key string) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, at, coalesce(instance, ''), coalesce(from_state::text, ''),
			to_state::text, fence, detail
		FROM operation_events WHERE op_key = $1 ORDER BY id`, key)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	defer rows.Close()
	var events []Event
	for rows.Next() {
		var e Event
		var from, to string
		if err := rows.Scan(&e.ID, &e.At, &e.Instance, &from, &to, &e.Fence, &e.Detail); err != nil {
			return nil, fmt.Errorf("store: %w", err)
		}
		e.From, e.To = State(from), State(to)
		events = append(events, e)
	}
	return events, rows.Err()
}
