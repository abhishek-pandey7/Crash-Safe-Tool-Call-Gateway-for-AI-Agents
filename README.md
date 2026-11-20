# Crash-Safe Tool-Call Gateway for AI Agents

A Go gateway that stops retried or interrupted AI agent tool calls from repeating real-world actions, such as refunds, emails or new tickets.

> **Status: all four tool policies working.** The gateway runs end to end against Postgres and a fake tool server. Its tests kill gateway processes at every step and at random under load, and check each outcome against the tool's own ledger. Postgres outages are tested too. Docker Compose, CI, published measurements and a check against a real MCP server are not done yet.

## The problem

An agent calls a tool. The action succeeds, but the response gets lost. The agent sees a timeout and tries again, so the action happens twice.

The Model Context Protocol (MCP) defines no rules for this case, and a retry carries a new request ID, so a server can't recognise it. Contributors have [reproduced the duplicate](https://github.com/modelcontextprotocol/modelcontextprotocol/issues/3394), and very few MCP tools accept an idempotency key.

## What it does

The gateway sits between unmodified MCP clients and servers:

1. **Records each operation in Postgres before forwarding it.**
2. **Lets only one instance send it**, using a conditional database update.
3. **Replays the stored result** when the same operation is retried.
4. **Reports "outcome unknown"** when it can't tell whether the action happened, and doesn't run it again.

Several instances can share one database and handle calls concurrently, so any instance can crash without losing track of an operation.

## What it can and can't guarantee

| Tool type | Target guarantee |
|---|---|
| Read-only | Passes straight through |
| No way to deduplicate or check downstream (default) | Sent **at most once**; an interrupted call becomes "outcome unknown" |
| A separate read can confirm the outcome | At most once, and resolved automatically when the read is conclusive |
| Accepts an idempotency key | **Effectively once**, if the tool honours the key |

No gateway can provide exactly-once execution with a tool that doesn't cooperate. After a lost response, "it ran" and "it did not run" look identical from outside. It also can't recognise a reworded call as the same operation unless the caller supplies a stable key.

## How it will be tested

Each test compares what actually happened (from the fake tool's own ledger) with what the gateway recorded and what the client was told. The suite kills the gateway at every step, pauses instances past their leases, drops responses and takes Postgres down. It all runs locally in Docker for free, with no LLM API key, GPU or cloud account.

## Progress

| Area | State |
|---|---|
| Duplicate reproduced with no gateway (lost response, then retry gives 2 refunds) | Done, `e2e/baseline_test.go` |
| Durable operation store: claims, leases, fencing, dispatch gate, audit trail | Done, tested against Postgres |
| Gateway: replay, outcome unknown, in progress, key conflicts, required keys, principal scoping | Done, tested end to end |
| Streamed progress, client disconnects, multi round-trip requests, Tasks | Done for one instance |
| `native_key` tools: key injection and safe resend | Done for one instance |
| Go's HTTP client never resends a dispatched call | Done, verified that the test fails if the protection is removed |
| Kill-point crash matrix: the gateway process killed at each of 7 steps, retried through a second instance | Done for all three effectful policies, `e2e/crash_test.go`. A planted resend bug fails every row it can reach. `reconcilable` and `native_key` end every row with exactly one effect |
| Several instances: takeover, fencing of paused instances, racing duplicates | Done, `e2e/multi_test.go` |
| Random kills under load (3 gateway processes, about 450 operations and 15 kills per seed) | Done. With seeds 1 to 3 there were no invariant violations, and 6 to 8% of `at_most_once` operations ended "unknown". Machine-dependent, not a published measurement |
| `reconcilable` tools: a read settles unknown outcomes, inline, on retry and in the background | Done, `e2e/reconcile_test.go`. "Not found" is only trusted after `settle_after`, and a planted bug that trusts it early is caught |
| Operator command, `gatewayctl` | Done: list, show (with audit trail), resolve with required evidence, sweep, purge |
| Result retention and hash-only results | Done. Expired results keep their key, so the tool never re-runs |
| Postgres outages: refused connections and partitions, before and during a call, and a crash while Postgres is down | Done, `e2e/outage_test.go`. Calls are refused before anything is sent, read-only tools keep working, and a lost write catches up when Postgres returns. Without a statement timeout a partitioned database hung callers; `store_timeout` now bounds every statement |
| Docker Compose with Toxiproxy, CI, published measurements, real-server check | Not yet |

## Running it

```sh
go build -o bin/gateway ./cmd/gateway
cp gateway.example.yaml gateway.yaml   # set upstream and tool policies
GATEWAY_DATABASE_URL=postgres://... bin/gateway -config gateway.yaml
```

Point MCP clients at the gateway instead of the server. `/-/healthz` and `/-/readyz` report liveness and database reachability.

A caller can give an operation a stable identity in `_meta`, using `io.github.abhishek-pandey7/operationKey`, or `runId` plus `stepId` in the same namespace. Without one, identical calls are grouped only within `fallback_window`.

### Reconcilable tools

A `reconcilable` tool names a read-only tool that answers one question: did this call take effect? The read is called with the original call's `match_argument` value. It is trusted only when it returns a boolean in `structuredContent` (`found` by default).

- **`true`** settles the operation as completed straight away.
- **`false`** is trusted only after `settle_after` has passed since dispatch, because until then the original call could still be in flight. After that, a retry runs the call once.
- **Anything else**, such as an error, a missing field or a non-boolean, leaves the outcome unknown. A failed lookup is never treated as proof that nothing happened.

The read uses the `upstream_headers` credentials, never a caller's.

`settle_after` assumes the gateway does not stay frozen mid-send for longer than that period. Its default is `upstream_timeout` plus `lease`.

### Resolving unknown outcomes

```sh
gatewayctl list -state unknown
gatewayctl show op_...
gatewayctl resolve op_... -outcome completed -evidence "refund rf_9 visible in the provider dashboard"
gatewayctl resolve op_... -outcome not_executed -evidence "no refund for order-1001 in the provider dashboard"
```

`resolve` only applies to unknown operations, or to dispatches whose owner has gone, and it requires evidence, which goes into the audit trail. After `completed`, retries are told that an operator confirmed the call. After `not_executed`, the next retry runs it.

## Testing

```sh
./scripts/test-db.sh          # or scripts/test-db.ps1; starts Postgres in Docker
export GATEWAY_TEST_DATABASE_URL=...   # printed by the script
go test ./...
```

Tests that need Postgres are skipped when `GATEWAY_TEST_DATABASE_URL` is unset. Each test gets its own schema. The crash tests build the gateway with `-tags killpoints` and run it as separate processes. `GATEWAY_CHAOS_SEEDS=1,2,3,4,5` runs the random-kill test with more seeds, and `-short` skips it.

## Layout

| Path | What it is |
|---|---|
| `cmd/gateway` | The gateway binary |
| `cmd/gatewayctl` | The operator command |
| `internal/operator` | Recording outcomes a person verified |
| `internal/gateway` | Request routing, the `tools/call` pipeline, dispatch, responses |
| `internal/store` | Postgres operation store and migrations |
| `internal/mcp` | JSON-RPC, 2026-07-28 request headers, SSE |
| `internal/opkey`, `internal/canonjson` | Operation identity and argument hashing |
| `internal/config` | YAML config and tool policies |
| `internal/faketool` | Fake MCP server with an effect ledger and fault injection |
| `internal/killpoint` | Kill points, compiled in only with `-tags killpoints` |
| `internal/faultproxy` | TCP proxy that cuts or stalls Postgres during tests |
| `e2e` | End-to-end tests against the ledger |
