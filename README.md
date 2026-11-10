# Crash-Safe Tool-Call Gateway for AI Agents

A Go gateway that stops retried or interrupted AI agent tool calls from repeating real-world actions, such as refunds, emails or new tickets.

> **Status: crash and multi-instance milestones working.** The gateway runs end to end against Postgres and a fake tool server. Its tests kill gateway processes at every step and at random under load, and check each outcome against the tool's own ledger. Reconciliation, an operator command and published measurements are not done yet.

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
| Kill-point crash matrix: the gateway process killed at each of 7 steps, retried through a second instance | Done for `at_most_once` and `native_key`, `e2e/crash_test.go`. A planted resend bug fails every row it can reach |
| Several instances: takeover, fencing of paused instances, racing duplicates | Done, `e2e/multi_test.go` |
| Random kills under load (3 gateway processes, about 450 operations and 15 kills per seed) | Done. With seeds 1 to 3 there were no invariant violations, and 6 to 8% of `at_most_once` operations ended "unknown". Machine-dependent, not a published measurement |
| `reconcilable` tools | Accepted in config but handled as `at_most_once` for now |
| Operator command to resolve unknown outcomes | Store support only |
| Docker Compose, CI, published measurements | Not yet |

## Running it

```sh
go build -o bin/gateway ./cmd/gateway
cp gateway.example.yaml gateway.yaml   # set upstream and tool policies
GATEWAY_DATABASE_URL=postgres://... bin/gateway -config gateway.yaml
```

Point MCP clients at the gateway instead of the server. `/-/healthz` and `/-/readyz` report liveness and database reachability.

A caller can give an operation a stable identity in `_meta`, using `io.github.abhishek-pandey7/operationKey`, or `runId` plus `stepId` in the same namespace. Without one, identical calls are grouped only within `fallback_window`.

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
| `internal/gateway` | Request routing, the `tools/call` pipeline, dispatch, responses |
| `internal/store` | Postgres operation store and migrations |
| `internal/mcp` | JSON-RPC, 2026-07-28 request headers, SSE |
| `internal/opkey`, `internal/canonjson` | Operation identity and argument hashing |
| `internal/config` | YAML config and tool policies |
| `internal/faketool` | Fake MCP server with an effect ledger and fault injection |
| `internal/killpoint` | Kill points, compiled in only with `-tags killpoints` |
| `e2e` | End-to-end tests against the ledger |
