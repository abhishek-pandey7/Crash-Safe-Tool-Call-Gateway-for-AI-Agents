# Crash-Safe Tool-Call Gateway for AI Agents

A Go gateway that stops retried or interrupted AI agent tool calls from repeating real-world actions, such as refunds, emails or new tickets.

> **Status: working end to end.** All four tool policies work. The tests kill gateway processes at every step and at random under load, cut Postgres off, and check every outcome against the tool's own ledger. CI runs them with the race detector, a Docker Compose demo runs against a live stack, the gateway works between the official MCP Go SDK's client and server, [measurements](#measurements) are published, and it has been [compared with similar tools](#comparison-with-similar-tools).

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
| Prometheus metrics at `/-/metrics` | Done, `e2e/metrics_test.go`. Tool labels are limited to configured tools, so callers cannot create unbounded series |
| CI on GitHub Actions | Done, `.github/workflows/ci.yml`: gofmt, vet, build and the full suite with the race detector against Postgres on every push. On `main`, the random-kill test also runs with five seeds |
| Docker Compose demo with Toxiproxy | Done, `compose.yaml` and `scripts/demo.sh`. CI builds the images, starts the stack and runs the demo, which fails if any effect count is wrong |
| Interop with the official MCP Go SDK (v1.8.0) on both sides | Done, `interop/`: tools/list, calls, replays, "outcome unknown" and reconciliation, with SSE and JSON responses, on protocol 2026-07-28. A baseline without the gateway shows the SDK client does not retry by itself, and an application retry after a lost response refunds twice |
| Published measurements | Done, `cmd/measure`; see [Measurements](#measurements) |
| Comparison with similar tools | Done, `compare/`; see [Comparison with similar tools](#comparison-with-similar-tools) |

## Trying it with Docker

```sh
docker compose up -d --build
./scripts/demo.sh            # on Windows, run it from Git Bash
docker compose down -v
```

The stack runs two gateway instances sharing one Postgres, in front of a fake tool server whose ledger records every effect that really happened. Toxiproxy sits on both the gateways' database connection and their upstream connection. The demo shows four cases and checks each count:

| Case | What happens | Effects |
|---|---|---|
| No gateway, refund response lost, agent retries | The refund runs twice | 2 |
| Email response lost, retried through the other instance | Both attempts get "outcome unknown"; nothing is resent | 1 |
| Refund response lost, retried through the other instance | A read confirms the refund, and both attempts get the confirmed result | 1 |
| Postgres cut off, then restored | Nothing is sent while it is down; the retry runs once | 0, then 1 |

Host ports default to 8080 and 8081 (gateways), 8090 (fake tool server) and 8474 (Toxiproxy), all on 127.0.0.1. If one is taken, set `GATEWAY_A_PORT`, `GATEWAY_B_PORT`, `FAKETOOL_PORT` or `TOXIPROXY_PORT` for both `docker compose` and the demo. `docker compose --profile metrics up -d` adds Prometheus on port 9090.

## Measurements

Lost responses on 10% of first attempts, 1,000 operations per row, 16 concurrent callers. The direct caller retries after a dropped connection, the way an agent does. Callers through the gateway also retry whenever the gateway says retrying is safe. The tool's ledger counts what really happened.

| Path | Policy | Lost responses | Duplicate effects | Told "unknown" | Told it ran | Wrong answers |
|---|---|---|---|---|---|---|
| Direct, agent retries | none | 105 | **105** | 0 | 1,000 | 0 |
| Through the gateway | `at_most_once` | 105 | **0** | 105 | 895 | 0 |
| Through the gateway | `reconcilable` | 105 | **0** | 0 | 1,000 | 0 |
| Through the gateway | `native_key` | 105 | **0** | 0 | 1,000 | 0 |

Every lost response became a duplicate without the gateway, and none did with it. A "wrong answer" would be a caller told a call ran when it had no effect, or the reverse. For `at_most_once` tools, each lost response becomes "unknown", because nothing can tell those calls apart from calls that never ran.

Latency and throughput, with no faults and a new operation on every call:

| Path | Concurrent callers | p50 | p99 | Calls per second |
|---|---|---|---|---|
| One Postgres commit, for reference | 1 | 2.17 ms | 4.63 ms | 438 |
| Direct to the tool | 1 | 0.26 ms | 1.27 ms | 2,899 |
| Direct to the tool | 16 | 1.36 ms | 6.58 ms | 9,046 |
| Through the gateway | 1 | 7.65 ms | 21.77 ms | 110 |
| Through the gateway | 16 | 9.40 ms | 38.87 ms | 1,539 |

The gateway adds about 7.4 ms at p50 with one caller, close to three Postgres commits: the claim, the dispatch gate and the outcome are each written durably before the next step. That is the price of surviving a crash at any point. It is small next to a model call or most real tools, but it is not free.

These numbers come from one machine: Docker Desktop (WSL2) on Windows 11 with 16 CPUs, Go 1.27.4 and Postgres 17.11 in Docker with default durability settings, measured on 20 November 2026. They are not a benchmark of anything else. To reproduce them:

```sh
./scripts/test-db.sh   # Postgres in Docker, as for the tests
docker run --rm --network container:crashsafe-pg -v "$PWD:/src" -w /src golang:1.27 \
  go run ./cmd/measure -db "postgres://postgres:postgres@127.0.0.1:5432/gateway?sslmode=disable"
```

On Linux, `go run ./cmd/measure -db "$GATEWAY_TEST_DATABASE_URL"` works directly. On Windows, run it in a Linux container as above: Go's clock there moves in steps of roughly 0.3 to 1 ms, which turns sub-millisecond times into zeros. Raw results go to `results/measurements.json`.

## Comparison with similar tools

The same three failures, through mcp-idempotent, once-gate, agentsafe-mcp and this gateway. Refunds are counted by the fake tool's own ledger.

| Layer | Tool dies before answering | Layer killed before answering | Agent times out and retries |
|---|---|---|---|
| None | **2** | n/a | **2** |
| mcp-idempotent, memory store (its default) | **2** | **2** | 1 |
| mcp-idempotent, Redis store | 1 | 1 | 1 |
| once-gate 0.2.1 | 1 | 1 | 1 |
| agentsafe-mcp v0.6.0 | 1 | 1 | 1 |
| This gateway, `at_most_once` | 1 | 1 | 1 |
| This gateway, `reconcilable` | 1 | 1 | 1 |

Apart from mcp-idempotent's default memory store, every layer prevented the duplicates. They differ in what the agent is told:
- **once-gate and agentsafe-mcp:** a person has to settle the uncertain cases.
- **mcp-idempotent's Redis store:** the agent can be left with no answer, or a timeout error.
- **This gateway:** it says "outcome unknown", or with a `reconcilable` tool it confirms the result through a lookup without a person.

The others are single-process stdio proxies, with strengths these scenarios don't test, such as per-call approvals. [`compare/README.md`](compare/README.md) has the full answers, versions, configurations, limitations and the command to rerun it.

## Running it

```sh
go build -o bin/gateway ./cmd/gateway
cp gateway.example.yaml gateway.yaml   # set upstream and tool policies
GATEWAY_DATABASE_URL=postgres://... bin/gateway -config gateway.yaml
```

Point MCP clients at the gateway instead of the server. `/-/healthz` and `/-/readyz` report liveness and database reachability, and `/-/metrics` serves Prometheus metrics. These three endpoints need no credentials, so keep them off the public network.

A caller can give an operation a stable identity in `_meta`, using `io.github.abhishek-pandey7/operationKey`, or `runId` plus `stepId` in the same namespace. Without one, identical calls are grouped only within `fallback_window`.

### Metrics

| Metric | What it shows |
|---|---|
| `gateway_tool_calls_total{tool,policy,outcome}` | How each `tools/call` was answered: `completed`, `replayed`, `outcome_unknown`, `in_progress`, `not_executed`, `store_unavailable`, `key_conflict`, `key_required`, `refused`, `invalid`, `read_only`, `input_required` or `client_left`. Tools missing from the config are counted as `_unlisted` |
| `gateway_dispatches_total{policy,result}` | Sends upstream: `response`, `unsent` (provably never left) or `maybe_sent` (no answer, may have run) |
| `gateway_dispatch_duration_seconds{policy}` | Upstream time per send |
| `gateway_operations{state}` | Unfinished operations across all instances: `claimed`, `dispatching`, `awaiting_input`, `unknown` |
| `gateway_store_up` | Whether Postgres answered a ping at scrape time |
| `gateway_store_errors_total{where}` | Failed store statements on the request path, in state writes and in background work |
| `gateway_pending_writes`, `gateway_pending_writes_dropped_total`, `gateway_pending_writes_abandoned_total` | State changes waiting for Postgres to come back, and those given up |
| `gateway_reconciliations_total{result}` | Reconciliation reads: `found`, `not_found_settled`, `not_found_too_early`, `inconclusive` |
| `gateway_abandoned_operations_settled_total{to}` | Operations whose owner vanished, settled as `released` or `unknown` |

A rising `gateway_operations{state="unknown"}` is the number to alert on: each one needs a reconciliation read or a person to settle it.

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

CI runs the same suite with `-race` on Linux, plus `cd interop && go test ./...` for the official SDK checks. Tests that need Postgres are skipped when `GATEWAY_TEST_DATABASE_URL` is unset. Each test gets its own schema. The crash tests build the gateway with `-tags killpoints` and run it as separate processes. `GATEWAY_CHAOS_SEEDS=1,2,3,4,5` runs the random-kill test with more seeds, and `-short` skips it.

## Layout

| Path | What it is |
|---|---|
| `cmd/gateway` | The gateway binary |
| `cmd/gatewayctl` | The operator command |
| `cmd/measure` | The measurements above |
| `cmd/faketool`, `cmd/mcpcall` | The fake tool server on its own, and a command-line MCP client, for the demo |
| `compose.yaml`, `deploy/` | The Docker Compose demo stack and its configs |
| `internal/operator` | Recording outcomes a person verified |
| `internal/gateway` | Request routing, the `tools/call` pipeline, dispatch, responses |
| `internal/store` | Postgres operation store and migrations |
| `internal/mcp` | JSON-RPC, 2026-07-28 request headers, SSE |
| `internal/opkey`, `internal/canonjson` | Operation identity and argument hashing |
| `internal/config` | YAML config and tool policies |
| `internal/faketool` | Fake MCP server with an effect ledger and fault injection |
| `internal/killpoint` | Kill points, compiled in only with `-tags killpoints` |
| `internal/faultproxy` | TCP proxy that cuts or stalls Postgres during tests |
| `internal/metrics` | Small Prometheus text exporter |
| `e2e` | End-to-end tests against the ledger |
| `interop` | The gateway between the official MCP Go SDK's client and server; a separate module |
| `compare` | The comparison with similar tools; a separate module, run in its own Docker image |
