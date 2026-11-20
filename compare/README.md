# Comparison with similar tools

The same three failures, put through each tool-call safety layer I could find that targets this problem. The fake refund tool behind every layer records each refund in a ledger file, so the counts below are what really happened, not what any layer reports.

## What was compared

| Layer | Version | How it was configured |
|---|---|---|
| None | | The fake tool alone, as the baseline |
| [mcp-idempotent](https://github.com/krish-shahh/mcp-idempotent) | Commit `17bb541` (August 2026); not published to npm, so built from the repository | Its CLI with the default memory store, and its Redis store (see the note below). The operation key is sent in `_meta["idempotency-key"]` |
| [once-gate](https://pypi.org/project/once-gate/) | 0.2.1 from PyPI | `proxy --db … --ledger … --tools create_refund`. Repeats are recognised by tool name and arguments |
| [agentsafe-mcp](https://github.com/Ashutosh2308Bhardwaj/agentsafe) | `mcp/v0.6.0` | Policy `{"identity": ["reference"], "key": "none"}`: the tool cannot take an idempotency key |
| This gateway | This repository | `at_most_once`, and `reconcilable` with `find_refund_by_reference`. Lease 5 s, wait timeout 5 s |

The three other layers are stdio proxies that start the tool server as a child process. This gateway is a Streamable HTTP service, so it runs in front of the HTTP version of the same fake tool. Each scenario starts with fresh state.

**Note on mcp-idempotent's Redis store:** its CLI exits at startup with `--store redis` in this build: `fatal error: Error: Dynamic require of "events" is not supported`. The CLI bundles the Redis client into an ES module that can't load it. The Redis rows therefore run the same proxy class and store through the package's library API, in [`mcp-idempotent-redis.mjs`](mcp-idempotent-redis.mjs), which mirrors the CLI otherwise.

## The scenarios

1. **The tool dies after the refund, before answering.** This is the lost-response case. Over stdio the tool process exits; over HTTP the connection closes, which looks the same to the layer.
2. **The safety layer is killed after the refund, before answering**, then started again.
3. **The agent times out while the tool is still running**: the refund takes 3 s, the agent waits 1 s and then retries.

The agent in the harness behaves like a typical one:
- After a dropped connection or a timeout it retries the same call, restarting a stdio process that has exited, and it gives up after 90 seconds.
- Each attempt waits up to 45 seconds.
- On a timeout it sends `notifications/cancelled`, as the SDKs do.
- Any other answer is final.

## Results

Measured on 20 November 2026, twice, with the same refund counts and the same kinds of answer both times.

**1. The tool dies after the refund, before answering**

| Layer | Refunds | What the agent was told |
|---|---|---|
| None | **2** | The retry ran the refund again and reported success |
| mcp-idempotent, memory store (default) | **2** | The proxy exits with its server. The restarted proxy has no record of the call, so the retry ran it again |
| mcp-idempotent, Redis store | 1 | The proxy keeps running after its server dies but stops answering: no answer within two 45 s attempts |
| once-gate | 1 | The first call got no answer before the 10 s timeout. The retry got `INDETERMINATE` (code -32010) at once, which a person releases with evidence |
| agentsafe-mcp | 1 | A tool error containing the transport error, `calling "tools/call": EOF`, replayed on the retry. Its source says the log records such a call as possibly having run, but the message doesn't tell the agent the outcome is unknown |
| This gateway, `at_most_once` | 1 | "Outcome unknown" at once, and the same on the retry |
| This gateway, `reconcilable` | 1 | The confirmed result at once: a lookup found the refund |

**2. The safety layer is killed after the refund, before answering**

| Layer | Refunds | What the agent was told |
|---|---|---|
| mcp-idempotent, memory store (default) | **2** | The record was in the killed process, so the retry ran the refund again |
| mcp-idempotent, Redis store | 1 | The retry waited 30 s for the original, then got `Duplicate "create_refund" call timed out waiting for the original to complete` (-32000). From its code (not tested here), the pending entry expires with the store's TTL, 10 minutes by default, after which the same call can run again |
| once-gate | 1 | `INDETERMINATE` at once |
| agentsafe-mcp | 1 | "Outcome unknown" at once |
| This gateway, `at_most_once` | 1 | "Outcome unknown", after the dead instance's 5 s lease |
| This gateway, `reconcilable` | 1 | The confirmed result, after the 5 s lease |

**3. The agent times out while the tool runs, then retries**

| Layer | Refunds | What the agent was told |
|---|---|---|
| None | **2** | The retry ran the refund a second time |
| mcp-idempotent, both stores | 1 | The retry waited for the original call and got its result, about 2 s later |
| once-gate | 1 | The same, about 2.5 s later |
| agentsafe-mcp | 1 | "Outcome unknown" at once, although the refund went through. It treats the cancelled call as in doubt |
| This gateway, both policies | 1 | The retry waited for the original call and got its result, about 2 s later |

## What this shows, and what it doesn't

- **Duplicates:** every layer except mcp-idempotent's default memory store prevented a duplicate refund in all three cases.
- **What the agent hears, and when:** this is where the layers differ.
  - Only this gateway's `reconcilable` policy turned a lost response into the confirmed result without a person.
  - once-gate and agentsafe-mcp leave the outcome for a person to settle. agentsafe-mcp's `reconcile.Audit` checks logs against systems of record separately, but it doesn't answer the agent.
- **Not tested here:** each layer has strengths these scenarios don't cover.
  - once-gate: per-call human approval and a STOP file.
  - agentsafe-mcp: approvals, a hash-chained and optionally sealed log, and resend with a key for tools that accept one.
  - This gateway: several instances sharing one store, which none of the others offer.
- **Limitations:**
  - It is one machine and one harness, written by the author of one of the layers being compared.
  - The transports differ: stdio for the others, HTTP for this gateway.
  - Corrections are welcome, especially to the configurations.

## Running it

```sh
./scripts/test-db.sh                                     # Postgres for this gateway's rows
docker build -f compare/Dockerfile -t crashsafe-compare .
docker run --rm --network container:crashsafe-pg -v "$PWD/results/compare:/work/results" crashsafe-compare \
  -db "postgres://postgres:postgres@127.0.0.1:5432/gateway?sslmode=disable"
```

The other tools are downloaded and run only inside that image. The report is printed, and the raw results and every process's output go to `results/compare/`. `-only "once-gate"` runs one layer.
