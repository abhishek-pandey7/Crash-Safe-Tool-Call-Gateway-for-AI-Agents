# Crash-Safe Tool-Call Gateway for AI Agents

A Go gateway that stops retried or interrupted AI agent tool calls from repeating real-world actions, such as refunds, emails or new tickets.

> **Status: pre-implementation.** Nothing is built or measured yet. The guarantees below are design targets that the test suite must prove.

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

## Documentation

**[Full project description](docs/PROJECT.md)**: the problem and evidence, existing projects, design, test plan, milestones, risks and all sources.
