// Runs mcp-idempotent's proxy with its Redis store through the package's
// own library API. Its CLI's "--store redis" fails to start in the build
// from commit 17bb541 ("Dynamic require of "events" is not supported",
// from bundling ioredis into the ESM CLI), so this mirrors bin/cli.ts with
// the store created here instead. The image copies it into the package
// directory so the imports resolve against the package's own dependencies.
import { Redis } from "ioredis";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { IdempotentProxy } from "./dist/index.js";
import { RedisStore } from "./dist/store/redis.js";

const sep = process.argv.indexOf("--");
if (sep === -1 || sep === process.argv.length - 1) {
  process.stderr.write("usage: node mcp-idempotent-redis.mjs -- <command> [args...]\n");
  process.exit(1);
}
const [command, ...args] = process.argv.slice(sep + 1);
const redis = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379");
const proxy = new IdempotentProxy({
  upstream: new StdioClientTransport({ command, args, stderr: "inherit" }),
  downstream: new StdioServerTransport(),
  store: new RedisStore(redis),
});
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    void proxy.close().finally(() => process.exit(0));
  });
}
await proxy.start();
process.stderr.write(`mcp-idempotent (library, Redis store): proxying "${command} ${args.join(" ")}"\n`);
