/**
 * Sandbox smoke test for the HTTP layer using mocked Redis and a stub
 * listener. Run with: npx tsx scripts/verify-server.ts
 */
// @ts-expect-error no types shipped
import RedisMock from "ioredis-mock";
import type { Redis } from "ioredis";
import { MetricsReader, MetricsWriter } from "../src/metrics.js";
import { startServer } from "../src/server.js";
import type { EventListener } from "../src/listener.js";

const redis = new RedisMock() as unknown as Redis;
(redis as any).zunionstore = async (dest: string, _n: number, ...keys: string[]) => {
  const union = new Map<string, number>();
  for (const key of keys) {
    const raw: string[] = await (redis as any).zrange(key, 0, -1, "WITHSCORES");
    for (let i = 0; i < raw.length; i += 2)
      union.set(raw[i]!, (union.get(raw[i]!) ?? 0) + Number(raw[i + 1]));
  }
  for (const [m, s] of union) await redis.zadd(dest, s, m);
  return union.size;
};
Object.defineProperty(redis, "status", { value: "ready" });

const stubListener = {
  getStatus: () => ({
    wsConnected: true,
    lastProcessedBlock: "23456789",
    lastBlockAt: new Date().toISOString(),
    reconnectAttempts: 0,
  }),
} as unknown as EventListener;

async function main() {
  const writer = new MetricsWriter(redis);
  await writer.recordBatch({
    fromBlock: 1n,
    toBlock: 1n,
    transfers: [
      { from: "0xAA", to: "0xBB", value: 5_000_000n, blockNumber: 1n, txHash: "0x00" },
    ],
  });

  const server = startServer(stubListener, new MetricsReader(redis), redis);
  await new Promise((r) => setTimeout(r, 300));

  const base = "http://localhost:" + (process.env.PORT ?? "3000");
  const checks: Array<[string, (body: string, status: number) => boolean]> = [
    ["/healthz", (b, s) => s === 200 && JSON.parse(b).ok === true],
    ["/api/metrics", (b, s) => {
      const m = JSON.parse(b);
      return s === 200 && m.totalTransfersLastHour === 1 && m.totalVolumeLastHour === "5000000"
        && m.topSenders[0].address === "0xaa" && m.listener.wsConnected === true;
    }],
    ["/", (b, s) => s === 200 && b.includes("USDC Transfer Indexer")],
    ["/nope", (_b, s) => s === 404],
  ];

  for (const [path, validate] of checks) {
    const res = await fetch(base + path);
    const body = await res.text();
    if (!validate(body, res.status)) {
      console.error(`FAIL ${path} (${res.status}): ${body.slice(0, 200)}`);
      process.exit(1);
    }
    console.log(`ok: ${path}`);
  }

  server.close();
  console.log("\nAll server checks passed.");
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
