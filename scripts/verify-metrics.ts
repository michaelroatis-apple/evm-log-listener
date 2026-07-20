/**
 * Sandbox verification of the metrics layer against ioredis-mock.
 * Not part of the shipped service — run with: npx tsx scripts/verify-metrics.ts
 */
// @ts-expect-error no types shipped
import RedisMock from "ioredis-mock";
import type { Redis } from "ioredis";
import { MetricsWriter, MetricsReader } from "../src/metrics.js";
import type { TransferBatch } from "../src/listener.js";

const redis = new RedisMock() as unknown as Redis;

// ioredis-mock doesn't implement ZUNIONSTORE; shim it (sum scores across keys).
(redis as any).zunionstore = async (dest: string, _numkeys: number, ...keys: string[]) => {
  const union = new Map<string, number>();
  for (const key of keys) {
    const raw: string[] = await (redis as any).zrange(key, 0, -1, "WITHSCORES");
    for (let i = 0; i < raw.length; i += 2) {
      union.set(raw[i]!, (union.get(raw[i]!) ?? 0) + Number(raw[i + 1]));
    }
  }
  for (const [member, score] of union) await redis.zadd(dest, score, member);
  return union.size;
};
const writer = new MetricsWriter(redis);
const reader = new MetricsReader(redis);

function batch(transfers: Array<[string, string, bigint]>, block: bigint): TransferBatch {
  return {
    fromBlock: block,
    toBlock: block,
    transfers: transfers.map(([from, to, value], i) => ({
      from,
      to,
      value,
      blockNumber: block,
      txHash: `0x${block.toString(16).padStart(8, "0")}${i.toString(16).padStart(56, "0")}`,
    })),
  };
}

const A = "0xAAAAaaaAAAaaaAaaAAaaaaAAAAaaAAaaaaAAAaa1";
const B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2";
const C = "0xccccccccccccccccccccccccccccccccccccccc3";

async function main() {
  // Block 1: A sends twice, B once. Volume 100 + 50 + 25 = 175 raw units.
  await writer.recordBatch(
    batch(
      [
        [A, B, 100n],
        [A, C, 50n],
        [B, C, 25n],
      ],
      1n,
    ),
  );
  // Block 2: C sends 4 times, 10 each.
  await writer.recordBatch(
    batch(
      [
        [C, A, 10n],
        [C, A, 10n],
        [C, B, 10n],
        [C, B, 10n],
      ],
      2n,
    ),
  );

  const snap = await reader.snapshot();

  const assert = (cond: boolean, label: string) => {
    if (!cond) {
      console.error(`FAIL: ${label}`);
      console.error(JSON.stringify(snap, null, 2));
      process.exit(1);
    }
    console.log(`ok: ${label}`);
  };

  assert(snap.topSenders.length === 3, "three distinct senders");
  assert(snap.topSenders[0]!.address === C.toLowerCase(), "C is most active (4 transfers)");
  assert(snap.topSenders[0]!.transfers === 4, "C has 4 transfers");
  assert(snap.topSenders[1]!.address === A.toLowerCase(), "A is second (2 transfers)");
  assert(snap.totalTransfersLastHour === 7, "7 transfers total");
  assert(
    snap.topReceivers[0]!.address === B.toLowerCase() && snap.topReceivers[0]!.transfers === 3,
    "B is top receiver (3 transfers)",
  );
  assert(snap.largestTransfers.length > 0, "largest transfers recorded");
  assert(
    snap.largestTransfers[0]!.value === "100" &&
      snap.largestTransfers[0]!.from === A.toLowerCase(),
    "largest transfer is A's 100",
  );
  assert(
    snap.largestTransfers.every((t, i, arr) => i === 0 || BigInt(arr[i - 1]!.value) >= BigInt(t.value)),
    "largest transfers sorted descending",
  );
  assert(snap.totalVolumeLastHour === "215", "total volume 215 raw units");
  assert(snap.volumeByMinute.length === 60, "60 minute buckets returned");
  assert(snap.spike.detected === false, "no spike on flat data");

  console.log("\nAll metrics assertions passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
