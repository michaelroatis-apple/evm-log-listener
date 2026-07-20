import type { Redis } from "ioredis";
import { config } from "./config.js";
import { logger } from "./logger.js";
import type { TransferBatch } from "./listener.js";

/**
 * Rolling 1-hour metrics in Redis, using per-minute time buckets:
 *
 *   senders:{epochMinute}  ZSET   member = sender address, score = transfer count
 *   volume:{epochMinute}   STRING raw token units transferred that minute (INCRBY)
 *
 * Every bucket gets a ~65 min TTL, so the rolling window cleans itself up —
 * no cron jobs, no explicit eviction.
 *
 * Burst optimization: transfers are pre-aggregated in-process per batch
 * (one ZINCRBY per unique sender, one INCRBY total, not per transfer), and
 * everything is flushed in a single pipeline — one Redis round-trip per
 * block regardless of how many transfers it contains.
 */

const WINDOW_MINUTES = 60;
const BUCKET_TTL_SECONDS = 65 * 60;

const senderKey = (minute: number) => `senders:${minute}`;
const receiverKey = (minute: number) => `receivers:${minute}`;
const volumeKey = (minute: number) => `volume:${minute}`;
const countKey = (minute: number) => `transfers:${minute}`;
const largestKey = (minute: number) => `largest:${minute}`;

/** Max entries kept per largest-transfers bucket. */
const LARGEST_PER_BUCKET = 20;

export const currentMinute = () => Math.floor(Date.now() / 60_000);

export class MetricsWriter {
  constructor(private readonly redis: Redis) {}

  /**
   * Buckets are keyed by ingestion time (wall clock), not block timestamp.
   * For a live "activity over the last 60 minutes" metric that's the honest
   * choice, and it keeps backfilled post-outage data inside the window
   * instead of writing to buckets that have already expired.
   */
  async recordBatch(batch: TransferBatch): Promise<void> {
    if (batch.transfers.length === 0) return;

    // In-process pre-aggregation: collapse N transfers into one command per
    // unique sender/receiver + one volume increment.
    const bySender = new Map<string, number>();
    const byReceiver = new Map<string, number>();
    let totalVolume = 0n;
    for (const t of batch.transfers) {
      const sender = t.from.toLowerCase();
      const receiver = t.to.toLowerCase();
      bySender.set(sender, (bySender.get(sender) ?? 0) + 1);
      byReceiver.set(receiver, (byReceiver.get(receiver) ?? 0) + 1);
      totalVolume += t.value;
    }

    // Largest transfers: only the batch's own top N can matter, so pick
    // them in-process and write just those, capping the bucket's size.
    const batchLargest = [...batch.transfers]
      .sort((a, b) => (a.value > b.value ? -1 : a.value < b.value ? 1 : 0))
      .slice(0, 5);

    const minute = currentMinute();
    const sKey = senderKey(minute);
    const rKey = receiverKey(minute);
    const vKey = volumeKey(minute);
    const cKey = countKey(minute);
    const lKey = largestKey(minute);

    const pipeline = this.redis.pipeline();
    for (const [sender, count] of bySender) {
      pipeline.zincrby(sKey, count, sender);
    }
    for (const [receiver, count] of byReceiver) {
      pipeline.zincrby(rKey, count, receiver);
    }
    for (const t of batchLargest) {
      // Score is a float (2^53 precision) — fine for ranking; the exact
      // value rides along in the member for display.
      pipeline.zadd(
        lKey,
        Number(t.value),
        `${t.txHash}|${t.from.toLowerCase()}|${t.to.toLowerCase()}|${t.value.toString()}`,
      );
    }
    pipeline.zremrangebyrank(lKey, 0, -(LARGEST_PER_BUCKET + 1));
    pipeline.incrby(vKey, totalVolume.toString());
    pipeline.incrby(cKey, batch.transfers.length);
    for (const key of [sKey, rKey, vKey, cKey, lKey]) {
      pipeline.expire(key, BUCKET_TTL_SECONDS);
    }

    const results = await pipeline.exec();
    const failed = results?.filter(([err]) => err != null) ?? [];
    if (failed.length > 0) {
      logger.error("some metric writes failed", { failed: failed.length });
    }
  }
}

export interface TopSender {
  address: string;
  transfers: number;
}

export interface LargeTransfer {
  txHash: string;
  from: string;
  to: string;
  value: string; // raw units, exact
}

export interface MinuteVolume {
  minute: number; // epoch minute
  volume: string; // raw token units
}

export interface MetricsSnapshot {
  topSenders: TopSender[];
  topReceivers: TopSender[];
  largestTransfers: LargeTransfer[];
  volumeByMinute: MinuteVolume[];
  totalTransfersLastHour: number;
  totalVolumeLastHour: string; // raw units
  totalVolumeLastHourFormatted: string; // human units (per TOKEN_DECIMALS)
  spike: {
    detected: boolean;
    lastMinuteVolume: string;
    trailingAverageVolume: string;
    ratio: number | null;
  };
}

const lastNMinutes = (n: number): number[] => {
  const now = currentMinute();
  return Array.from({ length: n }, (_, i) => now - (n - 1) + i);
};

function formatUnits(raw: bigint, decimals: number): string {
  const divisor = 10n ** BigInt(decimals);
  const whole = raw / divisor;
  const frac = ((raw % divisor) * 100n) / divisor; // 2 decimal places
  return `${whole}.${frac.toString().padStart(2, "0")}`;
}

export class MetricsReader {
  constructor(private readonly redis: Redis) {}

  async snapshot(): Promise<MetricsSnapshot> {
    const minutes = lastNMinutes(WINDOW_MINUTES);

    const [topSenders, topReceivers, largestTransfers, volumes, totalTransfers] =
      await Promise.all([
        this.topOfUnion(minutes.map(senderKey), "tmp:topsenders", 5),
        this.topOfUnion(minutes.map(receiverKey), "tmp:topreceivers", 5),
        this.largestTransfers(minutes, 5),
        this.volumeByMinute(minutes),
        this.totalTransfers(minutes),
      ]);

    const totalVolume = volumes.reduce((acc, v) => acc + BigInt(v.volume), 0n);

    return {
      topSenders,
      topReceivers,
      largestTransfers,
      volumeByMinute: volumes,
      totalTransfersLastHour: totalTransfers,
      totalVolumeLastHour: totalVolume.toString(),
      totalVolumeLastHourFormatted: formatUnits(totalVolume, config.tokenDecimals),
      spike: this.detectSpike(volumes),
    };
  }

  /**
   * ZUNIONSTORE sums per-minute counts across the window server-side, then
   * we read just the top N — the full member set never crosses the network.
   * The tmp key expires quickly; it's throwaway.
   */
  private async topOfUnion(
    keys: string[],
    tmpPrefix: string,
    n: number,
  ): Promise<TopSender[]> {
    const tmpKey = `${tmpPrefix}:${currentMinute()}`;
    await this.redis.zunionstore(tmpKey, keys.length, ...keys);
    const topRaw = await this.redis.zrevrange(tmpKey, 0, n - 1, "WITHSCORES");
    await this.redis.expire(tmpKey, 5);

    const out: TopSender[] = [];
    for (let i = 0; i < topRaw.length; i += 2) {
      out.push({
        address: topRaw[i] as string,
        transfers: Number(topRaw[i + 1]),
      });
    }
    return out;
  }

  /**
   * Members are unique per transfer (txHash-keyed), so the union's SUM
   * aggregation leaves scores untouched and ZREVRANGE ranks by value.
   */
  private async largestTransfers(
    minutes: number[],
    n: number,
  ): Promise<LargeTransfer[]> {
    const tmpKey = `tmp:largest:${currentMinute()}`;
    const keys = minutes.map(largestKey);
    await this.redis.zunionstore(tmpKey, keys.length, ...keys);
    const raw = await this.redis.zrevrange(tmpKey, 0, n - 1);
    await this.redis.expire(tmpKey, 5);

    return raw.flatMap((member) => {
      const [txHash, from, to, value] = member.split("|");
      if (!txHash || !from || !to || !value) return [];
      return [{ txHash, from, to, value }];
    });
  }

  private async totalTransfers(minutes: number[]): Promise<number> {
    const values = await this.redis.mget(minutes.map(countKey));
    return values.reduce((acc, v) => acc + (v ? Number(v) : 0), 0);
  }

  private async volumeByMinute(minutes: number[]): Promise<MinuteVolume[]> {
    const values = await this.redis.mget(minutes.map(volumeKey));
    return minutes.map((minute, i) => ({
      minute,
      volume: values[i] ?? "0",
    }));
  }

  /**
   * Spike heuristic: compare the last *complete* minute against the trailing
   * average of the rest of the window. Flag when it exceeds 3x a non-trivial
   * average. (The current, partial minute is excluded — it would always look
   * low early in the minute and cause flapping.)
   */
  private detectSpike(volumes: MinuteVolume[]): MetricsSnapshot["spike"] {
    if (volumes.length < 3) {
      return { detected: false, lastMinuteVolume: "0", trailingAverageVolume: "0", ratio: null };
    }
    const complete = volumes.slice(0, -1); // drop current partial minute
    const last = BigInt(complete[complete.length - 1]!.volume);
    const trailing = complete.slice(0, -1);
    const sum = trailing.reduce((acc, v) => acc + BigInt(v.volume), 0n);
    const avg = sum / BigInt(trailing.length);

    const ratio = avg > 0n ? Number((last * 100n) / avg) / 100 : null;
    return {
      detected: avg > 0n && last > avg * 3n,
      lastMinuteVolume: last.toString(),
      trailingAverageVolume: avg.toString(),
      ratio,
    };
  }
}
