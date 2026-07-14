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
const volumeKey = (minute: number) => `volume:${minute}`;
const countKey = (minute: number) => `transfers:${minute}`;

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
    // unique sender + one volume increment.
    const bySender = new Map<string, number>();
    let totalVolume = 0n;
    for (const t of batch.transfers) {
      const sender = t.from.toLowerCase();
      bySender.set(sender, (bySender.get(sender) ?? 0) + 1);
      totalVolume += t.value;
    }

    const minute = currentMinute();
    const sKey = senderKey(minute);
    const vKey = volumeKey(minute);
    const cKey = countKey(minute);

    const pipeline = this.redis.pipeline();
    for (const [sender, count] of bySender) {
      pipeline.zincrby(sKey, count, sender);
    }
    pipeline.incrby(vKey, totalVolume.toString());
    pipeline.incrby(cKey, batch.transfers.length);
    pipeline.expire(sKey, BUCKET_TTL_SECONDS);
    pipeline.expire(vKey, BUCKET_TTL_SECONDS);
    pipeline.expire(cKey, BUCKET_TTL_SECONDS);

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

export interface MinuteVolume {
  minute: number; // epoch minute
  volume: string; // raw token units
}

export interface MetricsSnapshot {
  topSenders: TopSender[];
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

    const [topSenders, volumes, totalTransfers] = await Promise.all([
      this.topSenders(minutes, 5),
      this.volumeByMinute(minutes),
      this.totalTransfers(minutes),
    ]);

    const totalVolume = volumes.reduce((acc, v) => acc + BigInt(v.volume), 0n);

    return {
      topSenders,
      volumeByMinute: volumes,
      totalTransfersLastHour: totalTransfers,
      totalVolumeLastHour: totalVolume.toString(),
      totalVolumeLastHourFormatted: formatUnits(totalVolume, config.tokenDecimals),
      spike: this.detectSpike(volumes),
    };
  }

  private async topSenders(minutes: number[], n: number): Promise<TopSender[]> {
    const keys = minutes.map(senderKey);
    const tmpKey = `tmp:topsenders:${minutes[minutes.length - 1]}`;

    // ZUNIONSTORE sums per-minute counts across the window server-side,
    // then we read just the top N — the full sender set never crosses the
    // network. The tmp key expires quickly; it's throwaway.
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
