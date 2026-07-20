import { Redis } from "ioredis";
import { config } from "./config.js";
import { logger } from "./logger.js";

/**
 * Shared Redis client.
 *
 * ioredis handles reconnection itself; we give it an exponential backoff
 * retry strategy (capped at 30s) and keep offline queueing enabled so that
 * metric writes issued during a brief Redis outage are flushed on reconnect
 * instead of being dropped.
 */
export function createRedis(): Redis {
  const redis = new Redis(config.redisUrl, {
    retryStrategy: (attempt) => {
      const delay = Math.min(250 * 2 ** attempt, 30_000);
      logger.warn("redis reconnecting", { attempt, delayMs: delay });
      return delay;
    },
    maxRetriesPerRequest: null, // never fail queued commands during outages
    enableOfflineQueue: true,
  });

  redis.on("connect", () => logger.info("redis connected"));
  redis.on("error", (err) => logger.error("redis error", { error: err.message }));
  redis.on("close", () => logger.warn("redis connection closed"));

  return redis;
}

const CURSOR_KEY = "cursor:lastProcessedBlock";

/**
 * Persists the block cursor so a restart (deploy, crash, systemd restart)
 * resumes where the previous run stopped and backfills its own downtime,
 * instead of silently starting fresh at the current head.
 */
export class RedisCursorStore {
  constructor(private readonly redis: Redis) {}

  async load(): Promise<bigint | null> {
    const raw = await this.redis.get(CURSOR_KEY);
    return raw === null ? null : BigInt(raw);
  }

  async save(block: bigint): Promise<void> {
    await this.redis.set(CURSOR_KEY, block.toString());
  }
}
