/** Shared mocked-Redis setup for the verify scripts. */
// @ts-expect-error no types shipped
import RedisMock from "ioredis-mock";
import type { Redis } from "ioredis";

/**
 * ioredis-mock doesn't implement ZUNIONSTORE; shim it (sum scores across
 * keys — matches real Redis' default SUM aggregation).
 */
export function createMockRedis(): Redis {
  const redis = new RedisMock() as unknown as Redis;
  (redis as any).zunionstore = async (
    dest: string,
    _numkeys: number,
    ...keys: string[]
  ) => {
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
  return redis;
}
