import { EventListener } from "./listener.js";
import { logger } from "./logger.js";
import { MetricsWriter } from "./metrics.js";
import { createRedis } from "./redis.js";

const redis = createRedis();
const metrics = new MetricsWriter(redis);

const listener = new EventListener(async (batch) => {
  await metrics.recordBatch(batch);
});

listener.start();

async function shutdown(signal: string) {
  logger.info("shutting down", { signal });
  listener.stop();
  await redis.quit().catch(() => redis.disconnect());
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
