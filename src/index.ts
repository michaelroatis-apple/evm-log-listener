import { EventListener } from "./listener.js";
import { logger } from "./logger.js";
import { MetricsReader, MetricsWriter } from "./metrics.js";
import { createRedis } from "./redis.js";
import { startServer } from "./server.js";

const redis = createRedis();
const writer = new MetricsWriter(redis);
const reader = new MetricsReader(redis);

const listener = new EventListener(async (batch) => {
  await writer.recordBatch(batch);
});

listener.start();
const server = startServer(listener, reader, redis);

async function shutdown(signal: string) {
  logger.info("shutting down", { signal });
  listener.stop();
  server.close();
  await redis.quit().catch(() => redis.disconnect());
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
