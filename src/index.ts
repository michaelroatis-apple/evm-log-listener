import { EventListener } from "./listener.js";
import { logger } from "./logger.js";

const listener = new EventListener(async (batch) => {
  // Placeholder consumer — replaced by the Redis metrics writer next.
  for (const t of batch.transfers.slice(0, 3)) {
    logger.debug("transfer", {
      from: t.from,
      to: t.to,
      value: t.value.toString(),
      block: t.blockNumber.toString(),
    });
  }
});

listener.start();

function shutdown(signal: string) {
  logger.info("shutting down", { signal });
  listener.stop();
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
