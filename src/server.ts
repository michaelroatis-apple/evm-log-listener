import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import type { Redis } from "ioredis";
import { config } from "./config.js";
import { logger } from "./logger.js";
import type { EventListener } from "./listener.js";
import type { MetricsReader } from "./metrics.js";

/**
 * Minimal HTTP layer (no framework):
 *   GET /            -> dashboard (static HTML)
 *   GET /api/metrics -> rolling 1h metrics snapshot
 *   GET /healthz     -> liveness + listener/redis status
 */
export function startServer(
  listener: EventListener,
  reader: MetricsReader,
  redis: Redis,
): ReturnType<typeof createServer> {
  const server = createServer(async (req, res) => {
    const url = req.url?.split("?")[0] ?? "/";

    try {
      if (url === "/healthz") {
        const status = listener.getStatus();
        const healthy = redis.status === "ready";
        res.writeHead(healthy ? 200 : 503, { "content-type": "application/json" });
        res.end(JSON.stringify({ ok: healthy, redis: redis.status, ...status }));
        return;
      }

      if (url === "/api/metrics") {
        const snapshot = await reader.snapshot();
        res.writeHead(200, {
          "content-type": "application/json",
          "cache-control": "no-store",
        });
        res.end(JSON.stringify({ ...snapshot, listener: listener.getStatus() }));
        return;
      }

      if (url === "/") {
        const html = await readFile(join(process.cwd(), "public", "index.html"));
        res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
        res.end(html);
        return;
      }

      res.writeHead(404, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "not found" }));
    } catch (err) {
      logger.error("http handler error", { url, error: (err as Error).message });
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "internal error" }));
    }
  });

  server.listen(config.port, () => {
    logger.info("http server listening", { port: config.port });
  });
  return server;
}
