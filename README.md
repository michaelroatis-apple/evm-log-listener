# Resilient EVM Log Listener & Cache

A real-time event indexing service that listens for live USDC `Transfer`
events on Ethereum mainnet, survives infrastructure hazards (websocket
drops, RPC rate-limiting), and maintains rolling 1-hour metrics in Redis.

## Quick start

```bash
# 1. Start Redis
docker compose up -d

# 2. Configure (defaults work out of the box — public RPC + local Redis)
cp .env.example .env

# 3. Install & run
npm install
npm run dev
```

You should see live block processing within seconds:

```json
{"ts":"...","level":"info","msg":"processed blocks","fromBlock":"23456789","toBlock":"23456789","transfers":14}
```

## Architecture

```
WSS (block heads) ──> EventListener ──> getLogs(range) ──> MetricsWriter ──> Redis
                        │                    HTTP                             │
                        └── reconnect w/ backoff + jitter                     └── per-minute buckets, 65min TTL
                        └── stale-head watchdog
                        └── gap backfill after outages
```

**Ingestion.** The service subscribes to new block numbers over WebSocket,
then fetches each new range's `Transfer` logs via `eth_getLogs` over HTTP.
Live processing and post-outage backfill share one code path: we always
process `(lastProcessedBlock + 1 .. head)`, so after any disconnect the next
range simply widens. **No block is ever skipped.**

**Resiliency.**

- WSS drops and subscription errors trigger reconnection with exponential
  backoff and full jitter (jitter avoids synchronized retry stampedes
  against public nodes).
- A watchdog detects silently dead subscriptions (no heads for 45s on a
  12s-block chain) and forces a reconnect.
- `getLogs` calls retry with backoff; HTTP 429 responses receive an extra
  flat delay penalty. If retries exhaust, the range remains unprocessed and
  is retried automatically on the next head.
- Backfill ranges are chunked (100 blocks per `getLogs`) to respect public
  node limits after long outages.
- Redis outages are absorbed by ioredis offline queueing — metric writes
  issued while Redis is down flush on reconnect.

**Redis metrics (rolling 1 hour).** Per-minute time buckets, each with a
65-minute TTL so the window cleans itself up with no cron:

| Key | Type | Contents |
|---|---|---|
| `senders:{minute}` | ZSET | transfer count per sender address |
| `volume:{minute}` | STRING | raw token units transferred |
| `transfers:{minute}` | STRING | transfer count |

Reads: top-5 senders via `ZUNIONSTORE` across the 60 buckets (aggregation
stays server-side); volume series via one `MGET`. Spike detection compares
the last complete minute against the trailing average (flagged at >3x).

**Burst optimization.** Transfers are pre-aggregated in-process per block
(one `ZINCRBY` per unique sender, one `INCRBY` for volume), then flushed in
a single pipeline — one Redis round-trip per block, regardless of how many
transfers a busy block contains.

## Dashboard & API

With the service running, open **http://localhost:3000** for a live
dashboard (volume/minute chart, top-5 senders, spike indicator, listener
health). Programmatic access:

- `GET /api/metrics` — full rolling 1h snapshot as JSON
- `GET /healthz` — liveness (200 when Redis is ready; includes listener status)

## Deploying to Railway

The repo ships with `railway.json` (build + start commands, `/healthz`
healthcheck, always-restart policy). To deploy:

1. Push this repo to GitHub and create a Railway project from it.
2. Add a **Redis** service to the project.
3. On the indexer service, set `REDIS_URL` to Railway's Redis reference
   variable (`${{Redis.REDIS_URL}}`). All other variables have workable
   defaults; set `RPC_WSS_URL`/`RPC_HTTP_URL` to a keyed endpoint for
   better rate limits.
4. Every push to `main` deploys automatically.

## Production daemonization (systemd)

The unit file is at [`deploy/indexer.service`](deploy/indexer.service). It
runs the service as a dedicated non-root `indexer` user, loads secrets from
a root-owned `EnvironmentFile` (mode 600), restarts automatically on any
crash or uncaught exception (`Restart=always` with a crash-loop limiter),
and applies standard sandbox hardening. Setup commands are in the file's
header comment.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `RPC_WSS_URL` | `wss://ethereum-rpc.publicnode.com` | live block subscription |
| `RPC_HTTP_URL` | `https://ethereum-rpc.publicnode.com` | log fetching / backfill |
| `CONTRACT_ADDRESS` | USDC mainnet | contract to index |
| `TOKEN_DECIMALS` | `6` | human-readable volume formatting |
| `REDIS_URL` | `redis://localhost:6379` | cache connection |
| `PORT` | `3000` | HTTP port (health + metrics API) |
| `LOG_LEVEL` | `info` | `debug` \| `info` \| `warn` \| `error` |

## Scripts

```bash
npm run dev        # run with hot reload (tsx)
npm run build      # compile to dist/
npm start          # run compiled output
npm run typecheck  # tsc --noEmit
npx tsx scripts/verify-metrics.ts  # metrics logic assertions (mocked Redis)
```
