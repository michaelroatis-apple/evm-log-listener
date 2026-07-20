import {
  createPublicClient,
  http,
  parseAbiItem,
  webSocket,
  type PublicClient,
} from "viem";
import { mainnet } from "viem/chains";
import { config } from "./config.js";
import { logger } from "./logger.js";
import { backoffDelay, EndpointPool, sleep, withRetry } from "./resilience.js";

export const transferEvent = parseAbiItem(
  "event Transfer(address indexed from, address indexed to, uint256 value)",
);

export interface Transfer {
  from: string;
  to: string;
  value: bigint;
  blockNumber: bigint;
  txHash: string;
}

/** A batch of decoded transfers for a contiguous block range. */
export interface TransferBatch {
  fromBlock: bigint;
  toBlock: bigint;
  transfers: Transfer[];
}

export type BatchHandler = (batch: TransferBatch) => Promise<void>;

export interface ListenerStatus {
  wsConnected: boolean;
  wssEndpoint: string;
  lastProcessedBlock: string | null;
  lastBlockAt: string | null;
  reconnectAttempts: number;
}

/** Max blocks per eth_getLogs call — keeps backfill after a long outage
 *  within public-node range limits. */
const MAX_LOGS_RANGE = 100n;

/** If no new head arrives for this long, assume the WSS subscription died
 *  silently (mainnet blocks land every ~12s). */
const STALE_HEAD_MS = 45_000;

/** Rotate to the next WSS endpoint after this many consecutive failed
 *  reconnects on the current one. Backoff handles transient trouble;
 *  rotation handles an endpoint that is down *for us* (e.g. a public node
 *  blocking our cloud provider's egress IPs for hours). */
const ROTATE_WSS_AFTER = 3;

const RETRY = { baseMs: 500, capMs: 30_000, maxAttempts: 5 };

/** Cap on startup backfill from a persisted cursor (~1h at 12s blocks).
 *  The metric window is only 60 minutes, so blocks older than that carry
 *  no useful signal — and ingestion-time bucketing would misplace them. */
const MAX_STARTUP_GAP = 300n;

/** Where the listener resumes from across restarts. */
export interface CursorStore {
  load(): Promise<bigint | null>;
  save(block: bigint): Promise<void>;
}

/**
 * Resilient live event listener.
 *
 * Strategy: subscribe to new block numbers over WebSocket, then fetch the
 * Transfer logs for each new block range via eth_getLogs over HTTP.
 *
 * Live ingestion and post-reconnect backfill share one code path: we always
 * process the range (lastProcessedBlock + 1 .. head), so a reconnect simply
 * widens the next range to fetch. No block is ever skipped.
 *
 * Failure handling:
 *  - WSS drop / subscription error  -> reconnect with exponential backoff + jitter
 *  - Silent subscription death      -> watchdog detects stale heads, forces reconnect
 *  - 429 / transient getLogs errors -> per-call retry with backoff (extra penalty on 429)
 *  - getLogs exhausts retries       -> range stays unprocessed; next head retries it
 */
export class EventListener {
  private readonly wssPool = new EndpointPool(config.rpcWssUrls);
  private readonly httpClients: PublicClient[];
  private wsClient: PublicClient | null = null;
  private unwatch: (() => void) | null = null;

  private lastProcessedBlock: bigint | null = null;
  private lastBlockAt: number | null = null;
  private reconnectAttempts = 0;
  private reconnecting = false;
  private stopped = false;

  private processing = Promise.resolve();
  private watchdog: NodeJS.Timeout | null = null;

  constructor(
    private readonly onBatch: BatchHandler,
    private readonly cursorStore?: CursorStore,
  ) {
    this.httpClients = config.rpcHttpUrls.map((url) =>
      createPublicClient({ chain: mainnet, transport: http(url) }),
    );
  }

  async start(): Promise<void> {
    if (this.cursorStore) {
      try {
        const stored = await this.cursorStore.load();
        if (stored !== null) {
          this.lastProcessedBlock = stored;
          logger.info("resuming from persisted cursor", {
            lastProcessedBlock: stored.toString(),
          });
        }
      } catch (err) {
        logger.warn("could not load persisted cursor, starting from head", {
          error: (err as Error).message,
        });
      }
    }
    this.subscribe();
    this.watchdog = setInterval(() => this.checkStale(), 10_000);
  }

  stop(): void {
    this.stopped = true;
    if (this.watchdog) clearInterval(this.watchdog);
    this.teardownWs();
  }

  getStatus(): ListenerStatus {
    return {
      wsConnected: this.wsClient !== null,
      wssEndpoint: this.wssPool.current,
      lastProcessedBlock: this.lastProcessedBlock?.toString() ?? null,
      lastBlockAt: this.lastBlockAt ? new Date(this.lastBlockAt).toISOString() : null,
      reconnectAttempts: this.reconnectAttempts,
    };
  }

  // ---- WSS lifecycle -------------------------------------------------

  private subscribe(): void {
    if (this.stopped) return;
    logger.info("starting block subscription", {
      wss: this.wssPool.current,
      attempt: this.reconnectAttempts,
    });

    this.wsClient = createPublicClient({
      chain: mainnet,
      // We own the reconnect loop; disable viem's so the two don't fight.
      transport: webSocket(this.wssPool.current, {
        reconnect: false,
        keepAlive: { interval: 15_000 },
      }),
    });

    this.unwatch = this.wsClient.watchBlockNumber({
      emitOnBegin: true,
      onBlockNumber: (blockNumber) => {
        this.lastBlockAt = Date.now();
        this.reconnectAttempts = 0; // healthy again
        this.enqueue(blockNumber);
      },
      onError: (err) => {
        logger.error("block subscription error", { error: err.message });
        this.scheduleReconnect("subscription error");
      },
    });
  }

  private teardownWs(): void {
    try {
      this.unwatch?.();
    } catch {
      /* already dead */
    }
    this.unwatch = null;
    this.wsClient = null;
  }

  private scheduleReconnect(reason: string): void {
    if (this.stopped || this.reconnecting) return;
    this.reconnecting = true;
    this.teardownWs();

    const delay = backoffDelay(this.reconnectAttempts, {
      baseMs: 1_000,
      capMs: 60_000,
    });
    this.reconnectAttempts++;

    // Backoff alone can't outlast an endpoint that is down for our IP —
    // after repeated failures, try a different provider.
    if (
      this.reconnectAttempts % ROTATE_WSS_AFTER === 0 &&
      this.wssPool.size > 1
    ) {
      const next = this.wssPool.rotate();
      logger.warn("rotating wss endpoint", {
        endpoint: next,
        consecutiveFailures: this.reconnectAttempts,
      });
    }

    logger.warn("reconnecting websocket", {
      reason,
      attempt: this.reconnectAttempts,
      delayMs: delay,
    });

    void sleep(delay).then(() => {
      this.reconnecting = false;
      this.subscribe();
    });
  }

  private checkStale(): void {
    if (this.stopped || this.reconnecting || this.lastBlockAt === null) return;
    const sinceLastHead = Date.now() - this.lastBlockAt;
    if (sinceLastHead > STALE_HEAD_MS) {
      logger.warn("no new heads received — subscription looks dead", {
        sinceLastHeadMs: sinceLastHead,
      });
      this.scheduleReconnect("stale heads");
    }
  }

  // ---- Block processing ----------------------------------------------

  /** Serialize range processing so blocks are handled in order. */
  private enqueue(head: bigint): void {
    this.processing = this.processing
      .then(() => this.processUpTo(head))
      .catch((err) => {
        // Range stays unprocessed; lastProcessedBlock is untouched, so the
        // next head widens the range and this data is retried, not lost.
        logger.error("failed processing block range, will retry on next head", {
          head: head.toString(),
          error: (err as Error).message,
        });
      });
  }

  private async processUpTo(head: bigint): Promise<void> {
    if (this.stopped) return;
    let fromBlock =
      this.lastProcessedBlock === null ? head : this.lastProcessedBlock + 1n;
    if (head < fromBlock) return; // duplicate/old head

    // A persisted cursor can be arbitrarily stale (long outage between
    // runs). Blocks older than the 1h metric window carry no signal, so
    // skip ahead rather than replaying history into current-time buckets.
    if (head - fromBlock + 1n > MAX_STARTUP_GAP) {
      const skippedTo = head - MAX_STARTUP_GAP + 1n;
      logger.warn("gap exceeds metric window, skipping ahead", {
        cursorBlock: (fromBlock - 1n).toString(),
        skippedTo: skippedTo.toString(),
        skippedBlocks: (skippedTo - fromBlock).toString(),
      });
      fromBlock = skippedTo;
    }

    if (head - fromBlock > 0n) {
      logger.info("backfilling missed blocks", {
        fromBlock: fromBlock.toString(),
        toBlock: head.toString(),
        gap: (head - fromBlock + 1n).toString(),
      });
    }

    // Chunk large ranges (post-outage backfill) to respect node limits.
    while (fromBlock <= head) {
      const toBlock =
        fromBlock + MAX_LOGS_RANGE - 1n < head ? fromBlock + MAX_LOGS_RANGE - 1n : head;
      await this.processRange(fromBlock, toBlock);
      this.lastProcessedBlock = toBlock;
      // Fire-and-forget: cursor persistence is an optimization for the next
      // restart; the current run's correctness never depends on it.
      void this.cursorStore?.save(toBlock).catch((err) => {
        logger.warn("cursor save failed", { error: (err as Error).message });
      });
      fromBlock = toBlock + 1n;
    }
  }

  private async processRange(fromBlock: bigint, toBlock: bigint): Promise<void> {
    // Anchor the first attempt to the provider currently serving WSS heads:
    // the node that just announced this block definitionally has it, while
    // other providers may lag a second or two behind ("Invalid parameters"
    // for a block they haven't seen). Later attempts rotate away, so the
    // anchor provider failing doesn't burn the whole retry budget.
    // (With default config both pools list the same providers in the same
    // order; if overridden unevenly, modulo keeps this safe, just unaligned.)
    const anchor = this.wssPool.currentIndex;
    const logs = await withRetry(
      (attempt) =>
        this.httpClients[(anchor + attempt) % this.httpClients.length]!.getLogs({
          address: config.contractAddress,
          event: transferEvent,
          fromBlock,
          toBlock,
        }),
      { ...RETRY, label: "getLogs" },
    );

    const transfers: Transfer[] = logs.map((log) => ({
      from: log.args.from as string,
      to: log.args.to as string,
      value: log.args.value as bigint,
      blockNumber: log.blockNumber,
      txHash: log.transactionHash,
    }));

    await this.onBatch({ fromBlock, toBlock, transfers });

    logger.info("processed blocks", {
      fromBlock: fromBlock.toString(),
      toBlock: toBlock.toString(),
      transfers: transfers.length,
    });
  }
}
