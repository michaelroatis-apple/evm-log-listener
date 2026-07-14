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
import { backoffDelay, sleep, withRetry } from "./resilience.js";

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

const RETRY = { baseMs: 500, capMs: 30_000, maxAttempts: 5 };

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
  private httpClient: PublicClient;
  private wsClient: PublicClient | null = null;
  private unwatch: (() => void) | null = null;

  private lastProcessedBlock: bigint | null = null;
  private lastBlockAt: number | null = null;
  private reconnectAttempts = 0;
  private reconnecting = false;
  private stopped = false;

  private processing = Promise.resolve();
  private watchdog: NodeJS.Timeout | null = null;

  constructor(private readonly onBatch: BatchHandler) {
    this.httpClient = createPublicClient({
      chain: mainnet,
      transport: http(config.rpcHttpUrl),
    });
  }

  start(): void {
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
      lastProcessedBlock: this.lastProcessedBlock?.toString() ?? null,
      lastBlockAt: this.lastBlockAt ? new Date(this.lastBlockAt).toISOString() : null,
      reconnectAttempts: this.reconnectAttempts,
    };
  }

  // ---- WSS lifecycle -------------------------------------------------

  private subscribe(): void {
    if (this.stopped) return;
    logger.info("starting block subscription", {
      wss: config.rpcWssUrl,
      attempt: this.reconnectAttempts,
    });

    this.wsClient = createPublicClient({
      chain: mainnet,
      // We own the reconnect loop; disable viem's so the two don't fight.
      transport: webSocket(config.rpcWssUrl, {
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
      fromBlock = toBlock + 1n;
    }
  }

  private async processRange(fromBlock: bigint, toBlock: bigint): Promise<void> {
    const logs = await withRetry(
      () =>
        this.httpClient.getLogs({
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
