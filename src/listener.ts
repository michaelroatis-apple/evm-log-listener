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

/**
 * Live event listener.
 *
 * Strategy: subscribe to new block numbers over WebSocket, then fetch the
 * Transfer logs for each new block range via eth_getLogs over HTTP.
 *
 * Compared to a raw log subscription this has two advantages:
 *  1. Live ingestion and post-reconnect backfill share one code path — a
 *     reconnect simply widens the next block range to fetch.
 *  2. Logs arrive naturally batched per block, which lets the metrics layer
 *     write to Redis in one pipeline per block instead of one write per log.
 */
export class EventListener {
  private wsClient: PublicClient;
  private httpClient: PublicClient;
  private lastProcessedBlock: bigint | null = null;
  private unwatch: (() => void) | null = null;
  private processing = Promise.resolve();
  private stopped = false;

  constructor(private readonly onBatch: BatchHandler) {
    this.wsClient = createPublicClient({
      chain: mainnet,
      transport: webSocket(config.rpcWssUrl),
    });
    this.httpClient = createPublicClient({
      chain: mainnet,
      transport: http(config.rpcHttpUrl),
    });
  }

  start(): void {
    logger.info("starting block subscription", { wss: config.rpcWssUrl });
    this.unwatch = this.wsClient.watchBlockNumber({
      emitOnBegin: true,
      onBlockNumber: (blockNumber) => this.enqueue(blockNumber),
      onError: (err) => {
        logger.error("block subscription error", { error: err.message });
      },
    });
  }

  stop(): void {
    this.stopped = true;
    this.unwatch?.();
  }

  /** Serialize range processing so blocks are handled in order. */
  private enqueue(head: bigint): void {
    this.processing = this.processing
      .then(() => this.processUpTo(head))
      .catch((err) => {
        logger.error("failed processing block range", {
          head: head.toString(),
          error: (err as Error).message,
        });
      });
  }

  private async processUpTo(head: bigint): Promise<void> {
    if (this.stopped) return;
    const fromBlock =
      this.lastProcessedBlock === null ? head : this.lastProcessedBlock + 1n;
    if (head < fromBlock) return; // already seen (duplicate head)

    const logs = await this.httpClient.getLogs({
      address: config.contractAddress,
      event: transferEvent,
      fromBlock,
      toBlock: head,
    });

    const transfers: Transfer[] = logs.map((log) => ({
      from: log.args.from as string,
      to: log.args.to as string,
      value: log.args.value as bigint,
      blockNumber: log.blockNumber,
      txHash: log.transactionHash,
    }));

    await this.onBatch({ fromBlock, toBlock: head, transfers });
    this.lastProcessedBlock = head;

    logger.info("processed blocks", {
      fromBlock: fromBlock.toString(),
      toBlock: head.toString(),
      transfers: transfers.length,
    });
  }
}
