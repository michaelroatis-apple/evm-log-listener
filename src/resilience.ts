import { logger } from "./logger.js";

export interface BackoffOptions {
  /** First delay in ms. */
  baseMs: number;
  /** Upper bound for any single delay. */
  capMs: number;
}

/**
 * Exponential backoff with full jitter (AWS-style).
 *
 * Jitter matters against public RPC nodes: if many clients reconnect on the
 * same schedule after an outage, synchronized retries re-trigger the 429s
 * that caused the outage in the first place.
 */
export function backoffDelay(attempt: number, opts: BackoffOptions): number {
  const exp = Math.min(opts.baseMs * 2 ** attempt, opts.capMs);
  return Math.floor(Math.random() * exp);
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRateLimit(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  return /\b429\b|rate limit|too many requests/i.test(msg);
}

/**
 * Round-robin pool of RPC endpoints.
 *
 * Backoff alone can't fix an endpoint that is effectively down for our IP
 * (e.g. a public node blocking cloud-provider egress ranges for hours).
 * The pool lets callers rotate to a different provider after repeated
 * failures instead of hammering one host forever.
 */
export class EndpointPool {
  private index = 0;

  constructor(private readonly endpoints: readonly string[]) {
    if (endpoints.length === 0) {
      throw new Error("EndpointPool requires at least one endpoint");
    }
  }

  get current(): string {
    return this.endpoints[this.index]!;
  }

  /** Position in the pool — lets callers align a second pool with this one. */
  get currentIndex(): number {
    return this.index;
  }

  get size(): number {
    return this.endpoints.length;
  }

  /** Advance round-robin; returns the new current endpoint. */
  rotate(): string {
    this.index = (this.index + 1) % this.endpoints.length;
    return this.current;
  }
}

export interface RetryOptions extends BackoffOptions {
  /** Give up after this many attempts (the error is rethrown). */
  maxAttempts: number;
  /** Label used in log lines. */
  label: string;
}

/**
 * Retry an async operation with exponential backoff + jitter.
 * 429s get an extra flat penalty on top of the computed delay so we back
 * off harder when the node is explicitly telling us to slow down.
 *
 * The attempt index is passed to `fn` so callers can rotate across
 * endpoints between attempts.
 */
export async function withRetry<T>(
  fn: (attempt: number) => Promise<T>,
  opts: RetryOptions,
): Promise<T> {
  let lastError: unknown;
  for (let attempt = 0; attempt < opts.maxAttempts; attempt++) {
    try {
      return await fn(attempt);
    } catch (err) {
      lastError = err;
      const rateLimited = isRateLimit(err);
      const delay = backoffDelay(attempt, opts) + (rateLimited ? 2_000 : 0);
      // A single retry is normal operation (e.g. provider head-lag); only
      // repeated failures warrant warning-level attention.
      const log = attempt === 0 ? logger.debug : logger.warn;
      log(`${opts.label} failed, retrying`, {
        attempt: attempt + 1,
        maxAttempts: opts.maxAttempts,
        delayMs: delay,
        rateLimited,
        error: err instanceof Error ? err.message : String(err),
      });
      await sleep(delay);
    }
  }
  throw lastError;
}
