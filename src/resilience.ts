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
 */
export async function withRetry<T>(
  fn: () => Promise<T>,
  opts: RetryOptions,
): Promise<T> {
  let lastError: unknown;
  for (let attempt = 0; attempt < opts.maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      const rateLimited = isRateLimit(err);
      const delay = backoffDelay(attempt, opts) + (rateLimited ? 2_000 : 0);
      logger.warn(`${opts.label} failed, retrying`, {
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
