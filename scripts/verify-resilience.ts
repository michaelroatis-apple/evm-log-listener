/**
 * Assertions for the backoff/retry logic.
 * Run with: npx tsx scripts/verify-resilience.ts
 */
import { backoffDelay, withRetry } from "../src/resilience.js";

function assert(cond: boolean, label: string) {
  if (!cond) {
    console.error(`FAIL: ${label}`);
    process.exit(1);
  }
  console.log(`ok: ${label}`);
}

async function main() {
  // 1. Backoff delays are jittered within [0, min(base * 2^n, cap)].
  const opts = { baseMs: 1000, capMs: 8000 };
  for (let attempt = 0; attempt < 10; attempt++) {
    const expectedMax = Math.min(1000 * 2 ** attempt, 8000);
    for (let i = 0; i < 200; i++) {
      const d = backoffDelay(attempt, opts);
      if (d < 0 || d >= expectedMax + 1) {
        assert(false, `delay ${d} out of range for attempt ${attempt}`);
      }
    }
  }
  assert(true, "backoff delays bounded by min(base*2^n, cap), jittered");

  // Jitter sanity: 100 samples at the same attempt should not all be equal.
  const samples = new Set(Array.from({ length: 100 }, () => backoffDelay(5, opts)));
  assert(samples.size > 10, "jitter produces varied delays");

  // 2. withRetry succeeds once the underlying call recovers.
  let calls = 0;
  const result = await withRetry(
    async () => {
      calls++;
      if (calls < 3) throw new Error("socket hang up");
      return "recovered";
    },
    { baseMs: 1, capMs: 5, maxAttempts: 5, label: "test" },
  );
  assert(result === "recovered" && calls === 3, "withRetry recovers after transient failures");

  // 3. withRetry rethrows after exhausting attempts.
  calls = 0;
  let threw = false;
  try {
    await withRetry(
      async () => {
        calls++;
        throw new Error("429 Too Many Requests");
      },
      { baseMs: 1, capMs: 2, maxAttempts: 3, label: "test429" },
    );
  } catch (err) {
    threw = true;
    assert((err as Error).message.includes("429"), "original error preserved");
  }
  assert(threw && calls === 3, "withRetry gives up after maxAttempts");

  console.log("\nAll resilience assertions passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
