import type { Address } from "viem";

function env(name: string, fallback?: string): string {
  const value = process.env[name] ?? fallback;
  if (value === undefined) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

const ADDRESS_RE = /^0x[0-9a-fA-F]{40}$/;

function envAddress(name: string, fallback: string): Address {
  const value = env(name, fallback);
  if (!ADDRESS_RE.test(value)) {
    throw new Error(`${name} is not a valid EVM address: ${value}`);
  }
  return value as Address;
}

export const config = {
  /** WebSocket RPC endpoint for live subscriptions. */
  rpcWssUrl: env("RPC_WSS_URL", "wss://ethereum-rpc.publicnode.com"),
  /** HTTP RPC endpoint for gap backfill via eth_getLogs. */
  rpcHttpUrl: env("RPC_HTTP_URL", "https://ethereum-rpc.publicnode.com"),
  /** Contract whose Transfer events we index (default: USDC on mainnet). */
  contractAddress: envAddress(
    "CONTRACT_ADDRESS",
    "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
  ),
  /** Token decimals used for human-readable volume (USDC = 6). */
  tokenDecimals: Number(env("TOKEN_DECIMALS", "6")),
  redisUrl: env("REDIS_URL", "redis://localhost:6379"),
  port: Number(env("PORT", "3000")),
  logLevel: env("LOG_LEVEL", "info") as "debug" | "info" | "warn" | "error",
} as const;
