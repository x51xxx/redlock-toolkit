/**
 * Real Redis test utilities.
 * Provides helpers for integration tests that connect to a real Redis instance.
 */

import Redis from 'ioredis';
import RedlockToolkit from '../src/index';
import type { RedlockToolkitConfig } from '../src/core/types';

/**
 * Test Redis target. Defaults to localhost:6379 (unchanged behavior), but each piece is
 * env-overridable so the FLUSHDB-heavy integration tests can be pointed at a DISPOSABLE instance
 * instead of a shared dev Redis:
 *   REDLOCK_TEST_REDIS_HOST  (default "localhost")
 *   REDLOCK_TEST_REDIS_PORT  (default 6379)
 *   REDLOCK_TEST_REDIS_DB    (default 0 — base db index; multi-client tests use db..db+count-1)
 */
const TEST_HOST = process.env.REDLOCK_TEST_REDIS_HOST ?? 'localhost';
const TEST_PORT = Number(process.env.REDLOCK_TEST_REDIS_PORT) || 6379;
const TEST_DB_BASE = Number(process.env.REDLOCK_TEST_REDIS_DB) || 0;

/**
 * Check if the (configured) Redis is reachable.
 */
export async function isRedisAvailable(): Promise<boolean> {
  const client = new Redis({
    host: TEST_HOST,
    port: TEST_PORT,
    db: TEST_DB_BASE,
    lazyConnect: true,
    connectTimeout: 1000,
    maxRetriesPerRequest: 0,
  });
  // Suppress ioredis unhandled error event
  client.on('error', () => {});
  try {
    await client.connect();
    const pong = await client.ping();
    await client.quit();
    return pong === 'PONG';
  } catch {
    try { await client.disconnect(); } catch { /* ignore */ }
    return false;
  }
}

/**
 * Create `count` ioredis clients on the configured host/port with db indices base..base+count-1.
 */
export function createRedisClients(count: number): Redis[] {
  return Array.from({ length: count }, (_, i) =>
    new Redis({
      host: TEST_HOST,
      port: TEST_PORT,
      db: TEST_DB_BASE + i,
      maxRetriesPerRequest: 3,
    }),
  );
}

/**
 * FLUSHDB on each client (flushes only the selected database).
 */
export async function flushClients(clients: Redis[]): Promise<void> {
  await Promise.all(clients.map((c) => c.flushdb()));
}

/**
 * Gracefully disconnect all clients.
 */
export async function disconnectClients(clients: Redis[]): Promise<void> {
  await Promise.all(clients.map((c) => c.quit().catch(() => { /* ignore */ })));
}

/**
 * Create a RedlockToolkit with sensible test defaults.
 */
export function createToolkit(
  clients: Redis[],
  overrides: Partial<RedlockToolkitConfig> = {},
): RedlockToolkit {
  return new RedlockToolkit({
    clients,
    defaultLockOptions: {
      ttl: 2000,
      retryCount: 3,
      retryDelay: 100,
      retryJitter: 50,
      driftFactor: 0.01,
      autoExtendThreshold: 500,
    },
    circuitBreaker: {
      failureThreshold: 1000,
      resetTimeout: 60000,
      maxRetries: 3,
      operationTimeout: 5000,
    },
    enableMetrics: true,
    ...overrides,
  });
}

/**
 * Create a single ioredis client that will never connect (for partial quorum tests).
 */
export function createUnreachableClient(): Redis {
  const client = new Redis({
    host: 'localhost',
    port: 16379, // nothing listens here
    connectTimeout: 200,
    maxRetriesPerRequest: 0,
    retryStrategy: () => null,
    enableOfflineQueue: false,
    lazyConnect: true,
  });
  client.on('error', () => {});
  return client;
}

/**
 * Async sleep helper.
 */
export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
