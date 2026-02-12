/**
 * Real Redis test utilities.
 * Provides helpers for integration tests that connect to a real Redis instance.
 */

import Redis from 'ioredis';
import RedlockToolkit from '../src/index';
import type { RedlockToolkitConfig } from '../src/core/types';

/**
 * Check if Redis is reachable on localhost:6379.
 */
export async function isRedisAvailable(): Promise<boolean> {
  const client = new Redis({
    host: 'localhost',
    port: 6379,
    db: 0,
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
 * Create `count` ioredis clients on localhost:6379 with db indices 0..count-1.
 */
export function createRedisClients(count: number): Redis[] {
  return Array.from({ length: count }, (_, i) =>
    new Redis({
      host: 'localhost',
      port: 6379,
      db: i,
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
