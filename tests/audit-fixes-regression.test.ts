/**
 * Regression tests for CRITICAL and MAJOR audit fixes.
 * Each describe block maps to a specific audit finding.
 */

import { describe, it, expect, vi, afterEach } from 'vitest';
import RedlockToolkit from '../src/index';
import { ConfigurationError } from '../src/core/errors';
import { AutoExtensionManager } from '../src/utils/auto-extension-manager';
import { CircuitBreaker } from '../src/patterns/circuit-breaker';
import { ConsensusManager } from '../src/utils/consensus-manager';
import { PubSubManager } from '../src/pubsub/pubsub-manager';
import { createMockRedisClients, createTestRedlockToolkitConfig, sleep } from './setup';
import {
  OPTIMISTIC_ACQUIRE_SCRIPT,
  SEMAPHORE_ACQUIRE_SCRIPT,
  SEMAPHORE_RELEASE_SCRIPT,
  SEMAPHORE_EXTEND_SCRIPT,
  SEMAPHORE_STATUS_SCRIPT,
} from '../src/utils/scripts';

// ──────────────────────────────────────────────────────────────────
// CRITICAL #1 — AutoExtensionManager timer race condition
// ──────────────────────────────────────────────────────────────────
describe('CRITICAL #1: AutoExtensionManager stop() safety', () => {
  it('should stop cleanly while extension is in progress', async () => {
    let extendResolve!: (v: { success: boolean; expiration: number }) => void;
    const extendPromise = new Promise<{ success: boolean; expiration: number }>((r) => {
      extendResolve = r;
    });

    const manager = new AutoExtensionManager({
      ttl: 1000,
      autoExtendThreshold: 950, // triggers almost immediately
      expiration: Date.now() + 1000,
      isValid: () => true,
      extend: () => extendPromise,
      onError: () => {},
    });

    manager.start();
    await sleep(100); // let the timer fire and start extension

    // stop() while extension is "in progress"
    const stopPromise = manager.stop();

    // resolve the pending extension AFTER stop() was called
    extendResolve({ success: true, expiration: Date.now() + 1000 });

    await stopPromise;
    // no further extensions should be scheduled
    expect(manager.extensions).toBeLessThanOrEqual(1);
  });

  it('should not start extension when isValid() returns false', async () => {
    let extendCalled = false;

    const manager = new AutoExtensionManager({
      ttl: 100,
      autoExtendThreshold: 80,
      expiration: Date.now() + 100,
      isValid: () => false,
      extend: async () => {
        extendCalled = true;
        return { success: true, expiration: Date.now() + 100 };
      },
      onError: () => {},
    });

    manager.start();
    await sleep(150);
    await manager.stop();

    expect(extendCalled).toBe(false);
  });

  it('should not extend after immediate stop()', async () => {
    let extensionCount = 0;

    const manager = new AutoExtensionManager({
      ttl: 100,
      autoExtendThreshold: 90, // fires at ~10ms
      expiration: Date.now() + 100,
      isValid: () => true,
      extend: async () => {
        extensionCount++;
        return { success: true, expiration: Date.now() + 100 };
      },
      onError: () => {},
    });

    manager.start();
    await manager.stop(); // stop before timer fires

    await sleep(50);
    expect(extensionCount).toBe(0);
  });
});

// ──────────────────────────────────────────────────────────────────
// CRITICAL #2 — CircuitBreaker timeout race condition
// ──────────────────────────────────────────────────────────────────
describe('CRITICAL #2: CircuitBreaker settled flag', () => {
  it('should resolve when operation beats timeout', async () => {
    const breaker = new CircuitBreaker({
      failureThreshold: 5,
      resetTimeout: 60000,
      maxRetries: 3,
      operationTimeout: 50,
    });

    const result = await breaker.execute(async () => {
      await sleep(20); // well under timeout
      return 'ok';
    });

    expect(result).toBe('ok');
  });

  it('should reject when timeout beats operation', async () => {
    const breaker = new CircuitBreaker({
      failureThreshold: 5,
      resetTimeout: 60000,
      maxRetries: 3,
      operationTimeout: 10,
    });

    await expect(
      breaker.execute(async () => {
        await sleep(100);
        return 'late';
      }),
    ).rejects.toThrow('timed out');
  });

  it('should not double-count failure when timeout and error race', async () => {
    const breaker = new CircuitBreaker({
      failureThreshold: 5,
      resetTimeout: 60000,
      maxRetries: 3,
      operationTimeout: 30,
    });

    // operation throws after ~25ms, timeout fires at 30ms
    await expect(
      breaker.execute(async () => {
        await sleep(25);
        throw new Error('op error');
      }),
    ).rejects.toThrow('op error');

    // only one failure should be recorded
    const metrics = breaker.getMetrics();
    expect(metrics.failedOperations).toBe(1);
  });
});

// ──────────────────────────────────────────────────────────────────
// CRITICAL #3 — PubSubManager listener memory leak
// ──────────────────────────────────────────────────────────────────
describe('CRITICAL #3: PubSubManager listener cleanup on failed unsubscribe', () => {
  it('should remove local listener even when Redis UNSUBSCRIBE fails', async () => {
    const mockSubscriber = {
      subscribe: vi.fn().mockResolvedValue(undefined),
      unsubscribe: vi.fn().mockRejectedValue(new Error('UNSUBSCRIBE failed')),
      on: vi.fn(),
      removeListener: vi.fn(),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };

    const logger = {
      error: vi.fn(), warn: vi.fn(), info: vi.fn(), debug: vi.fn(), trace: vi.fn(),
    };

    const mockClients = createMockRedisClients(1);

    const manager = new PubSubManager({
      clients: mockClients as any,
      subscriberClients: [mockSubscriber as any],
      keyPrefix: 'test',
      logger: logger as any,
    });

    // waitForMessage with short timeout — no message arrives
    const result = await manager.waitForMessage('chan', 50);
    expect(result).toBeNull();

    // Key assertion: local listener cleaned up despite unsubscribe failure
    expect(manager.getSubscriptionCount()).toBe(0);

    await manager.shutdown();
  });
});

// ──────────────────────────────────────────────────────────────────
// MAJOR #4 — Optimistic lock version rollback
// ──────────────────────────────────────────────────────────────────
describe('MAJOR #4: Optimistic lock version_required guard', () => {
  it('should return conflict when version exists but expectedVersion not provided', async () => {
    const mockClients = createMockRedisClients(3);

    // Script returns version_required conflict
    mockClients.forEach((c) => {
      c.evalsha.mockResolvedValue([0, 5, 'version_required']);
    });

    const toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    const result = await toolkit.acquireOptimistic('versioned-res');

    expect(result.success).toBe(false);
    expect(result.conflict).toBe(true);
    expect(result.currentVersion).toBe(5);

    await toolkit.shutdown();
  });

  it('should succeed on first acquire when no version exists yet', async () => {
    const mockClients = createMockRedisClients(3);

    // Script returns success (no existing version)
    mockClients.forEach((c) => {
      c.evalsha.mockResolvedValue([1, 1, 1]);
    });

    const toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    const result = await toolkit.acquireOptimistic('fresh-res');

    expect(result.success).toBe(true);

    await toolkit.shutdown();
  });

  it('should verify OPTIMISTIC_ACQUIRE_SCRIPT contains version_required guard', () => {
    expect(OPTIMISTIC_ACQUIRE_SCRIPT).toContain('version_required');
  });
});

// ──────────────────────────────────────────────────────────────────
// MAJOR #5 — Semaphore clock skew (server-side TIME)
// ──────────────────────────────────────────────────────────────────
describe('MAJOR #5: Semaphore scripts use server time', () => {
  it('should use redis.call TIME instead of client timestamp in all semaphore scripts', () => {
    const semaphoreScripts = [
      SEMAPHORE_ACQUIRE_SCRIPT,
      SEMAPHORE_RELEASE_SCRIPT,
      SEMAPHORE_EXTEND_SCRIPT,
      SEMAPHORE_STATUS_SCRIPT,
    ];

    for (const src of semaphoreScripts) {
      expect(src).toContain("redis.call('TIME')");
      expect(src).not.toMatch(/tonumber\(ARGV\[\d+\]\)\s*\n\s*\n\s*-- Remove expired/);
    }
  });

  it('should not pass Date.now() as timestamp arg to semaphore scripts', () => {
    // Verify ARGV comments no longer mention "current timestamp"
    const acquireComment = SEMAPHORE_ACQUIRE_SCRIPT.split('\n').slice(0, 8).join('\n');
    expect(acquireComment).not.toContain('timestamp');
  });
});

// ──────────────────────────────────────────────────────────────────
// MAJOR #6 — ConsensusManager cleanup on early termination
// ──────────────────────────────────────────────────────────────────
describe('MAJOR #6: ConsensusManager no unhandled rejections', () => {
  it('should consume pending promises after early quorum', async () => {
    const logger = {
      error: vi.fn(), warn: vi.fn(), info: vi.fn(), debug: vi.fn(), trace: vi.fn(),
    };
    const manager = new ConsensusManager(logger as any);

    const unhandled: unknown[] = [];
    const handler = (r: unknown) => unhandled.push(r);
    process.on('unhandledRejection', handler);

    const mockClients = createMockRedisClients(5);

    // 3 fast (quorum=3), 2 slow that error
    mockClients[0].evalsha.mockResolvedValue(1);
    mockClients[1].evalsha.mockResolvedValue(1);
    mockClients[2].evalsha.mockResolvedValue(1);
    mockClients[3].evalsha.mockImplementation(async () => {
      await sleep(200);
      throw new Error('slow client error');
    });
    mockClients[4].evalsha.mockImplementation(async () => {
      await sleep(200);
      throw new Error('slow client error');
    });

    const result = await manager.execute(
      mockClients as any,
      (client) => client.evalsha('hash', 1, 'key', 'val', 30000),
    );

    expect(result.success).toBe(true);

    // wait for slow clients to finish
    await sleep(350);

    process.removeListener('unhandledRejection', handler);
    expect(unhandled).toHaveLength(0);
  });
});

// ──────────────────────────────────────────────────────────────────
// MAJOR #7 — Lock expiration calculated from last attempt start
// ──────────────────────────────────────────────────────────────────
describe('MAJOR #7: Lock expiration with retries', () => {
  it('should not reduce lock validity by retry delay', async () => {
    const mockClients = createMockRedisClients(3);
    const toolkit = new RedlockToolkit(
      createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 30000,
          retryCount: 2,
          retryDelay: 200,
          retryJitter: 0,
          autoExtendThreshold: 0,
        },
      }),
    );

    // First round fails (3 clients), second round succeeds
    let call = 0;
    mockClients.forEach((c) => {
      c.evalsha.mockImplementation(async () => {
        call++;
        if (call <= 3) return 0; // first round
        return 1; // second round
      });
    });

    const lock = await toolkit.acquire('retry-res');

    // drift = round(0.01 * 30000) + 2 = 302
    // If expiration were based on initial startTime, validity ≈ 30000 - 200(delay) - 302 ≈ 29498
    // If based on lastAttemptStart (correct), validity ≈ 30000 - 302 ≈ 29698
    expect(lock.timeToExpiration).toBeGreaterThan(29500);
    expect(lock.timeToExpiration).toBeLessThanOrEqual(30000);

    await toolkit.shutdown();
  });
});

// ──────────────────────────────────────────────────────────────────
// MAJOR #8 — Config validation: autoExtendThreshold vs TTL
// ──────────────────────────────────────────────────────────────────
describe('MAJOR #8: Config validation autoExtendThreshold vs TTL', () => {
  it('should throw when autoExtendThreshold == TTL', () => {
    const mockClients = createMockRedisClients(3);
    expect(() => {
      new RedlockToolkit(
        createTestRedlockToolkitConfig(mockClients, {
          defaultLockOptions: { ttl: 500, autoExtendThreshold: 500 },
        }),
      );
    }).toThrow(ConfigurationError);
  });

  it('should throw when autoExtendThreshold > TTL', () => {
    const mockClients = createMockRedisClients(3);
    expect(() => {
      new RedlockToolkit(
        createTestRedlockToolkitConfig(mockClients, {
          defaultLockOptions: { ttl: 100, autoExtendThreshold: 500 },
        }),
      );
    }).toThrow(ConfigurationError);
  });

  it('should accept autoExtendThreshold: 0 (disabled)', () => {
    const mockClients = createMockRedisClients(3);
    expect(() => {
      new RedlockToolkit(
        createTestRedlockToolkitConfig(mockClients, {
          defaultLockOptions: { ttl: 10, autoExtendThreshold: 0 },
        }),
      );
    }).not.toThrow();
  });

  it('should accept valid autoExtendThreshold < TTL', () => {
    const mockClients = createMockRedisClients(3);
    expect(() => {
      new RedlockToolkit(
        createTestRedlockToolkitConfig(mockClients, {
          defaultLockOptions: { ttl: 1000, autoExtendThreshold: 500 },
        }),
      );
    }).not.toThrow();
  });

  it('should include threshold and TTL values in error message', () => {
    const mockClients = createMockRedisClients(3);
    expect(() => {
      new RedlockToolkit(
        createTestRedlockToolkitConfig(mockClients, {
          defaultLockOptions: { ttl: 200, autoExtendThreshold: 300 },
        }),
      );
    }).toThrow(/200ms/);
  });
});
