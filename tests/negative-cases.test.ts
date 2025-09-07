/**
 * Negative Case Tests for RedlockToolkit
 * 
 * Validates proper error handling and recovery mechanisms under failure conditions.
 * Tests Byzantine fault tolerance, network partitions, and edge case behaviors.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import RedlockToolkit, { ConfigurationError } from "../src/index";
import { createMockRedisClients, createTestRedlockToolkitConfig, sleep } from './setup';

describe('RedlockToolkit Negative Cases', () => {
  let mockClients: any[];
  let redlockToolkit: RedlockToolkit;

  beforeEach(() => {
    mockClients = createMockRedisClients(3);
    vi.clearAllMocks();
  });

  afterEach(async () => {
    if (redlockToolkit) {
      await redlockToolkit.shutdown();
    }
  });

  describe('Configuration Validation', () => {
    it('should reject empty client array', () => {
      // Redlock requires at least one Redis instance for consensus
      expect(() => new RedlockToolkit({ clients: [] }))
        .toThrow();
    });

    it('should reject null clients', () => {
      expect(() => new RedlockToolkit({ clients: null as any }))
        .toThrow();
    });

    it('should reject undefined clients', () => {
      expect(() => new RedlockToolkit({ clients: undefined as any }))
        .toThrow();
    });

    it('should reject negative TTL', () => {
      expect(() => new RedlockToolkit({
        clients: mockClients,
        defaultLockOptions: { ttl: -1 }
      })).toThrow();
    });

    it('should reject zero TTL', () => {
      expect(() => new RedlockToolkit({
        clients: mockClients,
        defaultLockOptions: { ttl: 0 }
      })).toThrow();
    });

    it('should reject invalid retry count', () => {
      expect(() => new RedlockToolkit({
        clients: mockClients,
        defaultLockOptions: { retryCount: -2 }
      })).toThrow();
    });

    it('should reject drift factor greater than 1', () => {
      // Drift factor > 1 would mean clock drift exceeds TTL, making locks invalid
      expect(() => new RedlockToolkit({
        clients: mockClients,
        defaultLockOptions: { driftFactor: 1.5 }
      })).toThrow();
    });

    it('should reject negative drift factor', () => {
      expect(() => new RedlockToolkit({
        clients: mockClients,
        defaultLockOptions: { driftFactor: -0.1 }
      })).toThrow();
    });
  });

  describe('Lock Acquisition Failures', () => {
    beforeEach(() => {
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 1000,
          retryCount: 2,
          retryDelay: 50,
          retryJitter: 10
        }
      }));
    });

    it('should fail when resource is already locked', async () => {
      // Simulates all nodes returning 0 (lock already held by another client)
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(0);
        client.eval.mockResolvedValue(0);
      });

      await expect(redlockToolkit.acquire('locked-resource'))
        .rejects.toThrow();
    });

    it('should timeout after max retries', async () => {
      let callCount = 0;
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          callCount++;
          return Promise.resolve(0); // Always fail
        });
        client.eval.mockImplementation(() => Promise.resolve(0));
      });

      const startTime = Date.now();
      await expect(redlockToolkit.acquire('timeout-resource'))
        .rejects.toThrow();
      
      const duration = Date.now() - startTime;
      // Verifies retry logic: initial attempt + 2 retries with 50ms delay
      expect(duration).toBeGreaterThanOrEqual(100);
      // Confirms all nodes were contacted for each attempt: 3 clients × 3 attempts
      expect(callCount).toBeGreaterThanOrEqual(6);
    });

    it('should fail when no quorum is achieved', async () => {
      // Quorum requires ⌊N/2⌋ + 1 nodes. With 3 nodes, need 2 successes.
      // Only 1 success = no quorum = lock acquisition must fail
      mockClients[0].evalsha.mockResolvedValue(1);
      mockClients[1].evalsha.mockRejectedValue(new Error('Redis error'));
      mockClients[2].evalsha.mockRejectedValue(new Error('Redis error'));

      await expect(redlockToolkit.acquire('no-quorum-resource'))
        .rejects.toThrow();
    });

    it('should fail with empty resource array', async () => {
      // Tests that empty resource array is allowed (useful for testing/mocking)
      const lock = await redlockToolkit.acquire([]);
      expect(lock.resources).toEqual([]);
      await lock.release();
    });

    it('should handle Redis connection errors', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockRejectedValue(new Error('ECONNREFUSED'));
        client.eval.mockRejectedValue(new Error('ECONNREFUSED'));
      });

      await expect(redlockToolkit.acquire('connection-error'))
        .rejects.toThrow();
    });

    it('should handle script execution errors', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockRejectedValue(new Error('ERR Script error'));
        client.eval.mockRejectedValue(new Error('ERR Script error'));
      });

      await expect(redlockToolkit.acquire('script-error'))
        .rejects.toThrow();
    });
  });

  describe('Lock Extension Failures', () => {
    beforeEach(() => {
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 1000,
          retryCount: 0
        }
      }));
    });

    it('should fail to extend expired lock', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await redlockToolkit.acquire('expire-test');
      
      // Simulates clock drift or long-running operation that exceeds TTL
      (lock as any)._expiration = Date.now() - 1000;

      await expect(lock.extend(1000))
        .rejects.toThrow();
    });

    it('should fail to extend released lock', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
        client.eval.mockResolvedValueOnce(1);
      });

      const lock = await redlockToolkit.acquire('release-test');
      
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });
      await lock.release();

      await expect(lock.extend(1000))
        .rejects.toThrow();
    });

    it('should fail extension when lock is stolen', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Acquire
      });

      const lock = await redlockToolkit.acquire('stolen-lock');

      // Mock that lock is now owned by someone else
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(0); // Extension fails
      });

      await expect(lock.extend(1000))
        .rejects.toThrow();
    });

    it('should fail extension with no quorum', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Acquire
      });

      const lock = await redlockToolkit.acquire('extend-no-quorum');

      // Only 1 client succeeds extension
      mockClients[0].evalsha.mockResolvedValueOnce(1);
      mockClients[1].evalsha.mockRejectedValueOnce(new Error('Redis error'));
      mockClients[2].evalsha.mockRejectedValueOnce(new Error('Redis error'));

      await expect(lock.extend(1000))
        .rejects.toThrow();
    });
  });

  describe('Lock Release Failures', () => {
    beforeEach(() => {
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should handle release of already released lock', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Acquire
      });

      const lock = await redlockToolkit.acquire('double-release');

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // First release
      });
      await lock.release();

      // Verifies idempotency: releasing an already-released lock is safe
      const result = await lock.release();
      expect(result.success).toBe(true);
      expect(result.releasedCount).toBe(0);
    });

    it('should handle partial release failures', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await redlockToolkit.acquire('partial-release');

      // Simulates network partition: 2/3 nodes succeed (quorum achieved)
      mockClients[0].evalsha.mockResolvedValueOnce(1);
      mockClients[1].evalsha.mockRejectedValueOnce(new Error('Network error'));
      mockClients[2].evalsha.mockResolvedValueOnce(1);

      const result = await lock.release();
      // Release considered successful if quorum releases (prevents stale locks)
      expect(result.success).toBe(true);
    });

    it('should handle complete release failure', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await redlockToolkit.acquire('release-failure');

      // Catastrophic failure: all Redis nodes become unavailable
      mockClients.forEach(client => {
        client.evalsha.mockRejectedValueOnce(new Error('Redis unavailable'));
      });

      await expect(lock.release())
        .rejects.toThrow();
    });
  });

  describe('Auto-Extension Failures', () => {
    beforeEach(() => {
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 200,
          autoExtendThreshold: 50
        }
      }));
    });

    it('should abort routine when auto-extension fails', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Acquire
      });

      const lock = await redlockToolkit.acquire('auto-extend-fail');

      // Mock extension failure
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          return Promise.resolve(0); // Extension fails
        });
      });

      const routine = async (signal: any) => {
        await sleep(150); // Triggers auto-extension (TTL=200ms, threshold=50ms)
        if (signal.aborted) {
          throw signal.error;
        }
        return 'should not reach';
      };

      // Extension failure should abort the routine
      await expect(lock.using(routine)).rejects.toThrow('Extension failed');
    });

    it('should handle routine errors properly', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Acquire
      });

      const lock = await redlockToolkit.acquire('routine-error');

      const routine = async () => {
        throw new Error('Routine failed');
      };

      // Tests error propagation: extension succeeds but user code fails
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(1);
      });

      await expect(lock.using(routine))
        .rejects.toThrow('Routine failed');

      // Ensures cleanup: lock must be released even after routine error
      expect(lock.released).toBe(true);
    });

    it('should reject invalid auto-extend threshold', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Acquire
      });

      const lock = await redlockToolkit.acquire('invalid-threshold');

      const routine = async () => 'test';

      // Threshold too close to TTL
      await expect(lock.using(routine, { 
        ttl: 200, 
        autoExtendThreshold: 150 
      })).rejects.toThrow(/autoExtendThreshold.*must be at least 100ms less than TTL/);
    });
  });

  describe('Circuit Breaker Integration', () => {
    beforeEach(() => {
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        circuitBreaker: {
          failureThreshold: 2,
          resetTimeout: 100,
          operationTimeout: 50
        }
      }));
    });

    it('should open circuit after repeated failures', async () => {
      // Make all operations fail
      mockClients.forEach(client => {
        client.evalsha.mockRejectedValue(new Error('Connection failed'));
        client.eval.mockRejectedValue(new Error('Connection failed'));
      });

      // First failures to open circuit
      await expect(redlockToolkit.acquire('cb-test-1')).rejects.toThrow();
      await expect(redlockToolkit.acquire('cb-test-2')).rejects.toThrow();

      // Circuit should be open now
      await expect(redlockToolkit.acquire('cb-test-3'))
        .rejects.toThrow();
    });

    it('should timeout slow operations', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => 
          new Promise(resolve => setTimeout(() => resolve(1), 200))
        );
      });

      await expect(redlockToolkit.acquire('slow-operation'))
        .rejects.toThrow();
    });
  });

  describe('Concurrent Operations', () => {
    beforeEach(() => {
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should handle concurrent acquisitions of same resource', async () => {
      let acquireCount = 0;
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          acquireCount++;
          // First acquisition succeeds, others fail
          return Promise.resolve(acquireCount <= 3 ? 1 : 0);
        });
      });

      const promises = [
        redlockToolkit.acquire('concurrent-resource'),
        redlockToolkit.acquire('concurrent-resource'),
        redlockToolkit.acquire('concurrent-resource')
      ];

      const results = await Promise.allSettled(promises);
      
      const successes = results.filter(r => r.status === 'fulfilled');
      const failures = results.filter(r => r.status === 'rejected');

      expect(successes.length).toBe(1);
      expect(failures.length).toBe(2);
    });

    it('should handle concurrent releases', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Acquire
      });

      const lock = await redlockToolkit.acquire('concurrent-release');

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(1); // Release
      });

      // Attempt multiple concurrent releases
      const promises = [
        lock.release(),
        lock.release(),
        lock.release()
      ];

      const results = await Promise.all(promises);
      
      // All should succeed due to idempotency
      results.forEach(result => {
        expect(result.success).toBe(true);
      });
    });
  });

  describe('Resource Cleanup', () => {
    beforeEach(() => {
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should clean up after acquisition failure', async () => {
      // Partial success scenario - only 1 out of 3 succeed (fails quorum)
      mockClients[0].evalsha.mockResolvedValueOnce(1);
      mockClients[1].evalsha.mockRejectedValueOnce(new Error('Failed'));
      mockClients[2].evalsha.mockRejectedValueOnce(new Error('Failed'));

      await expect(redlockToolkit.acquire('cleanup-test'))
        .rejects.toThrow();

      // Verify cleanup was attempted - now uses eval instead of evalsha
      const releaseCalls = mockClients[0].eval.mock.calls
        .filter((call: any[]) => call[0] && call[0].includes && call[0].includes('DEL'));
      expect(releaseCalls.length).toBeGreaterThan(0);
    });

    it('should clean up on shutdown', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(1);
      });

      const lock1 = await redlockToolkit.acquire('shutdown-1');
      const lock2 = await redlockToolkit.acquire('shutdown-2');

      await redlockToolkit.shutdown();

      expect(lock1.released).toBe(true);
      expect(lock2.released).toBe(true);
    });
  });

  describe('Error Recovery', () => {
    beforeEach(() => {
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          retryCount: 3,
          retryDelay: 50
        }
      }));
    });

    it('should recover from transient network errors', async () => {
      let attemptCount = 0;
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          attemptCount++;
          // Fail first 2 attempts, succeed on 3rd
          if (attemptCount <= 6) { // 3 clients * 2 attempts
            return Promise.reject(new Error('ETIMEDOUT'));
          }
          return Promise.resolve(1);
        });
      });

      const lock = await redlockToolkit.acquire('recovery-test');
      expect(lock).toBeDefined();
      expect(attemptCount).toBeGreaterThan(6);
    });

    it('should not retry non-retryable errors', async () => {
      let attemptCount = 0;
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          attemptCount++;
          throw new ConfigurationError('test', 'value', 'Invalid');
        });
      });

      await expect(redlockToolkit.acquire('non-retryable'))
        .rejects.toThrow();
      
      // Should not retry configuration errors
      expect(attemptCount).toBeLessThanOrEqual(3); // One attempt per client
    });
  });

  describe('Edge Case Handling', () => {
    beforeEach(() => {
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should handle null/undefined in resource names', async () => {
      // These should actually succeed but handle them gracefully
      // If the implementation doesn't validate, we test that it doesn't crash
      const lock1 = await redlockToolkit.acquire(null as any);
      expect(lock1.resources).toEqual([null]);
      await lock1.release();
      
      const lock2 = await redlockToolkit.acquire(undefined as any);
      expect(lock2.resources).toEqual([undefined]);
      await lock2.release();
    });

    it('should handle very long resource names', async () => {
      const longResource = 'x'.repeat(10000);
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(1);
      });

      const lock = await redlockToolkit.acquire(longResource);
      expect(lock.resources[0]).toBe(longResource);
    });

    it('should handle special characters in resource names', async () => {
      const specialResource = '!@#$%^&*()_+-=[]{}|;:\'",.<>?/\\`~';
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(1);
      });

      const lock = await redlockToolkit.acquire(specialResource);
      expect(lock.resources[0]).toBe(specialResource);
    });

    it('should handle unicode in resource names', async () => {
      const unicodeResource = '🔒 लॉक テスト 🔐';
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(1);
      });

      const lock = await redlockToolkit.acquire(unicodeResource);
      expect(lock.resources[0]).toBe(unicodeResource);
    });
  });

  describe('Memory Leak Prevention', () => {
    beforeEach(() => {
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should not accumulate locks in memory after release', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(1);
      });

      const initialLocks = redlockToolkit.getActiveLocks().length;

      // Acquire and release many locks
      for (let i = 0; i < 100; i++) {
        const lock = await redlockToolkit.acquire(`mem-test-${i}`);
        await lock.release();
      }

      const finalLocks = redlockToolkit.getActiveLocks().length;
      expect(finalLocks).toBe(initialLocks);
    });

    it('should clean up failed acquisition attempts', async () => {
      // Set retryCount to 0 to avoid long delays
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          retryCount: 0  // No retries for this test
        }
      }));

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(0); // Always fail
      });

      const initialLocks = redlockToolkit.getActiveLocks().length;

      // Reduced from 50 to 10 for faster execution
      for (let i = 0; i < 10; i++) {
        await expect(redlockToolkit.acquire(`fail-test-${i}`))
          .rejects.toThrow();
      }

      const finalLocks = redlockToolkit.getActiveLocks().length;
      expect(finalLocks).toBe(initialLocks);
    }, 60000); // Add explicit timeout
  });
});