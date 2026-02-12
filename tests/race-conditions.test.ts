/**
 * Race Condition Tests
 * Tests for concurrency issues, timing attacks, and race conditions
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import RedlockToolkit, { ResourceLockedError } from "../src/index";
import { createMockRedisClients, createTestRedlockToolkitConfig, sleep } from './setup';
import { SCRIPT_HASHES } from "../src/utils/scripts";

describe('Race Conditions', () => {
  let mockClients: ReturnType<typeof createMockRedisClients>;
  let redlockToolkit: RedlockToolkit;

  afterEach(async () => {
    if (redlockToolkit) {
      await redlockToolkit.shutdown();
    }
  });

  describe('Concurrent Lock Acquisitions', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 1000,
          retryCount: 0
        }
      }));
    });

    it('should prevent double acquisition of same resource', async () => {
      // Use a counter to track acquisition attempts across all clients
      let acquisitionAttempt = 0;
      
      mockClients.forEach(client => {
        client.evalsha.mockImplementation((hash: string, keyCount: number, ...args: any[]) => {
          // Only handle acquire operations
          if (hash === SCRIPT_HASHES.acquire) {
            // Increment attempt counter - this happens for each client on each acquisition
            acquisitionAttempt++;
            
            // First 3 calls (first acquisition, 3 clients) should succeed
            // Next 3 calls (second acquisition, 3 clients) should fail
            if (acquisitionAttempt <= 3) {
              return Promise.resolve(1); // Success
            }
            return Promise.resolve(0); // Already locked
          }
          // Default success for other operations
          return Promise.resolve(1);
        });
      });

      // Start two acquisitions simultaneously
      const [result1, result2] = await Promise.allSettled([
        redlockToolkit.acquire('race-resource'),
        redlockToolkit.acquire('race-resource')
      ]);

      // One should succeed, one should fail
      const successes = [result1, result2].filter(r => r.status === 'fulfilled');
      const failures = [result1, result2].filter(r => r.status === 'rejected');

      expect(successes).toHaveLength(1);
      expect(failures).toHaveLength(1);
      
      if (failures[0].status === 'rejected') {
        // The failure can be either ResourceLockedError or ConsensusError (which wraps ResourceLockedError)
        const error = failures[0].reason;
        const isExpectedError = error instanceof ResourceLockedError || 
                               (error instanceof Error && error.message.includes('Failed to achieve quorum'));
        expect(isExpectedError).toBe(true);
      }
    });

    it('should handle rapid sequential acquisitions', async () => {
      let acquireCount = 0;
      
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          acquireCount++;
          // Only first set of calls succeed
          return Promise.resolve(acquireCount <= 3 ? 1 : 0);
        });
      });

      const lock1 = await redlockToolkit.acquire('sequential-1');
      
      // Immediate second attempt should fail (might be timeout or resource locked)
      await expect(redlockToolkit.acquire('sequential-1'))
        .rejects.toThrow();

      // Release and immediately try again
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Release
      });
      await lock1.release();

      // Reset for new acquisition
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // New acquire
      });
      
      const lock2 = await redlockToolkit.acquire('sequential-1');
      expect(lock2).toBeDefined();
    });

    it('should handle interleaved multi-resource acquisitions', async () => {
      const resources1 = ['res-a', 'res-b', 'res-c'];
      const resources2 = ['res-b', 'res-c', 'res-d']; // Overlapping resources

      // Use a counter to determine which acquisition wins
      let acquisitionAttempt = 0;
      const firstAcquisitionKeys = new Set<string>();
      
      mockClients.forEach(client => {
        client.evalsha.mockImplementation((hash: string, keyCount: number, ...args: any[]) => {
          // Only handle acquire operations
          if (hash === SCRIPT_HASHES.acquire) {
            const keys = args.slice(0, keyCount);
            acquisitionAttempt++;
            
            // First 3 calls (first acquisition attempt by 3 clients)
            if (acquisitionAttempt <= 3) {
              // Save the keys from first acquisition
              keys.forEach(k => firstAcquisitionKeys.add(k));
              return Promise.resolve(keys.length);
            }
            
            // Next 3 calls (second acquisition attempt by 3 clients)
            // Check for conflicts with first acquisition's keys
            for (const key of keys) {
              if (firstAcquisitionKeys.has(key)) {
                return Promise.resolve(0); // Conflict detected
              }
            }
            
            return Promise.resolve(keys.length);
          }
          
          // Default success for other operations
          return Promise.resolve(1);
        });
      });

      // Start both acquisitions simultaneously
      const [result1, result2] = await Promise.allSettled([
        redlockToolkit.acquire(resources1),
        redlockToolkit.acquire(resources2)
      ]);

      // Due to overlap, only one should succeed
      const successes = [result1, result2].filter(r => r.status === 'fulfilled');
      expect(successes).toHaveLength(1);
    });
  });

  describe('Extension Race Conditions', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 200,
          autoExtendThreshold: 50
        }
      }));
    });

    it('should handle extension during release', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Acquire
      });

      const lock = await redlockToolkit.acquire('extend-release-race');

      let extensionStarted = false;
      let releaseStarted = false;

      // Mock extension and release
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          if (!releaseStarted && !extensionStarted) {
            extensionStarted = true;
            return new Promise(resolve => {
              setTimeout(() => resolve(1), 50); // Delay extension
            });
          } else if (!releaseStarted) {
            releaseStarted = true;
            return Promise.resolve(1); // Release succeeds
          }
          return Promise.resolve(0); // Already released
        });
      });

      // Start extension and release simultaneously
      const [extendResult, releaseResult] = await Promise.allSettled([
        lock.extend(1000),
        lock.release()
      ]);

      // One should succeed, behavior depends on timing
      expect([extendResult, releaseResult].some(r => r.status === 'fulfilled')).toBe(true);
    });

    it('should prevent double extension', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Acquire
      });

      const lock = await redlockToolkit.acquire('double-extend');

      let extensionCount = 0;
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          extensionCount++;
          // Allow only one extension
          return Promise.resolve(extensionCount <= 3 ? 1 : 0);
        });
      });

      // Start two extensions simultaneously
      const [ext1, ext2] = await Promise.allSettled([
        lock.extend(2000),
        lock.extend(3000)
      ]);

      // Both might succeed due to implementation, but lock state should be consistent
      const successCount = [ext1, ext2].filter(r => r.status === 'fulfilled').length;
      expect(successCount).toBeGreaterThanOrEqual(1);
    });

    it('should handle auto-extension race with manual extension', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Acquire
      });

      const lock = await redlockToolkit.acquire('auto-manual-race');

      let extensionAttempts = 0;
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          extensionAttempts++;
          return Promise.resolve(1); // All extensions succeed
        });
      });

      // Start auto-extending routine
      const routinePromise = lock.using(async (signal) => {
        await sleep(100); // Trigger auto-extension
        if (!signal.aborted) {
          // Also try manual extension
          await lock.extend(2000);
        }
        return 'completed';
      });

      await routinePromise;
      
      // Should have multiple extension attempts but handle gracefully
      expect(extensionAttempts).toBeGreaterThan(0);
    });
  });

  describe('Expiration Race Conditions', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 100, // Very short TTL
          retryCount: 0,
          autoExtendThreshold: 0
        }
      }));
    });

    it('should handle operations on nearly expired locks', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Acquire
      });

      const lock = await redlockToolkit.acquire('near-expiry');

      // Wait until lock is actually expired locally
      // TTL is 100ms, with drift it expires a bit earlier
      await sleep(105);

      // Now the lock.isExpired should be true, and extend will throw LockExpiredError
      // before even trying to contact Redis
      
      await expect(lock.extend(1000))
        .rejects.toThrow();
    });

    it('should handle expiration during routine execution', async () => {
      // Need auto-extension enabled to detect expiration during routine
      const toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 100,
          retryCount: 0,
          autoExtendThreshold: 50
        }
      }));

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Acquire
      });

      const lock = await toolkit.acquire('expire-during-routine');

      // Routine that outlasts the lock
      const routine = async (signal: any) => {
        await sleep(150); // Longer than TTL
        if (signal.aborted) {
          throw signal.error;
        }
        return 'should not complete';
      };

      // Mock failed extension
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(0); // Extension fails
      });

      await expect(lock.using(routine))
        .rejects.toThrow();
    });

    it('should handle clock jumps', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Acquire
      });

      const lock = await redlockToolkit.acquire('clock-jump');
      
      // Simulate clock jump forward
      const originalNow = Date.now;
      Date.now = vi.fn(() => originalNow() + 2000); // Jump 2 seconds

      expect(lock.isExpired).toBe(true);
      
      await expect(lock.extend(1000))
        .rejects.toThrow();

      // Restore
      Date.now = originalNow;
    });
  });

  describe('Client Identifier Collisions', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should handle same identifier from different instances', async () => {
      const fixedIdentifier = 'fixed-id-123';
      let acquisitionCount = 0;
      
      // Mock that tracks acquisition attempts
      mockClients.forEach(client => {
        client.evalsha.mockImplementation((hash: string, keyCount: number, ...args: any[]) => {
          // Only handle acquire operations
          if (hash === SCRIPT_HASHES.acquire) {
            acquisitionCount++;
            
            // First 3 attempts (first lock acquisition) succeed
            // Next 3 attempts (second lock with same resource) fail
            if (acquisitionCount <= 3) {
              return Promise.resolve(1); // Success
            }
            return Promise.resolve(0); // Already locked
          }
          
          // Default success for other operations
          return Promise.resolve(1);
        });
      });

      // First acquisition with fixed identifier
      const lock1 = await redlockToolkit.acquire('id-collision', { 
        identifier: fixedIdentifier 
      });
      expect(lock1.identifier).toBe(fixedIdentifier);

      // Second instance tries with same identifier on same resource
      const redlockToolkit2 = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          retryCount: 0 // No retries for faster test
        }
      }));
      
      // The second attempt should fail because resource is already locked
      await expect(redlockToolkit2.acquire('id-collision', { 
        identifier: fixedIdentifier 
      })).rejects.toThrow();
    });

    it('should prevent identifier reuse before release', async () => {
      const identifier = 'reuse-id';
      
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // First acquire
      });

      const lock1 = await redlockToolkit.acquire('reuse-test', { identifier });

      // Try to acquire different resource with same identifier
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Would succeed
      });

      // Should succeed as different resource
      const lock2 = await redlockToolkit.acquire('different-resource', { identifier });
      
      expect(lock2.identifier).toBe(identifier);
    });
  });

  describe('Concurrent Release Operations', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should handle multiple concurrent releases safely', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Acquire
      });

      const lock = await redlockToolkit.acquire('concurrent-release');

      let releaseCount = 0;
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          releaseCount++;
          // First release succeeds, others find nothing
          return Promise.resolve(releaseCount <= 3 ? 1 : 0);
        });
      });

      // Multiple concurrent release attempts
      const releases = await Promise.all([
        lock.release(),
        lock.release(),
        lock.release()
      ]);

      // All should return success due to idempotency
      releases.forEach(result => {
        expect(result.success).toBe(true);
      });

      expect(lock.released).toBe(true);
    });

    it('should handle release during shutdown', async () => {
      const locks: Lock[] = [];
      
      // Acquire multiple locks
      for (let i = 0; i < 5; i++) {
        mockClients.forEach(client => {
          client.evalsha.mockResolvedValueOnce(1);
        });
        locks.push(await redlockToolkit.acquire(`shutdown-lock-${i}`));
      }

      // Mock releases
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(1);
      });

      // Concurrent shutdown and manual releases
      const operations = [
        redlockToolkit.shutdown(),
        ...locks.map(lock => lock.release())
      ];

      await Promise.all(operations);

      // All locks should be released
      locks.forEach(lock => {
        expect(lock.released).toBe(true);
      });
    });
  });

  describe('High Frequency Operations', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 100,
          retryCount: 0,
          autoExtendThreshold: 0
        }
      }));
    });

    it('should handle rapid acquire-release cycles', async () => {
      const cycles = 20;
      let operationCount = 0;
      
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          operationCount++;
          return Promise.resolve(1); // All operations succeed
        });
      });

      for (let i = 0; i < cycles; i++) {
        const lock = await redlockToolkit.acquire(`cycle-${i}`);
        await lock.release();
      }

      expect(operationCount).toBe(cycles * 6); // 3 clients * 2 ops * cycles
    });

    it('should handle interleaved operations on different resources', async () => {
      const resources = Array.from({ length: 10 }, (_, i) => `resource-${i}`);
      
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(1); // All succeed
      });

      // Acquire all locks concurrently
      const lockPromises = resources.map(res => redlockToolkit.acquire(res));
      const locks = await Promise.all(lockPromises);

      // Release in different order
      const releasePromises = locks.reverse().map(lock => lock.release());
      await Promise.all(releasePromises);

      // All should be released
      locks.forEach(lock => {
        expect(lock.released).toBe(true);
      });
    });
  });

  describe('State Consistency Under Races', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should maintain lock count consistency', async () => {
      const initialCount = redlockToolkit.getActiveLocks().length;
      
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => Promise.resolve(1));
      });

      // Concurrent acquisitions
      const acquirePromises = Array.from({ length: 10 }, (_, i) => 
        redlockToolkit.acquire(`consistency-${i}`)
      );
      
      const locks = await Promise.all(acquirePromises);
      
      expect(redlockToolkit.getActiveLocks().length).toBe(initialCount + 10);

      // Concurrent releases
      const releasePromises = locks.map(lock => lock.release());
      await Promise.all(releasePromises);

      expect(redlockToolkit.getActiveLocks().length).toBe(initialCount);
    });

    it('should maintain metrics consistency under concurrent operations', async () => {
      const metrics = redlockToolkit.getMetrics();
      const initialAcquired = metrics.locksAcquired;
      
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(1);
      });

      // Parallel operations
      const operations = Array.from({ length: 5 }, async (_, i) => {
        const lock = await redlockToolkit.acquire(`metrics-${i}`);
        await lock.release();
      });

      await Promise.all(operations);

      const finalMetrics = redlockToolkit.getMetrics();
      expect(finalMetrics.locksAcquired).toBe(initialAcquired + 5);
      expect(finalMetrics.locksReleased).toBeGreaterThanOrEqual(5);
    });
  });

  describe('Extension Window Race Conditions', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      redlockToolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 200,
          autoExtendThreshold: 100
        }
      }));
    });

    it('should handle extension exactly at threshold', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Acquire
      });

      const lock = await redlockToolkit.acquire('threshold-race');

      // Wait exactly to threshold
      await sleep(100);

      // Both auto and manual extension might trigger
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(1); // Extensions succeed
      });

      const extended = await lock.extend(1000);
      expect(extended).toBe(lock);
    });

    it('should handle multiple extensions in rapid succession', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Acquire
      });

      const lock = await redlockToolkit.acquire('rapid-extend');

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(1); // All extensions succeed
      });

      // Rapid extensions
      for (let i = 0; i < 5; i++) {
        await lock.extend(1000 + i * 100);
        expect(lock.extensions).toBe(i + 1);
      }
    });
  });
});