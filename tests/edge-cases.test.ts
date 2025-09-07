/**
 * Edge Cases Tests
 * 
 * Validates behavior at system boundaries, extreme values, and unusual scenarios.
 * Tests resilience against pathological inputs and timing conditions.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import RedlockToolkit, { ConfigurationError, LockExpiredError, LockTimeoutError } from '../src/index';
import { createMockRedisClients, createTestRedlockToolkitConfig, sleep } from './setup';

describe('Edge Cases', () => {
  let mockClients: any[];
  let neolock: RedlockToolkit;

  afterEach(async () => {
    if (neolock) {
      await neolock.shutdown();
    }
  });

  describe('Extreme TTL Values', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
    });

    it('should handle minimum viable TTL (1ms)', async () => {
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 1,
          retryCount: 0
        }
      }));

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      // TTL=1ms with default drift (2ms minimum) creates negative validity window
      await expect(neolock.acquire('min-ttl')).rejects.toThrow(LockTimeoutError);
    });

    it('should handle maximum TTL (1 year)', async () => {
      // Tests system behavior with extremely long-lived locks
      const oneYear = 365 * 24 * 60 * 60 * 1000;
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: oneYear,
          retryCount: 0
        }
      }));

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await neolock.acquire('max-ttl');
      expect(lock.timeToExpiration).toBeLessThanOrEqual(oneYear);
      expect(lock.isValid).toBe(true);
    });

    it('should handle TTL with drift factor edge cases', async () => {
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 10,
          driftFactor: 0.99, // Extreme drift: assumes 99% clock skew
          retryCount: 0
        }
      }));

      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          // Network delay consumes most of the validity window
          return new Promise(resolve => setTimeout(() => resolve(1), 9));
        });
      });

      // Drift (9.9ms) + network delay (9ms) exceeds TTL (10ms)
      await expect(neolock.acquire('extreme-drift')).rejects.toThrow(LockTimeoutError);
    });
  });

  describe('Resource Name Edge Cases', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should handle empty string resource name', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await neolock.acquire('');
      expect(lock.resources).toEqual(['']);
    });

    it('should handle resource names with special characters', async () => {
      // Tests Redis key compatibility with various special characters
      const specialChars = [
        '!@#$%^&*()',
        '{}[]|\\',
        '"\';:<>?,./`~',
        '\n\r\t',
        '\\x00\\xFF',
        'key:with:colons',  // Redis namespace separator
        'key with spaces',
        'key\twith\ttabs',
        'key\nwith\nnewlines'
      ];

      for (const resource of specialChars) {
        mockClients.forEach(client => {
          client.evalsha.mockResolvedValueOnce(1);
        });

        const lock = await neolock.acquire(resource);
        expect(lock.resources[0]).toBe(resource);
        
        mockClients.forEach(client => {
          client.evalsha.mockResolvedValueOnce(1);
        });
        await lock.release();
      }
    });

    it('should handle unicode and emoji in resource names', async () => {
      // Validates UTF-8 support across different character sets
      const unicodeResources = [
        '🔒🔑🔓',
        '中文资源',
        'ресурс',
        'משאב',  // Right-to-left text
        'مورد',   // Right-to-left text
        '日本語リソース',
        '한국어자원',
        '🏁混合emoji和文字🎯',
        '\u0000\uFFFF'  // Unicode boundary values
      ];

      for (const resource of unicodeResources) {
        mockClients.forEach(client => {
          client.evalsha.mockResolvedValueOnce(1);
        });

        const lock = await neolock.acquire(resource);
        expect(lock.resources[0]).toBe(resource);
        
        mockClients.forEach(client => {
          client.evalsha.mockResolvedValueOnce(1);
        });
        await lock.release();
      }
    });

    it('should handle extremely long resource names', async () => {
      const lengths = [1000, 10000, 100000, 1000000]; // Up to 1MB
      
      for (const length of lengths) {
        const longResource = 'x'.repeat(length);
        
        mockClients.forEach(client => {
          client.evalsha.mockResolvedValueOnce(1);
        });

        const lock = await neolock.acquire(longResource);
        expect(lock.resources[0].length).toBe(length);
        
        mockClients.forEach(client => {
          client.evalsha.mockResolvedValueOnce(1);
        });
        await lock.release();
      }
    });
  });

  describe('Array Boundary Cases', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should handle single resource in array', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await neolock.acquire(['single']);
      expect(lock.resources).toEqual(['single']);
    });

    it('should handle 10000 resources in single lock', async () => {
      const resourceCount = 10000;
      const resources = Array.from({ length: resourceCount }, (_, i) => `res-${i}`);
      
      mockClients.forEach(client => {
        client.evalsha.mockImplementation((script: string, count: number) => {
          expect(count).toBe(resourceCount);
          return Promise.resolve(resourceCount);
        });
      });

      const lock = await neolock.acquire(resources);
      expect(lock.resources).toHaveLength(resourceCount);
      expect(lock.resources).toEqual(resources);
    });

    it('should handle duplicate resources in array', async () => {
      const resources = ['dup', 'dup', 'unique', 'dup'];
      
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(resources.length);
      });

      const lock = await neolock.acquire(resources);
      expect(lock.resources).toEqual(resources); // Preserves duplicates
    });

    it('should handle resources array with mixed types', async () => {
      const resources = ['string', '', '123', 'null', 'undefined'];
      
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(resources.length);
      });

      const lock = await neolock.acquire(resources);
      expect(lock.resources).toEqual(resources);
    });
  });

  describe('Timing Edge Cases', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 100,
          autoExtendThreshold: 30
        }
      }));
    });

    it('should handle lock expiration at exact moment of operation', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await neolock.acquire('exact-expiry');
      
      // Wait until exact expiration moment
      const timeToWait = lock.timeToExpiration;
      await sleep(timeToWait);
      
      // Operation at exact expiration - should fail due to expiration
      expect(lock.isExpired).toBe(true);
      // The extend operation should fail when lock is expired
      await expect(lock.extend(1000))
        .rejects.toThrow();
    });

    it('should handle zero auto-extend threshold', async () => {
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 100,
          autoExtendThreshold: 0 // Disabled
        }
      }));

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await neolock.acquire('zero-threshold');
      
      // Reset mocks and track extension attempts
      let extended = false;
      mockClients.forEach(client => {
        client.evalsha.mockReset();
        client.evalsha.mockImplementation((scriptHash: string, numKeys: number, ...keysAndArgs: any[]) => {
          const argv = keysAndArgs.slice(numKeys);
          // Check if this is an extend operation (has 2 args: identifier and ttl)
          // Release has 1 arg (identifier), extend has 2 (identifier, ttl)
          if (argv.length === 2) {
            extended = true; // This is an extension attempt
          }
          return Promise.resolve(1);
        });
      });

      await lock.using(async () => {
        await sleep(50);
        return 'done';
      }, { autoExtendThreshold: 0 });

      expect(extended).toBe(false); // No extension occurred
    });

    it('should handle negative retry delays (treated as 0)', async () => {
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 100,
          retryCount: 2,
          retryDelay: -100, // Invalid but might occur
          retryJitter: 0
        }
      }));

      let attemptTimes: number[] = [];
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          attemptTimes.push(Date.now());
          return Promise.resolve(0); // Always fail
        });
      });

      await expect(neolock.acquire('negative-delay'))
        .rejects.toThrow();

      // Retries should be immediate
      const delays = [];
      for (let i = 3; i < attemptTimes.length; i += 3) {
        delays.push(attemptTimes[i] - attemptTimes[i - 3]);
      }
      
      delays.forEach(delay => {
        expect(delay).toBeLessThan(50); // Very quick retries
      });
    });
  });

  describe('Identifier Edge Cases', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should handle custom empty identifier', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await neolock.acquire('custom-id', { identifier: '' });
      expect(lock.identifier).toBe('');
    });

    it('should handle very long custom identifier', async () => {
      const longId = 'x'.repeat(100000);
      
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await neolock.acquire('long-id', { identifier: longId });
      expect(lock.identifier).toBe(longId);
    });

    it('should handle identifier with special characters', async () => {
      const specialId = '!@#$%^&*(){}[]|\\:";\'<>?,./`~\n\r\t';
      
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await neolock.acquire('special-id', { identifier: specialId });
      expect(lock.identifier).toBe(specialId);
    });
  });

  describe('Client Count Edge Cases', () => {
    it('should work with single Redis client', async () => {
      mockClients = createMockRedisClients(1);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));

      mockClients[0].evalsha.mockResolvedValueOnce(1);

      const lock = await neolock.acquire('single-client');
      expect(lock).toBeDefined();
      
      // Quorum is 1 for single client
      mockClients[0].evalsha.mockResolvedValueOnce(1);
      await lock.release();
    });

    it('should work with even number of clients', async () => {
      mockClients = createMockRedisClients(4); // Even number
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));

      // Need 3 out of 4 for quorum
      mockClients[0].evalsha.mockResolvedValueOnce(1);
      mockClients[1].evalsha.mockResolvedValueOnce(1);
      mockClients[2].evalsha.mockResolvedValueOnce(1);
      mockClients[3].evalsha.mockRejectedValueOnce(new Error('Failed'));

      const lock = await neolock.acquire('even-clients');
      expect(lock).toBeDefined();
    });

    it('should handle large number of clients', async () => {
      mockClients = createMockRedisClients(100);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));

      // Need 51 for quorum
      mockClients.forEach((client, index) => {
        if (index < 51) {
          client.evalsha.mockResolvedValueOnce(1);
        } else {
          client.evalsha.mockRejectedValueOnce(new Error('Failed'));
        }
      });

      const lock = await neolock.acquire('many-clients');
      expect(lock).toBeDefined();
    });
  });

  describe('Extension Edge Cases', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should handle extension with same TTL', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await neolock.acquire('same-ttl', { ttl: 1000 });
      
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const extended = await lock.extend(1000); // Same TTL
      expect(extended).toBe(lock);
      expect(lock.extensions).toBe(1);
    });

    it('should handle extension to shorter TTL', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await neolock.acquire('shorter-ttl', { ttl: 5000 });
      
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      await lock.extend(1000); // Shorter TTL
      expect(lock.timeToExpiration).toBeLessThanOrEqual(1000);
    });

    it('should handle many rapid extensions', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await neolock.acquire('rapid-extend');
      
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(1); // All extensions succeed
      });

      // Perform 100 rapid extensions
      for (let i = 0; i < 100; i++) {
        await lock.extend(1000 + i);
      }

      expect(lock.extensions).toBe(100);
      expect(lock.isValid).toBe(true);
    });
  });

  describe('Boundary Retry Scenarios', () => {
    it('should handle infinite retries (-1)', async () => {
      mockClients = createMockRedisClients(3);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 5000, // Increased TTL to allow for retries
          retryCount: -1, // Infinite
          retryDelay: 10
        }
      }));

      let attemptCount = 0;
      const maxAttempts = 10;
      
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          attemptCount++;
          // Succeed after several attempts
          if (attemptCount > maxAttempts * 3) {
            return Promise.resolve(1);
          }
          return Promise.resolve(0);
        });
      });

      const lock = await neolock.acquire('infinite-retry');
      expect(lock).toBeDefined();
      expect(attemptCount).toBeGreaterThan(maxAttempts * 3);
    });

    it('should handle zero retries', async () => {
      mockClients = createMockRedisClients(3);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          retryCount: 0
        }
      }));

      let attemptCount = 0;
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          attemptCount++;
          return Promise.resolve(0); // Always fail
        });
      });

      await expect(neolock.acquire('zero-retry'))
        .rejects.toThrow();
      
      expect(attemptCount).toBe(3); // Only one attempt per client
    });
  });

  describe('Floating Point Edge Cases', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
    });

    it('should handle drift factor of exactly 0', async () => {
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 1000,
          driftFactor: 0 // No drift
        }
      }));

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await neolock.acquire('zero-drift');
      // Validity should be TTL minus 2ms (fixed overhead)
      expect(lock.timeToExpiration).toBeLessThanOrEqual(1000);
      expect(lock.timeToExpiration).toBeGreaterThan(990);
    });

    it('should handle drift factor of exactly 1', async () => {
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 1000,
          driftFactor: 1, // 100% drift (maximum)
          retryCount: 0
        }
      }));

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      // With 100% drift factor, lock should fail immediately
      await expect(neolock.acquire('max-drift')).rejects.toThrow(LockTimeoutError);
    });

    it('should handle floating point precision in TTL calculations', async () => {
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 999.999,
          driftFactor: 0.333333
        }
      }));

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await neolock.acquire('float-precision');
      expect(lock).toBeDefined();
      // Should handle floating point without errors
    });
  });

  describe('State Transition Edge Cases', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should handle shutdown with no active locks', async () => {
      await neolock.shutdown();
      expect(neolock.getActiveLocks()).toHaveLength(0);
    });

    it('should handle metrics with no operations', () => {
      const metrics = neolock.getMetrics();
      expect(metrics.locksAcquired).toBe(0);
      expect(metrics.locksReleased).toBe(0);
      expect(metrics.failedAcquisitions).toBe(0);
    });

    it('should handle status check for non-existent resources', async () => {
      // Mock should return array with null holder for non-existent resource
      mockClients[0].evalsha.mockResolvedValueOnce('[{"key":"neolock:non-existent","holder":null,"ttl":-2}]');

      const status = await neolock.getStatus(['non-existent']);
      expect(status).toHaveLength(1);
      expect(status[0].locked).toBe(false);
    });
  });
});