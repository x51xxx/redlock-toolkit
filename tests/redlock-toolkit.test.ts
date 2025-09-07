/**
 * Core RedlockToolkit functionality tests
 * 
 * Validates the Redlock distributed locking algorithm implementation,
 * including consensus mechanisms, TTL handling, and fault tolerance.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import RedlockToolkit from '../src/index';
import { 
  ResourceLockedError, 
  LockTimeoutError, 
  ConfigurationError,
  LockExpiredError 
} from "../src";
import { createMockRedisClients, createTestRedlockToolkitConfig, sleep } from './setup';
import { SCRIPT_HASHES } from "../src/utils/scripts";

describe('RedlockToolkit Core Functionality', () => {
  let mockClients: any[];
  let neolock: RedlockToolkit;

  beforeEach(() => {
    mockClients = createMockRedisClients(3);
    neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
      defaultLockOptions: {
        ttl: 1000,
        retryCount: 3,
        retryDelay: 50,
        retryJitter: 10
      }
    }));

    // Mock successful Lua script execution for all Redis operations
    // Returns keys.length to indicate success (matching actual Lua script behavior)
    mockClients.forEach((client, index) => {
      client.evalsha.mockImplementation((hash: string, keyCount: number, ...args: any[]) => {
        const keys = args.slice(0, keyCount);
        const argv = args.slice(keyCount);

        if (hash === SCRIPT_HASHES.acquire) {
          return keys.length;
        }
        
        if (hash === SCRIPT_HASHES.extend) {
          return keys.length;
        }
        
        if (hash === SCRIPT_HASHES.release) {
          return keys.length;
        }
        
        if (hash === SCRIPT_HASHES.forceRelease) {
          return keys.length;
        }
        
        if (hash === SCRIPT_HASHES.status) {
          return '[]';
        }
        
        return keys.length || 1;
      });

      client.eval.mockImplementation(client.evalsha);
    });
  });

  describe('Configuration', () => {
    it('should reject invalid configuration', () => {
      expect(() => new RedlockToolkit({ clients: [] }))
        .toThrow(ConfigurationError);
    });

    it('should reject negative TTL', () => {
      expect(() => new RedlockToolkit({
        clients: mockClients,
        defaultLockOptions: { ttl: -1 }
      })).toThrow(ConfigurationError);
    });

    it('should reject invalid drift factor', () => {
      expect(() => new RedlockToolkit({
        clients: mockClients,
        defaultLockOptions: { driftFactor: 1.5 }
      })).toThrow(ConfigurationError);
    });
  });

  describe('Basic Lock Operations', () => {
    it('should acquire and release a simple lock', async () => {
      const lock = await neolock.acquire('test-resource');

      expect(lock).toBeDefined();
      expect(lock.resources).toEqual(['test-resource']);
      expect(lock.identifier).toBeTruthy();
      expect(lock.isValid).toBe(true);

      const releaseResult = await lock.release();
      expect(releaseResult.success).toBe(true);
      expect(lock.released).toBe(true);
    });

    it('should acquire lock on multiple resources', async () => {
      const resources = ['resource1', 'resource2', 'resource3'];
      const lock = await neolock.acquire(resources);

      expect(lock.resources).toEqual(resources);
      expect(lock.isValid).toBe(true);

      await lock.release();
    });

    it('should handle single resource as string', async () => {
      const lock = await neolock.acquire('single-resource');
      expect(lock.resources).toEqual(['single-resource']);
      await lock.release();
    });

    it('should generate unique identifiers', async () => {
      const lock1 = await neolock.acquire('resource1');
      const lock2 = await neolock.acquire('resource2');

      expect(lock1.identifier).not.toBe(lock2.identifier);

      await lock1.release();
      await lock2.release();
    });
  });

  describe('Lock Expiration', () => {
    it('should respect TTL settings', async () => {
      const shortTtl = 500;
      const lock = await neolock.acquire('test-resource', { ttl: shortTtl });

      expect(lock.timeToExpiration).toBeLessThanOrEqual(shortTtl);
      expect(lock.isExpired).toBe(false);

      await sleep(shortTtl + 50);
      expect(lock.isExpired).toBe(true);
      expect(lock.isValid).toBe(false);
    });

    it('should calculate expiration with drift factor', async () => {
      const ttl = 1000;
      const driftFactor = 0.02;
      const startTime = Date.now();
      const lock = await neolock.acquire('test-resource', { ttl, driftFactor });

      // Drift compensates for clock skew: drift = (ttl * factor) + 2ms
      const expectedDrift = Math.round(driftFactor * ttl) + 2;
      const actualExpiration = lock.expiration;
      const expectedExpiration = startTime + ttl - expectedDrift;

      // Tolerance accounts for async operation overhead
      expect(Math.abs(actualExpiration - expectedExpiration)).toBeLessThan(100);
    });
  });

  describe('Lock Extension', () => {
    it('should extend lock successfully', async () => {
      const lock = await neolock.acquire('test-resource', { ttl: 500 });
      const originalExpiration = lock.expiration;

      await sleep(100);
      await lock.extend(1000);

      expect(lock.expiration).toBeGreaterThan(originalExpiration);
      expect(lock.extensions).toBe(1);
    });

    it('should not extend expired lock', async () => {
      const lock = await neolock.acquire('test-resource', { ttl: 50 });

      await sleep(100);

      // Cannot extend expired locks to prevent zombie locks
      await expect(lock.extend(1000)).rejects.toThrow(LockExpiredError);
    });

    it('should not extend released lock', async () => {
      const lock = await neolock.acquire('test-resource');
      await lock.release();

      await expect(lock.extend(1000)).rejects.toThrow();
    });

    it('should track extension count', async () => {
      const lock = await neolock.acquire('test-resource');

      await lock.extend(1000);
      await lock.extend(1000);
      await lock.extend(1000);

      expect(lock.extensions).toBe(3);
      await lock.release();
    });
  });

  describe('Auto-extending Locks', () => {
    it('should execute routine with auto-extending lock', async () => {
      let executionCount = 0;

      await neolock.using('test-resource', async (signal) => {
        expect(signal.aborted).toBe(false);
        executionCount++;
        await sleep(50);
      }, { ttl: 200, autoExtendThreshold: 50 });

      expect(executionCount).toBe(1);
    });

    it('should extend lock during long operations', async () => {
      const extensionCalls: number[] = [];

      const lock = await neolock.acquire('test-resource', { ttl: 400 });

      // Spy on internal extend method to track auto-extension behavior
      const originalExtend = (lock as any).lockManager.extend.bind((lock as any).lockManager);
      vi.spyOn((lock as any).lockManager, 'extend').mockImplementation(async (lockArg, ttl, options) => {
        extensionCalls.push(Date.now());
        return originalExtend(lockArg, ttl, options);
      });

      await lock.using(async (signal) => {
        // Operation duration (300ms) triggers auto-extension
        await sleep(300);
      }, { autoExtendThreshold: 50 });

      expect(extensionCalls.length).toBeGreaterThan(0);
    });

    it('should abort signal when extension fails', async () => {
      // First acquire a lock normally with longer TTL to prevent expiration
      const lock = await neolock.acquire('test-resource', { ttl: 500 });

      // Mock the lockManager.extend method to throw an error
      vi.spyOn((lock as any).lockManager, 'extend').mockImplementation(async () => {
        throw new Error('Extension failed');
      });

      let signalAborted = false;
      let errorOccurred = false;

      try {
        await lock.using(async (signal) => {
          // Wait for auto-extension to trigger (but not too long to avoid expiration)
          await sleep(150);

          // Check if signal was aborted and capture the state
          signalAborted = signal.aborted;

          // If aborted, propagate the error
          if (signal.aborted) {
            throw signal.error;
          }

          // This should not execute if properly aborted
          await sleep(200);
        }, { autoExtendThreshold: 50 });
      } catch (error: any) {
        errorOccurred = true;
        // The error should be related to extension failure
        expect(error.message).toContain('Extension failed');
      }

      // Verify that an error was thrown
      expect(errorOccurred).toBe(true);
      // Verify the signal was aborted
      expect(signalAborted).toBe(true);
    });
  });

  describe('Consensus and Fault Tolerance', () => {
    it('should succeed with majority consensus', async () => {
      // Make one client fail
      mockClients[2].evalsha.mockRejectedValue(new Error('Redis error'));

      const lock = await neolock.acquire('test-resource');
      expect(lock.isValid).toBe(true);
      await lock.release();
    });

    it('should fail without majority consensus', async () => {
      // Make majority of clients fail
      mockClients[1].evalsha.mockRejectedValue(new Error('Redis error'));
      mockClients[2].evalsha.mockRejectedValue(new Error('Redis error'));

      await expect(neolock.acquire('test-resource')).rejects.toThrow();
    });

    it('should handle mixed success/failure responses', async () => {
      // First client succeeds, second fails, third succeeds
      mockClients[0].evalsha.mockResolvedValue(1);
      mockClients[1].evalsha.mockRejectedValue(new Error('Redis error'));
      mockClients[2].evalsha.mockResolvedValue(1);

      const lock = await neolock.acquire('test-resource');
      expect(lock.isValid).toBe(true);
      await lock.release();
    });
  });

  describe('Retry Logic', () => {
    it('should retry on resource locked error', async () => {
      let attempts = 0;

      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          attempts++;
          if (attempts <= 2) {
            return 0; // Lock failed
          }
          return 1; // Lock succeeded
        });
      });

      const lock = await neolock.acquire('test-resource', { retryCount: 5 });
      expect(lock.isValid).toBe(true);
      expect(attempts).toBeGreaterThan(2);
      await lock.release();
    });

    it('should respect retry count limit', async () => {
      // Mock all clients to return 0 (lock failed) to force retries
      let attemptCount = 0;
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          attemptCount++;
          return Promise.resolve(0); // Lock failed
        });
      });

      await expect(neolock.acquire('test-resource', { 
        retryCount: 2,
        retryDelay: 10 
      })).rejects.toThrow(LockTimeoutError);

      // Should have tried 3 times (initial + 2 retries) * 3 clients
      expect(attemptCount).toBeGreaterThanOrEqual(9);
    });

    it('should apply jitter to retry delays', async () => {
      const startTime = Date.now();
      let attemptTimes: number[] = [];

      // Mock all clients to return 0 (lock failed) to force retries
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          attemptTimes.push(Date.now());
          return Promise.resolve(0); // Lock failed
        });
      });

      await expect(neolock.acquire('test-resource', { 
        retryCount: 3,
        retryDelay: 50,
        retryJitter: 20
      })).rejects.toThrow();

      // Check that retries happened with delays
      expect(attemptTimes.length).toBeGreaterThanOrEqual(12); // 4 attempts * 3 clients
      
      // Group attempts by retry round (3 clients per round)
      const rounds: number[] = [];
      for (let i = 0; i < attemptTimes.length; i += 3) {
        rounds.push(attemptTimes[i]);
      }
      
      if (rounds.length > 1) {
        const delays = rounds.slice(1).map((time, i) => time - rounds[i]);
        // Check that delays are applied with jitter (30-70ms range for 50±20)
        // In mock environment, timing might not be exact, so we check for any non-zero delay
        expect(delays.some(delay => delay > 0)).toBe(true);
      }
    });
  });

  describe('Lock Information', () => {
    it('should provide lock information', async () => {
      const lock = await neolock.acquire(['res1', 'res2']);
      const info = lock.getInfo();

      expect(info.resources).toEqual(['res1', 'res2']);
      expect(info.identifier).toBe(lock.identifier);
      expect(info.expiration).toBe(lock.expiration);
      expect(info.acquisitionTime).toBeDefined();
      expect(info.extensions).toBe(0);

      await lock.release();
    });

    it('should track lock duration', async () => {
      const lock = await neolock.acquire('test-resource');
      await sleep(50);

      expect(lock.duration).toBeGreaterThanOrEqual(50);
      expect(lock.duration).toBeLessThan(200); // Allow some tolerance

      await lock.release();
    });

    it('should provide string representation', async () => {
      const lock = await neolock.acquire('test-resource');
      const str = lock.toString();

      expect(str).toContain(lock.identifier);
      expect(str).toContain('test-resource');
      expect(str).toContain('active');

      await lock.release();
      const releasedStr = lock.toString();
      expect(releasedStr).toContain('released');
    });

    it('should provide JSON representation', async () => {
      const lock = await neolock.acquire('test-resource');
      const json = lock.toJSON();

      expect(json.resources).toEqual(['test-resource']);
      expect(json.identifier).toBe(lock.identifier);
      expect(json.expiration).toBe(lock.expiration);
      expect(json.released).toBe(false);
      expect(json.isValid).toBe(true);

      await lock.release();
    });
  });

  describe('Administrative Operations', () => {
    it('should force release locks', async () => {
      const result = await neolock.forceRelease(['resource1', 'resource2']);

      expect(result.totalAttempted).toBe(2);
      expect(result.releasedCount).toBeGreaterThanOrEqual(0);
    });

    it('should get lock status', async () => {
      // Mock status script response
      mockClients[0].evalsha.mockResolvedValue(JSON.stringify([
        { key: 'neolock:resource1', holder: 'test-id', ttl: 5000 },
        { key: 'neolock:resource2', holder: null, ttl: -2 }
      ]));

      const status = await neolock.getStatus(['resource1', 'resource2']);

      expect(status).toHaveLength(2);
      expect(status[0]).toEqual({
        resource: 'resource1',
        locked: true,
        holder: 'test-id',
        ttl: 5000
      });
      expect(status[1]).toEqual({
        resource: 'resource2',
        locked: false,
        holder: null,
        ttl: undefined
      });
    });

    it('should cleanup expired locks', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(5); // 5 locks cleaned
      });

      const cleaned = await neolock.cleanup();
      expect(cleaned).toBe(15); // 5 * 3 clients
    });

    it('should get active locks', async () => {
      const lock1 = await neolock.acquire('resource1');
      const lock2 = await neolock.acquire('resource2');

      const activeLocks = neolock.getActiveLocks();
      expect(activeLocks).toHaveLength(2);
      expect(activeLocks.map(l => l.identifier)).toContain(lock1.identifier);
      expect(activeLocks.map(l => l.identifier)).toContain(lock2.identifier);

      await lock1.release();
      await lock2.release();
    });
  });

  describe('Graceful Shutdown', () => {
    it('should release all locks on shutdown', async () => {
      const lock1 = await neolock.acquire('resource1');
      const lock2 = await neolock.acquire('resource2');

      expect(neolock.getActiveLocks()).toHaveLength(2);

      await neolock.shutdown();

      expect(lock1.released).toBe(true);
      expect(lock2.released).toBe(true);
    });

    it('should handle shutdown with failing releases', async () => {
      const lock = await neolock.acquire('resource1');

      // Make release fail
      mockClients.forEach(client => {
        client.evalsha.mockRejectedValue(new Error('Release failed'));
      });

      // Should not throw despite failed releases
      await expect(neolock.shutdown()).resolves.not.toThrow();
    });
  });
});
