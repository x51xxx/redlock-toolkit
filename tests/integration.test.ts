/**
 * Integration tests with Redis mocks
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import RedlockToolkit, { LockTimeoutError } from '../src/index';
import { createMockRedisClients, createTestRedlockToolkitConfig, sleep } from './setup';
import { SCRIPTS } from "../src/utils/scripts";

describe('RedlockToolkit Integration Tests', () => {
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
  });

  afterEach(async () => {
    // Properly shutdown neolock to clean up any pending operations
    if (neolock) {
      await neolock.shutdown();
    }
    // Clear all mocks to prevent interference between tests
    vi.clearAllMocks();
  });

  describe('Real-world Lua Script Execution', () => {
    beforeEach(() => {
      // Reset mocks for clean state
      mockClients.forEach(client => {
        client.evalsha.mockReset();
        client.eval.mockReset();

        // Setup simple mock implementation that always succeeds
        client.evalsha.mockImplementation(async () => {
          await sleep(1); // Minimal latency
          return 1; // Success
        });

        client.eval.mockImplementation(async () => {
          await sleep(1); // Minimal latency
          return 1; // Success
        });
      });
    });

    it('should acquire lock with proper Lua script execution', async () => {
      const lock = await neolock.acquire('user:123');

      expect(lock.resources).toEqual(['user:123']);
      expect(lock.identifier).toBeTruthy();

      // Verify script was called on all clients
      mockClients.forEach(client => {
        expect(client.evalsha).toHaveBeenCalledWith(
          SCRIPTS.acquire.hash,
          1,
          'neolock:user:123',
          lock.identifier,
          1000
        );
      });

      await lock.release();
    });

    it('should handle script fallback from evalsha to eval', async () => {
      // Make evalsha fail with NOSCRIPT, then succeed with eval
      mockClients.forEach(client => {
        client.evalsha.mockRejectedValueOnce(new Error('NOSCRIPT No matching script'));
      });

      const lock = await neolock.acquire('user:123');
      expect(lock.isValid).toBe(true);

      // Verify fallback to eval was called
      mockClients.forEach(client => {
        expect(client.eval).toHaveBeenCalled();
      });

      await lock.release();
    });

    it('should extend lock with proper script parameters', async () => {
      const lock = await neolock.acquire('user:123', { ttl: 500 });

      await lock.extend(1500);

      mockClients.forEach(client => {
        expect(client.evalsha).toHaveBeenCalledWith(
          SCRIPTS.extend.hash,
          1,
          'neolock:user:123',
          lock.identifier,
          1500
        );
      });

      await lock.release();
    });

    it('should release lock with proper cleanup', async () => {
      const lock = await neolock.acquire('user:123');
      const identifier = lock.identifier;

      await lock.release();

      mockClients.forEach(client => {
        expect(client.evalsha).toHaveBeenCalledWith(
          SCRIPTS.release.hash,
          1,
          'neolock:user:123',
          identifier
        );
      });
    });
  });

  describe('Consensus Scenarios', () => {
    it('should succeed with majority consensus (2/3)', async () => {
      // Create a new NeoLock instance with fresh mocks to avoid interference
      const freshMocks = createMockRedisClients(3);
      const freshNeolock = new RedlockToolkit(createTestRedlockToolkitConfig(freshMocks, {
        defaultLockOptions: {
          ttl: 1000,
          retryCount: 0, // No retries to make test faster and more predictable
          retryDelay: 10,
          retryJitter: 0
        }
      }));

      // First two clients succeed (majority)
      freshMocks[0].evalsha.mockResolvedValue(1);
      freshMocks[1].evalsha.mockResolvedValue(1);
      // Third client fails
      freshMocks[2].evalsha.mockRejectedValue(new Error('Redis connection failed'));

      // Should succeed with majority
      const lock = await freshNeolock.acquire('resource:test');
      expect(lock.isValid).toBe(true);

      // Reset mocks for release to succeed
      freshMocks.forEach(client => {
        client.evalsha.mockResolvedValue(1);
      });

      await lock.release();
    });

    it('should fail without majority consensus (1/3)', async () => {
      // Make two clients fail
      mockClients[1].evalsha.mockRejectedValue(new Error('Redis connection failed'));
      mockClients[2].evalsha.mockRejectedValue(new Error('Redis connection failed'));

      await expect(neolock.acquire('resource:test'))
        .rejects.toThrow();
    });

    it('should handle mixed responses correctly', async () => {
      mockClients[0].evalsha.mockResolvedValue(1); // Success
      mockClients[1].evalsha.mockResolvedValue(0); // Lock exists
      mockClients[2].evalsha.mockResolvedValue(1); // Success

      // Should still succeed with 2/3 majority
      const lock = await neolock.acquire('resource:test');
      expect(lock.isValid).toBe(true);

      await lock.release();
    });
  });

  describe('Resource Contention Simulation', () => {
    it('should handle resource already locked scenario', async () => {
      // First acquisition succeeds
      const lock1 = await neolock.acquire('shared:resource');

      // Reset mocks and make all clients return 0 for the locked resource
      mockClients.forEach(client => {
        client.evalsha.mockReset();
        client.evalsha.mockImplementation(async (script, keyCount, ...args) => {
          if (script === SCRIPTS.acquire.hash) {
            // Always return 0 for the shared resource to simulate it being locked
            const keys = args.slice(0, keyCount);
            if (keys.some(key => key.includes('shared:resource'))) {
              return 0; // Resource is locked
            }
          }
          await sleep(1); // Minimal latency
          return 1; // Success for other operations
        });
      });

      // Second acquisition should retry and eventually fail with a timeout
      await expect(neolock.acquire('shared:resource', { 
        retryCount: 2,
        retryDelay: 10,
        ttl: 100 
      }))
        .rejects.toThrow(LockTimeoutError);

      await lock1.release();
    });

    it('should succeed after resource is released', async () => {
      // Reset mocks for clean state
      mockClients.forEach(client => {
        client.evalsha.mockReset();
        client.evalsha.mockImplementation(async () => {
          await sleep(1); // Minimal latency
          return 1; // Success
        });
      });

      const lock1 = await neolock.acquire('sequential:resource');
      await lock1.release();

      const lock2 = await neolock.acquire('sequential:resource');
      expect(lock2.isValid).toBe(true);

      await lock2.release();
    });
  });

  describe('Network Partition Simulation', () => {
    it('should handle client disconnections gracefully', async () => {
      // Reset mocks for clean state
      mockClients.forEach(client => {
        client.evalsha.mockReset();
        client.evalsha.mockImplementation(async () => {
          await sleep(1); // Minimal latency
          return 1; // Success
        });
      });

      const lock = await neolock.acquire('network:test');

      // Simulate network partition - one client becomes unavailable
      mockClients[1].evalsha.mockRejectedValue(new Error('ECONNREFUSED'));

      // Extension should succeed with quorum (2 out of 3 clients)
      await lock.extend(1000);
      expect(lock.isValid).toBe(true);

      await lock.release();
    });

    it('should fail when majority of clients are unavailable', async () => {
      // Reset mocks for clean state
      mockClients.forEach(client => {
        client.evalsha.mockReset();
        client.evalsha.mockImplementation(async () => {
          await sleep(1); // Minimal latency
          return 1; // Success
        });
      });

      const lock = await neolock.acquire('network:test');

      // Simulate majority partition
      mockClients[1].evalsha.mockRejectedValue(new Error('ECONNREFUSED'));
      mockClients[2].evalsha.mockRejectedValue(new Error('ECONNREFUSED'));

      // Extension should fail without majority
      await expect(lock.extend(1000)).rejects.toThrow();
    });
  });

  describe('Clock Drift Compensation', () => {
    it('should apply drift factor to expiration calculation', async () => {
      const ttl = 10000;
      const driftFactor = 0.02;

      const startTime = Date.now();
      const lock = await neolock.acquire('drift:test', { ttl, driftFactor });

      const expectedDrift = Math.round(driftFactor * ttl) + 2;
      const expectedExpiration = startTime + ttl - expectedDrift;

      // Allow some tolerance for execution time
      expect(Math.abs(lock.expiration - expectedExpiration)).toBeLessThan(100);

      await lock.release();
    });
  });

  describe('Auto-extension Integration', () => {
    it('should perform auto-extension during long operations', async () => {
      // Reset mocks for clean state
      mockClients.forEach(client => {
        client.evalsha.mockReset();
      });

      // Track extension calls with a counter
      let extensionCount = 0;

      // Setup clients with specific behaviors
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(async (script, ...args) => {
          if (script === SCRIPTS.extend.hash) {
            extensionCount++;
          }
          await sleep(1); // Minimal latency
          return 1; // Success
        });
      });

      await neolock.using('auto:extend', async (signal) => {
        expect(signal.aborted).toBe(false);
        await sleep(250); // Wait longer than initial TTL
        expect(signal.aborted).toBe(false);
      }, {
        ttl: 500,
        autoExtendThreshold: 50
      });

      // Should have performed at least one extension
      expect(extensionCount).toBeGreaterThan(0);
    });

    it('should abort operation when extension fails', async () => {
      // Reset mocks for clean state
      mockClients.forEach(client => {
        client.evalsha.mockReset();
      });

      // Setup clients to allow acquisition but fail extension
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(async (script, ...args) => {
          // Allow acquisition to succeed
          if (script === SCRIPTS.acquire.hash) {
            return 1;
          }
          // Make extension fail
          if (script === SCRIPTS.extend.hash) {
            throw new Error('Extension failed');
          }
          return 1;
        });
      });

      // Use a flag to track if the signal was aborted
      let wasAborted = false;

      try {
        await neolock.using('auto:extend:fail', async (signal) => {
          // Wait for auto-extension to trigger
          await sleep(300);

          // Check if signal was aborted and capture the state
          wasAborted = signal.aborted;

          // If aborted, propagate the error
          if (signal.aborted) {
            throw signal.error;
          }

          // This should not execute if properly aborted
          await sleep(200);
        }, {
          ttl: 500,
          autoExtendThreshold: 30
        });

        // If we get here, the test should fail
        expect('Test should have thrown').toBe('But it did not');
      } catch (error) {
        // Verify the error contains our expected message
        expect(error.message).toContain('Extension failed');
        // Verify the signal was aborted
        expect(wasAborted).toBe(true);
      }
    });
  });

  describe('Metrics Integration', () => {
    it('should collect comprehensive metrics during operations', async () => {
      // Reset mocks for clean state
      mockClients.forEach(client => {
        client.evalsha.mockReset();
        client.evalsha.mockImplementation(async () => {
          await sleep(1); // Minimal latency
          return 1; // Success
        });
      });

      const lock1 = await neolock.acquire('metrics:test1');
      const lock2 = await neolock.acquire('metrics:test2');

      await lock1.extend(2000);
      await lock1.release();

      // Simulate failed acquisition for all attempts
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(async (script, ...args) => {
          if (script === SCRIPTS.acquire.hash) {
            return 0; // Lock exists for all attempts
          }
          await sleep(1); // Minimal latency
          return 1; // Success for other operations
        });
      });

      await expect(neolock.acquire('metrics:test3', { retryCount: 1 }))
        .rejects.toThrow();

      await lock2.release();

      const metrics = neolock.getMetrics();
      expect(metrics.locksAcquired).toBe(2);
      expect(metrics.locksReleased).toBe(2);
      expect(metrics.lockExtensions).toBe(1);
      expect(metrics.failedAcquisitions).toBe(1);
      expect(metrics.activeLocks).toBe(0);
    });

    it('should provide performance summary', async () => {
      // Reset mocks for clean state
      mockClients.forEach(client => {
        client.evalsha.mockReset();
        client.evalsha.mockImplementation(async () => {
          await sleep(10); // Add some latency to ensure measurable metrics
          return 1; // Success
        });
      });

      // Reset metrics
      neolock.resetMetrics();

      // Perform several operations to generate metrics
      const lock1 = await neolock.acquire('performance:test1');
      const lock2 = await neolock.acquire('performance:test2');
      await lock1.extend(2000);
      await lock1.release();
      await lock2.release();

      // Get performance summary
      const summary = neolock.getPerformanceSummary();

      // Verify metrics
      expect(summary.successRate).toBe(1);
      expect(summary.averageAcquisitionLatency).toBeGreaterThan(0);
      expect(summary.circuitBreakerHealth.overallState).toBe('healthy');
    });
  });

  describe('Administrative Operations', () => {
    it('should get lock status correctly', async () => {
      // Mock status script to return lock information
      mockClients[0].evalsha.mockImplementation((script, keyCount, ...args) => {
        if (script === SCRIPTS.status.hash) {
          return JSON.stringify([
            {
              key: 'neolock:resource1',
              holder: 'lock-123',
              ttl: 5000
            },
            {
              key: 'neolock:resource2', 
              holder: null,
              ttl: -2
            }
          ]);
        }
        return simulateDefaultScript(script, keyCount, ...args);
      });

      const status = await neolock.getStatus(['resource1', 'resource2']);

      expect(status).toEqual([
        {
          resource: 'resource1',
          locked: true,
          holder: 'lock-123',
          ttl: 5000
        },
        {
          resource: 'resource2',
          locked: false,
          holder: null,
          ttl: undefined
        }
      ]);
    });

    it('should perform cleanup operations', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockImplementation((script, keyCount, ...args) => {
          if (script === SCRIPTS.cleanup.hash) {
            return 3; // Cleaned 3 locks per client
          }
          return simulateDefaultScript(script, keyCount, ...args);
        });
      });

      const cleaned = await neolock.cleanup();
      expect(cleaned).toBe(9); // 3 clients * 3 locks each
    });
  });

  describe('Shutdown Integration', () => {
    it('should gracefully shutdown with active locks', async () => {
      const lock1 = await neolock.acquire('shutdown:test1');
      const lock2 = await neolock.acquire('shutdown:test2');

      expect(neolock.getActiveLocks()).toHaveLength(2);

      await neolock.shutdown();

      expect(lock1.released).toBe(true);
      expect(lock2.released).toBe(true);
      expect(neolock.getActiveLocks()).toHaveLength(0);
    });
  });
});

// Helper functions for script simulation
function simulateAcquireScript(client: any, keys: string[], argv: string[]): number {
  const [identifier, ttl] = argv;

  // Check if any key exists
  for (const key of keys) {
    if (client.storage.has(key)) {
      const entry = client.storage.get(key);
      if (entry.value !== identifier) {
        return 0; // Lock exists with different identifier
      }
    }
  }

  // Acquire all locks
  for (const key of keys) {
    client.storage.set(key, {
      value: identifier,
      expiration: Date.now() + parseInt(ttl)
    });
  }

  return keys.length;
}

function simulateExtendScript(client: any, keys: string[], argv: string[]): number {
  const [identifier, ttl] = argv;

  // Check ownership of all keys
  for (const key of keys) {
    const entry = client.storage.get(key);
    if (!entry || entry.value !== identifier) {
      return 0; // Not owner of all locks
    }
  }

  // Extend all locks
  for (const key of keys) {
    const entry = client.storage.get(key);
    if (entry) {
      entry.expiration = Date.now() + parseInt(ttl);
    }
  }

  return keys.length;
}

function simulateReleaseScript(client: any, keys: string[], argv: string[]): number {
  const [identifier] = argv;
  let released = 0;

  for (const key of keys) {
    const entry = client.storage.get(key);
    if (entry && entry.value === identifier) {
      client.storage.delete(key);
      released++;
    }
  }

  return released;
}

function simulateStatusScript(client: any, keys: string[], argv: string[]): string {
  const result = keys.map(key => {
    const entry = client.storage.get(key);
    if (entry) {
      const ttl = entry.expiration ? entry.expiration - Date.now() : -1;
      return {
        key,
        holder: entry.value,
        ttl: ttl > 0 ? ttl : -2
      };
    } else {
      return {
        key,
        holder: null,
        ttl: -2
      };
    }
  });

  return JSON.stringify(result);
}

function simulateDefaultScript(script: string, keyCount: number, ...args: any[]): any {
  // Default successful response
  if (script.includes('acquire') || script.includes('extend')) {
    return keyCount;
  } else if (script.includes('release')) {
    return keyCount;
  }
  return 0;
}
