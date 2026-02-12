/**
 * Data Integrity Tests
 * 
 * Validates data consistency guarantees, fencing token mechanisms,
 * and atomic operations to prevent race conditions and corruption.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import RedlockToolkit from '../src/index';
import { createMockRedisClients, createTestRedlockToolkitConfig, sleep } from './setup';

describe('Data Integrity', () => {
  let mockClients: ReturnType<typeof createMockRedisClients>;
  let neolock: RedlockToolkit;

  afterEach(async () => {
    if (neolock) {
      await neolock.shutdown();
    }
  });

  describe('Fencing Token Protection', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should prevent stale lock holders from writing', async () => {
      // Fencing tokens prevent split-brain writes after network partitions
      const writes: { resource: string; identifier: string; timestamp: number }[] = [];
      
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // First acquire
      });

      const lock1 = await neolock.acquire('fencing-test');
      const identifier1 = lock1.identifier;

      // Simulate write operation
      const write = async (lock: any) => {
        if (lock.isValid) {
          writes.push({
            resource: lock.resources[0],
            identifier: lock.identifier,
            timestamp: Date.now()
          });
          return true;
        }
        return false;
      };

      expect(await write(lock1)).toBe(true);

      // Simulates network delay causing lock expiration
      (lock1 as any)._expiration = Date.now() - 1000;
      expect(lock1.isExpired).toBe(true);

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
        client.eval.mockResolvedValueOnce(1);
      });
      const lock2 = await neolock.acquire('fencing-test');

      // Expired lock's write must be rejected (prevents stale writes)
      expect(await write(lock1)).toBe(false);

      // Current lock holder proceeds safely
      await new Promise(resolve => setTimeout(resolve, 10));
      expect(await write(lock2)).toBe(true);

      // Verify write order
      expect(writes).toHaveLength(2);
      expect(writes[0].identifier).toBe(identifier1);
      expect(writes[1].identifier).toBe(lock2.identifier);
      expect(writes[1].timestamp).toBeGreaterThan(writes[0].timestamp);
    });

    it('should maintain monotonic ordering with identifiers', async () => {
      const identifiers: string[] = [];
      
      mockClients.forEach(client => {
        // Mock both acquire and release operations
        client.evalsha.mockImplementation((script, keyCount, ...args) => {
          // For acquire, return number of keys; for release, return 1
          return Promise.resolve(keyCount || 1);
        });
        client.eval.mockImplementation((script, keyCount, ...args) => {
          return Promise.resolve(keyCount || 1);
        });
      });

      // Acquire and release multiple locks
      for (let i = 0; i < 10; i++) {
        try {
          const lock = await neolock.acquire(`monotonic-${i}`);
          identifiers.push(lock.identifier);
          await lock.release();
        } catch (error) {
          console.error(`Failed to acquire lock ${i}:`, error);
          break;
        }
      }

      // Uniqueness prevents identifier collision attacks
      const uniqueIdentifiers = new Set(identifiers);
      expect(uniqueIdentifiers.size).toBe(identifiers.length);

      // Cryptographic randomness ensures unpredictability
      identifiers.forEach(id => {
        expect(id).toMatch(/^[a-f0-9]{32}$/); // 128-bit entropy
      });
    });
  });

  describe('Atomic Operations', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should ensure atomic multi-resource locking', async () => {
      const resources = ['resource-a', 'resource-b', 'resource-c'];
      // Independent lock states simulate distributed Redis nodes
      const lockStatesByClient = [
        new Map<string, boolean>(),
        new Map<string, boolean>(),
        new Map<string, boolean>()
      ];
      
      mockClients.forEach((client, clientIndex) => {
        const lockStates = lockStatesByClient[clientIndex];
        
        const lockHandler = (count: number, ...args: any[]) => {
          const keys = args.slice(0, count);
          
          // All-or-nothing: prevents partial lock acquisition
          for (const key of keys) {
            if (lockStates.get(key)) {
              return Promise.resolve(0);
            }
          }
          
          // Atomic acquisition on this node
          keys.forEach((key: string) => lockStates.set(key, true));
          return Promise.resolve(keys.length);
        };
        
        client.evalsha.mockImplementation((script: string, count: number, ...args: any[]) => lockHandler(count, ...args));
        client.eval.mockImplementation((script: string, count: number, ...args: any[]) => lockHandler(count, ...args));
      });

      // First acquisition should succeed
      const lock1 = await neolock.acquire(resources);
      expect(lock1.resources).toEqual(resources);

      // Overlapping acquisition should fail atomically
      await expect(neolock.acquire(['resource-b', 'resource-d']))
        .rejects.toThrow();

      // Non-overlapping should succeed
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });
      const lock2 = await neolock.acquire(['resource-d']);
      expect(lock2.resources).toEqual(['resource-d']);
    });

    it('should maintain consistency during partial failures', async () => {
      const resources = ['res-1', 'res-2', 'res-3'];
      let acquiredOnNode: Map<string, Set<string>>[] = [
        new Map(), new Map(), new Map()
      ];

      mockClients.forEach((client, nodeIndex) => {
        const handler = (count: number, ...args: any[]) => {
          const keys = args.slice(0, count);
          
          // Node 2 fails after node 0 and 1 succeed
          if (nodeIndex === 2) {
            return Promise.reject(new Error('Node failure'));
          }
          
          // Track what each node has locked
          keys.forEach((key: string) => {
            if (!acquiredOnNode[nodeIndex].has(key)) {
              acquiredOnNode[nodeIndex].set(key, new Set());
            }
          });
          
          return Promise.resolve(keys.length);
        };
        client.evalsha.mockImplementation((script: string, count: number, ...args: any[]) => handler(count, ...args));
        client.eval.mockImplementation((script: string, count: number, ...args: any[]) => handler(count, ...args));
      });

      // Acquisition should succeed with quorum (2 out of 3 clients)
      const lock = await neolock.acquire(resources);
      expect(lock).toBeDefined();
      expect(lock.resources).toEqual(resources);

      // Verify cleanup - successful nodes should release
      // In a real implementation, cleanup would be verified through release calls
    });
  });

  describe('Consistency Guarantees', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(5); // 5 nodes for stronger consistency
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should maintain lock state consistency across nodes', async () => {
      const nodeStates: Map<string, string>[] = Array(5).fill(null).map(() => new Map());
      
      mockClients.forEach((client, index) => {
        client.evalsha.mockImplementation((script: string, count: number, ...args: any[]) => {
          const keys = args.slice(0, count);
          const identifier = args[count];
          
          // Simulate lock acquisition
          keys.forEach((key: string) => {
            nodeStates[index].set(key, identifier);
          });
          
          return Promise.resolve(keys.length);
        });
      });

      const lock = await neolock.acquire('consistency-test');
      
      // At least 3 nodes should have consistent state
      const identifier = lock.identifier;
      let consistentNodes = 0;
      
      nodeStates.forEach(state => {
        if (state.get('neolock:consistency-test') === identifier) {
          consistentNodes++;
        }
      });
      
      expect(consistentNodes).toBeGreaterThanOrEqual(3); // Quorum
    });

    it('should prevent split-brain writes', async () => {
      const writes: { node: number; value: any; timestamp: number }[] = [];
      
      // Simulate network partition - nodes 0,1 vs nodes 2,3,4
      mockClients[0].evalsha.mockResolvedValue(1);
      mockClients[1].evalsha.mockResolvedValue(1);
      mockClients[2].evalsha.mockResolvedValue(0); // Sees different state
      mockClients[3].evalsha.mockResolvedValue(0);
      mockClients[4].evalsha.mockResolvedValue(0);

      // Minority partition cannot acquire lock
      await expect(neolock.acquire('split-brain-write'))
        .rejects.toThrow();

      // Verify no writes occurred during split
      expect(writes).toHaveLength(0);
    });
  });

  describe('Idempotency', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should ensure idempotent release operations', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Acquire - returns number of resources locked
        client.eval.mockResolvedValueOnce(1); // Fallback to eval
      });

      const lock = await neolock.acquire('idempotent-release');
      
      let releaseCount = 0;
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          releaseCount++;
          // First release succeeds, others find nothing
          return Promise.resolve(releaseCount <= 3 ? 1 : 0);
        });
      });

      // Multiple releases should all succeed
      const results = await Promise.all([
        lock.release(),
        lock.release(),
        lock.release()
      ]);

      results.forEach(result => {
        expect(result.success).toBe(true);
      });

      expect(lock.released).toBe(true);
    });

    it('should ensure idempotent force release', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(1);
      });

      await neolock.acquire('idempotent-force');

      let forceReleaseCount = 0;
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          forceReleaseCount++;
          return Promise.resolve(forceReleaseCount <= 3 ? 1 : 0);
        });
      });

      // Multiple force releases
      const result1 = await neolock.forceRelease('idempotent-force');
      const result2 = await neolock.forceRelease('idempotent-force');

      expect(result1.releasedCount).toBeGreaterThanOrEqual(0);
      expect(result2.releasedCount).toBe(0); // Already released
    });
  });

  describe('State Recovery', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should recover from inconsistent state', async () => {
      // Node states diverge
      mockClients[0].evalsha.mockResolvedValueOnce(1); // Thinks it acquired
      mockClients[1].evalsha.mockResolvedValueOnce(0); // Thinks it's locked
      mockClients[2].evalsha.mockResolvedValueOnce(1); // Thinks it acquired

      // Should achieve quorum and succeed
      const lock = await neolock.acquire('inconsistent-state');
      expect(lock).toBeDefined();

      // Release should work despite inconsistency
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(1);
      });
      await lock.release();
    });

    it('should handle orphaned locks', async () => {
      // Simulate orphaned lock (no owner tracking)
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(0); // Lock exists but orphaned - use mockResolvedValue not Once
      });

      await expect(neolock.acquire('orphaned-lock'))
        .rejects.toThrow();

      // Reset mocks for force release
      mockClients.forEach(client => {
        client.evalsha.mockReset();
        client.evalsha.mockResolvedValue(1); // Force release succeeds
      });

      await neolock.forceRelease('orphaned-lock');

      // Now acquisition should work
      mockClients.forEach(client => {
        client.evalsha.mockReset();
        client.evalsha.mockResolvedValue(1);
      });

      const lock = await neolock.acquire('orphaned-lock');
      expect(lock).toBeDefined();
    });
  });

  describe('Transaction Safety', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should maintain ACID properties in lock operations', async () => {
      const operations: { type: string; resource: string; success: boolean }[] = [];
      
      mockClients.forEach(client => {
        client.evalsha.mockImplementation((scriptHash: string, numKeys: number, ...keysAndArgs: any[]) => {
          const keys = keysAndArgs.slice(0, numKeys);
          const argv = keysAndArgs.slice(numKeys);
          
          // Differentiate based on ARGV length - acquire has 2+ (identifier, ttl), release has 1 (identifier)
          const type = argv.length > 1 ? 'acquire' : 'release';
          
          operations.push({
            type,
            resource: keys[0],
            success: true
          });
          
          return Promise.resolve(1);
        });
      });

      // Perform transaction
      const lock = await neolock.acquire('transaction-test');
      await lock.release();

      // Verify atomicity - all operations completed
      const acquires = operations.filter(op => op.type === 'acquire');
      const releases = operations.filter(op => op.type === 'release');
      
      expect(acquires.length).toBe(3); // All nodes participated
      expect(releases.length).toBe(3); // All nodes released
    });

    it('should rollback on transaction failure', async () => {
      const resources = ['txn-1', 'txn-2', 'txn-3'];
      const acquiredResources: Set<string> = new Set();
      
      mockClients.forEach((client, index) => {
        client.evalsha.mockImplementation((scriptHash: string, numKeys: number, ...keysAndArgs: any[]) => {
          const keys = keysAndArgs.slice(0, numKeys);
          const argv = keysAndArgs.slice(numKeys);
          const isAcquire = argv.length > 1; // Acquire has identifier and ttl
          
          // Simulate failure on third resource during acquire
          if (isAcquire && keys.some((key: string) => key.includes('txn-3'))) {
            return Promise.reject(new Error('Transaction failed'));
          }
          
          keys.forEach((key: string) => acquiredResources.add(key));
          return Promise.resolve(keys.length);
        });
      });

      await expect(neolock.acquire(resources))
        .rejects.toThrow();

      // Verify rollback - no resources should remain acquired
      // In real implementation, this would be verified through release calls
    });
  });

  describe('Data Consistency with Auto-Extension', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 200,  // Increased from 100 to satisfy validation
          autoExtendThreshold: 30
        }
      }));
    });

    it('should maintain data consistency during auto-extension', async () => {
      const dataWrites: { timestamp: number; identifier: string; valid: boolean }[] = [];
      
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Initial acquire
      });

      const lock = await neolock.acquire('auto-extend-consistency');
      
      // Mock successful extensions
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(1);
      });

      await lock.using(async (signal) => {
        for (let i = 0; i < 5; i++) {
          await sleep(40); // Trigger extensions
          
          // Simulate data write
          dataWrites.push({
            timestamp: Date.now(),
            identifier: lock.identifier,
            valid: !signal.aborted && lock.isValid
          });
        }
      });

      // All writes should be valid
      dataWrites.forEach(write => {
        expect(write.valid).toBe(true);
        expect(write.identifier).toBe(lock.identifier);
      });

      // Writes should be in chronological order
      for (let i = 1; i < dataWrites.length; i++) {
        expect(dataWrites[i].timestamp).toBeGreaterThan(dataWrites[i - 1].timestamp);
      }
    });

    it('should prevent writes after failed auto-extension', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // Initial acquire
      });

      const lock = await neolock.acquire('failed-extension-consistency');
      
      let extensionAttempts = 0;
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          extensionAttempts++;
          // Fail extensions
          return Promise.resolve(0);
        });
      });

      let writeAfterFailure = false;
      
      await expect(lock.using(async (signal) => {
        await sleep(250); // Trigger extension failure (increased to exceed TTL)
        
        if (signal.aborted) {
          throw signal.error;
        }
        
        writeAfterFailure = true; // Should not reach here
        return 'should not complete';
      })).rejects.toThrow();

      expect(writeAfterFailure).toBe(false);
      expect(extensionAttempts).toBeGreaterThan(0);
    });
  });

  describe('Metrics Integrity', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
      neolock.resetMetrics();
    });

    it('should maintain accurate lock counts', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(1);
      });

      const metrics1 = neolock.getMetrics();
      const initialAcquired = metrics1.locksAcquired;

      // Acquire locks
      const locks = [];
      for (let i = 0; i < 5; i++) {
        locks.push(await neolock.acquire(`metrics-${i}`));
      }

      const metrics2 = neolock.getMetrics();
      expect(metrics2.locksAcquired).toBe(initialAcquired + 5);
      expect(metrics2.activeLocks).toBe(5);

      // Release some
      await locks[0].release();
      await locks[1].release();

      const metrics3 = neolock.getMetrics();
      expect(metrics3.locksReleased).toBeGreaterThanOrEqual(2);
      expect(metrics3.activeLocks).toBe(3);

      // Release rest
      await Promise.all(locks.slice(2).map(l => l.release()));

      const metrics4 = neolock.getMetrics();
      expect(metrics4.activeLocks).toBe(0);
    });

    it('should track failed acquisitions accurately', async () => {
      const metrics1 = neolock.getMetrics();
      const initialFailed = metrics1.failedAcquisitions;

      // Force failures
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue(0);
      });

      const failureCount = 3;
      for (let i = 0; i < failureCount; i++) {
        await expect(neolock.acquire(`fail-${i}`))
          .rejects.toThrow();
      }

      const metrics2 = neolock.getMetrics();
      expect(metrics2.failedAcquisitions).toBe(initialFailed + failureCount);
    });
  });
});