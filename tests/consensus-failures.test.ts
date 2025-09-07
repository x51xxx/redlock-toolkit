/**
 * Consensus Failure Tests
 * 
 * Validates Byzantine fault tolerance, network partition handling,
 * and distributed consensus mechanisms under failure conditions.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import RedlockToolkit from '../src/index';
import { ConsensusError, ResourceLockedError } from '../src/errors';
import { createMockRedisClients, createTestRedlockToolkitConfig, sleep } from './setup';

describe('Consensus Failures', () => {
  let mockClients: any[];
  let neolock: RedlockToolkit;

  afterEach(async () => {
    if (neolock) {
      await neolock.shutdown();
    }
  });

  describe('Quorum Loss Scenarios', () => {
    beforeEach(() => {
      // 5 nodes: quorum = floor(5/2) + 1 = 3 (tolerates 2 failures)
      mockClients = createMockRedisClients(5);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 1000,
          retryCount: 0
        }
      }));
    });

    it('should fail with exactly N/2 nodes responding (no quorum)', async () => {
      // Split-brain scenario: 2 vs 3 partition, neither side has quorum
      mockClients[0].evalsha.mockResolvedValue(1);
      mockClients[1].evalsha.mockResolvedValue(1);
      mockClients[2].evalsha.mockRejectedValue(new Error('Network partition'));
      mockClients[3].evalsha.mockRejectedValue(new Error('Network partition'));
      mockClients[4].evalsha.mockRejectedValue(new Error('Network partition'));

      await expect(neolock.acquire('no-quorum'))
        .rejects.toThrowError();
      
      try {
        await neolock.acquire('no-quorum');
      } catch (error: any) {
        expect(error.name).toBe('ConsensusError');
        expect(error.message).toContain('Failed to achieve quorum');
      }
    });

    it('should succeed with minimum quorum (N/2 + 1)', async () => {
      // Byzantine fault tolerance: can proceed with exactly quorum nodes
      mockClients[0].evalsha.mockResolvedValue(1);
      mockClients[1].evalsha.mockResolvedValue(1);
      mockClients[2].evalsha.mockResolvedValue(1);
      mockClients[3].evalsha.mockRejectedValue(new Error('Failed'));
      mockClients[4].evalsha.mockRejectedValue(new Error('Failed'));

      const lock = await neolock.acquire('minimum-quorum');
      expect(lock).toBeDefined();
      expect(lock.isValid).toBe(true);
    });

    it('should handle majority failure correctly', async () => {
      // Only 1 out of 5 nodes respond
      mockClients[0].evalsha.mockResolvedValue(1);
      mockClients[1].evalsha.mockRejectedValue(new Error('Failed'));
      mockClients[2].evalsha.mockRejectedValue(new Error('Failed'));
      mockClients[3].evalsha.mockRejectedValue(new Error('Failed'));
      mockClients[4].evalsha.mockRejectedValue(new Error('Failed'));

      await expect(neolock.acquire('majority-failure'))
        .rejects.toThrowError();
      
      try {
        await neolock.acquire('majority-failure');
      } catch (error: any) {
        expect(error.name).toBe('ConsensusError');
        expect(error.message).toContain('Failed to achieve quorum');
      }
    });

    it('should clean up partial locks when quorum fails', async () => {
      let releaseCalled = false;
      
      // Partial success: must rollback locks on minority nodes
      mockClients[0].evalsha.mockResolvedValue(1);
      mockClients[0].eval.mockImplementation((script: string) => {
        if (script.includes('DEL')) {  // Cleanup via release script
          releaseCalled = true;
          return Promise.resolve(1);
        }
        return Promise.resolve(1);
      });
      
      mockClients[1].evalsha.mockResolvedValue(1); // Acquire succeeds
      mockClients[1].eval.mockImplementation((script: string) => {
        if (script.includes('DEL')) {
          return Promise.resolve(1);
        }
        return Promise.resolve(1);
      });
      
      mockClients[2].evalsha.mockRejectedValue(new Error('Failed'));
      mockClients[3].evalsha.mockRejectedValue(new Error('Failed'));
      mockClients[4].evalsha.mockRejectedValue(new Error('Failed'));

      await expect(neolock.acquire('cleanup-partial'))
        .rejects.toThrowError();
      
      try {
        await neolock.acquire('cleanup-partial');
      } catch (error: any) {
        expect(error.name).toBe('ConsensusError');
        expect(error.message).toContain('Failed to achieve quorum');
      }

      // Verify cleanup was attempted
      expect(releaseCalled).toBe(true);
    });
  });

  describe('Split-Brain Scenarios', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(6); // Even number for split-brain
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 1000,
          retryCount: 0
        }
      }));
    });

    it('should handle network partition with even split', async () => {
      // 3 nodes see lock as available, 3 see it as taken
      mockClients[0].evalsha.mockResolvedValue(1);
      mockClients[1].evalsha.mockResolvedValue(1);
      mockClients[2].evalsha.mockResolvedValue(1);
      mockClients[3].evalsha.mockResolvedValue(0); // Lock exists
      mockClients[4].evalsha.mockResolvedValue(0); // Lock exists
      mockClients[5].evalsha.mockResolvedValue(0); // Lock exists

      // Should fail as we don't have clear consensus
      await expect(neolock.acquire('split-brain'))
        .rejects.toThrow();
    });

    it('should prevent simultaneous lock holders', async () => {
      // First client acquires lock
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });
      const lock1 = await neolock.acquire('prevent-dual-holder');

      // Network partition occurs, some nodes lose lock info
      // Simulate another client trying to acquire during partition
      mockClients[0].evalsha.mockResolvedValueOnce(1); // Thinks lock is free
      mockClients[1].evalsha.mockResolvedValueOnce(1); // Thinks lock is free
      mockClients[2].evalsha.mockResolvedValueOnce(1); // Thinks lock is free
      mockClients[3].evalsha.mockResolvedValueOnce(0); // Knows lock exists
      mockClients[4].evalsha.mockResolvedValueOnce(0); // Knows lock exists
      mockClients[5].evalsha.mockResolvedValueOnce(0); // Knows lock exists

      // Second acquisition should fail despite some nodes thinking it's free
      await expect(neolock.acquire('prevent-dual-holder'))
        .rejects.toThrow();

      // Original lock should still be valid
      expect(lock1.isValid).toBe(true);
    });
  });

  describe('Clock Drift Scenarios', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 1000,
          driftFactor: 0.01, // 1% drift
          retryCount: 0
        }
      }));
    });

    it('should account for clock drift in lock validity', async () => {
      const startTime = Date.now();
      
      // Simulate slow network responses
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          return new Promise(resolve => {
            setTimeout(() => resolve(1), 50); // 50ms delay
          });
        });
      });

      const lock = await neolock.acquire('drift-test');
      
      // Lock validity should be reduced by drift + network time
      const expectedReduction = Math.round(0.01 * 1000) + 2; // drift + 2ms
      const actualValidity = lock.expiration - startTime;
      
      expect(actualValidity).toBeLessThan(1000);
      expect(actualValidity).toBeLessThanOrEqual(1000 - expectedReduction);
    });

    it('should fail if drift calculation makes lock invalid', async () => {
      // Use very short TTL where drift matters
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 10, // 10ms TTL
          driftFactor: 0.5, // 50% drift (extreme)
          retryCount: 0
        }
      }));

      // Simulate network delay longer than validity
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(() => {
          return new Promise(resolve => {
            setTimeout(() => resolve(1), 15); // Longer than TTL
          });
        });
      });

      await expect(neolock.acquire('invalid-drift'))
        .rejects.toThrow();
    });
  });

  describe('Byzantine Failures', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(7); // 7 nodes for Byzantine fault tolerance
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 1000,
          retryCount: 0
        }
      }));
    });

    it('should tolerate Byzantine nodes returning incorrect data', async () => {
      // 4 honest nodes, 3 Byzantine (malicious/faulty)
      mockClients[0].evalsha.mockResolvedValue(1); // Honest
      mockClients[1].evalsha.mockResolvedValue(1); // Honest
      mockClients[2].evalsha.mockResolvedValue(1); // Honest
      mockClients[3].evalsha.mockResolvedValue(1); // Honest
      mockClients[4].evalsha.mockResolvedValue(-1); // Byzantine - invalid response
      mockClients[5].evalsha.mockResolvedValue(null); // Byzantine - null response
      mockClients[6].evalsha.mockResolvedValue('invalid'); // Byzantine - wrong type

      const lock = await neolock.acquire('byzantine-tolerance');
      expect(lock).toBeDefined(); // Should succeed with 4/7 honest nodes
    });

    it('should fail with too many Byzantine nodes', async () => {
      // Only 3 honest nodes, 4 Byzantine
      mockClients[0].evalsha.mockResolvedValue(1); // Honest
      mockClients[1].evalsha.mockResolvedValue(1); // Honest
      mockClients[2].evalsha.mockResolvedValue(1); // Honest
      mockClients[3].evalsha.mockRejectedValue(new Error('Corrupted')); // Byzantine
      mockClients[4].evalsha.mockResolvedValue(undefined); // Byzantine
      mockClients[5].evalsha.mockResolvedValue({}); // Byzantine
      mockClients[6].evalsha.mockResolvedValue([]); // Byzantine

      await expect(neolock.acquire('byzantine-failure'))
        .rejects.toThrowError();
      
      try {
        await neolock.acquire('byzantine-failure');
      } catch (error: any) {
        expect(error.name).toBe('ConsensusError');
        expect(error.message).toContain('Failed to achieve quorum');
      }
    });
  });

  describe('Cascading Failures', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(5);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
        defaultLockOptions: {
          ttl: 1000,
          retryCount: 2,
          retryDelay: 50
        }
      }));
    });

    it('should handle progressive node failures', async () => {
      let attemptCount = 0;
      
      mockClients.forEach((client, index) => {
        client.evalsha.mockImplementation(() => {
          attemptCount++;
          
          // Nodes fail progressively
          if (attemptCount <= 5) {
            // First attempt: all succeed
            return Promise.resolve(1);
          } else if (attemptCount <= 10) {
            // Second attempt: some fail
            return index < 3 ? Promise.resolve(1) : Promise.reject(new Error('Failed'));
          } else {
            // Third attempt: more fail
            return index < 2 ? Promise.resolve(1) : Promise.reject(new Error('Failed'));
          }
        });
      });

      // Should eventually fail as nodes progressively fail
      await expect(neolock.acquire('cascading-failure'))
        .rejects.toThrow();
    });

    it('should recover from transient consensus loss', async () => {
      let attemptCount = 0;
      
      mockClients.forEach((client, index) => {
        client.evalsha.mockImplementation(() => {
          attemptCount++;
          
          // First attempt: no quorum
          if (attemptCount <= 5) {
            return index < 2 ? Promise.resolve(1) : Promise.reject(new Error('Transient'));
          }
          // Second attempt: quorum restored
          return index < 4 ? Promise.resolve(1) : Promise.reject(new Error('Still down'));
        });
      });

      const lock = await neolock.acquire('transient-recovery');
      expect(lock).toBeDefined();
      expect(attemptCount).toBeGreaterThan(5); // Required retry
    });
  });

  describe('Asymmetric Failures', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(5);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should handle asymmetric acquire/release failures', async () => {
      // All nodes succeed on acquire
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await neolock.acquire('asymmetric-test');

      // But only some succeed on release
      mockClients[0].evalsha.mockResolvedValueOnce(1);
      mockClients[1].evalsha.mockResolvedValueOnce(1);
      mockClients[2].evalsha.mockResolvedValueOnce(1);
      mockClients[3].evalsha.mockRejectedValueOnce(new Error('Release failed'));
      mockClients[4].evalsha.mockRejectedValueOnce(new Error('Release failed'));

      const result = await lock.release();
      expect(result.success).toBe(true); // Quorum still achieved
    });

    it('should handle asymmetric extend failures', async () => {
      // All nodes succeed on acquire
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await neolock.acquire('asymmetric-extend');

      // Mixed responses on extend
      mockClients[0].evalsha.mockResolvedValueOnce(1);
      mockClients[1].evalsha.mockResolvedValueOnce(1);
      mockClients[2].evalsha.mockResolvedValueOnce(0); // Lost lock somehow
      mockClients[3].evalsha.mockResolvedValueOnce(1);
      mockClients[4].evalsha.mockRejectedValueOnce(new Error('Extend failed'));

      // Should still succeed with 3/5 nodes
      const extended = await lock.extend(2000);
      expect(extended).toBe(lock); // Same lock instance
    });
  });

  describe('Quorum Changes During Operation', () => {
    it('should handle node addition during lock hold', async () => {
      // Start with 3 nodes
      mockClients = createMockRedisClients(3);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await neolock.acquire('node-addition');

      // Simulate adding 2 more nodes (would need to recreate neolock in practice)
      // Extension should still work with original quorum
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      await lock.extend(2000);
      expect(lock.isValid).toBe(true);
    });

    it('should handle node removal during lock hold', async () => {
      // Start with 5 nodes
      mockClients = createMockRedisClients(5);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });

      const lock = await neolock.acquire('node-removal');

      // Simulate 2 nodes going down permanently
      mockClients[3].evalsha.mockRejectedValue(new Error('Node removed'));
      mockClients[4].evalsha.mockRejectedValue(new Error('Node removed'));
      
      // Remaining 3 nodes respond
      mockClients[0].evalsha.mockResolvedValueOnce(1);
      mockClients[1].evalsha.mockResolvedValueOnce(1);
      mockClients[2].evalsha.mockResolvedValueOnce(1);

      // Should still maintain lock with remaining nodes
      await lock.extend(2000);
      expect(lock.isValid).toBe(true);
    });
  });

  describe('Consensus Under Load', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(5);
      neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should maintain consensus with varying response times', async () => {
      // Nodes respond with different delays
      mockClients[0].evalsha.mockImplementation(() => 
        new Promise(resolve => setTimeout(() => resolve(1), 10))
      );
      mockClients[1].evalsha.mockImplementation(() => 
        new Promise(resolve => setTimeout(() => resolve(1), 50))
      );
      mockClients[2].evalsha.mockImplementation(() => 
        new Promise(resolve => setTimeout(() => resolve(1), 100))
      );
      mockClients[3].evalsha.mockImplementation(() => 
        new Promise(resolve => setTimeout(() => resolve(1), 150))
      );
      mockClients[4].evalsha.mockImplementation(() => 
        new Promise(resolve => setTimeout(() => resolve(1), 200))
      );

      const startTime = Date.now();
      const lock = await neolock.acquire('varying-response');
      const duration = Date.now() - startTime;

      expect(lock).toBeDefined();
      // Should wait for quorum (3/5) - which is the 100ms node
      expect(duration).toBeGreaterThanOrEqual(100);
      expect(duration).toBeLessThan(200); // Should not wait for all nodes
    });

    it('should handle partial slow nodes', async () => {
      // 3 fast nodes, 2 very slow nodes
      mockClients[0].evalsha.mockResolvedValue(1);
      mockClients[1].evalsha.mockResolvedValue(1);
      mockClients[2].evalsha.mockResolvedValue(1);
      mockClients[3].evalsha.mockImplementation(() => 
        new Promise(resolve => setTimeout(() => resolve(1), 5000)) // Very slow
      );
      mockClients[4].evalsha.mockImplementation(() => 
        new Promise(resolve => setTimeout(() => resolve(1), 5000)) // Very slow
      );

      const startTime = Date.now();
      const lock = await neolock.acquire('partial-slow');
      const duration = Date.now() - startTime;

      expect(lock).toBeDefined();
      // Should succeed quickly with fast quorum
      expect(duration).toBeLessThan(1000);
    });
  });
});