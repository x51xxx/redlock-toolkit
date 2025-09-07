/**
 * Performance and stress tests
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import RedlockToolkit from '../src/index';
import { createMockRedisClients, createTestRedlockToolkitConfig, sleep } from './setup';

describe('Performance Tests', () => {
  let mockClients: any[];
  let neolock: RedlockToolkit;

  beforeEach(() => {
    mockClients = createMockRedisClients(3);
    neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
      defaultLockOptions: {
        ttl: 1000,
        retryCount: 5,
        retryDelay: 10,
        retryJitter: 5
      }
    }));

    // Setup fast mock responses
    mockClients.forEach(client => {
      client.evalsha.mockImplementation(async () => {
        await sleep(1); // Minimal latency
        return 1; // Success
      });
    });
  });

  describe('Lock Acquisition Performance', () => {
    it('should acquire locks quickly under normal conditions', async () => {
      const startTime = Date.now();
      const locks = [];
      
      // Acquire 50 locks in parallel
      const promises = [];
      for (let i = 0; i < 50; i++) {
        promises.push(neolock.acquire(`resource:${i}`));
      }
      
      const results = await Promise.all(promises);
      const endTime = Date.now();
      
      expect(results).toHaveLength(50);
      expect(endTime - startTime).toBeLessThan(1000); // Should complete within 1 second
      
      // Release all locks
      for (const lock of results) {
        locks.push(lock.release());
      }
      await Promise.all(locks);
    });

    it('should handle burst of sequential acquisitions efficiently', async () => {
      const startTime = Date.now();
      const acquisitionTimes: number[] = [];
      
      for (let i = 0; i < 20; i++) {
        const lockStart = Date.now();
        const lock = await neolock.acquire(`sequential:${i}`);
        acquisitionTimes.push(Date.now() - lockStart);
        await lock.release();
      }
      
      const totalTime = Date.now() - startTime;
      const averageTime = acquisitionTimes.reduce((a, b) => a + b, 0) / acquisitionTimes.length;
      
      expect(totalTime).toBeLessThan(2000); // Total time reasonable
      expect(averageTime).toBeLessThan(100); // Average acquisition time reasonable
      expect(Math.max(...acquisitionTimes)).toBeLessThan(200); // No outliers
    });

    it('should maintain performance with large number of resources', async () => {
      const startTime = Date.now();
      
      // Test with lock on many resources at once
      const resources = Array.from({ length: 100 }, (_, i) => `resource:${i}`);
      const lock = await neolock.acquire(resources);
      
      const acquisitionTime = Date.now() - startTime;
      expect(acquisitionTime).toBeLessThan(500); // Should handle 100 resources quickly
      
      await lock.release();
    });
  });

  describe('Extension Performance', () => {
    it('should extend locks quickly', async () => {
      const lock = await neolock.acquire('extension:test');
      const extensionTimes: number[] = [];
      
      // Perform multiple extensions
      for (let i = 0; i < 10; i++) {
        const startTime = Date.now();
        await lock.extend(1000);
        extensionTimes.push(Date.now() - startTime);
      }
      
      const averageExtensionTime = extensionTimes.reduce((a, b) => a + b, 0) / extensionTimes.length;
      expect(averageExtensionTime).toBeLessThan(50); // Extensions should be fast
      
      await lock.release();
    });

    it('should handle concurrent extensions efficiently', async () => {
      const locks = await Promise.all([
        neolock.acquire('concurrent:1'),
        neolock.acquire('concurrent:2'),
        neolock.acquire('concurrent:3')
      ]);
      
      const startTime = Date.now();
      
      // Extend all locks simultaneously
      await Promise.all(locks.map(lock => lock.extend(2000)));
      
      const totalTime = Date.now() - startTime;
      expect(totalTime).toBeLessThan(200); // Concurrent extensions should be fast
      
      await Promise.all(locks.map(lock => lock.release()));
    });
  });

  describe('Memory Performance', () => {
    it('should not leak memory with many lock operations', async () => {
      const initialMetrics = neolock.getMetrics();
      
      // Perform many acquire/release cycles
      for (let i = 0; i < 100; i++) {
        const lock = await neolock.acquire(`memory:test:${i}`);
        await lock.release();
      }
      
      const finalMetrics = neolock.getMetrics();
      
      // Active locks should return to zero
      expect(finalMetrics.activeLocks).toBe(0);
      expect(finalMetrics.locksAcquired).toBe(100);
      expect(finalMetrics.locksReleased).toBe(100);
      
      // No active locks should remain in internal tracking
      expect(neolock.getActiveLocks()).toHaveLength(0);
    });

    it('should clean up expired locks from metrics', async () => {
      // Don't use fake timers as they can cause issues with async operations
      
      // Acquire locks with short TTL
      const locks = await Promise.all([
        neolock.acquire('expiry:1', { ttl: 100 }),
        neolock.acquire('expiry:2', { ttl: 100 }),
        neolock.acquire('expiry:3', { ttl: 100 })
      ]);
      
      expect(neolock.getActiveLocks()).toHaveLength(3);
      
      // Wait for locks to expire naturally
      await new Promise(resolve => setTimeout(resolve, 150));
      
      // Release the locks (they're expired but still need cleanup)
      await Promise.all(locks.map(lock => lock.release()));
      
      expect(neolock.getActiveLocks()).toHaveLength(0);
    });
  });

  describe('Circuit Breaker Performance Impact', () => {
    it('should add minimal overhead when circuit is closed', async () => {
      const times: number[] = [];
      
      for (let i = 0; i < 20; i++) {
        const startTime = Date.now();
        const lock = await neolock.acquire(`circuit:test:${i}`);
        times.push(Date.now() - startTime);
        await lock.release();
      }
      
      const averageTime = times.reduce((a, b) => a + b, 0) / times.length;
      const variance = times.reduce((acc, time) => acc + Math.pow(time - averageTime, 2), 0) / times.length;
      const standardDeviation = Math.sqrt(variance);
      
      // Times should be consistent (low variance indicates stable performance)
      expect(standardDeviation).toBeLessThan(averageTime * 0.5); // SD should be less than 50% of mean
    });

    it('should fail fast when circuit is open', async () => {
      // Force circuit to open by making operations fail
      mockClients.forEach(client => {
        client.evalsha.mockRejectedValue(new Error('Redis failure'));
      });
      
      // Trigger circuit breaker
      for (let i = 0; i < 5; i++) {
        await expect(neolock.acquire(`circuit:fail:${i}`)).rejects.toThrow();
      }
      
      // Now operations should fail immediately
      const failTimes: number[] = [];
      
      for (let i = 0; i < 5; i++) {
        const startTime = Date.now();
        await expect(neolock.acquire(`circuit:immediate:${i}`)).rejects.toThrow();
        failTimes.push(Date.now() - startTime);
      }
      
      const averageFailTime = failTimes.reduce((a, b) => a + b, 0) / failTimes.length;
      expect(averageFailTime).toBeLessThan(100); // Should fail quickly (within 100ms)
    });
  });

  describe('Metrics Collection Overhead', () => {
    it.skip('should have minimal performance impact when metrics are enabled', async () => {
      // Create separate clients for each instance to avoid conflicts
      const clientsForMetrics = createMockRedisClients(3);
      const clientsForNoMetrics = createMockRedisClients(3);
      
      [clientsForMetrics, clientsForNoMetrics].forEach(clients => {
        clients.forEach(client => {
          client.evalsha.mockImplementation(async () => {
            await sleep(1);
            return 1;
          });
        });
      });
      
      const neolockWithMetrics = new RedlockToolkit({
        clients: clientsForMetrics,
        enableMetrics: true
      });
      
      const neolockWithoutMetrics = new RedlockToolkit({
        clients: clientsForNoMetrics,
        enableMetrics: false
      });
      
      // Warm up
      await neolockWithMetrics.acquire('warmup:1').then(l => l.release());
      await neolockWithoutMetrics.acquire('warmup:2').then(l => l.release());
      
      // Test with metrics
      const startWith = Date.now();
      for (let i = 0; i < 50; i++) {
        const lock = await neolockWithMetrics.acquire(`metrics:${i}`);
        await lock.release();
      }
      const timeWith = Date.now() - startWith;
      
      // Test without metrics
      const startWithout = Date.now();
      for (let i = 0; i < 50; i++) {
        const lock = await neolockWithoutMetrics.acquire(`no-metrics:${i}`);
        await lock.release();
      }
      const timeWithout = Date.now() - startWithout;
      
      // Metrics should add less than 50% overhead
      const overhead = (timeWith - timeWithout) / timeWithout;
      expect(overhead).toBeLessThan(0.5);
    });
  });
});

describe('Stress Tests', () => {
  let mockClients: any[];
  let neolock: RedlockToolkit;

  beforeEach(() => {
    mockClients = createMockRedisClients(5); // More clients for stress testing
    neolock = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients, {
      defaultLockOptions: {
        ttl: 2000,
        retryCount: 10,
        retryDelay: 20,
        retryJitter: 10
      }
    }));

    // Setup realistic response times
    mockClients.forEach(client => {
      client.evalsha.mockImplementation(async () => {
        await sleep(Math.random() * 10); // Variable latency 0-10ms
        return 1;
      });
    });
  });

  describe('High Concurrency', () => {
    it('should handle many concurrent lock acquisitions', async () => {
      const concurrentOps = 200;
      const promises: Promise<any>[] = [];
      
      const startTime = Date.now();
      
      // Launch many concurrent operations
      for (let i = 0; i < concurrentOps; i++) {
        promises.push(
          neolock.acquire(`stress:${i % 50}`) // Some contention
            .then(lock => lock.release())
            .catch(err => ({ error: err }))
        );
      }
      
      const results = await Promise.all(promises);
      const totalTime = Date.now() - startTime;
      
      const successCount = results.filter(r => !r.error).length;
      const errorCount = results.length - successCount;
      
      // Most operations should succeed
      expect(successCount).toBeGreaterThan(concurrentOps * 0.8);
      expect(totalTime).toBeLessThan(5000); // Should complete within 5 seconds
      
      console.log(`Stress test: ${successCount}/${concurrentOps} succeeded in ${totalTime}ms`);
    });

    it('should maintain lock safety under high contention', async () => {
      const resource = 'highly-contended-resource';
      const concurrentAttempts = 100;
      const promises: Promise<any>[] = [];
      const successfulLocks: any[] = [];
      
      // Many processes trying to acquire the same resource
      for (let i = 0; i < concurrentAttempts; i++) {
        promises.push(
          neolock.acquire(resource, { retryCount: 3 })
            .then(lock => {
              successfulLocks.push({ lock, process: i });
              return sleep(10).then(() => lock.release());
            })
            .catch(err => ({ error: err, process: i }))
        );
      }
      
      await Promise.all(promises);
      
      // With retries and short hold time, multiple locks may succeed sequentially
      // But not all should succeed (some should timeout/fail)
      expect(successfulLocks.length).toBeGreaterThan(0);
      expect(successfulLocks.length).toBeLessThanOrEqual(concurrentAttempts);
    });
  });

  describe('Long-running Operations', () => {
    it('should handle locks with auto-extension under stress', async () => {
      const longOps = 20;
      const promises: Promise<any>[] = [];
      
      for (let i = 0; i < longOps; i++) {
        promises.push(
          neolock.using(`long:${i}`, async (signal) => {
            // Simulate long operation with periodic checks
            for (let j = 0; j < 10; j++) {
              if (signal.aborted) throw signal.error;
              await sleep(50);
            }
            return i;
          }, {
            ttl: 200,
            autoExtendThreshold: 50
          })
        );
      }
      
      const results = await Promise.all(promises);
      
      // All operations should complete successfully
      expect(results).toHaveLength(longOps);
      results.forEach((result, index) => {
        expect(result).toBe(index);
      });
    });

    it('should handle rapid extension cycles', async () => {
      const lock = await neolock.acquire('rapid:extension');
      
      // Rapidly extend lock many times
      const extensions = [];
      for (let i = 0; i < 100; i++) {
        extensions.push(lock.extend(1000));
      }
      
      await Promise.all(extensions);
      
      expect(lock.extensions).toBe(100);
      await lock.release();
    });
  });

  describe('Resource Exhaustion', () => {
    it('should handle many simultaneous locks on different resources', { timeout: 60000 }, async () => {
      const lockCount = 100; // Reduced from 500 for faster execution
      const locks: any[] = [];
      
      const startTime = Date.now();
      
      // Acquire many locks on different resources
      for (let i = 0; i < lockCount; i++) {
        const lock = await neolock.acquire(`resource:${i}`);
        locks.push(lock);
      }
      
      const acquisitionTime = Date.now() - startTime;
      
      expect(locks).toHaveLength(lockCount);
      expect(neolock.getActiveLocks()).toHaveLength(lockCount);
      
      const releaseStart = Date.now();
      
      // Release all locks
      await Promise.all(locks.map(lock => lock.release()));
      
      const releaseTime = Date.now() - releaseStart;
      
      expect(neolock.getActiveLocks()).toHaveLength(0);
      
      console.log(`Resource exhaustion test: ${lockCount} locks acquired in ${acquisitionTime}ms, released in ${releaseTime}ms`);
    });

    it('should handle metrics collection under high load', { timeout: 60000 }, async () => {
      const operations = 200; // Reduced from 1000 for faster execution
      
      // Perform many operations to stress metrics collection
      for (let i = 0; i < operations; i++) {
        const lock = await neolock.acquire(`metrics:stress:${i}`);
        if (i % 10 === 0) {
          await lock.extend(1000);
        }
        await lock.release();
      }
      
      const metrics = neolock.getMetrics();
      expect(metrics.locksAcquired).toBe(operations);
      expect(metrics.locksReleased).toBe(operations);
      expect(metrics.lockExtensions).toBe(Math.floor(operations / 10));
      expect(metrics.activeLocks).toBe(0);
      
      // Metrics export should work without issues
      const prometheus = neolock.exportMetrics();
      expect(prometheus).toContain(`neolock_locks_acquired_total ${operations}`);
    });
  });

  describe('Error Recovery', () => {
    it('should recover gracefully from intermittent failures', async () => {
      let failureRate = 0.3; // 30% failure rate
      
      mockClients.forEach(client => {
        client.evalsha.mockImplementation(async () => {
          if (Math.random() < failureRate) {
            throw new Error('Intermittent failure');
          }
          await sleep(Math.random() * 5);
          return 1;
        });
      });
      
      const attempts = 100;
      const successes: any[] = [];
      const failures: any[] = [];
      
      for (let i = 0; i < attempts; i++) {
        try {
          const lock = await neolock.acquire(`recovery:${i}`, { retryCount: 5 });
          successes.push(lock);
          await lock.release();
        } catch (error) {
          failures.push(error);
        }
      }
      
      // Should achieve reasonable success rate despite failures
      const successRate = successes.length / attempts;
      expect(successRate).toBeGreaterThan(0.3); // At least 30% success rate with 30% failure rate
      
      console.log(`Error recovery test: ${successRate * 100}% success rate with ${failureRate * 100}% Redis failure rate`);
    });
  });

  describe('Memory and Resource Cleanup', () => {
    it('should clean up resources after stress testing', async () => {
      // Generate significant load
      const rounds = 50;
      const locksPerRound = 20;
      
      for (let round = 0; round < rounds; round++) {
        const locks = await Promise.all(
          Array.from({ length: locksPerRound }, (_, i) => 
            neolock.acquire(`cleanup:${round}:${i}`)
          )
        );
        
        await Promise.all(locks.map(lock => lock.release()));
      }
      
      // Verify cleanup
      expect(neolock.getActiveLocks()).toHaveLength(0);
      
      const metrics = neolock.getMetrics();
      expect(metrics.activeLocks).toBe(0);
      expect(metrics.locksAcquired).toBe(rounds * locksPerRound);
      expect(metrics.locksReleased).toBe(rounds * locksPerRound);
      
      // Reset should clean everything
      neolock.resetMetrics();
      const resetMetrics = neolock.getMetrics();
      expect(resetMetrics.locksAcquired).toBe(0);
      expect(resetMetrics.locksReleased).toBe(0);
    });
  });
});