/**
 * Distributed Semaphore Tests
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import RedlockToolkit from '../src/index';
import { createMockRedisClients, createTestRedlockToolkitConfig } from './setup';

describe('Distributed Semaphore', () => {
  let mockClients: ReturnType<typeof createMockRedisClients>;
  let toolkit: RedlockToolkit;

  afterEach(async () => {
    if (toolkit) {
      await toolkit.shutdown();
    }
  });

  describe('Basic Permit Lifecycle', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should acquire a semaphore permit', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce([1, 1, 3]); // acquired, 1 active, max 3
      });

      const permit = await toolkit.acquireSemaphore('api-calls', { maxPermits: 3 });
      expect(permit).toBeDefined();
      expect(permit.resource).toBe('api-calls');
      expect(permit.identifier).toBeTruthy();
      expect(permit.isValid).toBe(true);
    });

    it('should release a semaphore permit', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce([1, 1, 3]); // acquire
      });

      const permit = await toolkit.acquireSemaphore('api-calls', { maxPermits: 3 });

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce([1, 2]); // release: removed=1, remaining=2
      });

      const result = await permit.release();
      expect(result.success).toBe(true);
      expect(permit.released).toBe(true);
    });

    it('should be idempotent on double release', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce([1, 1, 3]);
      });

      const permit = await toolkit.acquireSemaphore('api-calls', { maxPermits: 3 });

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce([1, 0]);
      });

      await permit.release();
      const result2 = await permit.release();
      expect(result2.success).toBe(true);
    });
  });

  describe('Permit Extension', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should extend a permit', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce([1, 1, 5]);
      });

      const permit = await toolkit.acquireSemaphore('res', { maxPermits: 5 });

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // extend success
      });

      await permit.extend(10000);
      expect(permit.extensions).toBe(1);
      expect(permit.isValid).toBe(true);
    });

    it('should throw on extending released permit', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce([1, 1, 5]);
      });

      const permit = await toolkit.acquireSemaphore('res', { maxPermits: 5 });

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce([1, 0]);
      });
      await permit.release();

      await expect(permit.extend(10000)).rejects.toThrow('Cannot extend released permit');
    });
  });

  describe('Capacity Limits', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should throw SemaphoreFullError when at capacity', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce([0, 3, 3]); // full: 3/3
      });

      await expect(
        toolkit.acquireSemaphore('limited', { maxPermits: 3, retryCount: 0 })
      ).rejects.toThrow('Semaphore full');
    });

    it('should retry and succeed after permit becomes available', async () => {
      // First attempt: full
      mockClients.forEach(client => {
        client.evalsha
          .mockResolvedValueOnce([0, 5, 5])  // full
          .mockResolvedValueOnce([1, 5, 5]); // success on retry
      });

      const permit = await toolkit.acquireSemaphore('limited', {
        maxPermits: 5,
        retryCount: 1,
        retryDelay: 10,
        retryJitter: 0,
      });

      expect(permit).toBeDefined();
      expect(permit.isValid).toBe(true);
    });
  });

  describe('Semaphore Status', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should return status with active holders', async () => {
      mockClients[0].evalsha.mockResolvedValueOnce([
        2, 5,
        'holder-1', String(Date.now() + 30000),
        'holder-2', String(Date.now() + 25000),
      ]);

      const status = await toolkit.getSemaphoreStatus('res', 5);
      expect(status.resource).toBe('res');
      expect(status.activePermits).toBe(2);
      expect(status.maxPermits).toBe(5);
      expect(status.holders).toHaveLength(2);
      expect(status.holders[0].identifier).toBe('holder-1');
    });

    it('should return empty status for unused semaphore', async () => {
      mockClients[0].evalsha.mockResolvedValueOnce([0, 10]);

      const status = await toolkit.getSemaphoreStatus('unused', 10);
      expect(status.activePermits).toBe(0);
      expect(status.holders).toHaveLength(0);
    });
  });

  describe('Shutdown', () => {
    it('should release all active permits on shutdown', async () => {
      mockClients = createMockRedisClients(3);
      toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));

      // Acquire two permits
      mockClients.forEach(client => {
        client.evalsha
          .mockResolvedValueOnce([1, 1, 10])
          .mockResolvedValueOnce([1, 2, 10]);
      });

      await toolkit.acquireSemaphore('res1', { maxPermits: 10 });
      await toolkit.acquireSemaphore('res2', { maxPermits: 10 });

      expect(toolkit.activeSemaphores.size).toBe(2);

      // Shutdown releases everything
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValue([1, 0]);
      });

      await toolkit.shutdown();
      expect(toolkit.activeSemaphores.size).toBe(0);
    });
  });
});
