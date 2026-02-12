/**
 * Distributed CountDownLatch Tests
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import RedlockToolkit from '../src/index';
import { createMockRedisClients, createTestRedlockToolkitConfig } from './setup';

describe('Distributed CountDownLatch', () => {
  let mockClients: ReturnType<typeof createMockRedisClients>;
  let toolkit: RedlockToolkit;

  afterEach(async () => {
    if (toolkit) {
      await toolkit.shutdown();
    }
  });

  describe('Latch Creation', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should create a countdown latch', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1); // created
      });

      const latch = await toolkit.createCountDownLatch('migration-ready', {
        count: 3,
        ttl: 60000,
      });

      expect(latch).toBeDefined();
      expect(latch.name).toBe('migration-ready');
      expect(latch.targetCount).toBe(3);
    });

    it('should throw LatchExistsError if latch already exists', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(0); // already exists
      });

      await expect(
        toolkit.createCountDownLatch('existing', { count: 3, ttl: 60000 })
      ).rejects.toThrow('already exists');
    });
  });

  describe('CountDown', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should count down successfully', async () => {
      // Create latch
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });
      const latch = await toolkit.createCountDownLatch('test', { count: 3, ttl: 60000 });

      // Count down
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce([2, 0, 1]); // remaining=2, status=1 (decremented)
      });

      const result = await latch.countDown('event-1');
      expect(result.success).toBe(true);
      expect(result.remainingCount).toBe(2);
      expect(result.justCompleted).toBe(false);
    });

    it('should report justCompleted when count reaches 0', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });
      const latch = await toolkit.createCountDownLatch('test', { count: 1, ttl: 60000 });

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce([0, 0, 2]); // remaining=0, status=2 (completed)
      });

      const result = await latch.countDown('final-event');
      expect(result.success).toBe(true);
      expect(result.remainingCount).toBe(0);
      expect(result.justCompleted).toBe(true);
    });

    it('should handle idempotent countdown (same eventId)', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });
      const latch = await toolkit.createCountDownLatch('test', { count: 3, ttl: 60000 });

      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce([2, 0, -2]); // status=-2 (already counted)
      });

      const result = await latch.countDown('duplicate-event');
      expect(result.success).toBe(true);
    });
  });

  describe('Latch Status', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should return status of existing latch', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });
      const latch = await toolkit.createCountDownLatch('test', { count: 5, ttl: 60000 });

      mockClients[0].evalsha.mockResolvedValueOnce([3, 5, 55000]); // remaining=3, target=5, ttl=55s

      const status = await latch.getStatus();
      expect(status.exists).toBe(true);
      expect(status.remainingCount).toBe(3);
      expect(status.targetCount).toBe(5);
      expect(status.completed).toBe(false);
      expect(status.ttl).toBe(55000);
    });

    it('should report non-existent latch', async () => {
      mockClients[0].evalsha.mockResolvedValueOnce([-1, -1, -1]);

      const status = await toolkit.getCountDownLatchStatus('gone');
      expect(status.exists).toBe(false);
      expect(status.completed).toBe(false);
    });

    it('should report completed latch', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });
      const latch = await toolkit.createCountDownLatch('test', { count: 1, ttl: 60000 });

      mockClients[0].evalsha.mockResolvedValueOnce([0, 1, 50000]);

      const status = await latch.getStatus();
      expect(status.exists).toBe(true);
      expect(status.completed).toBe(true);
      expect(status.remainingCount).toBe(0);
    });
  });

  describe('Await (Polling)', () => {
    beforeEach(() => {
      mockClients = createMockRedisClients(3);
      toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(mockClients));
    });

    it('should return immediately if latch is already completed', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });
      const latch = await toolkit.createCountDownLatch('test', { count: 1, ttl: 60000 });

      // getStatus returns completed
      mockClients[0].evalsha.mockResolvedValueOnce([0, 1, 50000]);

      const completed = await latch.await(5000);
      expect(completed).toBe(true);
    });

    it('should poll until latch completes', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });
      const latch = await toolkit.createCountDownLatch('test', {
        count: 2,
        ttl: 60000,
        pollInterval: 20,
      });

      // First status check: not completed, then polling: not completed, then completed
      mockClients[0].evalsha
        .mockResolvedValueOnce([1, 2, 59000])  // initial check: remaining=1
        .mockResolvedValueOnce([1, 2, 58000])  // poll 1
        .mockResolvedValueOnce([0, 2, 57000]); // poll 2: completed

      const completed = await latch.await(5000);
      expect(completed).toBe(true);
    });

    it('should throw LatchTimeoutError on timeout', async () => {
      mockClients.forEach(client => {
        client.evalsha.mockResolvedValueOnce(1);
      });
      const latch = await toolkit.createCountDownLatch('test', {
        count: 3,
        ttl: 60000,
        pollInterval: 10,
      });

      // Always return not completed
      mockClients[0].evalsha.mockResolvedValue([2, 3, 55000]);

      await expect(latch.await(50)).rejects.toThrow('timed out');
    });
  });
});
