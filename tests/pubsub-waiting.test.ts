/**
 * Pub/Sub Waiting Infrastructure Tests
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { PubSubManager } from '../src/pubsub/pubsub-manager';
import { PubSubWaiter } from '../src/pubsub/pubsub-waiter';
import { Logger } from '../src/core/logger';

const createMockLogger = (): Logger => ({
  debug: vi.fn(),
  info: vi.fn(),
  warn: vi.fn(),
  error: vi.fn(),
});

const createMockRedisClient = () => {
  const listeners = new Map<string, Set<(...args: unknown[]) => void>>();
  return {
    options: { host: 'localhost', port: 6379 },
    subscribe: vi.fn().mockResolvedValue(undefined),
    unsubscribe: vi.fn().mockResolvedValue(undefined),
    publish: vi.fn().mockResolvedValue(1),
    disconnect: vi.fn().mockResolvedValue(undefined),
    on: vi.fn().mockImplementation((event: string, listener: (...args: unknown[]) => void) => {
      if (!listeners.has(event)) listeners.set(event, new Set());
      listeners.get(event)!.add(listener);
    }),
    removeListener: vi.fn().mockImplementation((event: string, listener: (...args: unknown[]) => void) => {
      listeners.get(event)?.delete(listener);
    }),
    duplicate: vi.fn().mockImplementation(function () {
      return createMockRedisClient();
    }),
    // Helper to simulate incoming messages
    _simulateMessage(channel: string, message: string) {
      const messageListeners = listeners.get('message');
      if (messageListeners) {
        for (const listener of messageListeners) {
          listener(channel, message);
        }
      }
    },
    _listeners: listeners,
  };
};

describe('PubSubManager', () => {
  let manager: PubSubManager;
  let client: ReturnType<typeof createMockRedisClient>;
  let subscriberClient: ReturnType<typeof createMockRedisClient>;

  beforeEach(() => {
    client = createMockRedisClient();
    subscriberClient = createMockRedisClient();
    // Make duplicate() return the subscriber client
    client.duplicate.mockReturnValue(subscriberClient);
  });

  afterEach(async () => {
    if (manager) {
      await manager.shutdown();
    }
  });

  describe('Initialization', () => {
    it('should lazily create subscriber via duplicate()', async () => {
      manager = new PubSubManager({
        clients: [client as any],
        keyPrefix: 'test',
        logger: createMockLogger(),
      });

      const listener = vi.fn();
      await manager.subscribe('test:channel', listener);

      expect(client.duplicate).toHaveBeenCalledOnce();
      expect(subscriberClient.subscribe).toHaveBeenCalledWith('test:channel');
    });

    it('should use provided subscriber clients', async () => {
      manager = new PubSubManager({
        clients: [client as any],
        subscriberClients: [subscriberClient as any],
        keyPrefix: 'test',
        logger: createMockLogger(),
      });

      const listener = vi.fn();
      await manager.subscribe('ch', listener);

      expect(client.duplicate).not.toHaveBeenCalled();
      expect(subscriberClient.subscribe).toHaveBeenCalledWith('ch');
    });

    it('should throw if client does not support duplicate()', async () => {
      const noDupClient = { ...client, duplicate: undefined } as any;
      delete noDupClient.duplicate;

      manager = new PubSubManager({
        clients: [noDupClient],
        keyPrefix: 'test',
        logger: createMockLogger(),
      });

      await expect(
        manager.subscribe('ch', vi.fn()),
      ).rejects.toThrow(/duplicate/i);
    });
  });

  describe('Subscribe/Unsubscribe', () => {
    beforeEach(() => {
      manager = new PubSubManager({
        clients: [client as any],
        keyPrefix: 'test',
        logger: createMockLogger(),
      });
    });

    it('should subscribe only once for multiple listeners on same channel', async () => {
      await manager.subscribe('ch', vi.fn());
      await manager.subscribe('ch', vi.fn());

      expect(subscriberClient.subscribe).toHaveBeenCalledTimes(1);
    });

    it('should route messages to correct channel listeners', async () => {
      const listener1 = vi.fn();
      const listener2 = vi.fn();
      const listener3 = vi.fn();

      await manager.subscribe('ch-a', listener1);
      await manager.subscribe('ch-a', listener2);
      await manager.subscribe('ch-b', listener3);

      // Simulate message on ch-a
      subscriberClient._simulateMessage('ch-a', 'hello');

      expect(listener1).toHaveBeenCalledWith('hello');
      expect(listener2).toHaveBeenCalledWith('hello');
      expect(listener3).not.toHaveBeenCalled();
    });

    it('should unsubscribe specific listener', async () => {
      const listener1 = vi.fn();
      const listener2 = vi.fn();

      await manager.subscribe('ch', listener1);
      await manager.subscribe('ch', listener2);

      await manager.unsubscribe('ch', listener1);

      subscriberClient._simulateMessage('ch', 'msg');

      expect(listener1).not.toHaveBeenCalled();
      expect(listener2).toHaveBeenCalledWith('msg');
    });

    it('should unsubscribe from Redis when last listener removed', async () => {
      const listener = vi.fn();
      await manager.subscribe('ch', listener);

      await manager.unsubscribe('ch', listener);

      expect(subscriberClient.unsubscribe).toHaveBeenCalledWith('ch');
    });

    it('should unsubscribe all listeners from channel', async () => {
      await manager.subscribe('ch', vi.fn());
      await manager.subscribe('ch', vi.fn());

      await manager.unsubscribe('ch');

      expect(subscriberClient.unsubscribe).toHaveBeenCalledWith('ch');
      expect(manager.getSubscriptionCount()).toBe(0);
    });
  });

  describe('Publish', () => {
    it('should publish using original client, not subscriber', async () => {
      manager = new PubSubManager({
        clients: [client as any],
        keyPrefix: 'test',
        logger: createMockLogger(),
      });

      await manager.publish('ch', 'data');

      expect(client.publish).toHaveBeenCalledWith('ch', 'data');
    });
  });

  describe('waitForMessage', () => {
    beforeEach(() => {
      manager = new PubSubManager({
        clients: [client as any],
        keyPrefix: 'test',
        logger: createMockLogger(),
      });
    });

    it('should resolve when message arrives before timeout', async () => {
      const promise = manager.waitForMessage('wait-ch', 5000);

      // Simulate message after short delay
      setTimeout(() => {
        subscriberClient._simulateMessage('wait-ch', 'released');
      }, 50);

      const result = await promise;
      expect(result).toBe('released');
    });

    it('should return null on timeout', async () => {
      const result = await manager.waitForMessage('timeout-ch', 50);
      expect(result).toBeNull();
    });

    it('should cleanup subscription after receiving message', async () => {
      const promise = manager.waitForMessage('cleanup-ch', 5000);

      setTimeout(() => {
        subscriberClient._simulateMessage('cleanup-ch', 'msg');
      }, 20);

      await promise;

      // Give time for cleanup
      await new Promise(r => setTimeout(r, 10));
      expect(manager.getSubscriptionCount()).toBe(0);
    });
  });

  describe('Shutdown', () => {
    it('should unsubscribe from all channels and disconnect', async () => {
      manager = new PubSubManager({
        clients: [client as any],
        keyPrefix: 'test',
        logger: createMockLogger(),
      });

      await manager.subscribe('ch1', vi.fn());
      await manager.subscribe('ch2', vi.fn());

      await manager.shutdown();

      expect(subscriberClient.unsubscribe).toHaveBeenCalledWith('ch1');
      expect(subscriberClient.unsubscribe).toHaveBeenCalledWith('ch2');
      expect(subscriberClient.disconnect).toHaveBeenCalled();
      expect(manager.getSubscriptionCount()).toBe(0);
    });

    it('should not disconnect user-provided subscriber', async () => {
      manager = new PubSubManager({
        clients: [client as any],
        subscriberClients: [subscriberClient as any],
        keyPrefix: 'test',
        logger: createMockLogger(),
      });

      await manager.subscribe('ch', vi.fn());
      await manager.shutdown();

      expect(subscriberClient.disconnect).not.toHaveBeenCalled();
    });
  });
});

describe('PubSubWaiter', () => {
  describe('with PubSubManager', () => {
    it('should use pub/sub channel for waiting', async () => {
      const client = createMockRedisClient();
      const subClient = createMockRedisClient();
      client.duplicate.mockReturnValue(subClient);

      const pubSubManager = new PubSubManager({
        clients: [client as any],
        keyPrefix: 'neolock',
        logger: createMockLogger(),
      });

      const waiter = new PubSubWaiter(pubSubManager, 'neolock');

      // Simulate release notification
      setTimeout(() => {
        subClient._simulateMessage('neolock:notify:lock:my-resource', 'released');
      }, 30);

      const result = await waiter.waitForRelease('lock', 'my-resource', 5000, 200);
      expect(result).toBe('notified');

      await pubSubManager.shutdown();
    });

    it('should return timeout when no message arrives', async () => {
      const client = createMockRedisClient();
      const subClient = createMockRedisClient();
      client.duplicate.mockReturnValue(subClient);

      const pubSubManager = new PubSubManager({
        clients: [client as any],
        keyPrefix: 'neolock',
        logger: createMockLogger(),
      });

      const waiter = new PubSubWaiter(pubSubManager, 'neolock');

      const result = await waiter.waitForRelease('sem', 'busy', 50, 200);
      expect(result).toBe('timeout');

      await pubSubManager.shutdown();
    });
  });

  describe('without PubSubManager (fallback)', () => {
    it('should fallback to timer delay', async () => {
      const waiter = new PubSubWaiter(null, 'neolock');

      const start = Date.now();
      const result = await waiter.waitForRelease('lock', 'res', 5000, 50);
      const elapsed = Date.now() - start;

      expect(result).toBe('fallback');
      expect(elapsed).toBeGreaterThanOrEqual(40); // ~50ms delay
    });
  });

  describe('Channel naming', () => {
    it('should build correct channel names', () => {
      const waiter = new PubSubWaiter(null, 'myprefix');

      expect(waiter.getChannel('lock', 'my-resource')).toBe('myprefix:notify:lock:my-resource');
      expect(waiter.getChannel('sem', 'api-calls')).toBe('myprefix:notify:sem:api-calls');
      expect(waiter.getChannel('latch', 'migration')).toBe('myprefix:notify:latch:migration');
    });
  });
});
