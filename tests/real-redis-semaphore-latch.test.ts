/**
 * Real Redis integration tests for Semaphore and CountDownLatch.
 * Connects to a real Redis instance on localhost:6379.
 * Skipped automatically when Redis is unavailable.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import Redis from 'ioredis';
import RedlockToolkit from '../src/index';
import {
  isRedisAvailable,
  flushClients,
  disconnectClients,
  createToolkit,
  sleep,
} from './real-redis-setup';

/**
 * Create Redis clients on DBs 4+ to avoid collision with real-redis.test.ts (DBs 0-2).
 */
function createRedisClientsOnHighDBs(count: number): Redis[] {
  return Array.from({ length: count }, (_, i) =>
    new Redis({
      host: 'localhost',
      port: 6379,
      db: 4 + i, // DBs 4, 5, 6 — separate from real-redis.test.ts
      maxRetriesPerRequest: 3,
    }),
  );
}

const available = await isRedisAvailable();

describe.skipIf(!available)('Real Redis: Semaphore & CountDownLatch', () => {
  let clients: Redis[];
  let toolkit: RedlockToolkit;

  beforeEach(async () => {
    clients = createRedisClientsOnHighDBs(1);
    await flushClients(clients);
    toolkit = createToolkit(clients);
  });

  afterEach(async () => {
    await toolkit.shutdown();
    await disconnectClients(clients);
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // Distributed Semaphore — Real Redis
  // ═══════════════════════════════════════════════════════════════════════════

  describe('Semaphore', () => {

    // ── Basic Lifecycle ───────────────────────────────────────────────────

    it('should acquire a permit and verify ZSET in Redis', async () => {
      const permit = await toolkit.acquireSemaphore('api-calls', {
        maxPermits: 5,
        ttl: 5000,
      });

      expect(permit).toBeDefined();
      expect(permit.resource).toBe('api-calls');
      expect(permit.isValid).toBe(true);

      // Verify ZSET exists in Redis
      const members = await clients[0].zrange('neolock:sem:api-calls', 0, -1, 'WITHSCORES');
      expect(members.length).toBe(2); // [identifier, score]
      expect(members[0]).toBe(permit.identifier);
      const score = Number(members[1]);
      expect(score).toBeGreaterThan(Date.now());

      await permit.release();
    });

    it('should release permit and remove from ZSET', async () => {
      const permit = await toolkit.acquireSemaphore('release-test', {
        maxPermits: 3,
        ttl: 5000,
      });

      const beforeRelease = await clients[0].zcard('neolock:sem:release-test');
      expect(beforeRelease).toBe(1);

      await permit.release();

      const afterRelease = await clients[0].zcard('neolock:sem:release-test');
      expect(afterRelease).toBe(0);
    });

    it('should be idempotent on double release', async () => {
      const permit = await toolkit.acquireSemaphore('double-release', {
        maxPermits: 3,
        ttl: 5000,
      });

      const r1 = await permit.release();
      expect(r1.success).toBe(true);

      const r2 = await permit.release();
      expect(r2.success).toBe(true);
    });

    // ── Multiple Permits ──────────────────────────────────────────────────

    it('should allow multiple concurrent permits up to max', async () => {
      const MAX = 3;
      const permits = [];

      for (let i = 0; i < MAX; i++) {
        const p = await toolkit.acquireSemaphore('multi-permits', {
          maxPermits: MAX,
          ttl: 5000,
        });
        permits.push(p);
      }

      const count = await clients[0].zcard('neolock:sem:multi-permits');
      expect(count).toBe(MAX);

      // Each permit has a unique identifier
      const ids = new Set(permits.map(p => p.identifier));
      expect(ids.size).toBe(MAX);

      // Release all
      for (const p of permits) {
        await p.release();
      }

      const afterRelease = await clients[0].zcard('neolock:sem:multi-permits');
      expect(afterRelease).toBe(0);
    });

    // ── Capacity Limits ───────────────────────────────────────────────────

    it('should reject acquire when at capacity', async () => {
      const MAX = 2;
      const permits = [];

      for (let i = 0; i < MAX; i++) {
        permits.push(
          await toolkit.acquireSemaphore('capacity-test', {
            maxPermits: MAX,
            ttl: 5000,
          }),
        );
      }

      // Next acquire should fail (no retries)
      await expect(
        toolkit.acquireSemaphore('capacity-test', {
          maxPermits: MAX,
          retryCount: 0,
          ttl: 5000,
        }),
      ).rejects.toThrow(/[Ss]emaphore full/);

      for (const p of permits) {
        await p.release();
      }
    });

    it('should allow acquire after a permit is released', async () => {
      const MAX = 1;

      const p1 = await toolkit.acquireSemaphore('release-reacquire', {
        maxPermits: MAX,
        ttl: 5000,
      });

      // Full — should fail
      await expect(
        toolkit.acquireSemaphore('release-reacquire', {
          maxPermits: MAX,
          retryCount: 0,
          ttl: 5000,
        }),
      ).rejects.toThrow();

      await p1.release();

      // Now should succeed
      const p2 = await toolkit.acquireSemaphore('release-reacquire', {
        maxPermits: MAX,
        ttl: 5000,
      });
      expect(p2.isValid).toBe(true);
      await p2.release();
    });

    // ── Expired Permit Cleanup ────────────────────────────────────────────

    it('should clean up expired permits on next acquire', async () => {
      const p1 = await toolkit.acquireSemaphore('expiry-test', {
        maxPermits: 1,
        ttl: 300,
      });

      // Wait for permit to expire
      await sleep(400);

      // Should succeed because expired permits are cleaned up atomically
      const p2 = await toolkit.acquireSemaphore('expiry-test', {
        maxPermits: 1,
        ttl: 5000,
      });
      expect(p2.isValid).toBe(true);

      await p2.release();
    });

    // ── Extension ─────────────────────────────────────────────────────────

    it('should extend a permit TTL in Redis', async () => {
      const permit = await toolkit.acquireSemaphore('extend-test', {
        maxPermits: 3,
        ttl: 2000,
      });

      const beforeExtend = await clients[0].zrangebyscore(
        'neolock:sem:extend-test', '-inf', '+inf', 'WITHSCORES',
      );
      const scoreBefore = Number(beforeExtend[1]);

      await permit.extend(10000);

      const afterExtend = await clients[0].zrangebyscore(
        'neolock:sem:extend-test', '-inf', '+inf', 'WITHSCORES',
      );
      const scoreAfter = Number(afterExtend[1]);

      expect(scoreAfter).toBeGreaterThan(scoreBefore);
      expect(permit.extensions).toBe(1);

      await permit.release();
    });

    it('should throw when extending a released permit', async () => {
      const permit = await toolkit.acquireSemaphore('extend-released', {
        maxPermits: 3,
        ttl: 5000,
      });
      await permit.release();

      await expect(permit.extend(5000)).rejects.toThrow(/[Cc]annot extend released permit/);
    });

    // ── Status ────────────────────────────────────────────────────────────

    it('should return accurate status', async () => {
      const p1 = await toolkit.acquireSemaphore('status-res', { maxPermits: 5, ttl: 5000 });
      const p2 = await toolkit.acquireSemaphore('status-res', { maxPermits: 5, ttl: 5000 });

      const status = await toolkit.getSemaphoreStatus('status-res', 5);
      expect(status.activePermits).toBe(2);
      expect(status.maxPermits).toBe(5);
      expect(status.holders).toHaveLength(2);

      const holderIds = status.holders.map(h => h.identifier);
      expect(holderIds).toContain(p1.identifier);
      expect(holderIds).toContain(p2.identifier);

      await p1.release();
      await p2.release();

      const afterStatus = await toolkit.getSemaphoreStatus('status-res', 5);
      expect(afterStatus.activePermits).toBe(0);
      expect(afterStatus.holders).toHaveLength(0);
    });

    // ── Concurrent Acquire Race ───────────────────────────────────────────

    it('should enforce max permits under concurrent pressure', async () => {
      const MAX = 3;
      const CONTENDERS = 10;

      const results = await Promise.all(
        Array.from({ length: CONTENDERS }, () =>
          toolkit
            .acquireSemaphore('race-test', { maxPermits: MAX, retryCount: 0, ttl: 5000 })
            .then(p => ({ ok: true as const, permit: p }))
            .catch((e: Error) => ({ ok: false as const, error: e })),
        ),
      );

      const wins = results.filter(r => r.ok);
      const losses = results.filter(r => !r.ok);

      expect(wins.length).toBe(MAX);
      expect(losses.length).toBe(CONTENDERS - MAX);

      // Verify ZSET count matches
      const count = await clients[0].zcard('neolock:sem:race-test');
      expect(count).toBe(MAX);

      // Cleanup
      for (const r of wins) {
        if (r.ok) await r.permit.release();
      }
    });

    // ── using() with Auto-Extension ───────────────────────────────────────

    it('should auto-extend permit during long operation via using()', async () => {
      const permit = await toolkit.acquireSemaphore('using-test', {
        maxPermits: 5,
        ttl: 500,
        autoExtendThreshold: 200,
      });

      const result = await permit.using(async (signal) => {
        await sleep(600);
        expect(signal.aborted).toBe(false);
        return 'completed';
      }, { ttl: 500, autoExtendThreshold: 200 });

      expect(result).toBe('completed');
      expect(permit.extensions).toBeGreaterThan(0);
    });

    // ── Shutdown ──────────────────────────────────────────────────────────

    it('should release all permits on shutdown', async () => {
      const shutdownClients = createRedisClientsOnHighDBs(1);
      await flushClients(shutdownClients);
      const tk = createToolkit(shutdownClients);

      await tk.acquireSemaphore('shutdown-s1', { maxPermits: 5, ttl: 10000 });
      await tk.acquireSemaphore('shutdown-s2', { maxPermits: 5, ttl: 10000 });

      expect(tk.activeSemaphores.size).toBe(2);

      await tk.shutdown();

      expect(tk.activeSemaphores.size).toBe(0);

      // Verify ZSET cleaned in Redis
      const count1 = await shutdownClients[0].zcard('neolock:sem:shutdown-s1');
      const count2 = await shutdownClients[0].zcard('neolock:sem:shutdown-s2');
      expect(count1).toBe(0);
      expect(count2).toBe(0);

      await disconnectClients(shutdownClients);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // Distributed CountDownLatch — Real Redis
  // ═══════════════════════════════════════════════════════════════════════════

  describe('CountDownLatch', () => {

    // ── Creation ──────────────────────────────────────────────────────────

    it('should create a latch and set keys in Redis', async () => {
      const latch = await toolkit.createCountDownLatch('migration', {
        count: 3,
        ttl: 60000,
      });

      expect(latch.name).toBe('migration');
      expect(latch.targetCount).toBe(3);

      // Verify Redis keys
      const countVal = await clients[0].get('neolock:latch:migration');
      expect(countVal).toBe('3');

      const targetVal = await clients[0].get('neolock:latch:migration:target');
      expect(targetVal).toBe('3');

      const pttl = await clients[0].pttl('neolock:latch:migration');
      expect(pttl).toBeGreaterThan(0);
      expect(pttl).toBeLessThanOrEqual(60000);
    });

    it('should throw when creating duplicate latch', async () => {
      await toolkit.createCountDownLatch('dup-test', { count: 2, ttl: 60000 });

      await expect(
        toolkit.createCountDownLatch('dup-test', { count: 5, ttl: 60000 }),
      ).rejects.toThrow(/already exists/);
    });

    // ── CountDown ─────────────────────────────────────────────────────────

    it('should decrement count in Redis on countDown', async () => {
      const latch = await toolkit.createCountDownLatch('decrement', {
        count: 3,
        ttl: 60000,
      });

      const r1 = await latch.countDown('event-1');
      expect(r1.success).toBe(true);
      expect(r1.remainingCount).toBe(2);
      expect(r1.justCompleted).toBe(false);

      const currentVal = await clients[0].get('neolock:latch:decrement');
      expect(currentVal).toBe('2');

      const r2 = await latch.countDown('event-2');
      expect(r2.remainingCount).toBe(1);

      const r3 = await latch.countDown('event-3');
      expect(r3.remainingCount).toBe(0);
      expect(r3.justCompleted).toBe(true);
    });

    it('should be idempotent for same eventId', async () => {
      const latch = await toolkit.createCountDownLatch('idempotent', {
        count: 3,
        ttl: 60000,
      });

      await latch.countDown('same-event');
      const countAfterFirst = await clients[0].get('neolock:latch:idempotent');
      expect(countAfterFirst).toBe('2');

      // Same eventId again — should NOT decrement
      await latch.countDown('same-event');
      const countAfterDuplicate = await clients[0].get('neolock:latch:idempotent');
      expect(countAfterDuplicate).toBe('2');
    });

    it('should store event IDs for audit trail', async () => {
      const latch = await toolkit.createCountDownLatch('audit', {
        count: 3,
        ttl: 60000,
      });

      await latch.countDown('worker-A');
      await latch.countDown('worker-B');

      const events = await clients[0].smembers('neolock:latch:audit:events');
      expect(events).toContain('worker-A');
      expect(events).toContain('worker-B');
      expect(events).toHaveLength(2);
    });

    // ── Status ────────────────────────────────────────────────────────────

    it('should return accurate status', async () => {
      const latch = await toolkit.createCountDownLatch('status-test', {
        count: 5,
        ttl: 60000,
      });

      await latch.countDown('e1');
      await latch.countDown('e2');

      const status = await latch.getStatus();
      expect(status.exists).toBe(true);
      expect(status.remainingCount).toBe(3);
      expect(status.targetCount).toBe(5);
      expect(status.completed).toBe(false);
      expect(status.ttl).toBeGreaterThan(0);
    });

    it('should report completed status when count reaches 0', async () => {
      const latch = await toolkit.createCountDownLatch('complete-status', {
        count: 1,
        ttl: 60000,
      });

      await latch.countDown('done');

      const status = await latch.getStatus();
      expect(status.exists).toBe(true);
      expect(status.completed).toBe(true);
      expect(status.remainingCount).toBe(0);
    });

    it('should report non-existent latch', async () => {
      const status = await toolkit.getCountDownLatchStatus('nonexistent');
      expect(status.exists).toBe(false);
    });

    // ── Await with Polling ────────────────────────────────────────────────

    it('should resolve await immediately if already completed', async () => {
      const latch = await toolkit.createCountDownLatch('await-done', {
        count: 1,
        ttl: 60000,
        pollInterval: 20,
      });

      await latch.countDown('finished');

      const start = Date.now();
      const completed = await latch.await(5000);
      const elapsed = Date.now() - start;

      expect(completed).toBe(true);
      expect(elapsed).toBeLessThan(200); // Should be near-instant
    });

    it('should poll until latch completes', async () => {
      const latch = await toolkit.createCountDownLatch('await-poll', {
        count: 2,
        ttl: 60000,
        pollInterval: 50,
      });

      // Background: count down after a delay (fire-and-forget with catch)
      const bgCountDown = (async () => {
        await sleep(150);
        await latch.countDown('e1');
        await latch.countDown('e2');
      })().catch(() => {});

      const completed = await latch.await(5000);
      expect(completed).toBe(true);

      await bgCountDown;
    });

    it('should throw LatchTimeoutError on timeout', async () => {
      const latch = await toolkit.createCountDownLatch('await-timeout', {
        count: 10,
        ttl: 60000,
        pollInterval: 30,
      });

      await expect(latch.await(200)).rejects.toThrow(/timed out/);
    });

    // ── Multi-Worker Simulation ───────────────────────────────────────────

    it('should coordinate multiple workers counting down', async () => {
      const WORKERS = 5;
      const latch = await toolkit.createCountDownLatch('multi-worker', {
        count: WORKERS,
        ttl: 60000,
        pollInterval: 30,
      });

      // Simulate N workers counting down concurrently
      const workerResults = await Promise.all(
        Array.from({ length: WORKERS }, (_, i) =>
          latch.countDown(`worker-${i}`),
        ),
      );

      // All should succeed
      for (const r of workerResults) {
        expect(r.success).toBe(true);
      }

      // Exactly one should report justCompleted
      const completers = workerResults.filter(r => r.justCompleted);
      expect(completers.length).toBe(1);

      // Final count should be 0
      const status = await latch.getStatus();
      expect(status.completed).toBe(true);
    });

    it('should work in a producer-consumer pattern', async () => {
      const latch = await toolkit.createCountDownLatch('producer-consumer', {
        count: 3,
        ttl: 60000,
        pollInterval: 30,
      });

      // Consumer waits for all 3 producers
      const consumerPromise = latch.await(5000);

      // Producers count down with small delays
      await sleep(50);
      await latch.countDown('producer-1');
      await sleep(30);
      await latch.countDown('producer-2');
      await sleep(30);
      await latch.countDown('producer-3');

      const completed = await consumerPromise;
      expect(completed).toBe(true);
    });

    // ── TTL Expiration ────────────────────────────────────────────────────

    it('should expire latch keys after TTL', async () => {
      await toolkit.createCountDownLatch('expire-test', {
        count: 5,
        ttl: 300,
      });

      // Verify keys exist
      expect(await clients[0].exists('neolock:latch:expire-test')).toBe(1);

      await sleep(400);

      // Keys should have expired
      expect(await clients[0].exists('neolock:latch:expire-test')).toBe(0);
      expect(await clients[0].exists('neolock:latch:expire-test:target')).toBe(0);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // 3-Node Quorum Tests
  // ═══════════════════════════════════════════════════════════════════════════

  describe('Quorum (3-node)', () => {
    let threeClients: Redis[];
    let quorumToolkit: RedlockToolkit;

    beforeEach(async () => {
      threeClients = createRedisClientsOnHighDBs(3);
      await flushClients(threeClients);
      quorumToolkit = createToolkit(threeClients);
    });

    afterEach(async () => {
      await quorumToolkit.shutdown();
      await disconnectClients(threeClients);
    });

    it('should acquire semaphore with quorum across 3 nodes', async () => {
      const permit = await quorumToolkit.acquireSemaphore('quorum-sem', {
        maxPermits: 5,
        ttl: 5000,
      });

      // Verify ZSET present on all 3 nodes
      for (const client of threeClients) {
        const members = await client.zrange('neolock:sem:quorum-sem', 0, -1);
        expect(members).toContain(permit.identifier);
      }

      await permit.release();

      // Verify cleanup on all nodes
      for (const client of threeClients) {
        const count = await client.zcard('neolock:sem:quorum-sem');
        expect(count).toBe(0);
      }
    });

    it('should create latch with quorum across 3 nodes', async () => {
      const latch = await quorumToolkit.createCountDownLatch('quorum-latch', {
        count: 3,
        ttl: 60000,
      });

      // Verify keys on at least 2 of 3 nodes (quorum)
      let nodesWithKey = 0;
      for (const client of threeClients) {
        const val = await client.get('neolock:latch:quorum-latch');
        if (val === '3') nodesWithKey++;
      }
      expect(nodesWithKey).toBeGreaterThanOrEqual(2);

      // CountDown and verify
      await latch.countDown('q-event-1');
      await latch.countDown('q-event-2');
      await latch.countDown('q-event-3');

      const status = await latch.getStatus();
      expect(status.completed).toBe(true);
    });
  });
});
