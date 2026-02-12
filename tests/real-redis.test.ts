/**
 * Real Redis integration tests.
 * Connects to a real Redis instance on localhost:6379.
 * Skipped automatically when Redis is unavailable.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import Redis from 'ioredis';
import RedlockToolkit from '../src/index';
import { SCRIPTS, SCRIPT_HASHES } from '../src/utils/scripts';
import {
  isRedisAvailable,
  createRedisClients,
  createUnreachableClient,
  flushClients,
  disconnectClients,
  createToolkit,
  sleep,
} from './real-redis-setup';

// ── Redis availability check (top-level await) ──────────────────────────────

const available = await isRedisAvailable();

// ── Main test suite ─────────────────────────────────────────────────────────

describe.skipIf(!available)('Real Redis Integration Tests', () => {
  let clients: Redis[];
  let toolkit: RedlockToolkit;

  beforeEach(async () => {
    clients = createRedisClients(1);
    await flushClients(clients);
    toolkit = createToolkit(clients);
  });

  afterEach(async () => {
    await toolkit.shutdown();
    await disconnectClients(clients);
  });

  // ── 1. Connection & Script Preload ──────────────────────────────────────

  describe('Connection & Script Preload', () => {
    it('should preload Lua scripts on real Redis', async () => {
      const hashes = Object.values(SCRIPT_HASHES);
      // Poll until scripts are loaded (preloadScripts is fire-and-forget)
      const deadline = Date.now() + 2000;
      let loaded = false;
      while (Date.now() < deadline) {
        const results = (await clients[0].script('EXISTS', ...hashes)) as number[];
        if (results.every((r) => r === 1)) { loaded = true; break; }
        await sleep(50);
      }
      expect(loaded).toBe(true);
    });
  });

  // ── 2. Basic Lock Lifecycle ─────────────────────────────────────────────

  describe('Basic Lock Lifecycle', () => {
    it('should acquire → verify in Redis → release → verify deleted', async () => {
      const lock = await toolkit.acquire('lifecycle-res');

      const value = await clients[0].get('neolock:lifecycle-res');
      expect(value).toBe(lock.identifier);

      const pttl = await clients[0].pttl('neolock:lifecycle-res');
      expect(pttl).toBeGreaterThan(0);
      expect(pttl).toBeLessThanOrEqual(2000 + 200);

      await lock.release();

      expect(await clients[0].get('neolock:lifecycle-res')).toBeNull();
    });
  });

  // ── 3. Lock Mutual Exclusion ────────────────────────────────────────────

  describe('Lock Mutual Exclusion', () => {
    it('should block second acquire on same resource', async () => {
      const lockA = await toolkit.acquire('exclusive-res');

      await expect(
        toolkit.acquire('exclusive-res', { retryCount: 0 }),
      ).rejects.toThrow(/consensus|quorum/i);

      await lockA.release();

      const lockB = await toolkit.acquire('exclusive-res');
      expect(lockB.isValid).toBe(true);
      await lockB.release();
    });
  });

  // ── 4. Lock TTL & Expiration ────────────────────────────────────────────

  describe('Lock TTL & Expiration', () => {
    it('should expire key in Redis after TTL', async () => {
      const lock = await toolkit.acquire('ttl-res', { ttl: 300 });

      await sleep(500);

      expect(await clients[0].get('neolock:ttl-res')).toBeNull();
      expect(lock.isExpired).toBe(true);
    });
  });

  // ── 5. Lock Extension ──────────────────────────────────────────────────

  describe('Lock Extension', () => {
    it('should refresh PTTL after extend()', async () => {
      const lock = await toolkit.acquire('extend-res', { ttl: 2000 });

      await lock.extend(5000);

      const pttl = await clients[0].pttl('neolock:extend-res');
      expect(pttl).toBeGreaterThan(2000);
      expect(pttl).toBeLessThanOrEqual(5000 + 200);

      await lock.release();
    });

    it('should throw when extending a released lock', async () => {
      const lock = await toolkit.acquire('extend-released', { ttl: 2000 });
      await lock.release();

      await expect(lock.extend(5000)).rejects.toThrow(/released/i);
    });
  });

  // ── 6. Auto-Extension (using()) ────────────────────────────────────────

  describe('Auto-Extension via using()', () => {
    it('should keep lock alive and verify PTTL refreshed', async () => {
      let midPttl = 0;

      const result = await toolkit.using(
        'auto-ext-res',
        async (signal) => {
          expect(signal.aborted).toBe(false);
          // Sleep past the auto-extension trigger (300 - 100 = 200ms)
          await sleep(250);
          // PTTL must have been refreshed — not decayed to near zero
          midPttl = await clients[0].pttl('neolock:auto-ext-res');
          await sleep(250);
          expect(signal.aborted).toBe(false);
          return 'done';
        },
        { ttl: 300, autoExtendThreshold: 100 },
      );

      expect(result).toBe('done');
      expect(midPttl).toBeGreaterThan(100); // proves extension happened

      expect(await clients[0].get('neolock:auto-ext-res')).toBeNull();
    });
  });

  // ── 7. Force Release ───────────────────────────────────────────────────

  describe('Force Release', () => {
    it('should delete key and prevent further extension', async () => {
      const lock = await toolkit.acquire('force-res', { ttl: 10000 });

      const result = await toolkit.forceRelease('force-res');
      expect(result.releasedCount).toBeGreaterThan(0);

      expect(await clients[0].get('neolock:force-res')).toBeNull();

      await expect(lock.extend(5000)).rejects.toThrow(/extension|consensus/i);
    });
  });

  // ── 8. Lock Status ─────────────────────────────────────────────────────

  describe('Lock Status', () => {
    it('should report locked / unlocked correctly', async () => {
      const lock = await toolkit.acquire('status-res', { ttl: 5000 });

      const [locked] = await toolkit.getStatus('status-res');
      expect(locked.locked).toBe(true);
      expect(locked.holder).toBe(lock.identifier);
      expect(locked.ttl).toBeGreaterThan(0);

      await lock.release();

      const [unlocked] = await toolkit.getStatus('status-res');
      expect(unlocked.locked).toBe(false);
      expect(unlocked.holder).toBeUndefined();
    });
  });

  // ── 9. Cleanup ─────────────────────────────────────────────────────────

  describe('Cleanup', () => {
    it('should delete persistent keys and keep TTL keys', async () => {
      await clients[0].set('neolock:persistent', 'stale');
      await clients[0].set('neolock:temporary', 'active', 'PX', 60000);

      const cleaned = await toolkit.cleanup();

      expect(await clients[0].get('neolock:persistent')).toBeNull();
      expect(await clients[0].get('neolock:temporary')).toBe('active');
      expect(cleaned).toBeGreaterThanOrEqual(1);
    });
  });

  // ── 10. Consensus (3-client quorum) ────────────────────────────────────

  describe('Consensus (3-client quorum)', () => {
    let threeClients: Redis[];
    let consensusToolkit: RedlockToolkit;

    beforeEach(async () => {
      threeClients = createRedisClients(3);
      await flushClients(threeClients);
      consensusToolkit = createToolkit(threeClients);
    });

    afterEach(async () => {
      await consensusToolkit.shutdown();
      await disconnectClients(threeClients);
    });

    it('should set key on all 3 DBs and delete on release', async () => {
      const lock = await consensusToolkit.acquire('consensus-res', { ttl: 5000 });

      for (const client of threeClients) {
        expect(await client.get('neolock:consensus-res')).toBe(lock.identifier);
      }

      await lock.release();

      for (const client of threeClients) {
        expect(await client.get('neolock:consensus-res')).toBeNull();
      }
    });
  });

  // ── 11. Optimistic Locking ─────────────────────────────────────────────

  describe('Optimistic Locking', () => {
    const identifier = 'opt-test-id';

    it('should create version key on acquireOptimistic', async () => {
      const result = await toolkit.acquireOptimistic('opt-res', { identifier });
      expect(result.success).toBe(true);
      expect(result.currentVersion).toBe(1);
      expect(await clients[0].get('neolock:opt-res:version')).toBe('1');
    });

    it('should increment version on updateOptimistic', async () => {
      await toolkit.acquireOptimistic('opt-upd', { identifier });

      const updated = await toolkit.updateOptimistic('opt-upd', 1, { identifier });
      expect(updated.success).toBe(true);
      expect(updated.currentVersion).toBe(2);
    });

    it('should detect version conflict', async () => {
      await toolkit.acquireOptimistic('opt-conflict', { identifier });
      await toolkit.updateOptimistic('opt-conflict', 1, { identifier });

      // Stale version → conflict result OR consensus wrapping the conflict
      try {
        const conflict = await toolkit.updateOptimistic('opt-conflict', 1, { identifier });
        expect(conflict.success).toBe(false);
        expect(conflict.conflict).toBe(true);
      } catch (err) {
        expect((err as Error).message).toMatch(/consensus|quorum|conflict/i);
      }

      // Redis must have version 2 regardless
      expect(await clients[0].get('neolock:opt-conflict:version')).toBe('2');
    });
  });

  // ── 12. Concurrent Lock Contention ─────────────────────────────────────

  describe('Concurrent Lock Contention', () => {
    it('should allow exactly 1 of 10 concurrent acquires', async () => {
      const results = await Promise.all(
        Array.from({ length: 10 }, () =>
          toolkit
            .acquire('contended-res', { retryCount: 0 })
            .then((lock) => ({ ok: true as const, lock }))
            .catch((e: Error) => ({ ok: false as const, error: e })),
        ),
      );

      const wins = results.filter((r) => r.ok);
      const losses = results.filter((r) => !r.ok);

      expect(wins).toHaveLength(1);
      expect(losses).toHaveLength(9);

      // Winner actually owns the Redis key
      if (wins[0].ok) {
        expect(await clients[0].get('neolock:contended-res')).toBe(wins[0].lock.identifier);
        await wins[0].lock.release();
      }

      expect(await clients[0].get('neolock:contended-res')).toBeNull();
    });
  });

  // ── 13. Metrics Integration ────────────────────────────────────────────

  describe('Metrics Integration', () => {
    it('should track acquire / release / failure counts', async () => {
      toolkit.resetMetrics();

      const lock = await toolkit.acquire('metrics-res', { ttl: 5000 });
      await lock.release();

      const blocker = await toolkit.acquire('metrics-blocked', { ttl: 5000 });
      await expect(
        toolkit.acquire('metrics-blocked', { retryCount: 0 }),
      ).rejects.toThrow();
      await blocker.release();

      const metrics = toolkit.getMetrics();
      expect(metrics.locksAcquired).toBe(2);
      expect(metrics.locksReleased).toBe(2);
      expect(metrics.failedAcquisitions).toBe(1);
    });

    it('should export Prometheus-format text', async () => {
      toolkit.resetMetrics();
      const lock = await toolkit.acquire('prom-res', { ttl: 5000 });
      await lock.release();

      const text = toolkit.exportMetrics();
      expect(text).toContain('neolock_locks_acquired_total');
    });
  });

  // ── 14. Circuit Breaker Integration ────────────────────────────────────

  describe('Circuit Breaker Integration', () => {
    it('should remain closed after successful operations', async () => {
      const lock = await toolkit.acquire('cb-res', { ttl: 5000 });
      await lock.release();

      const cbMetrics = toolkit.getCircuitBreakerMetrics();
      expect(cbMetrics.state).toBe('closed');
    });
  });

  // ── 15. High Concurrency ──────────────────────────────────────────────

  describe('High Concurrency', () => {
    it('should acquire 50 locks on different resources simultaneously', async () => {
      const N = 50;
      const locks = await Promise.all(
        Array.from({ length: N }, (_, i) =>
          toolkit.acquire(`hc-diff-${i}`, { ttl: 5000, retryCount: 0 }),
        ),
      );

      for (let i = 0; i < N; i++) {
        expect(await clients[0].get(`neolock:hc-diff-${i}`)).toBe(locks[i].identifier);
      }

      await Promise.all(locks.map((l) => l.release()));

      for (let i = 0; i < N; i++) {
        expect(await clients[0].get(`neolock:hc-diff-${i}`)).toBeNull();
      }
    });

    it('should serialize 50 concurrent acquires on the same resource', async () => {
      const N = 50;
      const results = await Promise.all(
        Array.from({ length: N }, () =>
          toolkit
            .acquire('hc-same-res', { retryCount: 0 })
            .then((lock) => ({ ok: true as const, lock }))
            .catch(() => ({ ok: false as const })),
        ),
      );

      const winners = results.filter((r) => r.ok);
      expect(winners).toHaveLength(1);

      if (winners[0].ok) await winners[0].lock.release();
      expect(await clients[0].get('neolock:hc-same-res')).toBeNull();
    });

    it('should let all contenders eventually acquire with retries', async () => {
      const N = 10;
      const acquired: string[] = [];

      const worker = async (id: number) => {
        const lock = await toolkit.acquire('hc-fair-res', {
          ttl: 500,
          retryCount: 50,
          retryDelay: 60,
          retryJitter: 30,
        });
        acquired.push(`w${id}`);
        await sleep(20);
        await lock.release();
      };

      await Promise.all(Array.from({ length: N }, (_, i) => worker(i)));

      expect(acquired).toHaveLength(N);
      expect(new Set(acquired).size).toBe(N);
    });

    it('should handle rapid acquire-release churn without leaking keys', async () => {
      const CYCLES = 100;
      for (let i = 0; i < CYCLES; i++) {
        const lock = await toolkit.acquire('hc-churn', { ttl: 2000, retryCount: 3 });
        await lock.release();
        // Periodic mid-cycle verification
        if (i % 25 === 0) {
          expect(await clients[0].get('neolock:hc-churn')).toBeNull();
          expect(toolkit.getActiveLocks()).toHaveLength(0);
        }
      }
      expect(await clients[0].get('neolock:hc-churn')).toBeNull();
      expect(toolkit.getActiveLocks()).toHaveLength(0);
    });

    it('should run 20 concurrent using() routines on different resources', async () => {
      const N = 20;
      const results = await Promise.all(
        Array.from({ length: N }, (_, i) =>
          toolkit.using(
            `hc-using-${i}`,
            async (signal) => {
              await sleep(400);
              expect(signal.aborted).toBe(false);
              return `result-${i}`;
            },
            { ttl: 300, autoExtendThreshold: 100 },
          ),
        ),
      );

      expect(results).toHaveLength(N);
      results.forEach((r, i) => expect(r).toBe(`result-${i}`));
    });

    it('should extend many locks concurrently without conflicts', async () => {
      const N = 20;
      const locks = await Promise.all(
        Array.from({ length: N }, (_, i) =>
          toolkit.acquire(`hc-ext-${i}`, { ttl: 2000 }),
        ),
      );

      await Promise.all(locks.map((l) => l.extend(5000)));

      for (let i = 0; i < N; i++) {
        const pttl = await clients[0].pttl(`neolock:hc-ext-${i}`);
        expect(pttl).toBeGreaterThan(2000);
      }

      await Promise.all(locks.map((l) => l.release()));
    });

    it('should handle mixed concurrent operations with result validation', async () => {
      const locks = await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          toolkit.acquire(`hc-mixed-${i}`, { ttl: 5000 }),
        ),
      );

      // Validate status payloads
      const statusResults = await Promise.all(
        locks.map((_, i) => toolkit.getStatus(`hc-mixed-${i}`)),
      );
      statusResults.forEach(([s], i) => {
        expect(s.locked).toBe(true);
        expect(s.holder).toBe(locks[i].identifier);
      });

      // Extend all and verify
      await Promise.all(locks.map((l) => l.extend(5000)));

      await Promise.all(locks.map((l) => l.release()));

      for (let i = 0; i < 10; i++) {
        expect(await clients[0].get(`neolock:hc-mixed-${i}`)).toBeNull();
      }
    });

    it('should maintain consensus under concurrent load with 3 nodes', async () => {
      const threeClients = createRedisClients(3);
      let tk: RedlockToolkit | undefined;
      try {
        await flushClients(threeClients);
        tk = createToolkit(threeClients);

        const N = 20;
        const locks = await Promise.all(
          Array.from({ length: N }, (_, i) =>
            tk!.acquire(`hc-quorum-${i}`, { ttl: 5000, retryCount: 0 }),
          ),
        );

        for (let i = 0; i < N; i++) {
          for (const c of threeClients) {
            expect(await c.get(`neolock:hc-quorum-${i}`)).toBe(locks[i].identifier);
          }
        }

        await Promise.all(locks.map((l) => l.release()));

        for (let i = 0; i < N; i++) {
          for (const c of threeClients) {
            expect(await c.get(`neolock:hc-quorum-${i}`)).toBeNull();
          }
        }
      } finally {
        if (tk) await tk.shutdown().catch(() => {});
        await disconnectClients(threeClients);
      }
    });
  });

  // ── 16. Stale Identifier Safety ────────────────────────────────────────

  describe('Stale Identifier Safety', () => {
    it('should not delete another holders lock on stale release', async () => {
      // Client A acquires with short TTL
      const lockA = await toolkit.acquire('stale-res', { ttl: 300 });
      const idA = lockA.identifier;

      // Wait for lock A to expire in Redis
      await sleep(500);
      expect(lockA.isExpired).toBe(true);

      // Client B acquires the now-free resource
      const lockB = await toolkit.acquire('stale-res', { ttl: 5000 });
      const idB = lockB.identifier;
      expect(idA).not.toBe(idB);

      // Client A tries to release its expired lock — must NOT delete B's key
      await lockA.release();

      const holder = await clients[0].get('neolock:stale-res');
      expect(holder).toBe(idB); // B's lock survives

      await lockB.release();
      expect(await clients[0].get('neolock:stale-res')).toBeNull();
    });
  });

  // ── 17. Multi-Resource Locking ─────────────────────────────────────────

  describe('Multi-Resource Locking', () => {
    it('should atomically lock multiple resources', async () => {
      const lock = await toolkit.acquire(['res-a', 'res-b', 'res-c'], { ttl: 5000 });

      for (const r of ['res-a', 'res-b', 'res-c']) {
        expect(await clients[0].get(`neolock:${r}`)).toBe(lock.identifier);
      }

      // Partial overlap must fail
      await expect(
        toolkit.acquire(['res-b', 'res-d'], { retryCount: 0 }),
      ).rejects.toThrow();

      await lock.release();

      for (const r of ['res-a', 'res-b', 'res-c']) {
        expect(await clients[0].get(`neolock:${r}`)).toBeNull();
      }
    });
  });

  // ── 18. Partial Quorum Failure ─────────────────────────────────────────

  describe('Partial Quorum Failure', () => {
    it('should acquire lock when 1 of 3 nodes is unreachable', async () => {
      const healthy = createRedisClients(2);
      const bad = createUnreachableClient();
      const allClients = [...healthy, bad];
      let tk: RedlockToolkit | undefined;

      try {
        await flushClients(healthy);
        tk = createToolkit(allClients);

        const lock = await tk.acquire('quorum-partial', { ttl: 5000 });

        // Key present on the 2 healthy nodes
        for (const c of healthy) {
          expect(await c.get('neolock:quorum-partial')).toBe(lock.identifier);
        }

        await lock.release();

        for (const c of healthy) {
          expect(await c.get('neolock:quorum-partial')).toBeNull();
        }
      } finally {
        if (tk) await tk.shutdown().catch(() => {});
        await disconnectClients(healthy);
        bad.disconnect();
      }
    });
  });

  // ── 19. Double Release Idempotency ─────────────────────────────────────

  describe('Double Release Idempotency', () => {
    it('should succeed on second release without error', async () => {
      const lock = await toolkit.acquire('double-rel', { ttl: 5000 });
      await lock.release();

      // Second release must not throw
      const result = await lock.release();
      expect(result.success).toBe(true);
      expect(await clients[0].get('neolock:double-rel')).toBeNull();
    });
  });

  // ── 20. Expired vs Released Lock ───────────────────────────────────────

  describe('Expired vs Released Lock', () => {
    it('should throw LockExpiredError when extending expired lock', async () => {
      const lock = await toolkit.acquire('expired-ext', { ttl: 200 });
      await sleep(400);
      expect(lock.isExpired).toBe(true);

      await expect(lock.extend(5000)).rejects.toThrow(/expired/i);
    });

    it('should throw about released lock when extending released lock', async () => {
      const lock = await toolkit.acquire('released-ext', { ttl: 5000 });
      await lock.release();

      await expect(lock.extend(5000)).rejects.toThrow(/released/i);
    });
  });

  // ── 21. Event Emission ─────────────────────────────────────────────────

  describe('Event Emission', () => {
    it('should emit lock:acquired and lock:released', async () => {
      const events: string[] = [];
      toolkit.on('lock:acquired', () => events.push('acquired'));
      toolkit.on('lock:released', () => events.push('released'));

      const lock = await toolkit.acquire('event-res', { ttl: 5000 });
      await lock.release();

      expect(events).toContain('acquired');
      expect(events).toContain('released');
    });

    it('should emit lock:failed on rejected acquire', async () => {
      const errors: Error[] = [];
      toolkit.on('lock:failed', (_resources: string[], err: Error) => errors.push(err));

      const blocker = await toolkit.acquire('event-fail', { ttl: 5000 });
      await toolkit.acquire('event-fail', { retryCount: 0 }).catch(() => {});
      await blocker.release();

      expect(errors.length).toBeGreaterThanOrEqual(1);
    });
  });

  // ── 22. Signal Abort on Extension Failure ──────────────────────────────

  describe('Signal Abort', () => {
    it('should abort signal when lock is force-released during using()', async () => {
      let wasAborted = false;

      try {
        await toolkit.using(
          'abort-res',
          async (signal) => {
            // Force-release the key externally — next auto-extension will fail
            await sleep(50);
            await toolkit.forceRelease('abort-res');

            // Wait for auto-extension to fire and fail
            await sleep(400);

            wasAborted = signal.aborted;
            if (signal.aborted) throw signal.error;
            return 'unreachable';
          },
          { ttl: 400, autoExtendThreshold: 150 },
        );
      } catch {
        // Expected — routine throws after signal abort
      }

      expect(wasAborted).toBe(true);
    });
  });

  // ── 23. Long-Running Auto-Extension Stress ─────────────────────────────

  describe('Long-Running Auto-Extension', () => {
    it('should sustain auto-extension over 3 seconds with many cycles', async () => {
      const result = await toolkit.using(
        'long-ext-res',
        async (signal) => {
          const start = Date.now();
          let checks = 0;

          while (Date.now() - start < 3000) {
            await sleep(100);
            expect(signal.aborted).toBe(false);
            const pttl = await clients[0].pttl('neolock:long-ext-res');
            expect(pttl).toBeGreaterThan(0);
            checks++;
          }

          return checks;
        },
        { ttl: 300, autoExtendThreshold: 100 },
      );

      // At least 25 checks over 3 seconds (100ms each)
      expect(result).toBeGreaterThanOrEqual(25);
    });
  });

  // ── 24. Concurrent using() with Contention ─────────────────────────────

  describe('Concurrent using() with Contention', () => {
    it('should serialize using() calls on the same resource', async () => {
      const N = 5;
      const order: number[] = [];

      const worker = (id: number) =>
        toolkit.using(
          'serial-using',
          async () => {
            order.push(id);
            await sleep(50);
            return id;
          },
          {
            ttl: 500,
            autoExtendThreshold: 150,
            retryCount: 30,
            retryDelay: 100,
            retryJitter: 50,
          },
        );

      const results = await Promise.all(
        Array.from({ length: N }, (_, i) => worker(i)),
      );

      expect(results.sort()).toEqual([0, 1, 2, 3, 4]);
      expect(new Set(order).size).toBe(N);
    });
  });

  // ── 25. Shutdown Semantics ─────────────────────────────────────────────

  describe('Shutdown', () => {
    it('should release all active locks on shutdown', async () => {
      const shutdownClients = createRedisClients(1);
      await flushClients(shutdownClients);
      const tk = createToolkit(shutdownClients);

      const lock1 = await tk.acquire('shutdown-a', { ttl: 10000 });
      const lock2 = await tk.acquire('shutdown-b', { ttl: 10000 });
      expect(tk.getActiveLocks()).toHaveLength(2);

      await tk.shutdown();

      expect(lock1.released).toBe(true);
      expect(lock2.released).toBe(true);
      expect(await shutdownClients[0].get('neolock:shutdown-a')).toBeNull();
      expect(await shutdownClients[0].get('neolock:shutdown-b')).toBeNull();

      await disconnectClients(shutdownClients);
    });
  });
});
