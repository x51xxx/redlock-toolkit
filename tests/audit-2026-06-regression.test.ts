/**
 * Regression tests for the June 2026 audit fixes (consolidated audit + Codex review).
 * Each describe block maps to a numbered finding from the consolidated critical list.
 */

import { describe, it, expect, vi } from 'vitest';
import RedlockToolkit from '../src/index';
import { CircuitBreakerOpenError } from '../src/core/errors';
import { InternalRedlock } from '../src/algorithms/redlock';
import OptimisticRedlock, {
  ROLLBACK_WRITE_SCRIPT,
  OPTIMISTIC_WRITE_SCRIPT,
  COMPARE_AND_SWAP_SCRIPT,
} from '../src/algorithms/optimistic-redlock';
import { CircuitBreaker } from '../src/patterns/circuit-breaker';
import { CacheManager } from '../src/managers/cache';
import { EXTEND_SCRIPT, RELEASE_SCRIPT } from '../src/utils/scripts';
import { createMockRedisClients, createTestRedlockToolkitConfig, sleep } from './setup';

// ──────────────────────────────────────────────────────────────────
// FIX #1 — failed extension must not force-delete another owner's lock
// ──────────────────────────────────────────────────────────────────
describe('FIX #1: extend() failure releases with ownership check, never DEL', () => {
  it('uses the ownership-checked release script and never calls DEL', async () => {
    const clients = createMockRedisClients(3);
    for (const client of clients) {
      client.eval.mockImplementation(async (script: string) => {
        if (script === EXTEND_SCRIPT) return 0; // lock stolen / expired on Redis side
        if (script === RELEASE_SCRIPT) return 0; // we no longer own it
        return 1; // ACQUIRE_SCRIPT: single resource acquired
      });
    }

    const redlock = new InternalRedlock(clients as any);
    const lock = await redlock.acquire('audit:extend-res', 1000);

    await expect(redlock.extend(lock, 1000)).rejects.toThrow(/quorum/);

    for (const client of clients) {
      // The old code called client.del(...) unconditionally, destroying a
      // lock that may belong to the new owner.
      expect(client.del).not.toHaveBeenCalled();
      const releaseCalls = client.eval.mock.calls.filter(
        (c: unknown[]) => c[0] === RELEASE_SCRIPT,
      );
      expect(releaseCalls.length).toBeGreaterThanOrEqual(1);
    }
  });
});

// ──────────────────────────────────────────────────────────────────
// FIX #2 — release of a stolen/expired lock must not report success
// ──────────────────────────────────────────────────────────────────
describe('FIX #2: releaseLockInternal honest success evaluation', () => {
  it('returns success: false when all nodes report the lock was not held', async () => {
    const clients = createMockRedisClients(3);
    const toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(clients));
    const lock = await toolkit.acquire('audit:stolen-res');

    // Simulate theft: the release script returns 0 (nothing deleted) everywhere.
    for (const client of clients) client.evalsha.mockResolvedValue(0);

    const result = await lock.release();
    expect(result.success).toBe(false);
    expect(result.releasedCount).toBe(0);
  });

  it('reports success and the real per-node released count when nodes confirm', async () => {
    const clients = createMockRedisClients(3);
    const toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(clients));
    const lock = await toolkit.acquire('audit:ok-res');

    const result = await lock.release();
    expect(result.success).toBe(true);
    // releasedCount is the number of confirming nodes (>= quorum), not a
    // hardcoded clients.length.
    expect(result.releasedCount).toBeGreaterThanOrEqual(2);
    expect(result.releasedCount).toBeLessThanOrEqual(3);
  });
});

// ──────────────────────────────────────────────────────────────────
// FIX #3 — hybrid/optimistic Lock must carry the identifier written to Redis
// ──────────────────────────────────────────────────────────────────
describe('FIX #3: hybrid optimistic lock identifier matches Redis state', () => {
  it('lock.identifier equals the identifier passed to the acquire script', async () => {
    const clients = createMockRedisClients(3);
    // Optimistic acquire script returns [status=1, version, count]
    for (const client of clients) client.evalsha.mockResolvedValue([1, 1, 1]);
    const toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(clients));

    const lock = await toolkit.acquireHybrid('audit:hybrid-res', {
      primaryStrategy: 'optimistic',
    });

    // evalsha args: (hash, numkeys, key, identifier, ttl, ...)
    const scriptCall = clients[0].evalsha.mock.calls.find((c: unknown[]) =>
      String(c[2]).includes('audit:hybrid-res'),
    );
    expect(scriptCall).toBeDefined();
    expect(lock.identifier).toBe(scriptCall![3]);
  });
});

// ──────────────────────────────────────────────────────────────────
// FIX #4 — failed quorum must release on ALL nodes (Redlock spec)
// ──────────────────────────────────────────────────────────────────
describe('FIX #4: failed quorum releases on all nodes, including failed ones', () => {
  it('sends the release script to every client after a failed acquire', async () => {
    const clients = createMockRedisClients(3);
    // Only one node grants the lock; the other two refuse / are partitioned.
    clients[0].eval.mockImplementation(async (script: string) =>
      script === RELEASE_SCRIPT ? 1 : 1,
    );
    clients[1].eval.mockImplementation(async (script: string) =>
      script === RELEASE_SCRIPT ? 0 : 0,
    );
    clients[2].eval.mockRejectedValue(new Error('ECONNREFUSED'));

    const redlock = new InternalRedlock(clients as any, {
      retryCount: 0,
      retryDelay: 1,
      retryJitter: 1,
    });

    await expect(redlock.acquire('audit:quorum-res', 1000)).rejects.toThrow();

    // A node where acquire "failed" may still have executed the SET (lost
    // ack), so release must be attempted everywhere.
    for (const client of clients) {
      const releaseCalls = client.eval.mock.calls.filter(
        (c: unknown[]) => c[0] === RELEASE_SCRIPT,
      );
      expect(releaseCalls.length).toBeGreaterThanOrEqual(1);
    }
  });
});

// ──────────────────────────────────────────────────────────────────
// FIX #5 — cache must be invalidated on release
// ──────────────────────────────────────────────────────────────────
describe('FIX #5: cache invalidation on release', () => {
  it('CacheManager.invalidate removes the entry for the resource set', () => {
    const cache = new CacheManager();
    cache.enable();
    cache.set('id-1', ['r1', 'r2'], 60000);
    expect(cache.get(['r1', 'r2'])).not.toBeNull();

    cache.invalidate(['r1', 'r2']);
    expect(cache.get(['r1', 'r2'])).toBeNull();
  });

  it('releasing a lock removes its stale "acquired" cache entry', async () => {
    const clients = createMockRedisClients(3);
    for (const client of clients) client.evalsha.mockResolvedValue([1, 1, 1]);
    const toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(clients));
    toolkit.enableCache();

    const lock = await toolkit.acquireHybrid('audit:cached-res', {
      primaryStrategy: 'optimistic',
    });
    expect((toolkit as any).cacheManager.get(['audit:cached-res'])).not.toBeNull();

    for (const client of clients) client.evalsha.mockResolvedValue(1); // release ok
    await lock.release();

    // Before the fix this entry stayed 'acquired' until its TTL, telling a
    // second caller the resource was still exclusively locked.
    expect((toolkit as any).cacheManager.get(['audit:cached-res'])).toBeNull();
  });
});

// ──────────────────────────────────────────────────────────────────
// FIX #6 — consensus cleanup must cover all clients, not only voters
// ──────────────────────────────────────────────────────────────────
describe('FIX #6: consensus failure cleanup reaches every client', () => {
  it('runs the cleanup script on all clients after a failed quorum', async () => {
    const clients = createMockRedisClients(3);
    clients[0].evalsha.mockResolvedValue(1); // acquire succeeded here
    clients[1].evalsha.mockResolvedValue(0); // lock held by someone else
    clients[2].evalsha.mockResolvedValue(0);

    const toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(clients));
    await expect(toolkit.acquire('audit:contested-res')).rejects.toThrow();

    // Cleanup uses client.eval with the release script source. A write that
    // completed after the early-exit quorum decision lives on a client that
    // never entered votesFor — it must still be cleaned.
    for (const client of clients) {
      const cleanupCalls = client.eval.mock.calls.filter((c: unknown[]) =>
        String(c[0]).includes('Release Script'),
      );
      expect(cleanupCalls.length).toBeGreaterThanOrEqual(1);
    }
  });
});

// ──────────────────────────────────────────────────────────────────
// FIX #7 — optimistic scripts: empty-TTL Lua guard + sub-quorum rollback
// ──────────────────────────────────────────────────────────────────
describe('FIX #7: optimistic write safety', () => {
  it('Lua TTL guards reject the empty string (truthy in Lua)', () => {
    for (const script of [OPTIMISTIC_WRITE_SCRIPT, COMPARE_AND_SWAP_SCRIPT]) {
      const guards = script.match(/if ARGV\[3\]/g) || [];
      const safeGuards = script.match(/if ARGV\[3\] and ARGV\[3\] ~= ""/g) || [];
      expect(guards.length).toBeGreaterThan(0);
      // Every ARGV[3] guard must also exclude '' — otherwise PEXPIRE key ""
      // throws mid-script when no TTL was provided.
      expect(safeGuards.length).toBe(guards.length);
    }
  });

  it('rollback script only reverts the version our own write produced', () => {
    expect(ROLLBACK_WRITE_SCRIPT).toContain('== tonumber(ARGV[1]) + 1');
  });

  it('sub-quorum optimisticWrite rolls back minority nodes', async () => {
    const clients = createMockRedisClients(3);
    clients[0].eval.mockImplementation(async (script: string) =>
      script === ROLLBACK_WRITE_SCRIPT ? 1 : '2', // write succeeded: version 2
    );
    clients[1].eval.mockImplementation(async (script: string) =>
      script === ROLLBACK_WRITE_SCRIPT ? 0 : null, // version mismatch
    );
    clients[2].eval.mockImplementation(async (script: string) =>
      script === ROLLBACK_WRITE_SCRIPT ? 0 : null,
    );

    const redlock = new OptimisticRedlock(clients as any);
    const result = await redlock.optimisticWrite('audit:ow-res', 'value', 1);

    expect(result).toBeNull();
    // Without rollback the minority stays at version 2 while the majority is
    // at 1 and no future CAS can ever reach quorum.
    for (const client of clients) {
      const rollbackCalls = client.eval.mock.calls.filter(
        (c: unknown[]) => c[0] === ROLLBACK_WRITE_SCRIPT,
      );
      expect(rollbackCalls.length).toBe(1);
      expect(rollbackCalls[0][3]).toBe('1'); // ARGV[1] = expectedVersion
    }
  });
});

// ──────────────────────────────────────────────────────────────────
// FIX #8 — Redis Cluster requires hash tags; warn loudly
// ──────────────────────────────────────────────────────────────────
describe('FIX #8: cluster client detection', () => {
  it('warns about CROSSSLOT when a cluster client is supplied', () => {
    const clients = createMockRedisClients(3);
    (clients[0] as any).isCluster = true;
    const warn = vi.fn();
    const logger = { debug: vi.fn(), info: vi.fn(), warn, error: vi.fn() };

    new RedlockToolkit(createTestRedlockToolkitConfig(clients, { logger }));

    expect(warn).toHaveBeenCalledWith(expect.stringContaining('CROSSSLOT'));
  });

  it('stays silent for plain single-instance clients', () => {
    const clients = createMockRedisClients(3);
    const warn = vi.fn();
    const logger = { debug: vi.fn(), info: vi.fn(), warn, error: vi.fn() };

    new RedlockToolkit(createTestRedlockToolkitConfig(clients, { logger }));

    expect(warn).not.toHaveBeenCalled();
  });
});

// ──────────────────────────────────────────────────────────────────
// FIX #9 — CountDownLatch countdown requires quorum, not "any"
// ──────────────────────────────────────────────────────────────────
describe('FIX #9: latch countDown consensus policy', () => {
  it('rejects when only a single node accepts the countdown', async () => {
    const clients = createMockRedisClients(3);
    const toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(clients));
    const latch = await toolkit.createCountDownLatch('audit-latch', { count: 2 });

    clients[0].evalsha.mockResolvedValue([1, 1, 1]); // counted down here only
    clients[1].evalsha.mockRejectedValue(new Error('ECONNREFUSED'));
    clients[2].evalsha.mockRejectedValue(new Error('ECONNREFUSED'));

    // Under the old successPolicy "any" this resolved successfully while two
    // of three nodes still held the old count — permanent divergence.
    await expect(latch.countDown()).rejects.toThrow();
  });
});

// ──────────────────────────────────────────────────────────────────
// FIX #10 — circuit breaker: consecutive-failure window + half-open gate
// ──────────────────────────────────────────────────────────────────
describe('FIX #10: circuit breaker correctness', () => {
  it('a success resets the failure window — only consecutive failures trip', async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 3, resetTimeout: 60000 });
    const fail = () =>
      breaker.execute(async () => {
        throw new Error('boom');
      }).catch(() => {});
    const ok = () => breaker.execute(async () => 'ok');

    await fail();
    await fail();
    await ok(); // closes the window — old failures must not accumulate
    await fail();
    await fail();
    expect(breaker.getState()).toBe('closed');

    await fail(); // third consecutive failure
    expect(breaker.getState()).toBe('open');
  });

  it('limits in-flight probes in half-open state to maxRetries', async () => {
    const breaker = new CircuitBreaker({
      failureThreshold: 1,
      resetTimeout: 30,
      maxRetries: 1,
    });

    await breaker.execute(async () => {
      throw new Error('boom');
    }).catch(() => {});
    expect(breaker.getState()).toBe('open');

    await sleep(50); // let resetTimeout elapse

    let started = 0;
    const probe = () =>
      breaker.execute(async () => {
        started++;
        await sleep(50);
        return 'ok';
      });

    const results = await Promise.allSettled([probe(), probe(), probe()]);

    // Exactly one probe may run; the rest are rejected instead of flooding
    // the recovering target.
    expect(started).toBe(1);
    const rejected = results.filter((r) => r.status === 'rejected');
    expect(rejected.length).toBe(2);
    for (const r of rejected) {
      expect((r as PromiseRejectedResult).reason).toBeInstanceOf(CircuitBreakerOpenError);
    }
  });
});
