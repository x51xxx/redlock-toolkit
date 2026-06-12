/**
 * Regression tests for the property that was BROKEN in ≤0.9.x: instantaneous mutual exclusion of
 * `acquire()` / `using()`, plus the backward-compat shims `acquireRedlock()` / `usingRedlock()`.
 *
 * Uses `retryCount: 0` so the loser fails IMMEDIATELY — this isolates exclusivity (the contended
 * acquire must reject right away) rather than retry/wait behaviour. Real Redis only (the bug was a
 * server-side check-then-set / consensus issue a mock cannot reproduce); skipped when unavailable.
 * Point at a disposable instance to avoid flushing a shared dev Redis:
 *   REDLOCK_TEST_REDIS_PORT=6399 npx vitest run redlock-compat-exclusivity
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import Redis from 'ioredis';
import RedlockToolkit from '../src/index';
import { Lock } from '../src/core/lock';
import { isRedisAvailable, createRedisClients, flushClients, disconnectClients } from './real-redis-setup';

const available = await isRedisAvailable();

describe.skipIf(!available)('Mutual exclusion + Redlock-named compat shims', () => {
  let clients: Redis[];
  let toolkit: RedlockToolkit;

  beforeEach(async () => {
    clients = createRedisClients(1);
    await flushClients(clients);
    // retryCount 0 ⇒ a contended acquire fails instantly: the cleanest exclusivity discriminator.
    toolkit = new RedlockToolkit({
      clients,
      keyPrefix: 'compat-test',
      defaultLockOptions: { ttl: 5000, retryCount: 0 },
    });
  });

  afterEach(async () => {
    await toolkit.shutdown();
    await disconnectClients(clients);
  });

  it('acquire(): two concurrent acquires of the same resource yield exactly ONE winner', async () => {
    const results = await Promise.allSettled([toolkit.acquire('seatX'), toolkit.acquire('seatX')]);
    const winners = results.filter((r) => r.status === 'fulfilled');
    expect(winners).toHaveLength(1);
    for (const w of winners) await (w as PromiseFulfilledResult<Lock>).value.release();
  });

  it('using(): concurrent routines on the same resource never overlap (max 1 inside)', async () => {
    let inside = 0;
    let maxInside = 0;
    const body = async () => {
      inside++;
      maxInside = Math.max(maxInside, inside);
      await new Promise((r) => setTimeout(r, 250));
      inside--;
      return 'ok';
    };
    await Promise.allSettled([toolkit.using('seatY', body), toolkit.using('seatY', body)]);
    expect(maxInside).toBe(1);
  });

  it('acquireRedlock(): positional ttl, returns a Lock, and is exclusive', async () => {
    const lock = await toolkit.acquireRedlock('seatR', 4000);
    expect(lock).toBeInstanceOf(Lock);
    // The positional ttl must be honored (a finite future expiration was set).
    expect(lock.expiration).toBeGreaterThan(Date.now());
    // A second contender for the same resource must NOT acquire while the first holds.
    await expect(toolkit.acquireRedlock('seatR', 4000)).rejects.toBeDefined();
    await lock.release();
    // Once released, the resource is free again.
    const lock2 = await toolkit.acquireRedlock('seatR', 4000);
    await lock2.release();
  });

  it('usingRedlock(): positional ttl, runs the routine with a signal, and serializes contention', async () => {
    let inside = 0;
    let maxInside = 0;
    let sawSignal = false;
    const body = async (signal: { aborted: boolean }) => {
      sawSignal = typeof signal?.aborted === 'boolean';
      inside++;
      maxInside = Math.max(maxInside, inside);
      await new Promise((r) => setTimeout(r, 250));
      inside--;
      return 42;
    };
    const [r1, r2] = await Promise.allSettled([
      toolkit.usingRedlock('seatU', 4000, body),
      toolkit.usingRedlock('seatU', 4000, body),
    ]);
    expect(maxInside).toBe(1);
    expect(sawSignal).toBe(true);
    // At least one routine completed and returned its value through the shim.
    const ok = [r1, r2].find((r) => r.status === 'fulfilled') as PromiseFulfilledResult<number> | undefined;
    expect(ok?.value).toBe(42);
  });
});
