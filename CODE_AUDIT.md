# Code Audit Report: redlock-toolkit

**Date:** 2026-03-21
**Scope:** Full codebase audit — source code, tests, configuration, examples

---

## Critical Issues

### 1. ACQUIRE_SCRIPT is NOT atomic (Race Condition)
**File:** `src/utils/scripts.ts:11-42`
**Severity:** CRITICAL

The acquire Lua script has a TOCTOU (time-of-check-time-of-use) race condition. It first checks all keys in a loop, then sets all keys in a second loop. Between the check and the set, another client could acquire a lock on one of the resources. While Lua scripts execute atomically within a single Redis instance, the two-phase approach means the check-then-set across multiple KEYS is fine within one Redis node. However, this is still semantically problematic when combined with the Redlock quorum — if a key check passes on node A but the same key is simultaneously acquired on node B between quorum votes, two clients can believe they own the same resource.

This is an inherent limitation of Redlock, but the comment "Acquire all locks atomically" at line 31 is misleading — it's only atomic per-node.

### 2. `executeWithConsensus` cleanup accesses `error.attempts[0]` as a Promise
**File:** `src/index.ts:376`
**Severity:** CRITICAL

```ts
const stats = await error.attempts[0];
const votesFor = stats.votesFor;
```

`ConsensusError.attempts` is typed as `Promise<LockStats>[]`. The code awaits `attempts[0]` correctly, but `votesFor` is a `Set<RedisClient>` — meaning the cleanup iterates over actual Redis clients. However, if the consensus error was thrown before any promise resolved, `attempts[0]` may resolve to stats where `votesFor` is empty, and the cleanup becomes a no-op. There's no fallback to track which clients actually succeeded if the operation failed mid-way.

### 3. `acquireLock` compatibility method overwrites lock keys with data
**File:** `src/index.ts:699-727`
**Severity:** CRITICAL

```ts
const lock = await this.acquire(key, { ttl, identifier: clientId });
if (data !== undefined) {
    const promises = this.clients.map((client) =>
        client.set(key, JSON.stringify(data), "PX", ttl),
    );
}
```

After acquiring the lock (which stores the `clientId` as the value at `key`), the code immediately OVERWRITES the same key with `JSON.stringify(data)`. This destroys the lock identifier, meaning:
- `release()` will fail because the stored value no longer matches the identifier
- Any concurrent `acquire()` check will see the data string instead of a lock identifier
- The lock is effectively broken after storing data

### 4. Optimistic write passes empty string as TTL when undefined
**File:** `src/algorithms/optimistic-redlock.ts:327`
**Severity:** HIGH

```ts
[value, expectedVersion.toString(), ttl?.toString() || ""]
```

When `ttl` is undefined, an empty string `""` is passed as ARGV[3] to the Lua script. In the script (line 63):
```lua
if ARGV[3] then
    redis.call("PEXPIRE", KEYS[1], ARGV[3])
```
An empty string is truthy in Lua, so `PEXPIRE` will be called with an empty string, causing a Redis error. Should be `ttl?.toString() || "0"` or handle explicitly.

---

## High Severity Issues

### 5. `forceRelease` has a race condition with `totalReleased` counter
**File:** `src/index.ts:647-669`
**Severity:** HIGH

```ts
let totalReleased = 0;
// ...
.then((result) => {
    totalReleased += result;
})
```

`totalReleased +=` is not atomic in JavaScript. While JS is single-threaded, the `.then()` callbacks can interleave with the `await Promise.allSettled()` call. Actually in this case it's fine because `allSettled` waits for all to complete, but the real issue is that `Promise.allSettled` is used after `.then()/.catch()` which already handles resolution — the `allSettled` is redundant and the `.catch(() => 0)` swallows errors silently without logging.

### 6. `getClientForRead` always returns first client — no load balancing
**File:** `src/index.ts:291-293`
**Severity:** HIGH

```ts
public getClientForRead(): RedisClient {
    return this.clients[0];
}
```

All read operations use only the first client. If that client is slow or overloaded, there's no failover for reads. Also `get()`, `publish()`, `fromEvent()`, and `getStatus()` all hardcode `this.clients[0]`, creating a single point of failure for read operations.

### 7. `ioredis` in both `dependencies` and `peerDependencies`
**File:** `package.json:89-90, 108-109`
**Severity:** HIGH

`ioredis` appears in both `dependencies` and `peerDependencies` with the same version range. For a library, it should only be in `peerDependencies` to avoid consumers ending up with duplicate copies of ioredis.

### 8. Optimistic locking operations are NOT consensus-based
**File:** `src/algorithms/optimistic-redlock.ts:314-345`
**Severity:** HIGH

`optimisticWrite` executes on all clients in parallel and considers success if `quorum` clients succeed. However, if some clients succeed and others fail, there's no rollback on successful clients. A partial write creates version divergence across nodes. The `rollbackWrite` method exists (line 509) but is never called.

### 9. CacheManager `getStats()` returns hardcoded zeros
**File:** `src/managers/cache.ts:103-109`
**Severity:** MEDIUM

```ts
getStats(): { size: number; hitRate: number; evictions: number } {
    return {
        size: this.lockCache.size,
        hitRate: 0, // Would need to track hits/misses for accurate rate
        evictions: 0, // Would need to track evictions
    };
}
```

`hitRate` and `evictions` are always 0. The public API promises stats but delivers stubs.

### 10. CacheManager `set()` is never called from the main flow
**File:** `src/managers/cache.ts`
**Severity:** MEDIUM

The `CacheManager` is instantiated in `RedlockToolkit` but its `set()` and `get()` methods are never called from `acquire()`, `extendLock()`, or `releaseLockInternal()`. The cache feature is dead code.

### 11. Lock `onError` callback in `using()` doesn't propagate the error
**File:** `src/core/lock.ts:211-223`
**Severity:** MEDIUM

```ts
onError: (error) => {
    if (error instanceof LockExpiredError) {
        // Already a proper error type
    } else {
        error = new LockExtensionError(/* ... */);
    }
},
```

The `onError` callback reassigns the local `error` parameter but doesn't do anything with it. The new `LockExtensionError` is created and immediately discarded. The error is never propagated to the signal or the caller.

### 12. `InternalRedlockLock.using()` calls `release()` twice
**File:** `src/algorithms/redlock.ts:328-341`
**Severity:** MEDIUM

`InternalRedlock.using()` calls `lock.using(routine)` which releases the lock in its `finally` block (line 179). Then the outer `finally` block also calls `lock.release()` (line 339). This causes a double-release. The second release will attempt to delete the key even though the first release already cleaned it up.

---

## Logic & Correctness Issues

### 13. `validateConfig` uses falsy check for `retryCount`
**File:** `src/index.ts:181`
**Severity:** MEDIUM

```ts
if (config.defaultLockOptions?.retryCount && config.defaultLockOptions.retryCount < -1)
```

Because `0` is falsy, `retryCount: 0` will never enter this validation block. This means `-0.5` or other invalid values could theoretically bypass validation (though not practically relevant since the type is `number`). The `driftFactor` check on line 192 has the same issue — `driftFactor: 0` is falsy and skips validation.

### 14. PubSub waiter only listens on `resources[0]`
**File:** `src/strategies/pessimistic-strategy.ts:52`
**Severity:** MEDIUM

```ts
await this.toolkit.pubSubWaiter.waitForRelease("lock", resources[0], maxWait, delay);
```

When locking multiple resources, only the first resource's release notification is listened to. If the blocking resource is the second or third one, the waiter will never receive the notification and will fall back to the timeout.

### 15. `cleanup()` may delete non-lock keys
**File:** `src/index.ts:992-1030`
**Severity:** MEDIUM

The cleanup scans for `${prefix}:*` which includes semaphore keys, latch keys, version keys, and data keys — not just lock keys. The `CLEANUP_CHECK_SCRIPT` deletes any key with `PTTL == -1` (no expiry), which could delete intentionally persistent keys like countdown latch target keys.

### 16. `OPTIMISTIC_WRITE_SCRIPT` — empty string TTL issue
**File:** `src/algorithms/optimistic-redlock.ts:63-66`
**Severity:** MEDIUM

```lua
if ARGV[3] then
    redis.call("PEXPIRE", KEYS[1], ARGV[3])
```

In Lua, empty string `""` is truthy, so when TTL is not provided and `""` is passed, `PEXPIRE` is called with `""` which will cause a Redis error. The script should check `ARGV[3] ~= ''`.

---

## Resource Leaks & Cleanup

### 17. `preloadScripts` returns a promise that's never awaited
**File:** `src/index.ts:157`
**Severity:** MEDIUM

```ts
this.preloadScripts(); // Called in constructor — can't await
```

The constructor calls `preloadScripts()` which is async, but the result is never awaited. If script preloading fails, the error is emitted as an event but if no `'error'` listener is registered yet (before `setupEventForwarding` or user listener), Node.js will throw an unhandled error and crash.

### 18. `Date.now` monkey-patch without cleanup guard
**File:** `tests/race-conditions.test.ts:329-347`
**Severity:** MEDIUM

`Date.now = vi.fn(...)` is monkey-patched without a `try/finally` or `afterEach` guard. If the test throws before `Date.now = originalNow`, subsequent tests in the same file will use the mocked `Date.now`.

### 19. `fromEvent` subscribes but never tracks subscriptions
**File:** `src/index.ts:875-904`
**Severity:** LOW

The `fromEvent` compatibility method subscribes to Redis channels but has no centralized tracking. The `shutdown()` method doesn't clean up these subscriptions, potentially leaving hanging Redis connections.

---

## Test Quality Issues

### 20. Always-passing assertion in stress test
**File:** `tests/stress.test.ts:405`
**Severity:** HIGH

```ts
expect(successful.length).toBeGreaterThanOrEqual(0);
```

This assertion passes for ANY value including 0. It doesn't verify that the thundering herd prevention actually works.

### 21. Misleading test name in negative cases
**File:** `tests/negative-cases.test.ts:136-141`
**Severity:** MEDIUM

Test "should fail with empty resource array" actually succeeds in acquiring a lock. The test name contradicts the assertion.

### 22. No-op assertion in metrics test
**File:** `tests/metrics.test.ts:144-151`
**Severity:** MEDIUM

"should record retry attempts" test has no assertions on the result.

### 23. Missing `Lock` type import
**File:** `tests/race-conditions.test.ts:458`
**Severity:** MEDIUM

`Lock` type is used without import: `const locks: Lock[] = [];`. Will fail TypeScript strict compilation.

### 24. No test coverage for `acquireHybrid()`, cache methods, or compatibility API
**Severity:** HIGH

Missing test files for: hybrid strategy, cache manager, compatibility methods (`acquireLock`, `releaseLock`, `get`, `publish`, `fromEvent`).

---

## Configuration & Packaging Issues

### 25. Duplicate keyword in package.json
**File:** `package.json:37,44`
**Severity:** LOW

`"fault-tolerance"` appears twice in the `keywords` array.

### 26. `tsconfig.build.json` has invalid negation pattern
**File:** `tsconfig.build.json`
**Severity:** LOW

`"!src/example.ts"` in the `include` array is not valid TypeScript glob negation syntax. Should be in `exclude` only (which is already done).

### 27. Examples import from `../src/index` instead of package name
**File:** `examples/*.ts`
**Severity:** MEDIUM

All examples use `import RedlockToolkit from '../src/index'` instead of `'@trishchuk/redlock-toolkit'`. Users who copy examples will get import errors.

### 28. Examples use single Redis instance with different `db` values
**File:** `examples/*.ts`
**Severity:** MEDIUM

All examples create 3 Redis clients on the same host with different `db` values, which provides zero fault tolerance. This defeats the entire purpose of the Redlock algorithm and could mislead users.

---

## Summary

| Category | Critical | High | Medium | Low |
|----------|----------|------|--------|-----|
| Security/Race Conditions | 2 | 1 | 2 | 0 |
| Logic Bugs | 1 | 2 | 4 | 0 |
| Resource Leaks | 0 | 0 | 3 | 1 |
| Test Quality | 0 | 2 | 3 | 0 |
| Configuration | 0 | 1 | 2 | 2 |
| **Total** | **3** | **6** | **14** | **3** |

### Recommended Priority Fixes

1. **[CRITICAL]** Fix `acquireLock` overwriting lock key with data (#3)
2. **[CRITICAL]** Fix empty string TTL in optimistic scripts (#4, #16)
3. **[HIGH]** Remove `ioredis` from `dependencies`, keep in `peerDependencies` (#7)
4. **[HIGH]** Call `rollbackWrite` on partial optimistic write failures (#8)
5. **[HIGH]** Fix always-passing stress test assertion (#20)
6. **[HIGH]** Add tests for hybrid strategy, cache, and compatibility API (#24)
7. **[MEDIUM]** Fix `onError` not propagating errors in `Lock.using()` (#11)
8. **[MEDIUM]** Fix double-release in `InternalRedlock.using()` (#12)
9. **[MEDIUM]** Actually integrate CacheManager into acquire/release flow (#10)
10. **[MEDIUM]** Fix PubSub waiter to listen on all locked resources (#14)
