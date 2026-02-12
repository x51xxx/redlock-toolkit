# Locking Reference

Pessimistic distributed lock API — the primary way to achieve mutual exclusion.

## Acquire and Release

```ts
const lock = await toolkit.acquire('order:123', { ttl: 15000 });
try {
  await processOrder('123');
} finally {
  await lock.release();
}
```

`acquire()` returns a `Lock` instance. If the resource is already locked, it throws (or retries based on `retryCount`).

### Parameters

```ts
toolkit.acquire(
  resources: string | string[],   // Resource name(s) to lock
  options?: Partial<LockOptions>,  // Override defaults
): Promise<Lock>
```

### Lock Properties

```ts
lock.resources       // string[] — Locked resources
lock.identifier      // string — Unique lock ID
lock.expiration      // number — Expiration timestamp (ms)
lock.isValid         // boolean — Not released and not expired
lock.isExpired       // boolean — Past expiration time
lock.released        // boolean — Whether release() was called
lock.extensions      // number — How many times extended
lock.duration        // number — Time held (ms)
lock.timeToExpiration // number — ms until expiry (min 0)
```

## Auto-Managed Locking with using()

`using()` acquires a lock, runs a function with auto-extension, and releases on completion or error:

```ts
const result = await toolkit.using('resource', async (signal) => {
  // Lock auto-extends while this runs
  // signal.aborted becomes true if lock is lost
  for (const item of items) {
    if (signal.aborted) throw signal.error;
    await process(item);
  }
  return items.length;
}, { ttl: 30000, autoExtendThreshold: 5000 });
```

The `signal` parameter implements `LockSignal`:

```ts
signal.aborted        // boolean — true if lock was lost
signal.error          // Error | undefined — reason for abortion
signal.expiration     // number — current lock expiration timestamp
signal.addEventListener('abort', () => { ... })
signal.removeEventListener('abort', () => { ... })
```

You can also call `lock.using()` on an already-acquired lock:

```ts
const lock = await toolkit.acquire('resource');
const result = await lock.using(async (signal) => {
  return await doWork();
});
// Lock is released automatically after using() completes
```

## Extend

Manually extend a lock's TTL:

```ts
const lock = await toolkit.acquire('resource', { ttl: 10000 });
// ... some work ...
await lock.extend(15000);  // Reset TTL to 15s from now
```

Returns the same `Lock` instance (chainable). Throws `LockExtensionError` if the lock is no longer owned.

## Multi-Resource Locking

Acquire multiple resources atomically:

```ts
const lock = await toolkit.acquire(['account:A', 'account:B'], {
  ttl: 10000,
});
try {
  await transfer(accountA, accountB, amount);
} finally {
  await lock.release();
}
```

All resources are acquired in a single consensus round. If any resource is held by another client, the entire operation fails.

## Retry Configuration

```ts
// No retry (fail immediately if locked) — default
await toolkit.acquire('res', { retryCount: 0 });

// Retry up to 5 times with exponential backoff + jitter
await toolkit.acquire('res', {
  retryCount: 5,
  retryDelay: 200,   // Base delay (ms)
  retryJitter: 100,  // Random jitter (ms)
});

// Infinite retry (blocks until acquired)
await toolkit.acquire('res', { retryCount: -1 });
```

With pub/sub enabled, retries wait for a release notification instead of sleeping, reducing latency.

## Lock Status

Check if a resource is locked without acquiring:

```ts
const statuses = await toolkit.getStatus(['user:1', 'user:2']);
// [
//   { resource: 'user:1', locked: true, holder: 'abc123', ttl: 14500 },
//   { resource: 'user:2', locked: false },
// ]
```

## Force Release

Admin operation that releases a lock regardless of who holds it:

```ts
const result = await toolkit.forceRelease('stuck:resource');
// { releasedCount: 3, totalAttempted: 1 }
```

Use with caution — bypasses ownership checks.

## Active Locks

```ts
const activeLocks = toolkit.getActiveLocks();
for (const lock of activeLocks) {
  console.log(lock.resources, lock.timeToExpiration);
}
```

## Release Result

```ts
const result = await lock.release();
// {
//   success: boolean,
//   releasedCount: number,    // Nodes that confirmed release
//   totalClients: number,
//   stats: Promise<LockStats>,
// }
```

Release is idempotent — calling `release()` twice returns success without error.
