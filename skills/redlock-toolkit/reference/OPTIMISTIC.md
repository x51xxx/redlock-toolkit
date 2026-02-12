# Optimistic Locking Reference

Version-based locking for low-contention scenarios where readers far outnumber writers.

## Optimistic Acquire

```ts
const result = await toolkit.acquireOptimistic('inventory:sku-42', {
  ttl: 10000,
});
// { success: true, currentVersion: 1 }
```

Returns without blocking if the resource is available. Tracks a version number that increments on each write.

### Parameters

```ts
toolkit.acquireOptimistic(
  resources: string | string[],
  options?: OptimisticLockOptions,
): Promise<OptimisticLockResult>
```

### OptimisticLockOptions

Extends `LockOptions` with:

| Property | Type | Description |
| --- | --- | --- |
| `expectedVersion` | `number` | Required version for CAS. Conflict if mismatch. |
| `expectedValue` | `unknown` | Expected stored value for compare-and-swap. |
| `conflictResolution` | `'fail' \| 'retry' \| 'fallback'` | What to do on conflict. |
| `maxRetries` | `number` | Max retries when `conflictResolution: 'retry'`. |

### OptimisticLockResult

```ts
{
  success: boolean,
  currentVersion?: number,   // Version after operation
  conflict?: boolean,        // Whether a conflict occurred
  retries?: number,          // Retries performed
}
```

## Optimistic Update

Update a resource only if the version matches:

```ts
const read = await toolkit.acquireOptimistic('counter');
// read.currentVersion === 5

// ... compute new value ...

const write = await toolkit.updateOptimistic('counter', 5, {
  ttl: 10000,
});

if (write.conflict) {
  // Version changed — re-read and retry
}
```

```ts
toolkit.updateOptimistic(
  resources: string | string[],
  expectedVersion: number,
  options?: OptimisticLockOptions,
): Promise<OptimisticLockResult>
```

## Conflict Types

When a conflict is detected, `OptimisticLockConflictError` is thrown with `conflictType`:

| Type | Meaning |
| --- | --- |
| `'version_mismatch'` | `expectedVersion` does not match current version. |
| `'value_mismatch'` | `expectedValue` does not match stored value. |
| `'locked'` | Resource is held by a different identifier (pessimistic lock). |

## Hybrid Locking

Combines pessimistic and optimistic strategies. Tries one first, falls back to the other:

```ts
const lock = await toolkit.acquireHybrid('resource', {
  primaryStrategy: 'optimistic',    // Try optimistic first
  fallbackStrategy: 'pessimistic',  // Fall back to pessimistic
  ttl: 15000,
});
try {
  await doWork();
} finally {
  await lock.release();
}
```

### HybridLockOptions

Extends both `LockOptions` and `OptimisticLockOptions`:

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `primaryStrategy` | `'pessimistic' \| 'optimistic' \| 'adaptive'` | `'pessimistic'` | First strategy to try. |
| `fallbackStrategy` | `'pessimistic' \| 'optimistic'` | `'optimistic'` | Strategy if primary fails. |
| `concurrencyThreshold` | `number` | — | For adaptive: switch strategy above this contention level. |
| `metricsWindow` | `number` | — | Window (ms) for adaptive strategy decisions. |

### Adaptive Mode

With `primaryStrategy: 'adaptive'`, the library monitors contention and automatically picks the best strategy:

- Low contention: uses optimistic (cheaper, no blocking).
- High contention: switches to pessimistic (avoids conflict storms).
