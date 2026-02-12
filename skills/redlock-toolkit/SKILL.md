---
name: redlock-toolkit
description: Use when working with @trishchuk/redlock-toolkit for distributed locking in Redis. Use when implementing mutex locks, semaphores, countdown latches, optimistic locking, circuit breakers, or pub/sub-based lock waiting. Use when debugging lock contention, consensus failures, circuit breaker trips, semaphore capacity errors, or latch timeouts. Use when you see RedlockToolkit, acquireSemaphore, createCountDownLatch, acquireOptimistic, acquireHybrid, or LockSignal in code.
---

# redlock-toolkit Skill

Redis distributed locking library implementing the Redlock algorithm with semaphores, countdown latches, optimistic locking, circuit breaker, and metrics.

## Quick Reference

| Task | Solution | Details |
| --- | --- | --- |
| Acquire a mutex lock | `toolkit.acquire('resource')` | [LOCKING.md](reference/LOCKING.md) |
| Auto-extending critical section | `toolkit.using('res', async (signal) => { ... })` | [LOCKING.md#using](reference/LOCKING.md#auto-managed-locking-with-using) |
| Lock multiple resources atomically | `toolkit.acquire(['r1', 'r2'])` | [LOCKING.md#multi-resource](reference/LOCKING.md#multi-resource-locking) |
| Retry on contention | `toolkit.acquire('res', { retryCount: 5 })` | [LOCKING.md#retry](reference/LOCKING.md#retry-configuration) |
| Optimistic lock with versioning | `toolkit.acquireOptimistic('res')` | [OPTIMISTIC.md](reference/OPTIMISTIC.md) |
| Hybrid pessimistic+optimistic | `toolkit.acquireHybrid('res')` | [OPTIMISTIC.md#hybrid](reference/OPTIMISTIC.md#hybrid-locking) |
| Limit concurrency (semaphore) | `toolkit.acquireSemaphore('res', { maxPermits: 5 })` | [PRIMITIVES.md#semaphore](reference/PRIMITIVES.md#semaphore) |
| Coordinate N workers (latch) | `toolkit.createCountDownLatch('ready', { count: 3 })` | [PRIMITIVES.md#latch](reference/PRIMITIVES.md#countdown-latch) |
| Get existing latch handle | `toolkit.getCountDownLatch('ready')` | [PRIMITIVES.md#latch](reference/PRIMITIVES.md#countdown-latch) |
| Monitor lock metrics | `toolkit.getMetrics()` | [MONITORING.md](reference/MONITORING.md) |
| Prometheus export | `toolkit.exportMetrics()` | [MONITORING.md#prometheus](reference/MONITORING.md#prometheus-export) |
| Check lock status | `toolkit.getStatus('resource')` | [LOCKING.md#status](reference/LOCKING.md#lock-status) |
| Handle errors correctly | `catch (e) { if (isRetryableError(e)) ... }` | [ERRORS.md](reference/ERRORS.md) |
| Configure circuit breaker | `{ circuitBreaker: { failureThreshold: 5 } }` | [MONITORING.md#circuit-breaker](reference/MONITORING.md#circuit-breaker) |
| Enable pub/sub waiting | `{ pubSub: { enabled: true } }` | [CONFIGURATION.md#pubsub](reference/CONFIGURATION.md#pubsub) |
| Graceful shutdown | `await toolkit.shutdown()` | [CONFIGURATION.md#shutdown](reference/CONFIGURATION.md#shutdown) |

## Essential Patterns

### Setup

```ts
import RedlockToolkit from '@trishchuk/redlock-toolkit';
import Redis from 'ioredis';

const toolkit = new RedlockToolkit({
  clients: [
    new Redis({ host: 'redis-1', port: 6379 }),
    new Redis({ host: 'redis-2', port: 6379 }),
    new Redis({ host: 'redis-3', port: 6379 }),
  ],
  defaultLockOptions: {
    ttl: 30000,
    retryCount: 3,
    retryDelay: 200,
    retryJitter: 100,
  },
  keyPrefix: 'myapp',
  enableMetrics: true,
});
```

### Acquire / Release

```ts
const lock = await toolkit.acquire('user:123:checkout', { ttl: 10000 });
try {
  // Critical section — only one process runs this
  await processPayment(userId);
} finally {
  await lock.release();
}
```

### Auto-Extending Critical Section

```ts
const result = await toolkit.using('job:export', async (signal) => {
  for (const batch of batches) {
    if (signal.aborted) throw signal.error;  // Check if lock lost
    await processBatch(batch);
  }
  return { processed: batches.length };
}, { ttl: 30000, autoExtendThreshold: 5000 });
```

### Semaphore — Limit Concurrent Access

```ts
const permit = await toolkit.acquireSemaphore('api:external', {
  maxPermits: 10,
  ttl: 60000,
  retryCount: 5,
});
try {
  await callExternalApi();
} finally {
  await permit.release();
}
```

### Countdown Latch — Wait for N Workers

```ts
// Coordinator
const latch = await toolkit.createCountDownLatch('migration:ready', {
  count: 3,
  ttl: 120000,
});
await latch.await(60000); // Wait up to 60s for all 3

// Worker (different process)
const latch = toolkit.getCountDownLatch('migration:ready');
await latch.countDown('worker-1');
```

### Error Handling

```ts
import {
  ResourceLockedError,
  ConsensusError,
  SemaphoreFullError,
  isRetryableError,
} from '@trishchuk/redlock-toolkit';

try {
  const lock = await toolkit.acquire('resource');
} catch (error) {
  if (error instanceof ResourceLockedError) {
    // Resource held by another process
  } else if (error instanceof ConsensusError) {
    // Quorum not reached — check Redis connectivity
  } else if (isRetryableError(error)) {
    // Safe to retry with backoff
  }
}
```

## Key Concepts

- **Consensus**: Every distributed operation requires `floor(N/2) + 1` Redis nodes to agree (quorum).
- **TTL & Drift**: Lock validity = `ttl - elapsed - drift`, where drift = `round(driftFactor * ttl) + 2`.
- **Auto-Extension**: Uses recursive `setTimeout` (not `setInterval`) to extend locks before they expire during long operations.
- **Lua Atomicity**: All Redis mutations are atomic Lua scripts — no multi-step race conditions.
- **Key Format**: `{keyPrefix}:{resource}` for locks, `{keyPrefix}:sem:{resource}` for semaphores, `{keyPrefix}:latch:{name}` for latches.

## Exports

```ts
// Main class
import RedlockToolkit from '@trishchuk/redlock-toolkit';

// Primitives
import { Lock, SemaphorePermit, CountDownLatch } from '@trishchuk/redlock-toolkit';

// Errors
import {
  ResourceLockedError, ConsensusError, LockTimeoutError,
  LockExpiredError, SemaphoreFullError, LatchTimeoutError,
  ConfigurationError, isRetryableError,
} from '@trishchuk/redlock-toolkit';

// Monitoring
import { MetricsCollector, CircuitBreakerManager } from '@trishchuk/redlock-toolkit';

// Pub/Sub
import { PubSubManager, PubSubWaiter } from '@trishchuk/redlock-toolkit';
```

## Reference Documentation

- **[CONFIGURATION.md](reference/CONFIGURATION.md)** — Setup, options, Redis requirements, pub/sub, shutdown.
- **[LOCKING.md](reference/LOCKING.md)** — Pessimistic locking: acquire, release, extend, using(), multi-resource, retry.
- **[OPTIMISTIC.md](reference/OPTIMISTIC.md)** — Optimistic locking with versioning, hybrid strategy.
- **[PRIMITIVES.md](reference/PRIMITIVES.md)** — Semaphore and CountDownLatch APIs.
- **[ERRORS.md](reference/ERRORS.md)** — Error hierarchy, retryable errors, error handling patterns.
- **[MONITORING.md](reference/MONITORING.md)** — Metrics, Prometheus export, circuit breaker, events.
