# Monitoring Reference

Metrics, circuit breaker, events, and observability.

## Metrics

Metrics are enabled by default (`enableMetrics: true`).

### Get Metrics

```ts
const metrics = toolkit.getMetrics();
// {
//   locksAcquired: 150,
//   locksReleased: 148,
//   lockExtensions: 42,
//   failedAcquisitions: 3,
//   averageLockDuration: 245,
//   activeLocks: 2,
//   circuitBreaker: { state: 'closed', ... },
// }
```

### Performance Summary

```ts
const summary = toolkit.getPerformanceSummary();
```

### Reset Metrics

```ts
toolkit.resetMetrics();
```

## Prometheus Export

```ts
const text = toolkit.exportMetrics();
// Returns text in Prometheus exposition format:
// # HELP redlock_locks_acquired_total Total locks acquired
// # TYPE redlock_locks_acquired_total counter
// redlock_locks_acquired_total 150
// ...
```

Integrate with an HTTP endpoint:

```ts
import express from 'express';
const app = express();

app.get('/metrics', (req, res) => {
  res.set('Content-Type', 'text/plain');
  res.send(toolkit.exportMetrics());
});
```

## Circuit Breaker

Each Redis client has an independent circuit breaker with three states:

```
CLOSED  ──(failures >= threshold)──>  OPEN
  ^                                     │
  │                              (resetTimeout)
  │                                     v
  └──────(success)──────────────  HALF-OPEN
                                 (maxRetries probes)
```

### Configuration

```ts
const toolkit = new RedlockToolkit({
  clients: [...],
  circuitBreaker: {
    failureThreshold: 5,     // Open after 5 consecutive failures
    resetTimeout: 60000,     // Try half-open after 60s
    maxRetries: 3,           // Allow 3 probes in half-open
    operationTimeout: 5000,  // Per-operation timeout
  },
});
```

### Get Circuit Breaker State

```ts
const cbMetrics = toolkit.getCircuitBreakerMetrics();
// {
//   state: 'closed',
//   successfulOperations: 1200,
//   failedOperations: 2,
//   resetTimeout: 60000,
//   failureThreshold: 5,
// }
```

### Events

```ts
toolkit.on('circuit:stateChanged', (newState) => {
  console.log(`Circuit breaker: ${newState}`); // 'open', 'half-open', 'closed'
});
```

When a circuit opens, operations for that client throw `CircuitBreakerOpenError` immediately instead of waiting for a timeout.

## Events

`RedlockToolkit` extends `EventEmitter`:

```ts
toolkit.on('lock:acquired', (resources: string[], identifier: string) => {
  console.log(`Lock acquired: ${resources.join(', ')}`);
});

toolkit.on('lock:released', (resources: string[], identifier: string) => {
  console.log(`Lock released: ${resources.join(', ')}`);
});

toolkit.on('lock:extended', (resources: string[], identifier: string, timestamp: number) => {
  console.log(`Lock extended: ${resources.join(', ')}`);
});

toolkit.on('lock:failed', (resources: string[], error: Error) => {
  console.error(`Lock failed: ${error.message}`);
});

toolkit.on('circuit:stateChanged', (state: 'closed' | 'open' | 'half-open') => {
  // Alert on circuit breaker state changes
});

toolkit.on('error', (error: Error) => {
  // Catch-all for operational errors (script load failures, etc.)
});
```

## Caching

Optional lock cache to avoid redundant Redis calls:

```ts
toolkit.enableCache({
  ttl: 5000,                // Cache entries live 5s
  maxSize: 1000,            // Max 1000 entries
  strategy: 'lru',          // 'lru' | 'lfu' | 'ttl'
  negativeCaching: false,   // Cache "not locked" results
});

const stats = toolkit.getCacheStats();
// { size: 42, hitRate: 0.85, evictions: 12 }

toolkit.disableCache();
```

## Cleanup

Remove stale locks (keys without TTL) using non-blocking SCAN:

```ts
const cleaned = await toolkit.cleanup();
console.log(`Removed ${cleaned} stale keys`);
```

This scans `{keyPrefix}:*` in batches of 100. Keys with `PTTL = -1` (no expiry) are deleted. Use in periodic maintenance jobs.
