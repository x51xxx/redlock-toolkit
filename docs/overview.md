# redlock-toolkit Overview

**Package:** `@trishchuk/redlock-toolkit`

redlock-toolkit is an advanced Redis distributed locking library for Node.js. It implements the Redlock algorithm with built-in support for distributed semaphores, countdown latches, optimistic locking, circuit breaker fault tolerance, automatic lock extension, and Prometheus-compatible metrics.

## Features

- **Distributed mutex locks** -- Redlock consensus algorithm requiring a quorum of `floor(N/2) + 1` Redis nodes.
- **Distributed semaphore** -- N-permit concurrent access backed by Redis sorted sets (ZSET).
- **Distributed CountDownLatch** -- Block until N events have been signaled across processes.
- **Optimistic locking** -- Version-controlled locking with conflict detection.
- **Circuit breaker pattern** -- Fault tolerance with automatic state transitions (Closed, Open, Half-Open).
- **Auto-extension** -- Locks are extended before expiration using recursive `setTimeout` (not `setInterval`) to prevent request accumulation.
- **Pub/Sub waiting** -- Instant lock-release notifications instead of polling.
- **Comprehensive metrics** -- Acquisition latency, success/failure rates, and Prometheus export.
- **Strategy pattern** -- Choose between pessimistic, optimistic, and hybrid locking strategies.

## Public API

The library exposes a single unified class as its public API:

```typescript
import RedlockToolkit from '@trishchuk/redlock-toolkit';
```

`RedlockToolkit` is the main entry point for interacting with the library. Internal implementation classes (`InternalRedlock`, `InternalRedlockLock`) are intentionally hidden. Supporting classes (`Lock`, `SemaphorePermit`, `CountDownLatch`, `OptimisticRedlock`, `CircuitBreakerManager`, `MetricsCollector`, `PubSubManager`, `PubSubWaiter`), error classes, and type definitions are also exported for consumer convenience.

For the full API surface, see [api-reference.md](./api-reference.md).

## Tech Stack

| Component   | Details                                  |
|-------------|------------------------------------------|
| Language    | TypeScript (strict mode, target ES2022)  |
| Runtime     | Node.js                                  |
| Data store  | Redis 3.2+ (Lua script support required) |
| Redis client| ioredis v4.x / v5.x                     |
| Test runner | Vitest                                   |
| Module      | CommonJS                                 |

## Project Structure

```
src/
  core/           Types, errors, Lock class, logger
  algorithms/     Redlock and optimistic-redlock internals
  strategies/     Pessimistic, optimistic, hybrid, semaphore, countdown latch
  patterns/       Circuit breaker
  primitives/     SemaphorePermit, CountDownLatch
  pubsub/         PubSubManager, PubSubWaiter
  managers/       Lock cache
  utils/          Lua scripts, metrics, consensus manager, auto-extension manager
  index.ts        Public API surface (RedlockToolkit + types + errors)

tests/            All tests, organized flat by concern
examples/         Runnable usage examples
docs/             Documentation
```

## Key Architectural Decisions

1. **Single public API.** `RedlockToolkit` is the sole entry point. This prevents confusion from multiple similar classes and keeps the public surface minimal.

2. **Recursive setTimeout for auto-extension.** Using `setInterval` can cause request accumulation under high latency. Recursive `setTimeout` schedules each extension only after the previous one completes.

3. **Atomic Lua scripts.** All Redis operations (acquire, release, extend, compare-and-delete) are implemented as Lua scripts with precomputed SHA1 hashes for `EVALSHA` execution.

4. **Consensus via ConsensusManager.** Lock acquisition requires agreement from a quorum of `floor(N/2) + 1` nodes. Clock drift is compensated with `drift = round(driftFactor * ttl) + 2`.

5. **Strategy pattern.** Locking behavior is pluggable via strategies -- pessimistic (traditional), optimistic (version-based), and hybrid (combined).

6. **Event-driven lifecycle.** Lock events (acquired, released, extended, failed, error) and circuit breaker state changes are forwarded through Node.js `EventEmitter`.

7. **Lock key format.** All keys follow the `${keyPrefix}:${resource}` pattern.

## Lock Instance Hierarchy

```
RedlockToolkit          (public API)
  Lock                  (public lock instance, core/lock.ts)
    InternalRedlock     (hidden, algorithms/redlock.ts)
      InternalRedlockLock (hidden, algorithms/redlock.ts)
```

## Error Handling

The library defines a typed error hierarchy rooted at `RedlockToolkitError`:

- `ResourceLockedError` -- Resource is already locked by another holder.
- `ConsensusError` -- Quorum could not be reached.
- `LockTimeoutError` -- Acquisition timed out.
- Additional error types for extension failures, circuit breaker trips, and version conflicts.

All errors carry a typed `context` of `Record<string, unknown>`. The helper `isRetryableError()` indicates whether a failed acquisition should be retried.

For a full error reference, see [troubleshooting.md](./troubleshooting.md).

## Performance Considerations

- **Connection pooling.** Use ioredis connection pools in production deployments.
- **Lock TTL sizing.** Set TTL based on P99 operation duration plus a safety buffer.
- **Retry strategy.** Built-in jittered retry delay with error-based scaling reduces contention under load.
- **Circuit breaker tuning.** Adjust failure threshold and reset timeout based on your SLA.
- **Metrics monitoring.** Track acquisition latency and success rates to detect degradation early.

For advanced patterns (semaphore usage, countdown latch coordination, hybrid strategies), see [advanced-usage.md](./advanced-usage.md).

## Further Reading

- [API Reference](./api-reference.md) -- Full API documentation with method signatures and options.
- [Advanced Usage](./advanced-usage.md) -- Patterns for semaphores, latches, hybrid strategies, and Pub/Sub waiting.
- [Redlock Algorithm](./redlock-algorithm.md) -- Detailed explanation of the Redlock consensus algorithm.
- [Troubleshooting](./troubleshooting.md) -- Common issues and their solutions.
- [AI Agent Guide](./ai-agent-guide.md) -- Instructions for AI agents working with this codebase.

