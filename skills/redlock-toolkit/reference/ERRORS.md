# Error Handling Reference

All errors extend `RedlockToolkitError` which provides `timestamp` and `context` properties.

## Error Hierarchy

```
RedlockToolkitError (abstract base)
├── ResourceLockedError          — Resource held by another client
├── ConsensusError               — Quorum not reached
├── LockTimeoutError             — Acquisition timed out after retries
├── LockExpiredError             — Operating on an expired lock
├── LockReleaseError             — Release failed on some nodes
├── LockExtensionError           — Extension failed (not owner or expired)
├── CircuitBreakerOpenError      — Redis client circuit is open
├── RedisOperationError          — Low-level Redis command failure
├── ConfigurationError           — Invalid constructor/method arguments
├── LockValidationError          — Lock state validation failed
├── OptimisticLockConflictError  — Version or value mismatch
├── HybridLockError              — Both primary and fallback strategies failed
├── SemaphoreFullError           — Semaphore at max capacity
├── PermitExtensionError         — Permit extend on released/expired permit
├── LatchExistsError             — Latch with this name already exists
├── LatchNotFoundError           — Latch expired or does not exist
└── LatchTimeoutError            — Await timed out before completion
```

## Retryable vs Non-Retryable

Use `isRetryableError()` to determine if retry is safe:

```ts
import { isRetryableError } from '@trishchuk/redlock-toolkit';

try {
  await toolkit.acquire('res');
} catch (error) {
  if (isRetryableError(error)) {
    // Safe to retry: ResourceLockedError, ConsensusError, SemaphoreFullError
  } else {
    // Do not retry: ConfigurationError, CircuitBreakerOpenError, etc.
  }
}
```

### Retryable Errors

| Error | Retry? | Why |
| --- | --- | --- |
| `ResourceLockedError` | Yes | Lock will eventually be released. |
| `ConsensusError` | Yes | Transient quorum failure. |
| `SemaphoreFullError` | Yes | Permits will be released. |
| `RedisOperationError` (network) | Yes | Transient connection issue. |

### Non-Retryable Errors

| Error | Retry? | Why |
| --- | --- | --- |
| `ConfigurationError` | No | Invalid arguments — fix the code. |
| `CircuitBreakerOpenError` | No | Client is fenced off — wait for reset. |
| `LockValidationError` | No | Logic error in lock state. |
| `PermitExtensionError` | No | Permit is already released or expired. |
| `LatchExistsError` | No | Latch already created. |
| `LatchNotFoundError` | No | Latch expired. |

## Retry Delay Calculation

`getRetryDelay(error, baseDelay, jitter)` computes adaptive backoff:

```ts
import { getRetryDelay } from '@trishchuk/redlock-toolkit';

const delay = getRetryDelay(error, 200, 100);
// SemaphoreFullError:          200 * 1.5 ± 100 = 200..400ms
// ResourceLockedError:         200 * 2.0 ± 100 = 300..500ms
// ConsensusError:              200 * 1.5 ± 100 = 200..400ms
// OptimisticLockConflictError: 200 * 0.5 ± 100 = 0..200ms
```

## Common Error Patterns

### Lock Acquisition

```ts
try {
  const lock = await toolkit.acquire('resource', { retryCount: 3 });
} catch (error) {
  if (error instanceof ResourceLockedError) {
    console.log(`Held by: ${error.currentHolder}`);
  } else if (error instanceof LockTimeoutError) {
    console.log(`Timed out after ${error.attemptsCount} attempts`);
  } else if (error instanceof ConsensusError) {
    console.log(`Quorum: ${error.achievedVotes}/${error.requiredQuorum}`);
  }
}
```

### Lock Extension

```ts
try {
  await lock.extend(30000);
} catch (error) {
  if (error instanceof LockExtensionError) {
    // Lock no longer owned — another process may have acquired it
    // Do NOT continue critical section work
  } else if (error instanceof LockExpiredError) {
    // Lock expired before extend could run
  }
}
```

### Semaphore

```ts
try {
  const permit = await toolkit.acquireSemaphore('api', { maxPermits: 10 });
} catch (error) {
  if (error instanceof SemaphoreFullError) {
    console.log(`${error.activePermits}/${error.maxPermits} permits in use`);
  } else if (error instanceof ConfigurationError) {
    console.log(`Invalid param: ${error.parameter} = ${error.value}`);
  }
}
```

### Countdown Latch

```ts
try {
  await latch.await(30000);
} catch (error) {
  if (error instanceof LatchTimeoutError) {
    console.log(`${error.remainingCount} events still pending`);
  } else if (error instanceof LatchNotFoundError) {
    console.log(`Latch '${error.latchName}' expired`);
  }
}
```

## Error Context

Every error has a `context` property with structured metadata:

```ts
try {
  await toolkit.acquire('res');
} catch (error) {
  if (error instanceof RedlockToolkitError) {
    console.log(error.context);   // { reason: '...', ... }
    console.log(error.timestamp); // when error occurred
    console.log(error.toJSON());  // full serializable representation
  }
}
```
