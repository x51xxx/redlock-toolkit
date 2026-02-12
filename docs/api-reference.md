# RedlockToolkit API Reference

## RedlockToolkit Class

## Important Limitation: Reentrancy

The current implementation **does not support a full reentrant lock model** (i.e., `owner + count` semantics).

What this means:
- Calling `acquire()` again from the same owner does not maintain a nested lock count.
- A single `release()` fully releases the lock, rather than decrementing one nesting level.
- For nested critical sections within the same thread/process, use **a single shared lock context** (`using` or pass the `Lock` instance down the call stack).

### Constructor

```typescript
constructor(config: RedlockToolkitConfig)
```

Creates a new RedlockToolkit instance with the specified configuration.

**Parameters:**
- `config` - RedlockToolkit configuration

**Example:**
```typescript
const redlockToolkit = new RedlockToolkit({
  clients: [redisClient1, redisClient2, redisClient3],
  defaultLockOptions: {
    ttl: 30000,
    retryCount: 10
  },
  pubSub: {
    enabled: true,
  },
});
```

### Lock Methods

#### `acquire(resources, options?): Promise<Lock>`

Acquires locks on the specified resources.

**Parameters:**
- `resources` (`string | string[]`) - Resource or array of resources to lock
- `options` (`Partial<LockOptions>`, optional) - Lock options

**Returns:** `Promise<Lock>` - Lock object

**Exceptions:**
- `ResourceLockedError` - Resource is already locked
- `LockTimeoutError` - Lock acquisition timeout
- `ConsensusError` - Failed to achieve quorum
- `CircuitBreakerOpenError` - Circuit breaker is open

**Examples:**
```typescript
// Single resource
const lock = await redlockToolkit.acquire('user:123');

// Multiple resources
const lock = await redlockToolkit.acquire(['user:123', 'account:456']);

// With options
const lock = await redlockToolkit.acquire('resource', {
  ttl: 60000,
  retryCount: 5,
  retryDelay: 200,
  retryJitter: 100
});
```

#### `using<T>(resources, routine, options?): Promise<T>`

Executes a function with automatic lock management.

**Parameters:**
- `resources` (`string | string[]`) - Resources to lock
- `routine` (`(signal: LockSignal) => Promise<T>`) - Function to execute
- `options` (`Partial<LockOptions>`, optional) - Lock options

**Returns:** `Promise<T>` - Result of routine execution

**Example:**
```typescript
const result = await redlockToolkit.using('resource', async (signal) => {
  if (signal.aborted) throw signal.error;

  // Your code here
  return await processData();
}, {
  ttl: 30000,
  autoExtendThreshold: 5000
});
```

#### `forceRelease(resources): Promise<{releasedCount: number, totalAttempted: number}>`

Forcefully releases locks regardless of owner.

**Parameters:**
- `resources` (`string | string[]`) - Resources to release

**Returns:** Object with count of released locks

**Example:**
```typescript
const result = await redlockToolkit.forceRelease(['resource1', 'resource2']);
console.log(`Released: ${result.releasedCount}/${result.totalAttempted}`);
```

#### `getStatus(resources): Promise<LockStatus[]>`

Gets the status of locks for specified resources.

**Parameters:**
- `resources` (`string | string[]`) - Resources to check

**Returns:** Array of lock statuses

**Example:**
```typescript
const statuses = await redlockToolkit.getStatus(['resource1', 'resource2']);
// [
//   { resource: 'resource1', locked: true, holder: 'abc123', ttl: 15000 },
//   { resource: 'resource2', locked: false }
// ]
```

#### `getActiveLocks(): Lock[]`

Returns all active locks for this RedlockToolkit instance.

**Returns:** Array of active locks

### Optimistic Locking Methods

#### `acquireOptimistic(resources, options?): Promise<OptimisticLockResult>`

Acquires an optimistic lock with version-based conflict detection. Ideal for high-read, low-write scenarios.

**Parameters:**
- `resources` (`string | string[]`) - Resources to lock
- `options` (`OptimisticLockOptions`, optional) - Optimistic lock options

**Returns:** `Promise<OptimisticLockResult>`

**Example:**
```typescript
const result = await redlockToolkit.acquireOptimistic('order:123', {
  expectedVersion: 5,
  conflictResolution: 'fail',
  ttl: 5000,
});

if (result.success) {
  await updateOrder(123);
}
```

#### `updateOptimistic(resources, expectedVersion, options?): Promise<OptimisticLockResult>`

Updates an optimistic lock with conflict detection.

**Parameters:**
- `resources` (`string | string[]`) - Resources to update
- `expectedVersion` (`number`) - Expected current version
- `options` (`OptimisticLockOptions`, optional) - Optimistic lock options

**Returns:** `Promise<OptimisticLockResult>`

**Exceptions:**
- `OptimisticLockConflictError` - Version conflict detected

**Example:**
```typescript
const result = await redlockToolkit.updateOptimistic('order:123', 5, {
  ttl: 5000,
});
```

### Hybrid Locking Methods

#### `acquireHybrid(resources, options?): Promise<Lock>`

Acquires a lock using a hybrid strategy that combines pessimistic and optimistic approaches.

**Parameters:**
- `resources` (`string | string[]`) - Resources to lock
- `options` (`HybridLockOptions`, optional) - Hybrid lock options

**Returns:** `Promise<Lock>`

**Exceptions:**
- `HybridLockError` - Both primary and fallback strategies failed

**Example:**
```typescript
const lock = await redlockToolkit.acquireHybrid('resource', {
  primaryStrategy: 'optimistic',
  fallbackStrategy: 'pessimistic',
  ttl: 5000,
});
```

### Cache Methods

#### `enableCache(options?): void`

Enables lock caching for performance optimization.

**Parameters:**
- `options` (`LockCacheOptions`, optional) - Cache options

**Example:**
```typescript
redlockToolkit.enableCache({
  ttl: 5000,
  maxSize: 1000,
  strategy: 'lru',
});
```

#### `disableCache(): void`

Disables lock caching.

#### `getCacheStats(): { size: number; hitRate: number; evictions: number }`

Returns current cache statistics.

**Returns:** Object with cache size, hit rate, and eviction count.

### CountDownLatch Handle Method

#### `getCountDownLatch(name, options?): CountDownLatch`

Gets a handle to an existing CountDownLatch (e.g., from another process). Does not create any keys in Redis — the latch must already exist.

**Parameters:**
- `name` (`string`) - Latch name
- `options` (`{ awaitTimeout?: number; pollInterval?: number }`, optional) - Wait options

**Returns:** `CountDownLatch` - Latch handle

**Example:**
```typescript
// From a different process, get a handle to an existing latch
const latch = redlockToolkit.getCountDownLatch('deploy-ready');
await latch.countDown('worker-api');
```

### Semaphore Methods

#### `acquireSemaphore(resource, options): Promise<SemaphorePermit>`

Acquires a permit from a distributed semaphore. Allows up to `maxPermits` concurrent holders for the same resource.

Uses Redis ZSET internally: member=identifier, score=expirationTimestamp. Expired permits are atomically cleaned on each acquire.

**Parameters:**
- `resource` (`string`) - Semaphore resource name
- `options` (`SemaphoreOptions`) - Semaphore configuration

**Returns:** `Promise<SemaphorePermit>` - Permit object

**Exceptions:**
- `SemaphoreFullError` - All permits are in use (retryable)
- `LockTimeoutError` - Retry attempts exhausted
- `ConsensusError` - Failed to achieve quorum

**Example:**
```typescript
const permit = await redlockToolkit.acquireSemaphore('api-calls', {
  maxPermits: 10,     // Allow 10 concurrent holders
  ttl: 30000,         // 30 second TTL per permit
  retryCount: 3,      // Retry if full
  retryDelay: 500,    // 500ms between retries
  retryJitter: 200,
});

try {
  await callExternalApi();
} finally {
  await permit.release();
}
```

#### `getSemaphoreStatus(resource, maxPermits): Promise<SemaphoreStatus>`

Gets current semaphore status: active permits, holders, and their expiration times.

**Parameters:**
- `resource` (`string`) - Semaphore resource name
- `maxPermits` (`number`) - Max permits (for reporting)

**Returns:** `Promise<SemaphoreStatus>`

**Example:**
```typescript
const status = await redlockToolkit.getSemaphoreStatus('api-calls', 10);
// {
//   resource: 'api-calls',
//   activePermits: 3,
//   maxPermits: 10,
//   holders: [
//     { identifier: 'abc123', expiresAt: 1700000000000 },
//     { identifier: 'def456', expiresAt: 1700000005000 },
//     { identifier: 'ghi789', expiresAt: 1700000003000 },
//   ]
// }
```

### CountDownLatch Methods

#### `createCountDownLatch(name, options): Promise<CountDownLatch>`

Creates a new distributed countdown latch. The latch starts at `count` and must reach 0 for waiters to proceed.

Uses Redis STRING for the atomic counter (DECR), with a LIST for event ID audit trail (idempotency).

**Parameters:**
- `name` (`string`) - Unique latch name
- `options` (`CountDownLatchOptions`) - Latch configuration

**Returns:** `Promise<CountDownLatch>` - Latch object

**Exceptions:**
- `LatchExistsError` - A latch with this name already exists

**Example:**
```typescript
const latch = await redlockToolkit.createCountDownLatch('deploy-ready', {
  count: 3,           // Wait for 3 events
  ttl: 120000,        // 2 minute TTL (safety net)
  awaitTimeout: 60000, // Default timeout for await()
  pollInterval: 100,   // Polling interval when no pub/sub
});
```

#### `getCountDownLatchStatus(name): Promise<CountDownLatchStatus>`

Gets the status of a countdown latch by name (without needing a latch instance).

**Parameters:**
- `name` (`string`) - Latch name

**Returns:** `Promise<CountDownLatchStatus>`

**Example:**
```typescript
const status = await redlockToolkit.getCountDownLatchStatus('deploy-ready');
// { exists: true, remainingCount: 1, targetCount: 3, completed: false, ttl: 95000 }
```

### Metrics Methods

#### `getMetrics(): LockMetrics`

Gets current performance metrics.

**Returns:** Metrics object

#### `getPerformanceSummary(): PerformanceSummary`

Gets a performance summary report.

**Returns:** Summary object

#### `exportMetrics(): string`

Exports metrics in Prometheus format.

**Returns:** String with metrics

#### `resetMetrics(): void`

Resets all collected metrics.

### Maintenance Methods

#### `cleanup(): Promise<number>`

Removes expired locks from Redis using non-blocking SCAN.

**Returns:** Number of removed locks

#### `shutdown(): Promise<void>`

Gracefully shuts down, releasing all active locks and semaphore permits, and disconnecting pub/sub subscribers.

**Shutdown order:**
1. Release all active locks
2. Release all active semaphore permits
3. Shut down pub/sub manager (unsubscribe, disconnect duplicated clients)
4. Clear internal state

---

## Lock Class

### Properties

#### `resources: string[]`
Array of resources that are locked.

#### `identifier: string`
Unique lock identifier.

#### `expiration: number`
Lock expiration time (timestamp).

#### `extensions: number`
Number of lock extensions.

#### `isValid: boolean`
Whether the lock is valid.

#### `isExpired: boolean`
Whether the lock has expired.

#### `released: boolean`
Whether the lock has been released.

#### `timeToExpiration: number`
Time until expiration (milliseconds).

#### `duration: number`
Duration lock has been held (milliseconds).

### Methods

#### `extend(ttl?: number, options?: Partial<LockOptions>): Promise<Lock>`

Extends the lock for the specified time.

**Parameters:**
- `ttl` (`number`, optional) - New time to live in milliseconds. Defaults to the original TTL.
- `options` (`Partial<LockOptions>`, optional) - Lock options

**Returns:** `Promise<Lock>` - The same lock instance for chaining.

**Example:**
```typescript
await lock.extend(30000); // Extend for 30 seconds
```

#### `release(options?: Partial<LockOptions>): Promise<LockReleaseResult>`

Releases the lock.

**Example:**
```typescript
await lock.release();
```

#### `using<T>(routine, options?): Promise<T>`

Executes a function with automatic lock extension.

**Parameters:**
- `routine` (`(signal: LockSignal) => Promise<T>`) - Function to execute
- `options` (`Partial<LockOptions>`, optional) - Options

**Returns:** Result of routine execution

**Example:**
```typescript
const result = await lock.using(async (signal) => {
  // Lock will be automatically extended
  return await longRunningOperation();
});
```

---

## SemaphorePermit Class

Represents a held permit in a distributed semaphore. Analogous to `Lock` for mutex operations.

### Properties

#### `resource: string`
Semaphore resource name.

#### `identifier: string`
Unique permit identifier.

#### `expiration: number`
Permit expiration time (timestamp).

#### `extensions: number`
Number of permit extensions.

#### `released: boolean`
Whether the permit has been released.

#### `isValid: boolean`
Whether the permit is valid (not released and not expired).

#### `isExpired: boolean`
Whether the permit has expired.

#### `timeToExpiration: number`
Time until expiration (milliseconds).

#### `duration: number`
Duration permit has been held (milliseconds).

### Methods

#### `extend(ttl?: number): Promise<SemaphorePermit>`

Extends the permit TTL.

**Parameters:**
- `ttl` (`number`, optional) - New TTL in milliseconds. Defaults to the original TTL.

**Returns:** `Promise<SemaphorePermit>` - Same instance for chaining.

**Exceptions:**
- `PermitExtensionError` - Permit is released, expired, or extension failed on Redis

**Example:**
```typescript
await permit.extend(30000);
console.log(`Extended ${permit.extensions} times`);
```

#### `release(): Promise<{ success: boolean; remainingCount: number }>`

Releases the permit. Idempotent: calling release() on an already-released permit succeeds without error.

**Returns:** Object with success flag and remaining permit count.

**Example:**
```typescript
const result = await permit.release();
// result.success === true
```

#### `using<T>(routine, options?): Promise<T>`

Executes a function with automatic permit extension and release.

**Parameters:**
- `routine` (`(signal: LockSignal) => Promise<T>`) - Function to execute
- `options` (`{ ttl?: number; autoExtendThreshold?: number }`, optional)

**Returns:** Result of routine execution. Permit is automatically released when routine completes (or throws).

**Example:**
```typescript
const result = await permit.using(async (signal) => {
  // Permit is automatically extended while this runs
  await longRunningApiCall();
  return response;
}, { ttl: 5000, autoExtendThreshold: 1000 });
```

---

## CountDownLatch Class

Distributed countdown synchronization primitive. Allows waiting until N events occur across distributed processes.

### Properties

#### `name: string`
Latch name.

#### `targetCount: number`
Original count (N).

### Methods

#### `countDown(eventId?: string): Promise<CountDownResult>`

Decrements the counter by 1. Idempotent per `eventId` — calling with the same eventId multiple times only decrements once.

**Parameters:**
- `eventId` (`string`, optional) - Unique event identifier for idempotency. Auto-generated if not provided.

**Returns:** `Promise<CountDownResult>`

**Example:**
```typescript
const result = await latch.countDown('worker-db-ready');
// {
//   success: true,
//   remainingCount: 2,
//   justCompleted: false
// }

// Last countdown
const final = await latch.countDown('worker-search-ready');
// { success: true, remainingCount: 0, justCompleted: true }
```

#### `await(timeoutMs?: number): Promise<boolean>`

Waits until the count reaches 0 or timeout expires.

**Waiting modes:**
- **Polling** (default): Checks status at `pollInterval` intervals
- **Pub/Sub** (when enabled): Subscribes to Redis channel, gets instant notification when count reaches 0

**Parameters:**
- `timeoutMs` (`number`, optional) - Maximum wait time in milliseconds. Defaults to `awaitTimeout` from options.

**Returns:** `Promise<boolean>` - `true` if completed, never returns `false` (throws on timeout).

**Exceptions:**
- `LatchTimeoutError` - Await timed out before count reached 0
- `LatchNotFoundError` - Latch expired or was deleted during waiting

**Example:**
```typescript
try {
  const completed = await latch.await(60000); // Wait up to 60 seconds
  console.log('All workers ready!');
} catch (error) {
  if (error instanceof LatchTimeoutError) {
    console.log(`Timed out, ${error.remainingCount} workers still pending`);
  }
}
```

#### `getStatus(): Promise<CountDownLatchStatus>`

Gets current latch status.

**Returns:** `Promise<CountDownLatchStatus>`

**Example:**
```typescript
const status = await latch.getStatus();
// {
//   exists: true,
//   remainingCount: 1,
//   targetCount: 3,
//   completed: false,
//   ttl: 55000
// }
```

---

## Interfaces and Types

### `RedlockToolkitConfig`

```typescript
interface RedlockToolkitConfig {
  clients: RedisClient[];
  defaultLockOptions?: Partial<LockOptions>;
  circuitBreaker?: CircuitBreakerOptions;
  enableMetrics?: boolean;
  keyPrefix?: string;
  logger?: Logger | boolean;
  pubSub?: PubSubConfig;
}
```

### `PubSubConfig`

```typescript
interface PubSubConfig {
  /** Enable pub/sub waiting for lock acquisition */
  enabled: boolean;
  /** User-provided subscriber clients (optional; uses client.duplicate() if not provided) */
  subscriberClients?: RedisClient[];
}
```

### `LockOptions`

```typescript
interface LockOptions {
  ttl?: number;                    // Lock time to live (ms), default 30000
  retryCount?: number;             // Number of retry attempts, default 0
  retryDelay?: number;             // Delay between retries (ms), default 200
  retryJitter?: number;            // Random jitter (ms), default 100
  driftFactor?: number;            // Clock drift factor, default 0.01
  autoExtendThreshold?: number;    // Auto-extension threshold (ms), default 500
  identifier?: string;             // Custom lock identifier
}
```

### `OptimisticLockOptions`

```typescript
interface OptimisticLockOptions extends LockOptions {
  expectedVersion?: number;          // Expected version number
  expectedValue?: unknown;           // Expected value for compare-and-swap
  conflictResolution?: 'fail' | 'retry' | 'fallback'; // Conflict strategy
  maxRetries?: number;               // Max retries for conflict resolution
}
```

### `OptimisticLockResult`

```typescript
interface OptimisticLockResult {
  success: boolean;                  // Whether operation succeeded
  currentVersion?: number;           // Current version after operation
  conflict?: boolean;                // Conflict detected
  retries?: number;                  // Number of retries performed
}
```

### `HybridLockOptions`

```typescript
interface HybridLockOptions extends LockOptions, OptimisticLockOptions {
  primaryStrategy?: 'pessimistic' | 'optimistic' | 'adaptive';
  fallbackStrategy?: 'pessimistic' | 'optimistic';
  concurrencyThreshold?: number;     // Threshold for adaptive switching
  metricsWindow?: number;            // Performance metrics window (ms)
}
```

### `LockCacheOptions`

```typescript
interface LockCacheOptions {
  ttl?: number;                      // Cache TTL (ms)
  maxSize?: number;                  // Maximum cache size
  strategy?: 'lru' | 'lfu' | 'ttl'; // Cache eviction strategy
  negativeCaching?: boolean;         // Enable negative caching
}
```

### `SemaphoreOptions`

```typescript
interface SemaphoreOptions {
  maxPermits: number;             // Maximum concurrent permit holders
  ttl?: number;                   // Permit time to live (ms), default 30000
  retryCount?: number;            // Retry attempts when full, default 0
  retryDelay?: number;            // Retry delay (ms), default 200
  retryJitter?: number;           // Retry jitter (ms), default 100
  driftFactor?: number;           // Clock drift factor, default 0.01
  autoExtendThreshold?: number;   // Auto-extend threshold (ms), default 500
  identifier?: string;            // Custom permit identifier
}
```

### `SemaphoreStatus`

```typescript
interface SemaphoreStatus {
  resource: string;               // Resource name
  activePermits: number;          // Current active permit count
  maxPermits: number;             // Maximum permits
  holders: Array<{
    identifier: string;           // Permit holder ID
    expiresAt: number;            // Expiration timestamp
  }>;
}
```

### `CountDownLatchOptions`

```typescript
interface CountDownLatchOptions {
  count: number;                  // Events to count down from
  ttl?: number;                   // Latch TTL (ms), default 60000
  awaitTimeout?: number;          // Default await timeout (ms), default 30000
  pollInterval?: number;          // Polling interval (ms), default 100
}
```

### `CountDownLatchStatus`

```typescript
interface CountDownLatchStatus {
  exists: boolean;                // Whether the latch exists
  remainingCount: number;         // Current remaining count
  targetCount: number;            // Original target count
  completed: boolean;             // Whether count reached 0
  ttl?: number;                   // Remaining TTL (ms)
}
```

### `CountDownResult`

```typescript
interface CountDownResult {
  success: boolean;               // Whether countdown was successful
  remainingCount: number;         // Remaining count after this countdown
  justCompleted: boolean;         // Whether THIS countdown caused completion
}
```

### `CircuitBreakerOptions`

```typescript
interface CircuitBreakerOptions {
  failureThreshold?: number;       // Failures before opening circuit, default 5
  resetTimeout?: number;           // Reset timeout (ms), default 60000
  maxRetries?: number;             // Maximum retries when half-open, default 3
  operationTimeout?: number;       // Operation timeout (ms), default 5000
}
```

### `LockSignal`

```typescript
interface LockSignal {
  readonly aborted: boolean;               // Whether the lock has been aborted
  readonly error?: Error;                  // Error that caused abortion
  readonly expiration: number;             // Current lock expiration timestamp
  addEventListener(type: 'abort', listener: () => void): void;
  removeEventListener(type: 'abort', listener: () => void): void;
}
```

### `LockStatus`

```typescript
interface LockStatus {
  resource: string;               // Resource name
  locked: boolean;                // Whether locked
  holder?: string;                // Lock holder identifier
  ttl?: number;                   // Time to expiration (ms)
}
```

### `LockMetrics`

```typescript
interface LockMetrics {
  locksAcquired: number;          // Total locks acquired
  locksReleased: number;          // Total locks released
  lockExtensions: number;         // Total extensions
  failedAcquisitions: number;     // Failed lock attempts
  averageLockDuration: number;    // Average lock duration (ms)
  activeLocks: number;            // Current active locks count

  circuitBreaker: {
    state: 'closed' | 'open' | 'half-open';
    totalOperations: number;
    successfulOperations: number;
    failedOperations: number;
    lastFailureTime?: number;
    lastOpenTime?: number;
  };
}
```

The `getMetrics()` method returns `LockMetrics` extended with a `detailed` section:

```typescript
{
  // ...LockMetrics fields above
  detailed: {
    acquisitionLatencyStats: HistogramStats;
    releaseLatencyStats: HistogramStats;
    extensionLatencyStats: HistogramStats;
    lockDurationStats: HistogramStats;
    activeLockDetails: Array<{
      identifier: string;
      resources: string[];
      durationMs: number;
      extensions: number;
    }>;
    uptimeMs: number;
  };
}
```

---

## Errors

### `RedlockToolkitError`

Base class for all RedlockToolkit errors.

**Properties:**
- `message: string` - Error message
- `context?: Record<string, unknown>` - Error context
- `timestamp: number` - Error occurrence time

### `ResourceLockedError`

Resource is already locked by another process.

**Properties:**
- `resources: string[]` - Locked resources
- `currentHolder?: string` - Current holder

### `LockTimeoutError`

Lock acquisition timeout.

**Properties:**
- `timeoutMs: number` - Wait time
- `attemptsCount: number` - Number of attempts

### `ConsensusError`

Failed to achieve Redis server quorum.

**Properties:**
- `requiredQuorum: number` - Required quorum
- `achievedVotes: number` - Achieved votes

### `LockExpiredError`

Lock has expired.

**Properties:**
- `expiration: number` - Expiration time
- `currentTime: number` - Current time

### `CircuitBreakerOpenError`

Circuit breaker is open due to failures.

**Properties:**
- `lastFailureTime: number` - Last failure time
- `failureCount: number` - Failure count

### `SemaphoreFullError`

All semaphore permits are in use. This error is **retryable** — the acquire loop will retry with backoff.

**Properties:**
- `resource: string` - Semaphore resource name
- `activePermits: number` - Current active permit count
- `maxPermits: number` - Maximum permits

### `PermitExtensionError`

Failed to extend a semaphore permit (released, expired, or removed from Redis).

**Properties:**
- `resource: string` - Semaphore resource name
- `identifier: string` - Permit identifier

### `LatchExistsError`

A CountDownLatch with the given name already exists.

**Properties:**
- `latchName: string` - Latch name

### `LatchNotFoundError`

CountDownLatch does not exist or has expired.

**Properties:**
- `latchName: string` - Latch name

### `LatchTimeoutError`

Awaiting a CountDownLatch timed out before the count reached 0.

**Properties:**
- `latchName: string` - Latch name
- `remainingCount: number` - Count remaining at timeout
- `timeoutMs: number` - Timeout duration

### `LockReleaseError`

Lock release failed (partial release across Redis nodes).

**Properties:**
- `resources: string[]` - Lock resources
- `identifier: string` - Lock identifier
- `releasedCount: number` - Number of clients that released
- `totalClients: number` - Total number of clients attempted

### `LockExtensionError`

Lock extension failed.

**Properties:**
- `resources: string[]` - Lock resources
- `identifier: string` - Lock identifier
- `currentExpiration: number` - Current expiration timestamp

### `RedisOperationError`

A Redis operation failed on a specific client.

**Properties:**
- `operation: string` - Operation name (e.g., "release", "script_load")
- `client: string` - Client identifier
- `originalError: Error` - Underlying Redis error

### `ConfigurationError`

Invalid configuration was provided.

**Properties:**
- `parameter: string` - Parameter name
- `value: unknown` - Invalid value

### `LockValidationError`

Lock validation check failed.

**Properties:**
- `validationType: string` - Validation type
- `expected: unknown` - Expected value
- `actual: unknown` - Actual value

### `OptimisticLockConflictError`

Optimistic locking detected a version conflict.

**Properties:**
- `resources: string[]` - Lock resources
- `expectedVersion: number` - Expected version
- `currentVersion: number` - Actual current version
- `conflictType: 'version' | 'value' | 'locked'` - Conflict type

### `HybridLockError`

Both primary and fallback locking strategies failed.

**Properties:**
- `primaryStrategy: string` - Primary strategy name
- `fallbackStrategy: string` - Fallback strategy name
- `primaryError: Error` - Error from primary strategy
- `fallbackError?: Error` - Error from fallback strategy (if attempted)

---

## Events

RedlockToolkit inherits EventEmitter and emits the following events:

### `lock:acquired`
```typescript
redlockToolkit.on('lock:acquired', (resources: string[], identifier: string) => {
  console.log(`Lock acquired: ${resources.join(', ')}`);
});
```

### `lock:released`
```typescript
redlockToolkit.on('lock:released', (resources: string[], identifier: string) => {
  console.log(`Lock released: ${resources.join(', ')}`);
});
```

### `lock:extended`
```typescript
redlockToolkit.on('lock:extended', (resources: string[], identifier: string, timestamp: number) => {
  console.log(`Lock extended for ${resources.join(', ')} at ${new Date(timestamp)}`);
});
```

### `lock:failed`
```typescript
redlockToolkit.on('lock:failed', (resources: string[], error: Error) => {
  console.error(`Lock failed for ${resources.join(', ')}:`, error);
});
```

### `circuit:stateChanged`
```typescript
redlockToolkit.on('circuit:stateChanged', (newState: 'closed' | 'open' | 'half-open') => {
  console.log(`Circuit breaker state: ${newState}`);
});
```

### `error`
```typescript
redlockToolkit.on('error', (error: Error) => {
  console.error('RedlockToolkit error:', error);
});
```

---

## Redis Key Patterns

RedlockToolkit uses the following key conventions in Redis:

| Pattern | Type | Purpose |
|---------|------|---------|
| `{prefix}:{resource}` | STRING | Mutex lock (value = identifier) |
| `{prefix}:{resource}:version` | STRING | Optimistic lock version counter |
| `{prefix}:sem:{resource}` | ZSET | Semaphore permits (member=id, score=expiration) |
| `{prefix}:latch:{name}` | STRING | Latch remaining count |
| `{prefix}:latch:{name}:target` | STRING | Latch original target count |
| `{prefix}:latch:{name}:events` | LIST | Latch event IDs (audit/idempotency) |

### Pub/Sub Channels

| Channel | Published When |
|---------|---------------|
| `{prefix}:notify:lock:{resource}` | Lock is released |
| `{prefix}:notify:sem:{resource}` | Semaphore permit is released |
| `{prefix}:notify:latch:{name}` | Latch count reaches 0 |

Default prefix is `neolock`.
