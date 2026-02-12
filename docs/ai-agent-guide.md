# AI Agent Quick Guide - redlock-toolkit

## Overview
redlock-toolkit provides distributed locking and synchronization primitives for Redis-based applications, ensuring data consistency and preventing race conditions in distributed systems.

## Key Interfaces

### Core Types
```typescript
// Main lock interface
interface Lock {
  resources: string[];        // Locked resource keys
  identifier: string;         // Unique lock ID
  expiration: number;        // Unix timestamp when lock expires
  duration: number;          // How long lock has been held (ms)
  isValid: boolean;          // Not expired and not released

  extend(ttl?: number, options?: Partial<LockOptions>): Promise<Lock>;
  release(options?: Partial<LockOptions>): Promise<LockReleaseResult>;
  using<T>(fn: (signal: LockSignal) => Promise<T>, options?: Partial<LockOptions>): Promise<T>;
}

// Semaphore permit interface
interface SemaphorePermit {
  resource: string;           // Semaphore resource name
  identifier: string;         // Unique permit ID
  expiration: number;         // Unix timestamp when permit expires
  extensions: number;         // Number of extensions performed
  released: boolean;          // Whether permit has been released
  isValid: boolean;           // Not expired and not released
  isExpired: boolean;         // Whether permit has expired
  timeToExpiration: number;   // Milliseconds until expiration

  extend(ttl?: number): Promise<SemaphorePermit>;
  release(): Promise<{ success: boolean; remainingCount: number }>;
  using<T>(fn: (signal: LockSignal) => Promise<T>, options?: { ttl?: number; autoExtendThreshold?: number }): Promise<T>;
}

// CountDownLatch interface
interface CountDownLatch {
  name: string;               // Latch name
  targetCount: number;        // Original count (N)

  countDown(eventId?: string): Promise<CountDownResult>;
  await(timeoutMs?: number): Promise<boolean>;
  getStatus(): Promise<CountDownLatchStatus>;
}

// Lock configuration
interface LockOptions {
  ttl?: number;              // Lock time-to-live (ms)
  retryCount?: number;       // Max retry attempts
  retryDelay?: number;       // Base delay between retries (ms)
  retryJitter?: number;      // Random jitter to add (ms)
  driftFactor?: number;      // Clock drift compensation (0.01 = 1%)
  autoExtendThreshold?: number; // Auto-extend when X ms remaining
  identifier?: string;       // Custom lock identifier
}

// Semaphore configuration
interface SemaphoreOptions {
  maxPermits: number;         // Max concurrent permit holders
  ttl?: number;               // Permit TTL (ms)
  retryCount?: number;        // Retry attempts when full
  retryDelay?: number;        // Retry delay (ms)
  retryJitter?: number;       // Retry jitter (ms)
  driftFactor?: number;       // Clock drift factor
  autoExtendThreshold?: number; // Auto-extend threshold (ms)
  identifier?: string;        // Custom permit identifier
}

// CountDownLatch configuration
interface CountDownLatchOptions {
  count: number;              // Events to count down from
  ttl?: number;               // Latch TTL (ms), expires if not completed
  awaitTimeout?: number;      // Default await timeout (ms)
  pollInterval?: number;      // Polling interval when waiting (ms)
}

// Pub/Sub configuration
interface PubSubConfig {
  enabled: boolean;           // Enable pub/sub waiting
  subscriberClients?: RedisClient[]; // Optional dedicated subscriber connections
}

// Lock signal for auto-extending locks
interface LockSignal {
  readonly aborted: boolean;  // Lock expired or released
  readonly error?: Error;      // Reason for abortion
  readonly expiration: number; // Current expiration time
  addEventListener(type: 'abort', listener: () => void): void;
  removeEventListener(type: 'abort', listener: () => void): void;
}

// Circuit breaker configuration
interface CircuitBreakerOptions {
  failureThreshold?: number;  // Failures before opening circuit
  resetTimeout?: number;       // Time before retry (ms)
  maxRetries?: number;         // Max retries when half-open
  operationTimeout?: number;   // Max operation time (ms)
}

// Optimistic lock options
interface OptimisticLockOptions extends LockOptions {
  expectedVersion?: number;    // Expected version number
  expectedValue?: any;         // Expected value for CAS
  conflictResolution?: 'fail' | 'retry' | 'fallback';
  maxRetries?: number;         // Max conflict retries
}
```

## Installation
```bash
npm install @trishchuk/redlock-toolkit
```

## Basic Usage

### Import
```typescript
// Main locking class, primitives, and errors
import RedlockToolkit, {
  SemaphorePermit,
  CountDownLatch,
  ResourceLockedError,
  LockTimeoutError,
  ConsensusError,
  CircuitBreakerOpenError,
  SemaphoreFullError,
  LatchExistsError,
  LatchTimeoutError,
  RedlockToolkitError
} from '@trishchuk/redlock-toolkit';
import Redis from 'ioredis';
```

### Initialize Client
```typescript
// Create Redis connection - use connection pool for production
const redis = new Redis({
  host: 'localhost',
  port: 6379,
  retryStrategy: (times) => Math.min(times * 50, 2000) // Exponential backoff
});

// Initialize RedlockToolkit with single or multiple Redis instances
// Multiple instances provide fault tolerance (recommended for production)
const redlock = new RedlockToolkit({
  clients: [redis],
  defaultLockOptions: {
    ttl: 30000,
    retryCount: 3,
    retryDelay: 200,
    retryJitter: 100,
    driftFactor: 0.01,
  },
  // Optional: enable pub/sub for faster lock release notifications
  pubSub: {
    enabled: true,
    // subscriberClients: [subscriberRedis], // optional dedicated connections
  },
});
```

### Acquire Lock

#### Manual Lock Management
```typescript
// Acquire lock with manual release - use when you need fine control
const lock = await redlock.acquire(
  'resource-key',    // Resource to lock (string or string[])
  { ttl: 5000 }     // Options
);

try {
  // Critical section - only one process can execute this code
  await doWork();

  // Extend lock if operation takes longer than expected
  if (needMoreTime) {
    await lock.extend(3000); // Extend by 3 seconds
  }
} finally {
  // Always release lock in finally block to avoid deadlocks
  await lock.release();
}
```

#### Automatic Lock Management (Recommended)
```typescript
// Using pattern - automatically handles acquire, extend, and release
const result = await redlock.using(
  'resource-key',      // Resources to lock
  async (signal) => {
    // signal tracks lock validity - check it for long operations
    while (!signal.aborted && hasMoreWork()) {
      await doWork();
    }
    return 'done';
    // Lock is automatically released when this function returns
  },
  { ttl: 5000, autoExtendThreshold: 1000 }
);
```

### Distributed Semaphore

Semaphore allows N concurrent holders (vs mutex which allows exactly 1). Backed by Redis ZSET with score=expirationTimestamp for atomic expired-permit cleanup.

```typescript
// Acquire a permit (up to maxPermits concurrent holders)
const permit = await redlock.acquireSemaphore('api-rate-limit', {
  maxPermits: 5,        // Allow 5 concurrent callers
  ttl: 30000,           // 30 second permit TTL
  retryCount: 3,        // Retry if full
  retryDelay: 200,
});

try {
  await callExternalApi();
} finally {
  await permit.release();
}

// Auto-extending semaphore with using()
const result = await permit.using(async (signal) => {
  // Permit is automatically extended while this runs
  await longRunningApiCall();
  return response;
});

// Check semaphore status
const status = await redlock.getSemaphoreStatus('api-rate-limit', 5);
// { resource: 'api-rate-limit', activePermits: 3, maxPermits: 5, holders: [...] }
```

### Distributed CountDownLatch

CountDownLatch synchronizes N distributed processes. One or more waiters block until the count reaches 0.

```typescript
// Create a latch that waits for 3 events
const latch = await redlock.createCountDownLatch('migration-ready', {
  count: 3,
  ttl: 120000,          // 2 minute TTL (safety net)
  pollInterval: 100,    // Polling interval for await()
});

// Each worker counts down when ready (idempotent per eventId)
await latch.countDown('worker-db');
await latch.countDown('worker-cache');
await latch.countDown('worker-search');

// Waiter blocks until count=0 or timeout
const completed = await latch.await(60000); // 60s timeout
// completed === true

// Check status
const status = await latch.getStatus();
// { exists: true, remainingCount: 0, targetCount: 3, completed: true, ttl: 115000 }

// Get status by name (without latch instance)
const status2 = await redlock.getCountDownLatchStatus('migration-ready');
```

### Optimistic Locking
Optimistic locking prevents lost updates when multiple clients modify the same resource. It's ideal for high-read, low-write scenarios.

```typescript
// Optimistic locking is available through the RedlockToolkit API
const result = await redlock.acquireOptimistic('order:123', {
  expectedVersion: 5,                   // Expected version
  ttl: 5000,
  conflictResolution: 'fail',           // 'fail' | 'retry' | 'fallback'
});

if (result.success) {
  // Safe to modify - no other process changed the data
  await updateOrder(123);

  // Update with new expected version
  await redlock.updateOptimistic('order:123', result.currentVersion!, {
    ttl: 5000,
  });
} else if (result.conflict) {
  // Version mismatch - another process modified the data
  console.log(`Order was modified: expected v5, found v${result.currentVersion}`);
}
```

### Circuit Breaker Pattern
The circuit breaker is built into `RedlockToolkit` and prevents cascading failures by failing fast when Redis is unavailable.

```typescript
// Circuit breaker is configured at the toolkit level
const redlock = new RedlockToolkit({
  clients: [redis],
  circuitBreaker: {
    failureThreshold: 5,   // Opens after 5 consecutive failures
    resetTimeout: 30000,   // Wait 30s before half-open test
    maxRetries: 3,         // Retries when half-open
    operationTimeout: 5000 // Per-operation timeout
  }
});

// Monitor circuit breaker state changes
redlock.on('circuit:stateChanged', (newState) => {
  console.log(`Circuit breaker state: ${newState}`);
  if (newState === 'open') {
    console.error('Redis appears unavailable - using fallback');
  }
});

try {
  // Fails immediately with CircuitBreakerOpenError if circuit is open
  const lock = await redlock.acquire('resource', { ttl: 5000 });
  await doWork();
  await lock.release();
} catch (error) {
  if (error instanceof CircuitBreakerOpenError) {
    // Circuit is open - Redis is likely down
    await fallbackStrategy();
  }
}
```

## Key Features

- **Distributed Mutex**: Redlock-algorithm based distributed locking with quorum consensus
- **Distributed Semaphore**: N-permit concurrent access control backed by Redis ZSET
- **CountDownLatch**: Wait for N distributed events before proceeding
- **Pub/Sub Waiting**: Optional instant lock-release notifications (vs polling)
- **Multi-instance support**: Pass array of Redis clients for consensus-based locking
- **Auto-retry**: Built-in retry with exponential backoff and jitter
- **Lock extension**: Dynamically extend locks/permits for long-running operations
- **Circuit Breaker**: Fault tolerance against Redis failures
- **Metrics**: Built-in performance tracking and Prometheus export
- **TypeScript**: Full type definitions with IntelliSense support
- **Abort signals**: Graceful handling of lock expiration during execution

## Error Handling

```typescript
import RedlockToolkit, {
  ResourceLockedError,
  LockTimeoutError,
  ConsensusError,
  SemaphoreFullError,
  PermitExtensionError,
  LatchExistsError,
  LatchNotFoundError,
  LatchTimeoutError,
  RedlockToolkitError
} from '@trishchuk/redlock-toolkit';

// Comprehensive error handling pattern
try {
  const lock = await redlock.acquire('critical-resource', { ttl: 5000 });

  try {
    await performCriticalOperation();
  } finally {
    await lock.release();
  }
} catch (error) {
  if (error instanceof ResourceLockedError) {
    // Another process holds the lock
    console.log('Resource is busy, retrying later...');
  } else if (error instanceof LockTimeoutError) {
    // Failed to acquire lock within the given time and retries
    console.error(`Could not acquire lock after ${error.attemptsCount} attempts.`);
  } else if (error instanceof ConsensusError) {
    // Not enough redis instances agreed to grant the lock
    console.error(`Failed to achieve quorum. Required: ${error.requiredQuorum}, got: ${error.achievedVotes}`);
  } else if (error instanceof SemaphoreFullError) {
    // All permits are taken
    console.log(`Semaphore full: ${error.activePermits}/${error.maxPermits}`);
  } else if (error instanceof LatchTimeoutError) {
    // Latch await timed out
    console.log(`Latch timed out with ${error.remainingCount} remaining`);
  } else if (error instanceof RedlockToolkitError) {
    // A known error from the library
    console.error('A managed error occurred:', error);
  } else {
    // Unexpected error (network, Redis connection, etc.)
    console.error('System error:', error);
  }
}
```

## Configuration

```typescript
const redlock = new RedlockToolkit({
  clients: [redis1, redis2, redis3],

  defaultLockOptions: {
    ttl: 30000,
    retryCount: 3,        // Max attempts (increase for critical operations)
    retryDelay: 200,      // Base delay between retries in ms
    retryJitter: 100,     // Random jitter (0-100ms) prevents synchronization
    driftFactor: 0.01,    // 1% of TTL reserved for clock drift
    autoExtendThreshold: 500, // Extend when 500ms remains
  },

  circuitBreaker: {
    failureThreshold: 5,
    resetTimeout: 60000,
    maxRetries: 3,
    operationTimeout: 5000,
  },

  // Pub/Sub: instant notifications instead of polling delays
  pubSub: {
    enabled: true,
    // subscriberClients: [subRedis1, subRedis2, subRedis3],
  },

  keyPrefix: 'neolock',     // Key prefix in Redis
  enableMetrics: true,       // Enable performance tracking
});
```

## Best Practices

### 1. Choose Appropriate TTL
```typescript
// Too short: Risk of lock expiring during operation
// Too long: Other processes wait unnecessarily if holder crashes

// Good: Estimate operation time and add buffer
const expectedTime = 2000;  // 2 seconds expected
const buffer = 1000;         // 1 second buffer
const ttl = expectedTime + buffer;

await redlock.using('resource', async () => {
  await operation();
}, { ttl });
```

### 2. Use Namespace Prefixes
```typescript
// Organize locks with clear namespaces
const locks = {
  user: (id) => `user:${id}:profile`,
  order: (id) => `order:${id}:processing`,
  cache: (key) => `cache:${key}:update`
};

await redlock.using(locks.user(123), async () => {
  await updateUserProfile(123);
}, { ttl: 5000 });
```

### 3. Handle Lock Contention
```typescript
// Implement backoff strategy for high-contention resources
async function withBackoff(fn, maxAttempts = 5) {
  for (let i = 0; i < maxAttempts; i++) {
    try {
      return await fn();
    } catch (error) {
      if (error instanceof ResourceLockedError && i < maxAttempts - 1) {
        // Exponential backoff with jitter
        const delay = Math.min(1000 * Math.pow(2, i), 10000);
        const jitter = Math.random() * 1000;
        await new Promise(r => setTimeout(r, delay + jitter));
      } else {
        throw error;
      }
    }
  }
}

// Usage
await withBackoff(async () => {
  await redlock.using('high-contention', async () => {
    await criticalOperation();
  }, { ttl: 3000 });
});
```

### 4. Monitor Lock Metrics
```typescript
// Track lock performance
redlock.on('lock:acquired', (resources, identifier) => {
  metrics.increment('locks.acquired', { resource: resources[0] });
});

redlock.on('lock:released', (resources, identifier) => {
  metrics.increment('locks.released');
});

redlock.on('lock:extended', (resources, identifier, timestamp) => {
  metrics.increment('locks.extended');
});

// Export Prometheus metrics
const prometheusText = redlock.exportMetrics();
```

## Common Patterns

### Distributed Queue Processing
```typescript
// Ensure only one worker processes each task
async function processQueueItem(taskId: string) {
  try {
    await redlock.using(
      `queue:task:${taskId}`,    // Unique lock per task
      async (signal) => {
        await db.tasks.update(taskId, { status: 'processing' });

        const result = await processTask(taskId, signal);

        if (!signal.aborted) {
          await db.tasks.update(taskId, {
            status: 'completed',
            result
          });
        }
      },
      { ttl: 30000 }
    );
  } catch (error) {
    if (error instanceof ResourceLockedError) {
      console.log(`Task ${taskId} already in progress`);
    } else {
      await db.tasks.update(taskId, { status: 'failed', error });
    }
  }
}
```

### Rate Limiting with Semaphore
```typescript
// Limit concurrent external API calls to 10
async function callExternalApi(request: Request) {
  const permit = await redlock.acquireSemaphore('external-api', {
    maxPermits: 10,
    ttl: 30000,
    retryCount: 5,
    retryDelay: 500,
  });

  try {
    return await fetch(request);
  } finally {
    await permit.release();
  }
}
```

### Multi-Service Coordination with CountDownLatch
```typescript
// Wait for all services to be ready before starting
async function coordinateStartup() {
  const latch = await redlock.createCountDownLatch('services-ready', {
    count: 3,
    ttl: 120000,
  });

  // Each service calls countDown when initialized
  // Service A: await latch.countDown('service-a');
  // Service B: await latch.countDown('service-b');
  // Service C: await latch.countDown('service-c');

  // Main coordinator waits for all services
  const allReady = await latch.await(60000);
  if (allReady) {
    console.log('All services ready, starting processing');
  }
}
```

### Cache Invalidation
```typescript
// Prevent cache stampede during updates
async function updateCacheWithLock(cacheKey: string) {
  try {
    await redlock.using(`cache:update:${cacheKey}`, async () => {
      const raw = await redis.get(cacheKey);
      if (raw) {
        const parsed = JSON.parse(raw);
        if (Date.now() - parsed.timestamp < 1000) {
          return; // Recently updated, skip
        }
      }

      const data = await fetchFromDatabase();

      await redis.setex(cacheKey, 3600, JSON.stringify({
        data,
        timestamp: Date.now()
      }));
    }, { ttl: 2000 });
  } catch (error) {
    if (error instanceof ResourceLockedError) {
      await new Promise(r => setTimeout(r, 100));
      return await redis.get(cacheKey);
    }
    throw error;
  }
}
```

### Database Migrations
```typescript
// Ensure only one instance runs migrations
async function runMigrations() {
  try {
    await redlock.using('db:migrations', async (signal) => {
      console.log('Acquired migration lock, starting migrations...');

      const currentVersion = await db.getVersion();
      const migrations = await getMigrationsSince(currentVersion);

      for (const migration of migrations) {
        if (signal.aborted) {
          throw new Error('Migration lock expired');
        }

        console.log(`Running migration: ${migration.version}`);
        await migration.up();
        await db.setVersion(migration.version);
      }

      console.log('Migrations completed successfully');
    }, { ttl: 300000 }); // 5 minutes for migrations
  } catch (error) {
    if (error instanceof ResourceLockedError) {
      console.log('Migrations already running on another instance');
    } else {
      console.error('Migration failed:', error);
      throw error;
    }
  }
}
```

## Performance Optimization

### 1. Connection Pooling
```typescript
// Use connection pool for better performance
const redis = new Redis.Cluster([
  { host: 'redis1', port: 6379 },
  { host: 'redis2', port: 6379 },
  { host: 'redis3', port: 6379 }
], {
  redisOptions: {
    connectionPoolSize: 10,
    lazyConnect: true
  }
});
```

### 2. Enable Pub/Sub for Faster Retry
```typescript
// With pub/sub, lock waiters get instant notifications instead of polling
const redlock = new RedlockToolkit({
  clients: [redis],
  pubSub: { enabled: true }, // Uses client.duplicate() automatically
});
// Now retrying acquires react immediately when a lock is released
```

### 3. Batch Operations
```typescript
// Lock multiple resources atomically for batch operations
await redlock.using(
  ['resource1', 'resource2', 'resource3'], // Multiple locks
  async () => {
    await batchUpdate();
  },
  { ttl: 5000 }
);
```

## Troubleshooting

### Common Issues and Solutions

1. **Lock Timeout During Long Operations**
   - Solution: Increase TTL or use automatic extension
   - Monitor signal.aborted to detect expiration

2. **High Lock Contention**
   - Solution: Implement exponential backoff
   - Consider sharding resources to reduce contention

3. **Redis Connection Failures**
   - Solution: Use Circuit Breaker pattern
   - Implement fallback strategies

4. **Clock Drift Issues**
   - Solution: Increase driftFactor setting
   - Ensure NTP synchronization on all servers

5. **Memory Leaks from Unreleased Locks**
   - Solution: Always use try/finally or using() pattern
   - Set appropriate TTLs as safety net

6. **Semaphore Permits Not Released**
   - Solution: Use `permit.using()` for auto-release, or try/finally
   - `shutdown()` releases all active permits automatically

7. **CountDownLatch Expired Before Completion**
   - Solution: Increase TTL to cover the maximum expected coordination time
   - Monitor status with `getStatus()` to detect stalled workers
