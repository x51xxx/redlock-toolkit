# AI Agent Quick Guide - redlock-toolkit

## Overview
redlock-toolkit provides distributed locking for Redis-based applications, ensuring data consistency and preventing race conditions in distributed systems.

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
  
  extend(ttl: number): Promise<Lock>;
  release(): Promise<LockReleaseResult>;
  using<T>(fn: (signal: LockSignal) => Promise<T>): Promise<T>;
}

// Lock configuration
interface LockOptions {
  ttl?: number;              // Lock time-to-live (ms)
  retryCount?: number;       // Max retry attempts
  retryDelay?: number;       // Base delay between retries (ms)
  retryJitter?: number;      // Random jitter to add (ms)
  driftFactor?: number;      // Clock drift compensation (0.01 = 1%)
  autoExtendThreshold?: number; // Auto-extend when X ms remaining
}

// Lock signal for auto-extending locks
interface LockSignal {
  readonly aborted: boolean;  // Lock expired or released
  readonly error?: Error;      // Reason for abortion
  readonly expiration: number; // Current expiration time
  addEventListener(type: 'abort', listener: () => void): void;
}

// Circuit breaker configuration
interface CircuitBreakerOptions {
  failureThreshold?: number;  // Failures before opening circuit
  resetTimeout?: number;       // Time before retry (ms)
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
// Main locking algorithms and patterns
import { Redlock, OptimisticRedlock, CircuitBreaker } from '@trishchuk/redlock-toolkit';
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

// Initialize Redlock with single or multiple Redis instances
// Multiple instances provide fault tolerance (recommended for production)
const redlock = new Redlock([redis], {
  // Clock drift compensation - accounts for time differences between servers
  driftFactor: 0.01, // multiplied by TTL to determine drift time
  
  // Retry configuration for lock acquisition
  retryCount: 3,     // Number of attempts before giving up
  retryDelay: 200,   // Base delay between retries (ms)
  retryJitter: 100   // Random jitter to prevent thundering herd
});
```

### Acquire Lock

#### Manual Lock Management
```typescript
// Acquire lock with manual release - use when you need fine control
const lock = await redlock.acquire(
  ['resource-key'],  // Array of resources to lock atomically
  5000               // TTL in milliseconds - lock auto-expires after this time
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
// Using pattern - automatically handles acquire and release
await redlock.using(
  ['resource-key'],  // Resources to lock
  5000,              // TTL in milliseconds
  async (signal) => {
    // signal is an AbortSignal - check it for lock expiration
    while (!signal.aborted && hasMoreWork()) {
      await doWork();
    }
    
    // Lock is automatically released when this function returns
    // Even if an error is thrown, lock will be released
  }
);
```

### Optimistic Locking
Optimistic locking prevents lost updates when multiple clients modify the same resource. It's ideal for high-read, low-write scenarios.

```typescript
const optimistic = new OptimisticRedlock([redis]);

// First, read current version
const currentVersion = await redis.get('order:123:version');

// Process order with optimistic lock
try {
  const lock = await optimistic.acquire(
    ['order:123'],      // Resource key
    5000,               // TTL
    currentVersion      // Expected version - lock fails if version changed
  );
  
  // Safe to modify - no other process changed the data
  await updateOrder(123);
  
  // Update version for next operation
  await redis.incr('order:123:version');
  
  await lock.release();
} catch (error) {
  // Version mismatch - another process modified the data
  // Retry with fresh data or handle conflict
  console.log('Order was modified by another process');
}
```

### Circuit Breaker Pattern
Circuit breaker prevents cascading failures by failing fast when Redis is unavailable.

```typescript
const breaker = new CircuitBreaker(redlock, {
  // Circuit opens after 5 consecutive failures
  failureThreshold: 5,
  
  // Time to wait before attempting to close circuit (ms)
  resetTimeout: 30000,
  
  // Optional: custom failure detection
  isFailure: (error) => {
    // Don't open circuit for lock contention, only for Redis failures
    return !(error instanceof ResourceLockedError);
  }
});

try {
  // Fails immediately if circuit is open, preventing Redis timeout delays
  const lock = await breaker.acquire(['resource'], 5000);
  await doWork();
  await lock.release();
} catch (error) {
  if (breaker.isOpen()) {
    // Circuit is open - Redis is likely down
    // Use fallback mechanism or queue for later
    await fallbackStrategy();
  }
}

## Key Features

- **Multi-instance support**: Pass array of Redis clients for consensus-based locking
- **Auto-retry**: Built-in retry with exponential backoff and jitter
- **Lock extension**: Dynamically extend locks for long-running operations
- **Metrics**: Built-in performance tracking and monitoring
- **TypeScript**: Full type definitions with IntelliSense support
- **Abort signals**: Graceful handling of lock expiration during execution

## Error Handling

```typescript
import { 
  ResourceLockedError,  // Thrown when resource is already locked
  ExecutionError,       // Thrown when execution fails within lock
  LockError            // Base error class for all lock-related errors
} from '@trishchuk/redlock-toolkit';

// Comprehensive error handling pattern
try {
  const lock = await redlock.acquire(['critical-resource'], 5000);
  
  try {
    await performCriticalOperation();
  } finally {
    await lock.release();
  }
} catch (error) {
  if (error instanceof ResourceLockedError) {
    // Another process holds the lock
    console.log('Resource is busy, retrying later...');
    await scheduleRetry();
  } else if (error instanceof ExecutionError) {
    // Operation failed but lock was properly released
    console.error('Operation failed:', error.cause);
    await rollback();
  } else {
    // Unexpected error (network, Redis connection, etc.)
    console.error('System error:', error);
    await alertOps();
  }
}
```

## Configuration

```typescript
const redlock = new Redlock([redis], {
  // Retry behavior - balance between responsiveness and load
  retryCount: 3,        // Max attempts (increase for critical operations)
  retryDelay: 200,      // Base delay between retries in ms
  retryJitter: 100,     // Random jitter (0-100ms) prevents synchronization
  
  // Clock drift compensation (important for distributed systems)
  driftFactor: 0.01,    // 1% of TTL reserved for clock drift
  
  // Automatic extension for long operations
  automaticExtensionThreshold: 500  // Extend when 500ms remains
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

await redlock.using(['resource'], ttl, async () => {
  await operation();
});
```

### 2. Use Namespace Prefixes
```typescript
// Organize locks with clear namespaces
const locks = {
  user: (id) => [`user:${id}:profile`],
  order: (id) => [`order:${id}:processing`],
  cache: (key) => [`cache:${key}:update`]
};

await redlock.using(locks.user(123), 5000, async () => {
  await updateUserProfile(123);
});
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
  await redlock.using(['high-contention'], 3000, async () => {
    await criticalOperation();
  });
});
```

### 4. Monitor Lock Metrics
```typescript
// Track lock performance
redlock.on('acquire', (lock) => {
  metrics.increment('locks.acquired', { resource: lock.resources[0] });
});

redlock.on('release', (lock) => {
  metrics.histogram('locks.held_time', lock.heldTime);
});

redlock.on('extend', (lock, extension) => {
  metrics.increment('locks.extended');
});
```

## Common Patterns

### Distributed Queue Processing
```typescript
// Ensure only one worker processes each task
async function processQueueItem(taskId: string) {
  try {
    await redlock.using(
      [`queue:task:${taskId}`],  // Unique lock per task
      30000,                      // 30 second timeout for processing
      async (signal) => {
        // Mark task as processing in database
        await db.tasks.update(taskId, { status: 'processing' });
        
        // Process with abort checking
        const result = await processTask(taskId, signal);
        
        // Mark complete only if not aborted
        if (!signal.aborted) {
          await db.tasks.update(taskId, { 
            status: 'completed',
            result 
          });
        }
      }
    );
  } catch (error) {
    if (error instanceof ResourceLockedError) {
      // Task already being processed by another worker
      console.log(`Task ${taskId} already in progress`);
    } else {
      // Processing failed - mark for retry
      await db.tasks.update(taskId, { status: 'failed', error });
    }
  }
}
```

### Cache Invalidation
```typescript
// Prevent cache stampede during updates
async function updateCacheWithLock(cacheKey: string) {
  const lockKey = [`cache:update:${cacheKey}`];
  
  try {
    // Short TTL - cache updates should be fast
    await redlock.using(lockKey, 2000, async () => {
      // Check if another process already updated
      const cached = await redis.get(cacheKey);
      if (cached && Date.now() - cached.timestamp < 1000) {
        return; // Recently updated, skip
      }
      
      // Fetch fresh data
      const data = await fetchFromDatabase();
      
      // Update cache with timestamp
      await redis.setex(cacheKey, 3600, JSON.stringify({
        data,
        timestamp: Date.now()
      }));
    });
  } catch (error) {
    if (error instanceof ResourceLockedError) {
      // Another process is updating - wait and use their result
      await new Promise(r => setTimeout(r, 100));
      return await redis.get(cacheKey);
    }
    throw error;
  }
}
```

### Rate Limiting
```typescript
// Implement sliding window rate limiting
async function rateLimitedOperation(userId: string, limit = 10) {
  const window = 60000; // 1 minute window
  const lockKey = [`ratelimit:${userId}`];
  
  await redlock.using(lockKey, 1000, async () => {
    const now = Date.now();
    const windowStart = now - window;
    
    // Get requests in current window
    const requests = await redis.zrangebyscore(
      `requests:${userId}`,
      windowStart,
      now
    );
    
    if (requests.length >= limit) {
      throw new Error('Rate limit exceeded');
    }
    
    // Add current request
    await redis.zadd(`requests:${userId}`, now, `${now}:${uuid()}`);
    
    // Clean old entries
    await redis.zremrangebyscore(`requests:${userId}`, 0, windowStart);
  });
  
  // Proceed with operation
  await performOperation();
}
```

### Database Migrations
```typescript
// Ensure only one instance runs migrations
async function runMigrations() {
  const lockKey = ['db:migrations'];
  const ttl = 300000; // 5 minutes for migrations
  
  try {
    await redlock.using(lockKey, ttl, async (signal) => {
      console.log('Acquired migration lock, starting migrations...');
      
      // Check current version
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
    });
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

### 2. Batch Operations
```typescript
// Lock multiple resources atomically for batch operations
await redlock.using(
  ['resource1', 'resource2', 'resource3'], // Multiple locks
  5000,
  async () => {
    // All resources locked - safe to perform batch operation
    await batchUpdate();
  }
);
```

### 3. Lock-Free Reads
```typescript
// Use optimistic patterns for read-heavy workloads
async function readWithOptimisticLock(key: string) {
  // Read without lock
  const data = await redis.get(key);
  const version = await redis.get(`${key}:version`);
  
  // Process data
  const processed = await processData(data);
  
  // Only lock for writes
  const lock = await optimistic.acquire([key], 1000, version);
  try {
    await redis.set(key, processed);
    await redis.incr(`${key}:version`);
  } finally {
    await lock.release();
  }
}
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