# Advanced Usage of RedlockToolkit

## Important Before You Start: Nested Lock Calls

In its current version, RedlockToolkit operates as a classic owner lock without a reentrant depth counter.
If the same code calls `acquire('resource')` again inside an already locked section, this does **not** create a safe "second level" of lock ownership.

Recommended approach:
- Acquire the lock once at the top level (`using` or `acquire` + `try/finally`).
- Pass control down the call stack without re-acquiring the same resource.
- If you need a stricter `N lock / N unlock` model, use a separate reentrant design.

## Usage Patterns

### 1. Payment Processing

```typescript
import RedlockToolkit, { ResourceLockedError } from '@trishchuk/redlock-toolkit';

class PaymentProcessor {
  constructor(private redlockToolkit: RedlockToolkit) {}

  async processPayment(userId: string, amount: number, orderId: string) {
    // Lock user and order simultaneously
    return await this.redlockToolkit.using(
      [`user:${userId}`, `order:${orderId}`],
      async (signal) => {
        if (signal.aborted) throw signal.error;

        // Check balance
        const balance = await this.getBalance(userId);
        if (balance < amount) {
          throw new Error('Insufficient funds');
        }

        // Atomic deduction
        await this.updateBalance(userId, balance - amount);
        await this.markOrderPaid(orderId);

        // Create transaction
        const transaction = await this.createTransaction({
          userId,
          orderId,
          amount,
          type: 'payment'
        });

        return {
          success: true,
          transactionId: transaction.id,
          newBalance: balance - amount
        };
      },
      {
        ttl: 30000,                  // 30 seconds for processing
        autoExtendThreshold: 5000,   // Extend 5 seconds before expiry
        retryCount: 3,               // 3 retries on conflict
        retryDelay: 1000             // 1 second between retries
      }
    );
  }

  private async getBalance(userId: string): Promise<number> {
    // Balance retrieval implementation
  }

  private async updateBalance(userId: string, newBalance: number): Promise<void> {
    // Balance update implementation
  }

  private async markOrderPaid(orderId: string): Promise<void> {
    // Mark order as paid implementation
  }

  private async createTransaction(data: any): Promise<{ id: string }> {
    // Transaction creation implementation
  }
}
```

### 2. Cache Warming with Fallback

```typescript
class CacheManager {
  constructor(
    private redlockToolkit: RedlockToolkit,
    private cache: Cache,
    private dataLoader: DataLoader
  ) {}

  async getCachedData(key: string): Promise<any> {
    // Check cache first
    let data = await this.cache.get(key);
    if (data) return data;

    // If no data, try to acquire a lock for loading
    try {
      const lock = await this.redlockToolkit.acquire(`cache_warm:${key}`, {
        ttl: 300000,  // 5 minutes for loading
        retryCount: 0  // Don't wait if someone else is loading
      });

      try {
        // Check cache again (someone may have loaded it in the meantime)
        data = await this.cache.get(key);
        if (data) return data;

        // Load data
        console.log(`Loading data for key: ${key}`);
        data = await this.dataLoader.load(key);

        // Cache for 1 hour
        await this.cache.set(key, data, 3600);

        return data;
      } finally {
        await lock.release();
      }
    } catch (error) {
      if (error instanceof ResourceLockedError) {
        // Someone else is loading the data, wait a bit
        console.log(`Waiting for cache to load for: ${key}`);
        await this.sleep(100);

        // Check cache again
        data = await this.cache.get(key);
        if (data) return data;

        // If still not available, load without caching
        return await this.dataLoader.load(key);
      }
      throw error;
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}
```

### 3. Batch Processing with Checkpoints

```typescript
class BatchProcessor {
  constructor(private redlockToolkit: RedlockToolkit) {}

  async processBatch(batchId: string): Promise<BatchResult> {
    return await this.redlockToolkit.using(
      `batch:${batchId}`,
      async (signal) => {
        const batch = await this.getBatch(batchId);
        const results: any[] = [];
        let processed = 0;

        // Resume from checkpoint if available
        const checkpoint = await this.getCheckpoint(batchId);
        const startIndex = checkpoint?.lastProcessedIndex || 0;

        for (let i = startIndex; i < batch.items.length; i++) {
          if (signal.aborted) {
            throw signal.error;
          }

          const item = batch.items[i];

          try {
            const result = await this.processItem(item);
            results.push(result);
            processed++;

            // Checkpoint every 10 items
            if (processed % 10 === 0) {
              await this.saveCheckpoint(batchId, i, results);
              console.log(`Processed ${processed}/${batch.items.length} items`);
            }
          } catch (error) {
            console.error(`Error processing item ${item.id}:`, error);
            results.push({ id: item.id, error: error.message });
          }
        }

        // Final checkpoint
        await this.saveCheckpoint(batchId, batch.items.length - 1, results);
        await this.markBatchComplete(batchId);

        return {
          batchId,
          totalItems: batch.items.length,
          processed,
          results
        };
      },
      {
        ttl: 3600000,                // 1 hour
        autoExtendThreshold: 300000  // Extend 5 minutes before expiry
      }
    );
  }

  private async getBatch(batchId: string) {
    // Batch retrieval implementation
  }

  private async getCheckpoint(batchId: string) {
    // Checkpoint retrieval implementation
  }

  private async saveCheckpoint(batchId: string, index: number, results: any[]) {
    // Checkpoint save implementation
  }

  private async processItem(item: any) {
    // Single item processing implementation
  }

  private async markBatchComplete(batchId: string) {
    // Mark batch as complete implementation
  }
}
```

### 4. Safe Pattern for Nested Services (Without Re-acquiring)

```typescript
class UserService {
  constructor(private redlockToolkit: RedlockToolkit) {}

  async updateUser(userId: string, payload: unknown) {
    return this.redlockToolkit.using(`user:${userId}`, async (signal) => {
      if (signal.aborted) throw signal.error;
      await this.validate(userId, payload);
      await this.writeChanges(userId, payload); // no re-acquire here
    });
  }

  private async validate(userId: string, payload: unknown) {
    // Business validations
  }

  private async writeChanges(userId: string, payload: unknown) {
    // Write to DB/cache
  }
}
```

## Monitoring and Observability

### Prometheus Integration

```typescript
import express from 'express';
import { register } from 'prom-client';

class LockMonitor {
  private app = express();

  constructor(private redlockToolkit: RedlockToolkit) {
    this.setupMetricsEndpoint();
    this.setupHealthCheck();
    this.startPeriodicMetricsReport();
  }

  private setupMetricsEndpoint() {
    this.app.get('/metrics', (req, res) => {
      res.set('Content-Type', 'text/plain');

      // Retrieve RedlockToolkit metrics
      const redlockToolkitMetrics = this.redlockToolkit.exportMetrics();

      // Combine with system metrics
      const systemMetrics = register.metrics();

      res.send(redlockToolkitMetrics + '\n' + systemMetrics);
    });
  }

  private setupHealthCheck() {
    this.app.get('/health', (req, res) => {
      const summary = this.redlockToolkit.getPerformanceSummary();
      const metrics = this.redlockToolkit.getMetrics();

      const health = {
        status: 'healthy' as string,
        timestamp: new Date().toISOString(),
        locks: {
          active: metrics.activeLocks,
          successRate: summary.successRate
        },
        circuitBreaker: summary.circuitBreakerHealth
      };

      // Check critical indicators
      if (summary.successRate < 0.9) {
        health.status = 'degraded';
      }

      if (summary.circuitBreakerHealth.overallState === 'unhealthy') {
        health.status = 'unhealthy';
      }

      const statusCode = health.status === 'healthy' ? 200 : 503;
      res.status(statusCode).json(health);
    });
  }

  private startPeriodicMetricsReport() {
    setInterval(() => {
      const metrics = this.redlockToolkit.getMetrics();
      console.log('RedlockToolkit Metrics:', {
        active_locks: metrics.activeLocks,
        locks_acquired: metrics.locksAcquired,
        locks_released: metrics.locksReleased,
        failed_acquisitions: metrics.failedAcquisitions,
        success_rate: ((metrics.locksAcquired / (metrics.locksAcquired + metrics.failedAcquisitions)) * 100).toFixed(2) + '%'
      });
    }, 60000); // Every minute
  }

  start(port: number = 3000) {
    this.app.listen(port, () => {
      console.log(`Monitoring started on port ${port}`);
      console.log(`Metrics: http://localhost:${port}/metrics`);
      console.log(`Health check: http://localhost:${port}/health`);
    });
  }
}

// Usage
const monitor = new LockMonitor(redlockToolkit);
monitor.start(3000);
```

### Structured Logging

```typescript
import winston from 'winston';

class LockLogger {
  private logger: winston.Logger;

  constructor(private redlockToolkit: RedlockToolkit) {
    this.logger = winston.createLogger({
      format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
      ),
      transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: 'locks.log' })
      ]
    });

    this.setupEventListeners();
  }

  private setupEventListeners() {
    this.redlockToolkit.on('lock:acquired', (resources, identifier) => {
      this.logger.info('Lock acquired', {
        event: 'lock_acquired',
        resources,
        identifier,
        timestamp: new Date().toISOString()
      });
    });

    this.redlockToolkit.on('lock:released', (resources, identifier) => {
      this.logger.info('Lock released', {
        event: 'lock_released',
        resources,
        identifier,
        timestamp: new Date().toISOString()
      });
    });

    this.redlockToolkit.on('lock:failed', (resources, error) => {
      this.logger.error('Lock acquisition failed', {
        event: 'lock_failed',
        resources,
        error: error.message,
        stack: error.stack,
        timestamp: new Date().toISOString()
      });
    });

    this.redlockToolkit.on('circuit:stateChanged', (newState) => {
      this.logger.warn('Circuit Breaker state changed', {
        event: 'circuit_state_changed',
        new_state: newState,
        timestamp: new Date().toISOString()
      });
    });
  }
}
```

## Performance Tuning

### Optimization for High Load

```typescript
const highPerformanceConfig = {
  clients: redisClients, // 5-7 clients for optimal performance
  defaultLockOptions: {
    ttl: 30000,          // Shorter TTL for faster recovery
    retryCount: 3,       // Fewer retries for faster failure
    retryDelay: 50,      // Shorter delay
    retryJitter: 25,     // Smaller jitter
    driftFactor: 0.01    // 1% drift compensation
  },
  circuitBreaker: {
    failureThreshold: 10,   // More tolerant to failures
    resetTimeout: 30000,    // Faster recovery
    maxRetries: 2,          // Fewer retries
    operationTimeout: 2000  // Short operation timeout
  },
  enableMetrics: true
};
```

### Optimization for Reliability

```typescript
const reliabilityConfig = {
  clients: redisClients, // 3-5 clients minimum
  defaultLockOptions: {
    ttl: 60000,          // Longer TTL for stability
    retryCount: 20,      // More retries
    retryDelay: 200,     // Longer delay
    retryJitter: 100,    // Larger jitter
    driftFactor: 0.02,   // 2% drift compensation
    autoExtendThreshold: 10000 // Early extension
  },
  circuitBreaker: {
    failureThreshold: 3,    // Less tolerant
    resetTimeout: 120000,   // Longer recovery
    maxRetries: 5,          // More retries
    operationTimeout: 10000 // Longer timeout
  },
  enableMetrics: true
};
```

## Testing

### Integration Tests

```typescript
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import Redis from 'ioredis';
import RedlockToolkit from '@trishchuk/redlock-toolkit';

describe('RedlockToolkit Integration Tests', () => {
  let redisClients: Redis[];
  let redlockToolkit: RedlockToolkit;

  beforeEach(async () => {
    // Create test Redis clients
    redisClients = [
      new Redis({ host: 'localhost', port: 6379, db: 15 }),
      new Redis({ host: 'localhost', port: 6379, db: 14 }),
      new Redis({ host: 'localhost', port: 6379, db: 13 })
    ];

    // Flush test databases
    await Promise.all(redisClients.map(client => client.flushdb()));

    redlockToolkit = new RedlockToolkit({
      clients: redisClients,
      defaultLockOptions: {
        ttl: 1000,
        retryCount: 3
      }
    });
  });

  afterEach(async () => {
    await redlockToolkit.shutdown();
    await Promise.all(redisClients.map(client => client.quit()));
  });

  it('should acquire and release a lock', async () => {
    const lock = await redlockToolkit.acquire('test-resource');
    expect(lock.isValid).toBe(true);

    await lock.release();
    expect(lock.released).toBe(true);
  });

  it('should prevent double locking', async () => {
    const lock1 = await redlockToolkit.acquire('test-resource');

    await expect(redlockToolkit.acquire('test-resource', { retryCount: 0 }))
      .rejects.toThrow();

    await lock1.release();
  });

  it('should work with auto-extending locks', async () => {
    const result = await redlockToolkit.using('test-resource', async (signal) => {
      expect(signal.aborted).toBe(false);

      // Simulate a long-running operation
      await new Promise(resolve => setTimeout(resolve, 1500));

      return 'success';
    }, {
      ttl: 800,
      autoExtendThreshold: 200
    });

    expect(result).toBe('success');
  });
});
```

### Unit Tests with Mocks

```typescript
import { vi } from 'vitest';

// Mock Redis client
const createMockRedisClient = () => ({
  evalsha: vi.fn().mockResolvedValue(1),
  eval: vi.fn().mockResolvedValue(1),
  script: vi.fn().mockResolvedValue('sha123'),
  options: { host: 'localhost', port: 6379 }
});

describe('RedlockToolkit Unit Tests', () => {
  it('should call Lua script for locking', async () => {
    const mockClients = [
      createMockRedisClient(),
      createMockRedisClient(),
      createMockRedisClient()
    ];

    const redlockToolkit = new RedlockToolkit({
      clients: mockClients as any,
      circuitBreaker: {
        failureThreshold: 1000 // Disabled for tests
      }
    });

    await redlockToolkit.acquire('test-resource');

    // Verify the script was called on all clients
    mockClients.forEach(client => {
      expect(client.evalsha).toHaveBeenCalled();
    });
  });
});
```

## Migration and Versioning

### Migrating from Other Libraries

```typescript
// Migration from node-redlock
class RedlockMigration {
  private redlockToolkit: RedlockToolkit;

  constructor(redisClients: Redis[]) {
    this.redlockToolkit = new RedlockToolkit({
      clients: redisClients,
      defaultLockOptions: {
        ttl: 30000,
        retryCount: 10,
        retryDelay: 200
      }
    });
  }

  // Compatible API with node-redlock
  async lock(resources: string | string[], ttl: number) {
    return await this.redlockToolkit.acquire(resources, { ttl });
  }

  async extend(lock: any, ttl: number) {
    return await lock.extend(ttl);
  }

  async unlock(lock: any) {
    return await lock.release();
  }

  async using(resources: string | string[], ttl: number, routine: Function) {
    return await this.redlockToolkit.using(resources, routine, { ttl });
  }
}
```

### Graceful Deployment

```typescript
import RedlockToolkit, { RedlockToolkitConfig } from '@trishchuk/redlock-toolkit';

class GracefulLockManager {
  private redlockToolkit: RedlockToolkit;
  private isShuttingDown = false;

  constructor(config: RedlockToolkitConfig) {
    this.redlockToolkit = new RedlockToolkit(config);
    this.setupGracefulShutdown();
  }

  private setupGracefulShutdown() {
    const shutdown = async (signal: string) => {
      console.log(`Received signal ${signal}, shutting down...`);
      this.isShuttingDown = true;

      // Wait for active operations to complete
      const activeLocks = this.redlockToolkit.getActiveLocks();
      if (activeLocks.length > 0) {
        console.log(`Waiting for ${activeLocks.length} active locks to complete...`);

        // Allow 30 seconds for completion
        setTimeout(() => {
          console.log('Forced shutdown due to timeout');
          process.exit(1);
        }, 30000);
      }

      await this.redlockToolkit.shutdown();
      console.log('Graceful shutdown complete');
      process.exit(0);
    };

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
  }

  async acquire(resources: string | string[], options?: any) {
    if (this.isShuttingDown) {
      throw new Error('Service is shutting down, new locks are not available');
    }
    return await this.redlockToolkit.acquire(resources, options);
  }

  async using(resources: string | string[], routine: Function, options?: any) {
    if (this.isShuttingDown) {
      throw new Error('Service is shutting down, new operations are not available');
    }
    return await this.redlockToolkit.using(resources, routine, options);
  }
}
```
