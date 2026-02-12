# RedlockToolkit Troubleshooting

## Common Problems and Solutions

### 1. Lock Not Acquired

**Symptoms:**
- `ResourceLockedError` or `LockTimeoutError`
- Long wait times

**Possible Causes and Solutions:**

#### Cause: Resource is genuinely locked
```typescript
try {
  const lock = await toolkit.acquire('resource');
} catch (error) {
  if (error instanceof ResourceLockedError) {
    console.log(`Resource locked by owner: ${error.currentHolder}`);

    // Check lock status
    const status = await toolkit.getStatus(['resource']);
    console.log('Status:', status[0]);

    // Consider waiting or using a different resource
  }
}
```

#### Cause: Stale lock
```typescript
// Check and clean up stale locks
const cleaned = await toolkit.cleanup();
console.log(`Cleaned up ${cleaned} stale locks`);

// Force release a specific resource (use with caution!)
await toolkit.forceRelease(['resource']);
```

#### Cause: Incorrect retry configuration
```typescript
// Increase retry count and delay
const lock = await toolkit.acquire('resource', {
  retryCount: 20,        // More attempts
  retryDelay: 500,       // Longer delay
  retryJitter: 200       // Larger jitter
});
```

### 2. Circuit Breaker Opens Too Frequently

**Symptoms:**
- `CircuitBreakerOpenError`
- Periodic operation failures

**Solution:**

#### Configuring Circuit Breaker
```typescript
const toolkit = new RedlockToolkit({
  clients: redisClients,
  circuitBreaker: {
    failureThreshold: 10,    // Increase threshold
    resetTimeout: 120000,    // Increase recovery time
    maxRetries: 5,           // More retries
    operationTimeout: 10000  // Longer timeout
  }
});
```

#### Monitoring Circuit Breaker State
```typescript
toolkit.on('circuit:stateChanged', (newState) => {
  console.log(`Circuit Breaker state: ${newState}`);

  if (newState === 'open') {
    // Send notification or trigger emergency procedures
    console.error('⚠️ Circuit Breaker open - check Redis connections');
  }
});

// Check Circuit Breaker health
const summary = toolkit.getPerformanceSummary();
console.log('Circuit Breaker health:', summary.circuitBreakerHealth);
```

### 3. Lock Expires Prematurely

**Symptoms:**
- `LockExpiredError` during operation execution
- Operations getting interrupted

**Solution:**

#### Increasing TTL and Auto-Extension
```typescript
await toolkit.using('resource', async (signal) => {
  // Long-running operation
  await longRunningOperation();
}, {
  ttl: 300000,                 // 5 minutes
  autoExtendThreshold: 60000   // Extend one minute before expiration
});
```

#### Manual Extension
```typescript
const lock = await toolkit.acquire('resource', { ttl: 30000 });

try {
  for (const item of items) {
    // Check time remaining until expiration
    if (lock.timeToExpiration < 10000) {
      await lock.extend(30000);
      console.log('Lock extended');
    }

    await processItem(item);
  }
} finally {
  await lock.release();
}
```

### 4. Performance Issues

**Symptoms:**
- Slow lock acquisition
- High memory consumption
- High latency

**Solution:**

#### Optimizing Configuration
```typescript
// For high performance
const fastConfig = {
  clients: redisClients,
  defaultLockOptions: {
    ttl: 10000,        // Shorter TTL
    retryCount: 3,     // Fewer retries
    retryDelay: 50,    // Shorter delay
    retryJitter: 25    // Smaller jitter
  },
  enableMetrics: false // Disable metrics for speed
};
```

#### Monitoring Performance
```typescript
// Periodic performance report
setInterval(() => {
  const summary = toolkit.getPerformanceSummary();

  if (summary.averageAcquisitionLatency > 1000) {
    console.warn(`⚠️ High latency: ${summary.averageAcquisitionLatency}ms`);
  }

  if (summary.successRate < 0.95) {
    console.warn(`⚠️ Low success rate: ${summary.successRate * 100}%`);
  }
}, 60000);
```

### 5. Redis Connection Issues

**Symptoms:**
- Connection errors
- Unstable operation

**Solution:**

#### Configuring Redis Clients
```typescript
import Redis from 'ioredis';

const redisConfig = {
  host: 'redis-server',
  port: 6379,
  connectTimeout: 10000,       // 10 seconds for connection
  commandTimeout: 5000,        // 5 seconds per command
  retryDelayOnFailover: 100,   // Delay on failover
  maxRetriesPerRequest: 3,     // Maximum retries
  lazyConnect: true,           // Lazy connection

  // Connection pool settings
  family: 4,
  keepAlive: true,

  // Event handlers
  retryStrategy: (times) => {
    const delay = Math.min(times * 50, 2000);
    console.log(`Redis retry ${times}, delay: ${delay}ms`);
    return delay;
  }
};

const clients = [
  new Redis({ ...redisConfig, host: 'redis1' }),
  new Redis({ ...redisConfig, host: 'redis2' }),
  new Redis({ ...redisConfig, host: 'redis3' })
];

// Monitor connections
clients.forEach((client, index) => {
  client.on('connect', () => {
    console.log(`✅ Redis ${index + 1} connected`);
  });

  client.on('error', (error) => {
    console.error(`❌ Redis ${index + 1} error:`, error.message);
  });

  client.on('close', () => {
    console.warn(`⚠️ Redis ${index + 1} connection closed`);
  });
});
```

### 6. Consensus Issues

**Symptoms:**
- `ConsensusError`
- Unstable lock acquisition

**Solution:**

#### Analyzing Network Topology
```typescript
// Diagnostics for Redis servers
async function diagnoseRedisCluster(clients: Redis[]) {
  const results = await Promise.allSettled(
    clients.map(async (client, index) => {
      const start = Date.now();
      await client.ping();
      const latency = Date.now() - start;

      return {
        client: index,
        latency,
        status: 'ok'
      };
    })
  );

  results.forEach((result, index) => {
    if (result.status === 'fulfilled') {
      console.log(`Redis ${index}: ${result.value.latency}ms`);
    } else {
      console.error(`Redis ${index}: ERROR - ${result.reason.message}`);
    }
  });
}

// Run diagnostics before critical operations
await diagnoseRedisCluster(clients);
```

#### Adaptive Quorum Configuration
```typescript
class AdaptiveRedlockToolkit {
  private toolkit: RedlockToolkit;
  private healthyClients: Set<number> = new Set();
  private totalClients: number;

  constructor(clients: Redis[]) {
    this.toolkit = new RedlockToolkit({ clients });
    this.totalClients = clients.length;
    this.monitorClientHealth(clients);
  }

  private monitorClientHealth(clients: Redis[]) {
    clients.forEach((client, index) => {
      client.on('connect', () => this.healthyClients.add(index));
      client.on('error', () => this.healthyClients.delete(index));
      client.on('close', () => this.healthyClients.delete(index));
    });
  }

  async acquire(resource: string, options: any = {}) {
    const healthyCount = this.healthyClients.size;
    const totalCount = this.totalClients;

    if (healthyCount < Math.floor(totalCount / 2) + 1) {
      throw new Error(`Not enough healthy Redis servers: ${healthyCount}/${totalCount}`);
    }

    return await this.toolkit.acquire(resource, options);
  }
}
```

## Diagnostic Tools

### 1. Health Check

```typescript
class LockHealthChecker {
  constructor(private toolkit: RedlockToolkit) {}

  async performHealthCheck(): Promise<HealthReport> {
    const report: HealthReport = {
      timestamp: new Date().toISOString(),
      overall: 'healthy',
      details: {}
    };

    try {
      // Basic locking test
      const testLock = await this.toolkit.acquire('health_check_test', {
        ttl: 5000,
        retryCount: 1
      });
      await testLock.release();
      report.details.basicLocking = 'ok';
    } catch (error) {
      report.details.basicLocking = 'failed';
      report.overall = 'unhealthy';
    }

    // Check metrics
    const metrics = this.toolkit.getMetrics();
    report.details.metrics = {
      activeLocks: metrics.activeLocks,
      successRate: metrics.locksAcquired / (metrics.locksAcquired + metrics.failedAcquisitions)
    };

    // Check Circuit Breaker
    const summary = this.toolkit.getPerformanceSummary();
    report.details.circuitBreaker = summary.circuitBreakerHealth;

    if (summary.circuitBreakerHealth.overallState !== 'healthy') {
      report.overall = 'degraded';
    }

    return report;
  }
}

interface HealthReport {
  timestamp: string;
  overall: 'healthy' | 'degraded' | 'unhealthy';
  details: Record<string, any>;
}
```

### 2. Diagnostic Logs

```typescript
class LockDiagnostics {
  constructor(private toolkit: RedlockToolkit) {
    this.setupDetailedLogging();
  }

  private setupDetailedLogging() {
    this.toolkit.on('lock:acquired', (resources, identifier) => {
      console.log(`🔒 ACQUIRED [${identifier.slice(0, 8)}] ${resources.join(', ')}`);
    });

    this.toolkit.on('lock:released', (resources, identifier) => {
      console.log(`🔓 RELEASED [${identifier.slice(0, 8)}] ${resources.join(', ')}`);
    });

    this.toolkit.on('lock:extended', (resources, identifier, timestamp) => {
      console.log(`⏰ EXTENDED [${identifier.slice(0, 8)}] ${resources.join(', ')} at ${new Date(timestamp).toISOString()}`);
    });

    this.toolkit.on('lock:failed', (resources, error) => {
      console.error(`❌ FAILED ${resources.join(', ')}: ${error.message}`);
    });
  }

  dumpActiveLocksInfo() {
    const activeLocks = this.toolkit.getActiveLocks();

    console.log(`📊 Active locks (${activeLocks.length}):`);
    activeLocks.forEach(lock => {
      console.log(`  • [${lock.identifier.slice(0, 8)}] ${lock.resources.join(', ')}`);
      console.log(`    TTL: ${lock.timeToExpiration}ms, Extensions: ${lock.extensions}`);
    });
  }

  dumpMetricsInfo() {
    const metrics = this.toolkit.getMetrics();
    const summary = this.toolkit.getPerformanceSummary();

    console.log('📈 Performance metrics:');
    console.log(`  Success rate: ${(summary.successRate * 100).toFixed(2)}%`);
    console.log(`  Average latency: ${summary.averageAcquisitionLatency.toFixed(2)}ms`);
    console.log(`  Active locks: ${metrics.activeLocks}`);
    console.log(`  Circuit Breaker: ${summary.circuitBreakerHealth.overallState}`);
  }
}
```

### 3. Automatic Recovery

```typescript
class SelfHealingLockManager {
  private toolkit: RedlockToolkit;
  private diagnostics: LockDiagnostics;
  private healthChecker: LockHealthChecker;

  constructor(config: RedlockToolkitConfig) {
    this.toolkit = new RedlockToolkit(config);
    this.diagnostics = new LockDiagnostics(this.toolkit);
    this.healthChecker = new LockHealthChecker(this.toolkit);

    this.startSelfHealing();
  }

  private startSelfHealing() {
    // Health check every 30 seconds
    setInterval(async () => {
      const health = await this.healthChecker.performHealthCheck();

      if (health.overall === 'unhealthy') {
        console.error('Issues detected, initiating recovery...');
        await this.performRecovery();
      }
    }, 30000);

    // Clean up stale locks every 5 minutes
    setInterval(async () => {
      try {
        const cleaned = await this.toolkit.cleanup();
        if (cleaned > 0) {
          console.log(`🧹 Cleaned up ${cleaned} stale locks`);
        }
      } catch (error) {
        console.error('Cleanup error:', error);
      }
    }, 300000);
  }

  private async performRecovery() {
    try {
      // Reset lock metrics counters
      this.toolkit.resetMetrics();

      // Force cleanup of problematic locks
      await this.toolkit.cleanup();

      console.log('✅ Recovery complete');
    } catch (error) {
      console.error('❌ Recovery error:', error);
    }
  }

  // Proxy methods with additional diagnostics
  async acquire(resources: string | string[], options?: any) {
    const start = Date.now();
    try {
      const result = await this.toolkit.acquire(resources, options);
      const duration = Date.now() - start;

      if (duration > 1000) {
        console.warn(`⚠️ Slow lock acquisition: ${duration}ms for ${Array.isArray(resources) ? resources.join(', ') : resources}`);
      }

      return result;
    } catch (error) {
      const duration = Date.now() - start;
      console.error(`❌ Lock acquisition error after ${duration}ms:`, error.message);
      throw error;
    }
  }
}
```

## Production Configuration

### 1. Recommended Parameters

```typescript
const productionConfig: RedlockToolkitConfig = {
  clients: redisClients,
  defaultLockOptions: {
    ttl: 30000,                  // 30 seconds
    retryCount: 10,              // 10 attempts
    retryDelay: 200,             // 200ms base delay
    retryJitter: 100,            // +/-100ms jitter
    driftFactor: 0.01,           // 1% drift compensation
    autoExtendThreshold: 5000    // Extend 5 seconds before expiration
  },
  circuitBreaker: {
    failureThreshold: 5,         // 5 failures to open
    resetTimeout: 60000,         // 1 minute to reset
    maxRetries: 3,               // 3 retries maximum
    operationTimeout: 5000       // 5 second timeout
  },
  enableMetrics: true,           // Enable metrics
  keyPrefix: 'app:locks'         // Key prefix
};
```

### 2. Monitoring Alerts

```typescript
class ProductionMonitoring {
  constructor(private toolkit: RedlockToolkit) {
    this.setupAlerts();
  }

  private setupAlerts() {
    // Alert on low success rate
    setInterval(() => {
      const summary = this.toolkit.getPerformanceSummary();

      if (summary.successRate < 0.95) {
        this.sendAlert('LOW_SUCCESS_RATE', {
          successRate: summary.successRate,
          severity: 'warning'
        });
      }

      if (summary.successRate < 0.8) {
        this.sendAlert('CRITICAL_SUCCESS_RATE', {
          successRate: summary.successRate,
          severity: 'critical'
        });
      }
    }, 60000);

    // Alert when Circuit Breaker opens
    this.toolkit.on('circuit:stateChanged', (newState) => {
      if (newState === 'open') {
        this.sendAlert('CIRCUIT_BREAKER_OPEN', {
          state: newState,
          severity: 'critical'
        });
      }
    });
  }

  private sendAlert(type: string, data: any) {
    // Integration with alerting systems (PagerDuty, Slack, etc.)
    console.error(`🚨 ALERT [${type}]:`, data);
  }
}
```
