# redlock-toolkit

Advanced Redis distributed locking library for Node.js/TypeScript. Implements the Redlock algorithm with distributed semaphores, countdown latches, circuit breaker, automatic lock extension, optimistic locking, and Prometheus-compatible metrics.

## Features

- **Distributed Mutex** — Redlock-algorithm based locking with quorum consensus
- **Distributed Semaphore** — N-permit concurrent access control backed by Redis ZSET
- **CountDownLatch** — Wait for N distributed events before proceeding
- **Pub/Sub Waiting** — Optional instant lock-release notifications (vs polling)
- **Circuit Breaker** — Fault tolerance against Redis failures
- **Automatic Lock Extension** — Prevents premature expiration of long-running operations
- **Optimistic Locking** — Version-based conflict detection for high-read/low-write workloads
- **Hybrid Strategies** — Combine pessimistic and optimistic approaches
- **Metrics & Prometheus Export** — Built-in performance tracking
- **TypeScript** — Full type definitions with IntelliSense support

## Installation

```bash
npm install @trishchuk/redlock-toolkit
# or
pnpm add @trishchuk/redlock-toolkit
```

**Requirements:** Node.js >= 18, Redis 3.2+, ioredis 4.x or 5.x

## Quick Start

```typescript
import RedlockToolkit from '@trishchuk/redlock-toolkit';
import Redis from 'ioredis';

// Use 3+ independent Redis instances for fault tolerance
const clients = [
  new Redis({ host: 'redis1.example.com', port: 6379 }),
  new Redis({ host: 'redis2.example.com', port: 6379 }),
  new Redis({ host: 'redis3.example.com', port: 6379 }),
];

const redlock = new RedlockToolkit({
  clients,
  defaultLockOptions: {
    ttl: 30000,
    retryCount: 3,
    retryDelay: 200,
    retryJitter: 100,
  },
});

// Manual acquire / release
const lock = await redlock.acquire('user:123');
try {
  await performCriticalWork();
} finally {
  await lock.release();
}

// Auto-managed lock with using() (recommended)
const result = await redlock.using(
  'payment:order:456',
  async (signal) => {
    if (signal.aborted) throw signal.error;
    return await processPayment();
    // Lock is automatically extended and released
  },
  { ttl: 60000, autoExtendThreshold: 5000 },
);
```

## Distributed Semaphore

Allow up to N concurrent holders for the same resource.

```typescript
const permit = await redlock.acquireSemaphore('api-rate-limit', {
  maxPermits: 5,
  ttl: 30000,
  retryCount: 3,
  retryDelay: 200,
});

try {
  await callExternalApi();
} finally {
  await permit.release();
}
```

## CountDownLatch

Synchronize N distributed processes.

```typescript
// Create a latch that waits for 3 events
const latch = await redlock.createCountDownLatch('migration-ready', {
  count: 3,
  ttl: 120000,
});

// Each worker counts down when ready (idempotent per eventId)
await latch.countDown('worker-db');
await latch.countDown('worker-cache');
await latch.countDown('worker-search');

// Waiter blocks until count reaches 0
const completed = await latch.await(60000);
```

## Optimistic Locking

Version-based conflict detection for high-read, low-write scenarios.

```typescript
const result = await redlock.acquireOptimistic('document:789', {
  expectedVersion: 5,
  conflictResolution: 'fail',
  ttl: 5000,
});

if (result.success) {
  await updateDocument(789);
  await redlock.updateOptimistic('document:789', result.currentVersion!, {
    ttl: 5000,
  });
}
```

## Circuit Breaker

Prevent cascading failures when Redis is unavailable.

```typescript
const redlock = new RedlockToolkit({
  clients,
  circuitBreaker: {
    failureThreshold: 5,
    resetTimeout: 60000,
    maxRetries: 3,
    operationTimeout: 5000,
  },
});

redlock.on('circuit:stateChanged', (newState) => {
  console.log(`Circuit breaker: ${newState}`); // 'closed' | 'open' | 'half-open'
});
```

## Metrics & Monitoring

```typescript
const redlock = new RedlockToolkit({
  clients,
  enableMetrics: true,
});

// Performance summary
const summary = redlock.getPerformanceSummary();
console.log(`Success rate: ${(summary.successRate * 100).toFixed(1)}%`);
console.log(`Avg acquire latency: ${summary.averageAcquisitionLatency.toFixed(2)}ms`);

// Prometheus export
const prometheusText = redlock.exportMetrics();
```

## Events

```typescript
redlock.on('lock:acquired', (resources, identifier) => { /* ... */ });
redlock.on('lock:released', (resources, identifier) => { /* ... */ });
redlock.on('lock:extended', (resources, identifier, timestamp) => { /* ... */ });
redlock.on('lock:failed', (resources, error) => { /* ... */ });
redlock.on('circuit:stateChanged', (newState) => { /* ... */ });
redlock.on('error', (error) => { /* ... */ });
```

## Configuration

```typescript
interface RedlockToolkitConfig {
  clients: RedisClient[];
  defaultLockOptions?: {
    ttl?: number;                    // Default: 30000 (30s)
    retryCount?: number;             // Default: 0 (no retries)
    retryDelay?: number;             // Default: 200ms
    retryJitter?: number;            // Default: 100ms
    driftFactor?: number;            // Default: 0.01 (1%)
    autoExtendThreshold?: number;    // Default: 500ms
  };
  circuitBreaker?: CircuitBreakerOptions;
  pubSub?: { enabled: boolean; subscriberClients?: RedisClient[] };
  enableMetrics?: boolean;           // Default: true
  keyPrefix?: string;                // Default: 'neolock'
  logger?: Logger | boolean;
}
```

## Testing

```bash
pnpm test               # Watch mode
pnpm run test:run       # Single run
pnpm run test:coverage  # Coverage report
```

## Examples

```bash
pnpm tsx examples/simple-demo.ts
pnpm tsx examples/basic-usage.ts
pnpm tsx examples/advanced-features.ts
```

## Documentation

- [Project Overview](docs/overview.md)
- [API Reference](docs/api-reference.md)
- [Advanced Usage](docs/advanced-usage.md)
- [Redlock Algorithm](docs/redlock-algorithm.md)
- [Troubleshooting](docs/troubleshooting.md)
- [AI Agent Guide](docs/ai-agent-guide.md)

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

```bash
git clone https://github.com/x51xxx/redlock-toolkit.git
cd redlock-toolkit
pnpm install
pnpm test
pnpm run build
```

## License

MIT - see [LICENSE](LICENSE).

## Acknowledgments

- [Salvatore Sanfilippo](https://github.com/antirez) for the Redlock algorithm
- [Mike Marcacci](https://github.com/mike-marcacci) for the original node-redlock implementation
