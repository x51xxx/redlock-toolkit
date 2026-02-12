# Configuration Reference

## Constructor

```ts
import RedlockToolkit from '@trishchuk/redlock-toolkit';
import Redis from 'ioredis';

const toolkit = new RedlockToolkit({
  clients: [new Redis()],                       // Required: 1+ ioredis clients
  defaultLockOptions: { ttl: 30000 },           // Optional lock defaults
  circuitBreaker: { failureThreshold: 5 },      // Optional circuit breaker
  enableMetrics: true,                          // Optional (default: true)
  keyPrefix: 'myapp',                           // Optional (default: 'neolock')
  logger: true,                                 // Optional: true=console, false=silent, or Logger
  pubSub: { enabled: false },                   // Optional pub/sub config
});
```

## RedlockToolkitConfig

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `clients` | `RedisClient[]` | *required* | One or more ioredis clients. Use 3+ for fault tolerance. |
| `defaultLockOptions` | `Partial<LockOptions>` | see below | Defaults applied to every `acquire()` call. |
| `circuitBreaker` | `CircuitBreakerOptions` | see below | Per-client circuit breaker tuning. |
| `enableMetrics` | `boolean` | `true` | Collect acquisition/release/extension metrics. |
| `keyPrefix` | `string` | `"neolock"` | Prefix for all Redis keys. Enables namespace isolation. |
| `logger` | `Logger \| boolean` | `false` | `true` = console logger, `false` = silent, or custom Logger. |
| `pubSub` | `PubSubConfig` | `{ enabled: false }` | Enable pub/sub-based lock waiting. |

## LockOptions (defaults)

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `ttl` | `number` | `30000` | Lock time-to-live in milliseconds. |
| `retryCount` | `number` | `0` | Retry attempts. `0` = no retry, `-1` = infinite. |
| `retryDelay` | `number` | `200` | Base delay between retries (ms). |
| `retryJitter` | `number` | `100` | Random jitter added to retry delay (ms). |
| `driftFactor` | `number` | `0.01` | Clock drift compensation factor (0..1). |
| `autoExtendThreshold` | `number` | `500` | Auto-extend when TTL drops below this (ms). |
| `identifier` | `string` | auto-generated | Custom lock identifier (16-byte hex by default). |

## CircuitBreakerOptions

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `failureThreshold` | `number` | `5` | Consecutive failures before opening circuit. |
| `resetTimeout` | `number` | `60000` | Time (ms) before half-open attempt. |
| `maxRetries` | `number` | `3` | Retries allowed in half-open state. |
| `operationTimeout` | `number` | `5000` | Per-operation timeout (ms). |

## PubSub

Enable pub/sub to reduce polling. When a lock or semaphore is released, a notification is published so waiting clients are woken immediately instead of sleeping.

```ts
const toolkit = new RedlockToolkit({
  clients: [new Redis()],
  pubSub: {
    enabled: true,
    subscriberClients: [subscriberRedis],  // Optional dedicated subscriber
  },
});
```

If `subscriberClients` is omitted, the library calls `client.duplicate()` to create a subscriber connection (ioredis clients in subscribe mode cannot execute normal commands).

**Channels used internally:**

| Channel | When Published |
| --- | --- |
| `{prefix}:notify:lock:{resource}` | Lock released |
| `{prefix}:notify:sem:{resource}` | Semaphore permit released |
| `{prefix}:notify:latch:{name}` | Latch completed (count reached 0) |

## Redis Requirements

- Redis >= 3.2 (Lua scripting support).
- ioredis ^4.0.0 or ^5.0.0.
- For fault tolerance, use 3 or 5 independent Redis instances (not replicas). Quorum = `floor(N/2) + 1`.

### Single-Instance Setup

```ts
const toolkit = new RedlockToolkit({
  clients: [new Redis({ host: 'localhost', port: 6379 })],
});
```

### Multi-Instance Setup (Recommended)

```ts
const toolkit = new RedlockToolkit({
  clients: [
    new Redis({ host: 'redis-1', port: 6379 }),
    new Redis({ host: 'redis-2', port: 6379 }),
    new Redis({ host: 'redis-3', port: 6379 }),
  ],
});
```

## Key Format

All Redis keys follow the pattern `{keyPrefix}:{type}:{name}`:

| Pattern | Type | Purpose |
| --- | --- | --- |
| `{prefix}:{resource}` | STRING | Mutex lock holder |
| `{prefix}:{resource}:data` | STRING | Lock-attached data |
| `{prefix}:{resource}:version` | STRING | Optimistic lock version |
| `{prefix}:sem:{resource}` | ZSET | Semaphore permits (member=id, score=expiry) |
| `{prefix}:latch:{name}` | STRING | Latch remaining count |
| `{prefix}:latch:{name}:target` | STRING | Latch original count |
| `{prefix}:latch:{name}:events` | SET | Latch event IDs (idempotency) |

## Shutdown

Always call `shutdown()` before process exit to release resources:

```ts
process.on('SIGTERM', async () => {
  await toolkit.shutdown();
  process.exit(0);
});
```

`shutdown()` performs:
1. Release all active locks.
2. Release all active semaphore permits.
3. Shut down pub/sub subscriber connections.
4. Clear circuit breaker, metrics, and event listeners.
