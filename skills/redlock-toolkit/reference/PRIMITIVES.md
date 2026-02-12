# Primitives Reference

Distributed synchronization primitives beyond mutex locks.

## Semaphore

A distributed counting semaphore that limits concurrent access to a shared resource.

### Acquire a Permit

```ts
const permit = await toolkit.acquireSemaphore('api:payments', {
  maxPermits: 5,       // Max 5 concurrent holders
  ttl: 60000,          // Permit expires in 60s
  retryCount: 10,      // Retry up to 10 times if full
  retryDelay: 500,
});
try {
  await callPaymentApi();
} finally {
  await permit.release();
}
```

### SemaphoreOptions

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `maxPermits` | `number` | *required* | Maximum concurrent permit holders. Must be > 0. |
| `ttl` | `number` | `30000` | Permit TTL (ms). Must be > 0. |
| `retryCount` | `number` | `0` | Retry attempts when semaphore is full. |
| `retryDelay` | `number` | `200` | Base delay between retries (ms). |
| `retryJitter` | `number` | `100` | Random jitter on delay (ms). |
| `driftFactor` | `number` | `0.01` | Clock drift factor. |
| `autoExtendThreshold` | `number` | `500` | Auto-extend threshold (ms). |
| `identifier` | `string` | auto-generated | Custom permit identifier. |

### SemaphorePermit Properties

```ts
permit.resource          // string — Semaphore resource name
permit.identifier        // string — Unique permit ID
permit.expiration        // number — Expiration timestamp
permit.isValid           // boolean — Not released and not expired
permit.released          // boolean
permit.extensions        // number — Times extended
permit.duration          // number — Time held (ms)
permit.timeToExpiration  // number — ms until expiry
```

### Extend a Permit

```ts
await permit.extend(60000);  // Reset TTL to 60s
```

Throws `PermitExtensionError` if the permit is released or expired.

### Auto-Extending Permit with using()

```ts
await permit.using(async (signal) => {
  // Permit auto-extends while this runs
  for (const batch of batches) {
    if (signal.aborted) throw signal.error;
    await processBatch(batch);
  }
}, { ttl: 60000, autoExtendThreshold: 10000 });
// Permit is released automatically
```

### Semaphore Status

```ts
const status = await toolkit.getSemaphoreStatus('api:payments', 5);
// {
//   resource: 'api:payments',
//   activePermits: 3,
//   maxPermits: 5,
//   holders: [
//     { identifier: 'abc123', expiresAt: 1700000060000 },
//     ...
//   ],
// }
```

### How It Works

Internally, a sorted set (ZSET) stores permits where:
- **Member** = permit identifier
- **Score** = expiration timestamp

Expired permits are cleaned on every acquire/release via `ZREMRANGEBYSCORE`. This means permits self-heal without a background cleaner.

---

## Countdown Latch

A distributed barrier that blocks until N events have occurred across any number of processes.

### Create a Latch

```ts
const latch = await toolkit.createCountDownLatch('deploy:ready', {
  count: 3,           // Wait for 3 events
  ttl: 120000,        // Latch expires in 2 minutes
  awaitTimeout: 60000, // Default await timeout
  pollInterval: 200,  // Polling interval (if no pub/sub)
});
```

### Get an Existing Latch Handle

Workers in other processes use `getCountDownLatch` to obtain a handle to an already-created latch (no Redis keys are created):

```ts
const latch = toolkit.getCountDownLatch('deploy:ready', {
  awaitTimeout: 60000,
  pollInterval: 200,
});
await latch.countDown('worker-42');
```

### CountDownLatchOptions

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `count` | `number` | *required* | Initial countdown value. Must be > 0. |
| `ttl` | `number` | `60000` | Latch TTL (ms). Must be > 0. |
| `awaitTimeout` | `number` | `30000` | Default timeout for `await()` (ms). |
| `pollInterval` | `number` | `100` | Status check interval when polling (ms). |

### Count Down

Each worker decrements the counter:

```ts
const result = await latch.countDown('worker-1');
// {
//   success: true,
//   remainingCount: 2,
//   justCompleted: false,  // true when this call reaches 0
// }
```

The `eventId` parameter enables **idempotency** — calling `countDown('worker-1')` twice only decrements once.

### Await Completion

Block until count reaches zero or timeout:

```ts
const completed = await latch.await(60000);
// true if count reached 0, throws LatchTimeoutError otherwise
```

**Waiting modes:**
- **Pub/Sub** (if enabled): Wakes immediately when count reaches 0.
- **Polling** (default): Checks status every `pollInterval` ms.

### Latch Status

```ts
const status = await latch.getStatus();
// {
//   exists: true,
//   remainingCount: 1,
//   targetCount: 3,
//   completed: false,
//   ttl: 85000,
// }
```

Or by name without a latch instance:

```ts
const status = await toolkit.getCountDownLatchStatus('deploy:ready');
```

### Pattern: Fan-Out / Fan-In

```ts
// Coordinator process
const latch = await toolkit.createCountDownLatch('batch:done', {
  count: workers.length,
  ttl: 300000,
});

// Dispatch work
for (const worker of workers) {
  dispatchJob(worker, 'batch:done');
}

// Wait for all workers
await latch.await(120000);
console.log('All workers finished');

// Worker process (separate machine)
async function handleJob(latchName: string) {
  await doWork();
  const latch = toolkit.getCountDownLatch(latchName);
  await latch.countDown(workerId);  // Unique eventId for idempotency
}
```

### Errors

| Error | When |
| --- | --- |
| `LatchExistsError` | `createCountDownLatch` called with a name that already exists. |
| `LatchNotFoundError` | `countDown` or `await` on an expired or non-existent latch. |
| `LatchTimeoutError` | `await` timeout exceeded before count reached 0. |
