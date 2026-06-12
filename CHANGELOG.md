# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-06-12

Stabilization release driven by a full distributed-locking code audit (internal four-track review plus an external Codex review). Nine of ten critical findings are fixed, each pinned by regression tests.

### Added

- Backward-compatible Redlock-named shims: `acquireRedlock()` / `usingRedlock()`.
- `CacheManager.invalidate(resources)` — release now clears the cache entry instead of leaving a stale `acquired` record until TTL.
- `identifier` field on `OptimisticLockResult` — the token actually written to Redis by the acquire script.
- Runtime CROSSSLOT warning when a Redis Cluster client is detected (multi-key Lua scripts and derived `:version`/`:value` keys require hash-tagged resource names).
- Regression test suites for all audit findings (`tests/audit-2026-06-regression.test.ts`, `tests/audit-findings-repro.test.ts`, `tests/audit-fixes-regression.test.ts`).

### Changed

- **BREAKING**: `ioredis` is now a peer dependency only — install it explicitly alongside the toolkit.
- **BREAKING**: releasing a lock that has expired or been taken by another owner now returns `{ success: false, releasedCount: 0 }` instead of reporting success; `releasedCount` is the real number of confirming nodes, not `clients.length`.
- `CountDownLatch.countDown()` uses quorum consensus instead of `"any"` — a single node can no longer advance the latch while the majority diverges.
- Consensus failures are no longer fail-fast: in-flight node operations are drained before the error is reported, so cleanup runs after late side effects have landed.
- Consensus cleanup targets **all** clients, not only confirmed voters — covers writes that succeeded after the quorum decision or whose ack was lost.
- `Lock.release()` serializes concurrent calls via a shared promise: repeated release of your own lock is an idempotent success, distinct from the honest failure for a stolen lock.
- Circuit breaker trips on **consecutive** failures only (a success closes the window) and bounds concurrent half-open probes; the `maxRetries` option is now effective instead of being silently ignored.
- Optimistic acquire validates existing versions on **all** keys when `expectedVersion` is omitted, not just `KEYS[1]`.
- Documentation corrected: Redis Cluster is only safe with hash-tagged resource names; full hash-tag key layout is deliberately deferred as a breaking change.
- Tooling: project migrated from npm to pnpm (`packageManager` field, `pnpm-lock.yaml`, docs updated).

### Fixed

- **Critical**: a failed lock extension force-deleted the lock keys with an unconditional `DEL`, destroying a lock already acquired by another owner — extension failure now releases through the ownership-checked Lua script.
- **Critical**: hybrid `acquireHybrid({ primaryStrategy: 'optimistic' })` returned a `Lock` with a freshly generated identifier instead of the one written to Redis, so `extend()`/`release()` operated on the wrong token and the key survived until TTL.
- **Critical**: a failed-quorum acquire released only nodes with confirmed success, violating the Redlock specification — release is now attempted on every node, covering lost-ack partitions.
- **Critical**: sub-quorum optimistic writes left split-brain versions across nodes (minority at `N+1`, majority at `N`, no future CAS able to reach quorum) — minority nodes are now rolled back via a CAS-guarded script that never touches versions written by other clients.
- Empty-string TTL passed Lua's truthiness check (`""` is truthy in Lua), causing `PEXPIRE key ""` errors mid-script in optimistic write/CAS operations.
- Empty-string lock identifier (the `defaultOptions` placeholder) could reach Redis through the optimistic strategy in the hybrid path, making all such locks match each other's ownership checks.
- Parallel real-Redis test suites flushed each other's database mid-test; suites are now isolated by Redis database index.

## [0.10.0] - 2026-02-11

### Added

- Distributed semaphore primitive (`acquireSemaphore`, `SemaphorePermit`).
- Distributed countdown latch primitive (`createCountDownLatch`, `CountDownLatch`).
- Pub/Sub-based waiting for lock acquisition and latch completion.
- `PubSubManager` and `PubSubWaiter` for notification-driven lock waiting.
- Lock caching layer (`CacheManager`) for performance optimization.
- `ILockToolkit` interface for strategy decoupling.
- Input validation for semaphore (`maxPermits`, `ttl`) and latch (`count`, `ttl`) options.

### Changed

- `executeWithConsensus` cleanup now accepts optional `script` and `args` in cleanup context, enabling correct per-strategy cleanup (e.g., semaphore release script instead of lock release script).
- Latch `create` operation uses quorum consensus (`successPolicy: "quorum"`) instead of `"any"` to prevent split-brain.
- Latch countdown Lua script uses `SISMEMBER`/`SADD` (O(1) idempotency check) instead of `LPOS`/`RPUSH` (O(n)).
- Latch `awaitLatch` verifies completion via `getStatus()` after receiving a pub/sub message instead of trusting the message blindly.
- Latch countdown captures `responseData` from the first successful consensus response only, preventing overwrite by later nodes.
- Semaphore expiration is computed from the timestamp after successful consensus, not before the retry loop.
- Semaphore `release()` returns actual `remainingCount` from the Lua script response instead of hardcoded `-1`.
- `acquireSemaphore()` wraps the returned permit's `release()` to clean up `activeSemaphores` map and publish a notification on the `notify:sem:*` channel.
- `PubSubManager.ensureInitialized()` uses a promise-based mutex to prevent concurrent duplicate subscriber creation.
- `PubSubManager.subscribe()` rolls back channel state on Redis SUBSCRIBE failure.
- `PubSubManager.subscribeSync()` cleans stale channel entries on async failure.
- Replaced silent `.catch(() => {})` with `.catch(err => logger.warn(...))` across `PubSubManager`, `RedlockToolkit` lock/semaphore release notifications, and shutdown paths.

### Fixed

- Semaphore permit leak on partial-consensus failure: cleanup now uses `SCRIPTS.semaphoreRelease` instead of the lock release script.
- `activeSemaphores` map entries never removed on individual permit release.
- Race condition in `PubSubManager.ensureInitialized()` where concurrent callers could create multiple subscriber connections.
- Stale channel listener map entries left behind when Redis SUBSCRIBE fails.
- Latch polling loop ignored the configured `pollInterval`, always using the default. Added `pollInterval` getter to `CountDownLatch`.

## [0.9.2] - 2025-09-08

### Changed

- Refactored lock acquisition logic for improved stability and error handling.
- Unified public API under `RedlockToolkit`; internal `InternalRedlock` and `InternalRedlockLock` are no longer exported.
- Pessimistic, optimistic, and hybrid locking extracted into dedicated strategy classes.
- Consensus logic extracted into `ConsensusManager`.
- Auto-extension switched from `setInterval` to recursive `setTimeout` to prevent request accumulation.

### Fixed

- Lock extension and release error propagation improved with structured `LockExtensionError` context.

## [0.9.1] - 2025-09-08

### Added

- Automatic lock extension with `AbortSignal`-based cancellation.
- `Logger` interface and `LoggerFactory` for pluggable logging.
- `AutoExtensionManager` for managing extension lifecycle.

### Changed

- Enhanced `Lock.using()` with auto-extension support and configurable thresholds.

## [0.9.0] - 2025-09-07

### Added

- Initial implementation of the Redlock distributed locking algorithm.
- Pessimistic lock acquisition with configurable retry (count, delay, jitter).
- Optimistic locking with version-based conflict detection and compare-and-swap.
- Hybrid locking strategy with adaptive primary/fallback selection.
- Circuit breaker pattern (`CircuitBreakerManager`) with closed/open/half-open states.
- Comprehensive metrics collection (`MetricsCollector`) with Prometheus export.
- Atomic Lua scripts for acquire, extend, release, force-release, status, and cleanup.
- Clock drift compensation per the Redlock specification.
- Custom error hierarchy: `ResourceLockedError`, `ConsensusError`, `LockTimeoutError`, `LockExpiredError`, `LockExtensionError`, `CircuitBreakerOpenError`, `ConfigurationError`, `OptimisticLockConflictError`, `HybridLockError`.
- Event-driven lock lifecycle: `lock:acquired`, `lock:released`, `lock:extended`, `lock:failed`, `circuit:stateChanged`.
- Full TypeScript strict-mode support with complete type definitions.
- Test suite: unit, integration, edge-case, stress, consensus-failure, data-integrity, and metrics tests.

[Unreleased]: https://github.com/x51xxx/redlock-toolkit/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/x51xxx/redlock-toolkit/compare/v0.10.0...v1.0.0
[0.10.0]: https://github.com/x51xxx/redlock-toolkit/compare/v0.9.2...v0.10.0
[0.9.2]: https://github.com/x51xxx/redlock-toolkit/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/x51xxx/redlock-toolkit/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/x51xxx/redlock-toolkit/releases/tag/v0.9.0
