# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/x51xxx/redlock-toolkit/compare/v0.10.0...HEAD
[0.10.0]: https://github.com/x51xxx/redlock-toolkit/compare/v0.9.2...v0.10.0
[0.9.2]: https://github.com/x51xxx/redlock-toolkit/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/x51xxx/redlock-toolkit/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/x51xxx/redlock-toolkit/releases/tag/v0.9.0
