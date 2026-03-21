# CLAUDE.md

## Project Overview

**redlock-toolkit** (`@trishchuk/redlock-toolkit` v0.10.0) is a TypeScript library implementing the Redlock distributed locking algorithm for Node.js with Redis. It provides consensus-based mutual exclusion across multiple Redis nodes with fault tolerance, monitoring, and multiple locking strategies.

Key features: distributed mutex locks, semaphores, countdown latches, optimistic/hybrid locking, circuit breaker pattern, auto-extension, pub/sub notifications, and Prometheus-compatible metrics.

## Tech Stack

- **Language:** TypeScript (strict mode, ES2022 target)
- **Runtime:** Node.js >= 18.0.0
- **Module system:** CommonJS output
- **Redis client:** ioredis ^4.0.0 || ^5.0.0 (peer dependency)
- **Test framework:** Vitest 3.2.4
- **Linting:** ESLint 9 (flat config) + Prettier
- **Build:** `tsc` (TypeScript compiler)

## Commands

```bash
# Build
npm run build                # Compile TS → dist/ (uses tsconfig.build.json)

# Test
npm test                     # Watch mode
npm run test:run             # Single run (CI-friendly)
npm run test:coverage        # With v8 coverage (80% threshold for branches/functions/lines/statements)
npx vitest run <file>        # Run a specific test file

# Lint & Format
npm run lint                 # ESLint check (src/**/*.ts only)
npm run lint:fix             # Auto-fix lint issues
npm run format               # Prettier formatting

# Examples
npm run example              # Run basic-usage.ts with tsx
```

## Project Structure

```
src/
├── index.ts                    # Main public API: RedlockToolkit class (entry point)
├── core/                       # Types, errors, Lock class, logger
│   ├── types.ts                # All interfaces and configuration types
│   ├── errors.ts               # Error class hierarchy (10+ types)
│   ├── lock.ts                 # Lock instance class
│   └── logger.ts               # Logger interface
├── algorithms/                 # Algorithm implementations
│   ├── redlock.ts              # Core Redlock consensus algorithm
│   └── optimistic-redlock.ts   # Optimistic locking variant
├── strategies/                 # Pluggable locking strategies
│   ├── pessimistic-strategy.ts
│   ├── optimistic-strategy.ts
│   ├── hybrid-strategy.ts
│   ├── semaphore-strategy.ts
│   └── countdown-latch-strategy.ts
├── patterns/
│   └── circuit-breaker.ts      # Circuit breaker implementation
├── primitives/                 # Distributed primitive classes
│   ├── semaphore.ts
│   └── countdown-latch.ts
├── managers/
│   └── cache.ts                # Lock cache manager
├── pubsub/                     # Pub/Sub lock waiting
│   ├── pubsub-manager.ts
│   └── pubsub-waiter.ts
└── utils/
    ├── scripts.ts              # Lua scripts for atomic Redis operations
    ├── metrics.ts              # Metrics collection & Prometheus export
    ├── consensus-manager.ts    # Quorum decision logic
    └── auto-extension-manager.ts

tests/                          # Flat structure, 22 test files
├── setup.ts                    # Mock clients, ChaosClient, test helpers
├── real-redis-setup.ts         # Real Redis integration setup
├── integration.test.ts
├── circuit-breaker.test.ts
├── optimistic-locking.test.ts
├── race-conditions.test.ts
├── stress.test.ts
└── ...

examples/                       # Runnable usage examples
docs/                           # Detailed documentation
skills/                         # AI agent skill definitions
```

## Code Conventions

- **Files:** kebab-case (`circuit-breaker.ts`, `auto-extension-manager.ts`)
- **Classes:** PascalCase (`RedlockToolkit`, `CircuitBreakerManager`)
- **Methods/functions:** camelCase (`acquire()`, `release()`, `extend()`)
- **Error classes:** PascalCase ending in `Error` (`ResourceLockedError`, `ConsensusError`)
- **Async/await** for all I/O operations — no raw Promises or callbacks
- **Atomic Lua scripts** for all Redis operations (no multi-step Redis commands)
- **Strategy pattern** for pluggable locking behaviors
- **EventEmitter** for lock lifecycle events and circuit breaker state changes
- **Recursive setTimeout** (not setInterval) for auto-extension

## Testing

- Tests live in `tests/` with a flat naming structure (not mirroring `src/`)
- Test files named by concern: `circuit-breaker.test.ts`, `race-conditions.test.ts`
- Setup file (`tests/setup.ts`) provides mock Redis clients and a `ChaosClient` for failure injection
- Vitest globals are enabled — no need to import `describe`, `it`, `expect`
- Test timeout: 30 seconds
- Coverage thresholds: 80% across branches, functions, lines, statements

## Architecture Notes

- Single public API surface: `RedlockToolkit` class in `src/index.ts`
- Quorum requirement: `floor(N/2) + 1` Redis nodes must agree for lock acquisition
- Clock drift compensation via configurable `driftFactor` (default 1%)
- All configuration is programmatic (constructor options) — no env vars or config files needed
- Key prefix default: `"neolock"`
- Subpath exports available for direct algorithm imports (`./algorithms/redlock`, `./algorithms/optimistic-redlock`)
