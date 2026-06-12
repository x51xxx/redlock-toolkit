# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

redlock-toolkit is an advanced Redis distributed locking library implementing the Redlock algorithm with circuit breaker pattern, automatic lock extension, optimistic locking, and comprehensive metrics collection.

## Development Commands

### Build & Development
```bash
npm run build          # Compile TypeScript to dist/ using tsconfig.build.json
npm run build:watch    # Watch mode compilation
npm run lint           # Run ESLint on src/**/*.ts
npm run lint:fix       # Fix linting issues
npm run format         # Format code with Prettier
```

### Testing
```bash
npm test               # Run Vitest in watch mode
npm run test:run       # Single test run (all tests)
npm run test:coverage  # Generate coverage report
npm run test:ui        # Open Vitest UI

# Run specific test file
npx vitest run tests/circuit-breaker.test.ts

# Run tests matching pattern
npx vitest run -t "should acquire lock"
```

### Examples
```bash
npm run example        # Run basic usage example
npx tsx examples/basic-usage.ts
npx tsx examples/simple-demo.ts
npx tsx examples/advanced-features.ts
```

## Architecture Overview

### Public API Design

**IMPORTANT**: The library exposes a single unified API through `RedlockToolkit`. Internal implementations (`InternalRedlock`, `InternalRedlockLock`) are intentionally hidden from the public API to maintain a clean interface.

```typescript
// Public API - the only way to use the library
import RedlockToolkit from '@trishchuk/redlock-toolkit';

// Internal classes are NOT exported (as of recent refactoring)
// ❌ import { Redlock, RedlockLock } from '@trishchuk/redlock-toolkit';
```

### Core Module Structure

**Core Components** (`src/core/`)
- `types.ts` - All TypeScript interfaces and type definitions
- `errors.ts` - Custom error hierarchy with `RedlockToolkitError` base class
- `lock.ts` - Public `Lock` class with auto-extension using recursive `setTimeout` (NOT `setInterval`)
- `logger.ts` - Logger interface and factory

**Algorithms** (`src/algorithms/`)
- `redlock.ts` - Contains `InternalRedlock` and `InternalRedlockLock` (internal only, not exported)
- `optimistic-redlock.ts` - Optimistic locking with version control

**Strategies** (`src/strategies/`)
- `pessimistic-strategy.ts` - Traditional pessimistic locking
- `optimistic-strategy.ts` - Optimistic locking with conflict detection
- `hybrid-strategy.ts` - Combines pessimistic and optimistic approaches

**Patterns** (`src/patterns/`)
- `circuit-breaker.ts` - Circuit breaker for fault tolerance

**Utils** (`src/utils/`)
- `scripts.ts` - Lua scripts for atomic Redis operations (precompiled with SHA1 hashes)
- `metrics.ts` - Metrics collection with detailed statistics
- `consensus-manager.ts` - Manages consensus across Redis nodes
- `auto-extension-manager.ts` - Handles automatic lock extension

**Managers** (`src/managers/`)
- `cache.ts` - Lock caching for performance optimization

**Main Entry** (`src/index.ts`)
- Exports `RedlockToolkit` as default and named export
- Exports error classes and types
- Does NOT export internal Redlock implementations

### Key Architectural Decisions

1. **Unified API**: `RedlockToolkit` is the sole public interface, preventing confusion from multiple similar classes
2. **Auto-Extension Fix**: Uses recursive `setTimeout` instead of `setInterval` to prevent request accumulation
3. **Consensus Algorithm**: Requires majority (⌊N/2⌋ + 1) agreement from Redis nodes
4. **Lua Script Atomicity**: All Redis operations use Lua scripts for atomicity
5. **Event-Driven**: Lock lifecycle events forwarded through EventEmitter
6. **Strategy Pattern**: Supports pessimistic, optimistic, and hybrid locking strategies

### Lock Instance Hierarchy

```
RedlockToolkit (public API)
  └── Lock (public lock instance from core/lock.ts)
       └── Uses InternalRedlock internally
            └── Creates InternalRedlockLock instances (private)
```

### Error Handling Strategy

- Base `RedlockToolkitError` with typed context
- Specific errors: `ResourceLockedError`, `ConsensusError`, `LockTimeoutError`, etc.
- Built-in retry logic with `isRetryableError()` helper
- All errors use `Record<string, unknown>` instead of `any` for type safety

### Testing Approach

Tests organized by concern in `/tests`:
- `redlock-toolkit.test.ts` - Core functionality
- `integration.test.ts` - Full workflow with mocked Redis
- `edge-cases.test.ts` - Boundary conditions
- `stress.test.ts` - Performance and load testing  
- `consensus-failures.test.ts` - Byzantine fault scenarios
- `data-integrity.test.ts` - Atomic operations verification
- `optimistic-locking.test.ts` - Optimistic locking tests
- `circuit-breaker.test.ts` - Circuit breaker pattern tests
- `metrics.test.ts` - Metrics collection tests

### Critical Implementation Details

1. **Clock Drift Compensation**: `drift = Math.round(driftFactor * ttl) + 2`
2. **Quorum**: Requires `Math.floor(n/2) + 1` nodes
3. **Lock Validity**: `validity = ttl - elapsed - drift`
4. **Auto-Extension Threshold**: Triggers when `remainingTTL <= threshold`
5. **Circuit Breaker States**: Closed → Open → Half-Open → Closed
6. **Lock Key Generation**: `${keyPrefix}:${resource}` pattern

## Redis Requirements

- Redis 3.2+ (for Lua script support)
- Compatible with ioredis v4.x and v5.x
- Designed for single-instance Redis nodes (one or many, per Redlock). Redis Cluster is only safe when every resource name carries a hash tag (e.g. `{order:42}`): the Lua scripts derive extra keys (`<key>:version`, `<key>:value`) and accept multiple KEYS, which otherwise fail with CROSSSLOT. The toolkit logs a warning when a Cluster client is detected.

## TypeScript Configuration

- Target: ES2022 (in tsconfig.json)
- Module: CommonJS
- Strict mode enabled
- Source maps included in development
- Build output to `dist/` directory

## Performance Considerations

1. **Connection Pooling**: Use ioredis connection pools in production
2. **Lock TTL**: Set based on P99 operation duration + buffer
3. **Retry Strategy**: Exponential backoff with jitter for high contention
4. **Circuit Breaker**: Tune failure threshold based on SLA requirements
5. **Metrics Monitoring**: Track acquisition latency and success rates