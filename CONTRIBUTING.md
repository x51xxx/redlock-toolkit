# Contributing to redlock-toolkit

Thank you for your interest in contributing to redlock-toolkit. This document provides guidelines and information for contributors.

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment for everyone. Be kind, constructive, and professional in all interactions.

## Getting Started

### Prerequisites

- Node.js >= 18.0.0
- Redis >= 3.2 (for Lua script support)
- pnpm >= 10

### Development Setup

1. Fork and clone the repository:

   ```bash
   git clone https://github.com/<your-username>/redlock-toolkit.git
   cd redlock-toolkit
   pnpm install
   ```

2. Start a local Redis instance (required for integration tests):

   ```bash
   docker run -d --name redis-test -p 6379:6379 redis:7-alpine
   ```

3. Verify your setup:

   ```bash
   pnpm run build
   pnpm run test:run
   pnpm run lint
   ```

### Development Commands

```bash
# Build
pnpm run build              # Compile TypeScript to dist/
pnpm run build:watch        # Watch mode compilation

# Testing
pnpm test                   # Run Vitest in watch mode
pnpm run test:run           # Single test run (all tests)
pnpm run test:coverage      # Generate coverage report
pnpm run test:ui            # Open Vitest UI
pnpm vitest run tests/circuit-breaker.test.ts   # Run specific test file

# Code quality
pnpm run lint               # Run ESLint
pnpm run lint:fix           # Auto-fix lint issues
pnpm run format             # Format with Prettier

# Examples
pnpm run example            # Run basic usage example
pnpm tsx examples/simple-demo.ts
```

### Project Structure

```
redlock-toolkit/
├── src/
│   ├── core/              # Types, errors, Lock class, logger
│   ├── algorithms/        # Redlock, optimistic-redlock internals
│   ├── strategies/        # Pessimistic, optimistic, hybrid, semaphore, latch
│   ├── patterns/          # Circuit breaker
│   ├── primitives/        # SemaphorePermit, CountDownLatch
│   ├── pubsub/            # PubSubManager, PubSubWaiter
│   ├── managers/          # Lock cache
│   ├── utils/             # Lua scripts, metrics, consensus, auto-extension
│   └── index.ts           # Public API surface (RedlockToolkit)
├── tests/                 # All test files (flat, by concern)
├── examples/              # Runnable usage examples
└── docs/                  # Additional documentation
```

## How to Contribute

### Reporting Bugs

1. Check [existing issues](https://github.com/x51xxx/redlock-toolkit/issues) to avoid duplicates.
2. Open a new issue with the following details:

- **Summary**: One-sentence description of the problem.
- **Environment**: Node.js version, Redis version, ioredis version, OS.
- **Steps to reproduce**: Minimal code snippet that triggers the issue.
- **Expected behavior**: What should happen.
- **Actual behavior**: What happens instead, including error messages and stack traces.

```typescript
// Minimal reproduction
import RedlockToolkit from '@trishchuk/redlock-toolkit';
import Redis from 'ioredis';

const toolkit = new RedlockToolkit({ clients: [new Redis()] });

async function reproduce() {
  // Steps that trigger the bug
}

reproduce().catch(console.error);
```

### Suggesting Features

1. Check [existing feature requests](https://github.com/x51xxx/redlock-toolkit/issues?q=is%3Aissue+label%3Aenhancement) to avoid duplicates.
2. Open an issue with the `enhancement` label. Include:

- **Use case**: What problem does this solve?
- **Proposed API**: How would the feature be used?
- **Alternatives considered**: Other approaches you evaluated.

### Submitting Changes

1. Create a feature branch from `main`:

   ```bash
   git checkout -b feat/your-feature
   ```

2. Make your changes. Follow the coding standards below.

3. Write or update tests. Every behavioral change needs test coverage.

4. Verify everything passes:

   ```bash
   pnpm run build
   pnpm run test:run
   pnpm run lint
   ```

5. Commit using [Conventional Commits](https://www.conventionalcommits.org/):

   - `feat:` New feature
   - `fix:` Bug fix
   - `docs:` Documentation changes
   - `test:` Test additions/changes
   - `refactor:` Code refactoring
   - `perf:` Performance improvements
   - `chore:` Maintenance tasks

   Examples:

   ```
   feat: add semaphore fairness queue
   fix: prevent permit leak on partial consensus failure
   docs: add migration guide for v1.0
   test: add stress test for countdown latch
   refactor: extract consensus logic into ConsensusManager
   perf: replace LPOS with SISMEMBER in latch script
   ```

6. Push and open a Pull Request against `main`.

## Coding Standards

### TypeScript

- Strict mode is enabled. Do not use `any` — use `unknown` and narrow.
- Use `Record<string, unknown>` instead of `any` for context objects.
- Prefer `readonly` for properties that should not change after construction.
- All public API methods must have JSDoc comments.

### Naming

| Element       | Convention           | Example                      |
|---------------|----------------------|------------------------------|
| Classes       | PascalCase           | `RedlockToolkit`             |
| Interfaces    | PascalCase           | `LockOptions`                |
| Methods       | camelCase            | `acquireSemaphore`           |
| Constants     | UPPER_SNAKE_CASE     | `DEFAULT_CONFIG`             |
| Lua scripts   | UPPER_SNAKE_CASE     | `SEMAPHORE_ACQUIRE_SCRIPT`   |
| Files         | kebab-case           | `consensus-manager.ts`       |

### Error Handling

- All custom errors extend `RedlockToolkitError`.
- Include structured context via the `context` parameter — no bare strings.
- Use `isRetryableError()` to classify errors for retry logic.
- Never swallow errors silently. Use `logger.warn(...)` in `.catch()` handlers.

### Lua Scripts

- All Redis operations must be atomic via Lua scripts in `src/utils/scripts.ts`.
- SHA1 hashes are precomputed at module load. When changing a script, the hash auto-recalculates.
- Validate with `validateScriptHashes()` if in doubt.

### Testing

Tests live in `tests/` and are organized by concern, not by file hierarchy.

| File pattern                  | Purpose                          |
|-------------------------------|----------------------------------|
| `redlock-toolkit.test.ts`     | Core public API                  |
| `integration.test.ts`         | Full workflows with mocked Redis |
| `real-redis.test.ts`          | Integration with real Redis      |
| `edge-cases.test.ts`          | Boundary conditions              |
| `stress.test.ts`              | Load and performance             |
| `consensus-failures.test.ts`  | Byzantine fault scenarios        |
| `<feature>.test.ts`           | Feature-specific tests           |

Guidelines:

- Use `vitest` with `describe`/`it`/`expect`.
- Mock Redis clients via `ioredis-mock` or the helpers in `tests/setup.ts`.
- Integration tests that need a real Redis must be skippable when Redis is unavailable.
- Test names should read as specifications: `"should reject acquire when at capacity"`.

### Testing with Redis

Most tests use mocked Redis clients and require no running Redis instance. Integration tests in `tests/real-redis*.test.ts` need a real Redis:

```bash
# Start Redis via Docker
docker run -d --name redis-test -p 6379:6379 redis:7-alpine

# Run only unit tests (no Redis needed)
pnpm vitest run --exclude 'tests/real-redis*'

# Run integration tests (Redis required)
pnpm vitest run tests/real-redis.test.ts
pnpm vitest run tests/real-redis-semaphore-latch.test.ts
```

Integration tests auto-skip when Redis is not available at `localhost:6379`.

## Pull Request Process

### Before Submitting

- [ ] `pnpm run build` succeeds with no errors.
- [ ] `pnpm run test:run` — all tests pass.
- [ ] `pnpm run lint` — no lint violations.
- [ ] New code has test coverage.
- [ ] No unrelated changes are included.

### Manual Testing Checklist

For changes to core locking functionality, verify:

- [ ] Lock acquire/release works with mocked Redis (`pnpm run test:run`).
- [ ] Integration tests pass with a real Redis instance (`tests/real-redis.test.ts`).
- [ ] Auto-extension keeps locks alive during long operations.
- [ ] Error handling returns meaningful messages (check custom error classes).
- [ ] Consensus works correctly with 3+ Redis nodes (mocked or real).
- [ ] No unhandled promise rejections under concurrent load (`tests/stress.test.ts`).

### PR Description Template

```
## Summary
Brief description of what this PR does and why.

## Changes
- Bullet list of specific changes

## Test Plan
- How the changes were verified
- New tests added

## Breaking Changes
List any breaking changes, or "None".
```

### Review Criteria

Reviewers evaluate:

- **Correctness**: Does the code do what it claims? Are edge cases handled?
- **Safety**: No race conditions, resource leaks, or injection vulnerabilities.
- **Performance**: No unnecessary allocations, O(n) where O(1) is possible, etc.
- **Consistency**: Follows existing patterns and conventions in the codebase.
- **Tests**: Adequate coverage for the change, including failure paths.

## Documentation

Project documentation lives in `docs/`:

```
docs/
├── overview.md              # Library overview
├── ai-agent-guide.md        # Guide for AI agents working with the codebase
├── api-reference.md         # Public API reference
├── examples.md              # Usage examples
├── advanced-usage.md        # Advanced patterns
├── redlock-algorithm.md     # Redlock algorithm explanation
└── troubleshooting.md       # Common issues and solutions
```

When adding a new feature or changing behavior, update the relevant documentation files alongside the code.

## Architecture Notes

Understanding these decisions helps you contribute effectively:

- **Single public API**: `RedlockToolkit` is the only exported entry point. Internal classes (`InternalRedlock`, `InternalRedlockLock`) are not exported.
- **Strategy pattern**: Locking strategies (pessimistic, optimistic, hybrid, semaphore, latch) are separate classes that receive the toolkit via `ILockToolkit` interface.
- **Consensus**: All distributed operations go through `ConsensusManager` with quorum requirement `floor(N/2) + 1`.
- **Auto-extension**: Uses recursive `setTimeout`, not `setInterval`, to prevent request accumulation.
- **Lua atomicity**: Every Redis mutation is a Lua script. No multi-step sequences via separate commands.

## Release Process

Maintainers handle releases. The process follows [Semantic Versioning](https://semver.org/):

- **MAJOR**: Breaking API changes.
- **MINOR**: New features, backward-compatible.
- **PATCH**: Bug fixes, backward-compatible.

Changes are documented in [CHANGELOG.md](./CHANGELOG.md) following [Keep a Changelog](https://keepachangelog.com/).

## Getting Help

- **Issues**: [github.com/x51xxx/redlock-toolkit/issues](https://github.com/x51xxx/redlock-toolkit/issues)
- **Discussions**: For questions about usage, architecture, or proposals.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](./LICENSE).
