# Repository Guidelines

## Project Structure & Module Organization

This is a TypeScript Redis distributed-locking library. Source lives in `src/`:

- `src/index.ts` exposes the public `RedlockToolkit` API.
- `src/core/` contains types, errors, lock implementation, and logging.
- `src/strategies/` implements pessimistic, optimistic, hybrid, semaphore, and latch strategies.
- `src/algorithms/` contains lower-level Redlock and optimistic Redlock internals.
- `src/utils/` contains Lua scripts, consensus, metrics, and auto-extension helpers.

Tests are in `tests/`, examples in `examples/`, docs in `docs/`, and generated build output in `dist/`.

## Build, Test, and Development Commands

Use npm scripts:

```bash
npm run build          # Compile TypeScript into dist/
npm run build:watch    # Compile in watch mode
npm test               # Run Vitest in watch mode
npm run test:run       # Run all tests once
npm run test:coverage  # Generate coverage report
npm run lint           # Run ESLint on src/**/*.ts
npm run lint:fix       # Auto-fix lint issues
npm run format         # Format src/**/*.ts with Prettier
npm run example        # Run examples/basic-usage.ts
```

Real Redis tests require Redis on `localhost:6379`; they skip automatically when unavailable.

## Coding Style & Naming Conventions

The project uses strict TypeScript, ESLint, and Prettier. Prefer explicit public types, `unknown` over `any`, and structured custom errors from `src/core/errors.ts`. Public APIs should include concise JSDoc.

Naming patterns:

- Classes/interfaces: `PascalCase` (`RedlockToolkit`, `LockOptions`)
- Methods/functions: `camelCase` (`acquireSemaphore`)
- Constants and Lua scripts: `UPPER_SNAKE_CASE` (`SEMAPHORE_ACQUIRE_SCRIPT`)
- Files: `kebab-case.ts` (`consensus-manager.ts`)

## Testing Guidelines

Tests use Vitest with `describe`, `it`, and `expect`. Keep tests in `tests/` and name files by concern, for example `semaphore.test.ts` or `consensus-failures.test.ts`.

Run targeted tests with:

```bash
npm test -- --run tests/semaphore.test.ts
```

Every behavioral change should include regression coverage. For distributed-safety changes, add tests around quorum, stale identifiers, TTL expiration, and partial Redis failures.

## Commit & Pull Request Guidelines

History follows Conventional Commits: `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `perf:`, and `chore:`. Example: `fix: prevent stale lock release from deleting new owner`.

Pull requests should include a clear summary, linked issue when applicable, test evidence (`npm run build`, `npm run test:run`, `npm run lint`), and notes about Redis requirements. Keep PRs scoped; avoid unrelated docs or formatting churn.

## Security & Configuration Tips

Do not run real Redis tests against shared production data: tests may call `FLUSHDB`. Use disposable local Redis databases. Keep lock ownership checks in Lua scripts; never replace token-checked release/extend logic with unconditional `DEL` outside explicit admin-only force release paths.
