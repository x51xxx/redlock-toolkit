# redlock-toolkit Skill

AI agent skill for the `@trishchuk/redlock-toolkit` distributed locking library.

## What's Included

- **SKILL.md** — Quick reference tables, essential patterns, and setup guide.
- **reference/CONFIGURATION.md** — Constructor options, Redis setup, pub/sub, shutdown.
- **reference/LOCKING.md** — Pessimistic lock acquisition, release, extension, auto-managed `using()`.
- **reference/OPTIMISTIC.md** — Version-based optimistic locking, hybrid strategy.
- **reference/PRIMITIVES.md** — Distributed semaphore and countdown latch.
- **reference/ERRORS.md** — Error class hierarchy and retry logic.
- **reference/MONITORING.md** — Metrics collection, Prometheus export, circuit breaker, events.

## When to Use

This skill is activated when working with:
- `RedlockToolkit` configuration and API usage
- Distributed lock acquisition patterns
- Semaphore and countdown latch primitives
- Lock contention debugging and error handling
- Metrics and monitoring setup

## Library Overview

`@trishchuk/redlock-toolkit` implements the Redlock algorithm for distributed mutual exclusion across Redis nodes. It provides:

- Pessimistic, optimistic, and hybrid locking strategies
- Distributed semaphore (concurrency limiter)
- Distributed countdown latch (barrier synchronization)
- Circuit breaker for fault tolerance
- Prometheus-compatible metrics
- Pub/Sub-based lock waiting
- Automatic lock extension for long-running operations
