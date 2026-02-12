/**
 * RedlockToolkit - Advanced Features Example
 *
 * Demonstrates: error handling, TTL management, metrics collection,
 * force release, lock status, and lock instance methods.
 *
 * Requires a running Redis instance on localhost:6379.
 *
 * Usage:
 *   npx tsx examples/advanced-features.ts
 */

import Redis from 'ioredis';
import RedlockToolkit, {
  LockTimeoutError,
  ResourceLockedError,
} from '../src/index';

async function main() {
  console.log('RedlockToolkit - Advanced Features\n');

  const clients = [
    new Redis({ port: 6379, host: 'localhost', db: 0 }),
    new Redis({ port: 6379, host: 'localhost', db: 1 }),
    new Redis({ port: 6379, host: 'localhost', db: 2 }),
  ];

  const redlock = new RedlockToolkit({
    clients,
    defaultLockOptions: {
      ttl: 5000,
      retryCount: 3,
      retryDelay: 200,
      retryJitter: 50,
      driftFactor: 0.01,
      autoExtendThreshold: 1000,
    },
    circuitBreaker: {
      failureThreshold: 5,
      resetTimeout: 30000,
      maxRetries: 3,
    },
    enableMetrics: true,
    keyPrefix: 'myapp',
  });

  // Listen for circuit breaker state changes
  redlock.on('circuit:stateChanged', (newState) => {
    console.log(`  [event] Circuit breaker state: ${newState}`);
  });

  try {
    // --- 1. Error Handling ---
    console.log('1. Error Handling\n');

    const lock1 = await redlock.acquire('resource:error-test');
    console.log('  First lock acquired');

    try {
      await redlock.acquire('resource:error-test', { retryCount: 0 });
    } catch (error) {
      if (error instanceof ResourceLockedError) {
        console.log(`  Expected: ResourceLockedError (holder: ${error.currentHolder?.slice(0, 8)}...)`);
      } else if (error instanceof LockTimeoutError) {
        console.log(`  Expected: LockTimeoutError after ${error.attemptsCount} attempts`);
      }
    }

    await lock1.release();
    console.log('  Lock released.\n');

    // --- 2. TTL Management ---
    console.log('2. TTL Management\n');

    const lock2 = await redlock.acquire('resource:ttl-test', { ttl: 3000 });
    console.log(`  Acquired with TTL: ${lock2.timeToExpiration}ms`);

    await new Promise(resolve => setTimeout(resolve, 1000));
    console.log(`  After 1s, remaining: ${lock2.timeToExpiration}ms`);

    await lock2.release();
    console.log('  Released before expiration.\n');

    // --- 3. Metrics Collection ---
    console.log('3. Metrics\n');

    for (let i = 0; i < 5; i++) {
      const lock = await redlock.acquire(`unique:metric:test:${i}:${Date.now()}`);
      await new Promise(resolve => setTimeout(resolve, Math.random() * 100));
      await lock.release();
    }

    const metrics = redlock.getMetrics();
    console.log('  Metrics summary:');
    console.log(`    Locks acquired:      ${metrics.locksAcquired}`);
    console.log(`    Locks released:      ${metrics.locksReleased}`);
    console.log(`    Failed acquisitions: ${metrics.failedAcquisitions}`);
    console.log(`    Active locks:        ${metrics.activeLocks}`);
    console.log(`    Avg lock duration:   ${metrics.averageLockDuration.toFixed(2)}ms\n`);

    // Prometheus export
    const prometheusText = redlock.exportMetrics();
    console.log('  Prometheus export (first 3 lines):');
    prometheusText.split('\n').slice(0, 3).forEach(line => {
      console.log(`    ${line}`);
    });
    console.log();

    // --- 4. Force Release (Admin Operation) ---
    console.log('4. Administrative Operations\n');

    const stuckLock = await redlock.acquire('admin:stuck-resource');
    console.log('  Created a stuck lock');

    const releaseResult = await redlock.forceRelease(['admin:stuck-resource']);
    console.log(`  Force released: ${releaseResult.releasedCount} locks`);

    const newLock = await redlock.acquire('admin:stuck-resource');
    console.log('  Successfully acquired previously stuck resource');
    await newLock.release();
    console.log();

    // --- 5. Lock Status Check ---
    console.log('5. Lock Status\n');

    const statusLock = await redlock.acquire('status:check-test', { ttl: 10000 });

    const statuses = await redlock.getStatus(['status:check-test', 'status:nonexistent']);
    for (const s of statuses) {
      if (s.locked) {
        console.log(`  ${s.resource}: locked by ${s.holder?.slice(0, 8)}... (TTL: ${s.ttl}ms)`);
      } else {
        console.log(`  ${s.resource}: not locked`);
      }
    }

    await statusLock.release();
    console.log();

    // --- 6. Lock Instance Methods ---
    console.log('6. Lock Instance Methods\n');

    const lock6 = await redlock.acquire('instance:test');
    const info = lock6.getInfo();
    console.log(`  Acquired at: ${new Date(info.acquisitionTime).toISOString()}`);
    console.log(`  Duration:    ${lock6.duration}ms`);
    console.log(`  TTL left:    ${lock6.timeToExpiration}ms`);
    console.log(`  isValid:     ${lock6.isValid}`);
    console.log(`  isExpired:   ${lock6.isExpired}`);

    await lock6.extend(10000);
    console.log(`  Extended (count: ${lock6.extensions}), new TTL: ${lock6.timeToExpiration}ms`);

    // JSON representation
    const json = lock6.toJSON();
    console.log(`  toJSON keys: ${Object.keys(json).join(', ')}`);

    await lock6.release();
    console.log();

    // --- 7. Performance Summary ---
    console.log('7. Performance Summary\n');

    const summary = redlock.getPerformanceSummary();
    console.log(`  Success rate:        ${(summary.successRate * 100).toFixed(1)}%`);
    console.log(`  Avg acquire latency: ${summary.averageAcquisitionLatency.toFixed(2)}ms`);
    console.log(`  Retry rate:          ${(summary.retryRate * 100).toFixed(1)}%`);
    console.log(`  Circuit breaker:     ${summary.circuitBreakerHealth.overallState}\n`);

    console.log('All advanced examples completed.');

  } catch (error) {
    console.error('Error:', error);
  } finally {
    await redlock.shutdown();
    for (const client of clients) {
      client.disconnect();
    }
  }
}

main().catch(console.error);
