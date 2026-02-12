/**
 * RedlockToolkit - Basic Usage Example
 *
 * Demonstrates fundamental distributed locking features:
 * simple locks, multi-resource locks, auto-extension, and retry logic.
 *
 * Requires a running Redis instance on localhost:6379.
 *
 * Usage:
 *   npx tsx examples/basic-usage.ts
 */

import Redis from 'ioredis';
// For published package: import RedlockToolkit from '@trishchuk/redlock-toolkit';
import RedlockToolkit from '../src/index';

async function main() {
  console.log('RedlockToolkit - Basic Usage\n');

  // Create Redis clients (in production, use separate Redis instances)
  const clients = [
    new Redis({ port: 6379, host: 'localhost', db: 0 }),
    new Redis({ port: 6379, host: 'localhost', db: 1 }),
    new Redis({ port: 6379, host: 'localhost', db: 2 }),
  ];

  const redlock = new RedlockToolkit({
    clients,
    defaultLockOptions: {
      ttl: 10000,
      retryCount: 3,
      retryDelay: 200,
      retryJitter: 100,
    },
  });

  try {
    // --- Example 1: Simple lock acquire and release ---
    console.log('Example 1: Simple Lock\n');

    const lock1 = await redlock.acquire('resource:1');
    console.log(`  Acquired lock: ${lock1.identifier.slice(0, 8)}...`);
    console.log(`  Resources: ${lock1.resources.join(', ')}`);
    console.log(`  Expires in: ${lock1.timeToExpiration}ms\n`);

    await new Promise(resolve => setTimeout(resolve, 1000));

    await lock1.release();
    console.log('  Lock released.\n');

    // --- Example 2: Multiple resources ---
    console.log('Example 2: Multiple Resources\n');

    const lock2 = await redlock.acquire(['user:123', 'account:456']);
    console.log(`  Acquired lock for: ${lock2.resources.join(', ')}`);
    console.log(`  Identifier: ${lock2.identifier.slice(0, 8)}...\n`);

    await lock2.release();
    console.log('  Released.\n');

    // --- Example 3: Auto-extension with using() ---
    console.log('Example 3: Auto-Extension\n');

    await redlock.using(
      'long-task',
      async (signal) => {
        console.log('  Started long task with auto-extension');

        for (let i = 0; i < 5; i++) {
          if (signal.aborted) {
            console.log('  Task aborted:', signal.error?.message);
            throw signal.error;
          }

          console.log(`  Step ${i + 1}/5 completed`);
          await new Promise(resolve => setTimeout(resolve, 500));
        }

        console.log('  Long task completed');
      },
      {
        ttl: 2000,
        autoExtendThreshold: 1000,
      },
    );

    console.log('  Auto-extending lock released.\n');

    // --- Example 4: Retry on contention ---
    console.log('Example 4: Retry Logic\n');

    // Acquire a lock to simulate contention
    const blockingLock = await redlock.acquire('contested-resource', { ttl: 2000 });
    console.log('  Resource locked by another holder');

    // Try to acquire the same resource with retries
    const acquirePromise = redlock.acquire('contested-resource', {
      ttl: 5000,
      retryCount: 5,
      retryDelay: 300,
    });

    // Release the blocking lock after 1 second
    setTimeout(async () => {
      await blockingLock.release();
      console.log('  Blocking lock released');
    }, 1000);

    try {
      const retriedLock = await acquirePromise;
      console.log('  Successfully acquired lock after retry');
      await retriedLock.release();
    } catch (err) {
      console.log(`  Failed to acquire after retries: ${(err as Error).message}`);
    }

    console.log('\nAll examples completed.');

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
