/**
 * RedlockToolkit - Simple Demo
 *
 * Minimal example demonstrating the core lock workflow:
 * acquire -> do work -> release.
 *
 * Requires a running Redis instance on localhost:6379.
 *
 * Usage:
 *   npx tsx examples/simple-demo.ts
 */

import Redis from 'ioredis';
import RedlockToolkit from '../src/index';

async function main() {
  // --- Setup ---
  const clients = [
    new Redis({ port: 6379, host: 'localhost', db: 0 }),
    new Redis({ port: 6379, host: 'localhost', db: 1 }),
    new Redis({ port: 6379, host: 'localhost', db: 2 }),
  ];

  const toolkit = new RedlockToolkit({
    clients,
    defaultLockOptions: {
      ttl: 5000,
      retryCount: 3,
      retryDelay: 200,
    },
  });

  try {
    // --- 1. Manual acquire / release ---
    console.log('1. Manual lock acquire / release');

    const lock = await toolkit.acquire('demo:resource');
    console.log(`   Acquired lock ${lock.identifier.slice(0, 8)}...`);
    console.log(`   Resources: ${lock.resources.join(', ')}`);
    console.log(`   Expires in: ${lock.timeToExpiration}ms`);

    // Simulate work
    await new Promise(resolve => setTimeout(resolve, 500));

    await lock.release();
    console.log('   Released.\n');

    // --- 2. Auto-managed lock with using() ---
    console.log('2. Auto-managed lock with using()');

    const result = await toolkit.using('demo:auto', async (signal) => {
      console.log('   Inside critical section...');

      // Check abort signal in long loops
      if (signal.aborted) throw signal.error;

      await new Promise(resolve => setTimeout(resolve, 300));
      return 42;
    }, {
      ttl: 3000,
      autoExtendThreshold: 500,
    });

    console.log(`   Result: ${result}`);
    console.log('   Lock released automatically.\n');

    // --- 3. Lock properties ---
    console.log('3. Lock instance properties');

    const lock3 = await toolkit.acquire('demo:props', { ttl: 10000 });
    console.log(`   isValid:          ${lock3.isValid}`);
    console.log(`   isExpired:        ${lock3.isExpired}`);
    console.log(`   duration:         ${lock3.duration}ms`);
    console.log(`   timeToExpiration: ${lock3.timeToExpiration}ms`);

    await lock3.extend(5000);
    console.log(`   After extend:     extensions=${lock3.extensions}`);

    await lock3.release();
    console.log(`   released:         ${lock3.released}\n`);

    console.log('Done.');
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await toolkit.shutdown();
    for (const client of clients) {
      client.disconnect();
    }
  }
}

main().catch(console.error);
