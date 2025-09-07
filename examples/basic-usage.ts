/**
 * RedlockToolkit Basic Usage Example
 * Demonstrates basic distributed locking features
 */

import Redis from 'ioredis';
// For local development, use relative path
// After publishing, you can use: import RedlockToolkit from '@trishchuk/redlock-toolkit';
import RedlockToolkit from '../src/index';

async function main() {
  console.log('🚀 RedlockToolkit Example\n');

  // Create Redis clients (use your actual Redis configuration)
  // For production, use different Redis instances
  const clients = [
    new Redis({ port: 6379, host: 'localhost', db: 0 }),
    new Redis({ port: 6379, host: 'localhost', db: 1 }),
    new Redis({ port: 6379, host: 'localhost', db: 2 }),
  ];

  // Create RedlockToolkit instance
  const redlock = new RedlockToolkit({
    clients,
    defaultLockOptions: {
      ttl: 10000, // 10 seconds
      retryCount: 3,
      retryDelay: 200,
      retryJitter: 100,
    },
  });

  try {
    // Example 1: Simple lock acquisition and release
    console.log('📝 Example 1: Simple Lock\n');
    
    const lock1 = await redlock.acquire('resource:1');
    console.log(`✅ Acquired lock: ${lock1.identifier}`);
    console.log(`   Resources: ${lock1.resources.join(', ')}`);
    console.log(`   Expires in: ${lock1.timeToExpiration}ms\n`);
    
    // Do some work...
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    await lock1.release();
    console.log('✅ Lock released\n');

    // Example 2: Multiple resources
    console.log('📝 Example 2: Multiple Resources\n');
    
    const lock2 = await redlock.acquire(['user:123', 'account:456']);
    console.log(`✅ Acquired locks for multiple resources`);
    console.log(`   Resources: ${lock2.resources.join(', ')}`);
    console.log(`   Identifier: ${lock2.identifier}\n`);
    
    await lock2.release();
    console.log('✅ Multiple locks released\n');

    // Example 3: Using auto-extension
    console.log('📝 Example 3: Auto-Extension\n');
    
    await redlock.using(
      'long-task',
      async (signal) => {
        console.log('✅ Started long task with auto-extension');
        
        for (let i = 0; i < 5; i++) {
          if (signal.aborted) {
            console.log('❌ Task aborted:', signal.error?.message);
            throw signal.error;
          }
          
          console.log(`   Step ${i + 1}/5 completed`);
          await new Promise(resolve => setTimeout(resolve, 500));
        }
        
        console.log('✅ Long task completed');
      },
      {
        ttl: 2000, // 2 second TTL
        autoExtendThreshold: 1000, // Extend when 1 second remains
      }
    );
    
    console.log('✅ Auto-extending lock released\n');

    // Example 4: Lock with retry on failure
    console.log('📝 Example 4: Retry Logic\n');
    
    // Acquire a lock to simulate contention
    const blockingLock = await redlock.acquire('contested-resource', { ttl: 2000 });
    console.log('🔒 Resource locked by another process');
    
    // Try to acquire the same resource (will retry)
    const acquirePromise = redlock.acquire('contested-resource', {
      ttl: 5000,
      retryCount: 5,
      retryDelay: 300,
    }).catch(err => {
      console.log(`❌ Failed to acquire after retries: ${err.message}`);
    });
    
    // Release the blocking lock after 1 second
    setTimeout(async () => {
      await blockingLock.release();
      console.log('🔓 Blocking lock released');
    }, 1000);
    
    const retriedLock = await acquirePromise;
    if (retriedLock) {
      console.log('✅ Successfully acquired lock after retry');
      await retriedLock.release();
    }
    
    console.log('\n✨ All examples completed successfully!');

  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    // Clean up
    await redlock.shutdown();
    
    // Close Redis connections
    for (const client of clients) {
      client.disconnect();
    }
  }
}

// Run the example
main().catch(console.error);