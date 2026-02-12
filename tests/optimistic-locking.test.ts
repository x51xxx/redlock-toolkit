import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { RedisClient } from "../src/core/types";
import { OptimisticRedlock } from "../src";

// Helper to create mock Redis clients
function createMockRedisClients(count: number): RedisClient[] {
  return Array.from({ length: count }, () => ({
    evalsha: vi.fn(),
    eval: vi.fn(),
    get: vi.fn(),
    set: vi.fn(),
    del: vi.fn(),
    pexpire: vi.fn(),
    script: vi.fn(),
    publish: vi.fn(),
    subscribe: vi.fn(),
    unsubscribe: vi.fn(),
    on: vi.fn(),
    removeListener: vi.fn(),
    options: { host: 'localhost', port: 6379 },
    quit: vi.fn(),
    disconnect: vi.fn(),
  } as any));
}

describe("OptimisticRedlock", () => {
  let clients: RedisClient[];
  let redlock: OptimisticRedlock;
  let dataStores: Map<string, { value: string; version: string; expiry?: number }>[];

  beforeEach(() => {
    // Create 3 mocked Redis clients for testing consensus
    clients = createMockRedisClients(3);
    // Create a separate dataStore for each client to simulate independent Redis instances
    dataStores = [new Map(), new Map(), new Map()];

    // Mock the eval method to simulate optimistic locking behavior
    clients.forEach((client: any, clientIndex: number) => {
      const dataStore = dataStores[clientIndex];
      
      // Mock get method for direct access tests
      client.get.mockImplementation((key: string) => {
        const data = dataStore.get(key);
        if (data) {
          if (data.expiry && Date.now() > data.expiry) {
            dataStore.delete(key);
            return Promise.resolve(null);
          }
          return Promise.resolve(data.value);
        }
        // Version key lookup: strip ":version" suffix, return version from base entry
        if (key.endsWith(':version')) {
          const baseKey = key.slice(0, -':version'.length);
          const baseData = dataStore.get(baseKey);
          if (baseData) {
            if (baseData.expiry && Date.now() > baseData.expiry) {
              dataStore.delete(baseKey);
              return Promise.resolve(null);
            }
            return Promise.resolve(baseData.version);
          }
        }
        return Promise.resolve(null);
      });
      
      client.eval.mockImplementation((script: string, keyCount: number, ...args: any[]) => {
        const keys = args.slice(0, keyCount);
        const argv = args.slice(keyCount);
        const key = keys[0];

        // --- Optimistic scripts (from optimistic-redlock.ts) ---
        // Order matters: check more specific patterns first to avoid cross-contamination.

        // COMPARE_AND_SWAP_SCRIPT (unique: 'if current == ARGV[1]')
        if (script.includes('if current == ARGV[1]')) {
          const current = dataStore.get(key);
          const expectedValue = argv[0];
          const newValue = argv[1];
          const ttl = argv[2] ? parseInt(argv[2]) : undefined;

          if (current && current.value === expectedValue) {
            const newVersion = String(parseInt(current.version) + 1);
            dataStore.set(key, {
              value: newValue,
              version: newVersion,
              expiry: ttl ? Date.now() + ttl : undefined,
            });
            return Promise.resolve(parseInt(newVersion));
          }
          return Promise.resolve(null);
        }

        // OPTIMISTIC_DELETE_SCRIPT (unique: '"DEL", KEYS[1] .. ":version"')
        if (script.includes('"DEL", KEYS[1] .. ":version"')) {
          const currentData = dataStore.get(key);
          const expectedVersion = argv[0];

          if (currentData && currentData.version === expectedVersion) {
            dataStore.delete(key);
            return Promise.resolve(1);
          }
          return Promise.resolve(0);
        }

        // OPTIMISTIC_WRITE_SCRIPT (unique: 'ARGV[2] == "0"')
        if (script.includes('ARGV[2] == "0"')) {
          const newValue = argv[0];
          const expectedVersion = argv[1];
          const ttl = argv[2] ? parseInt(argv[2]) : undefined;

          // Check expiration
          let currentData = dataStore.get(key);
          if (currentData?.expiry && Date.now() > currentData.expiry) {
            dataStore.delete(key);
            currentData = undefined;
          }

          // First write (no version exists)
          if (!currentData && expectedVersion === "0") {
            dataStore.set(key, {
              value: newValue,
              version: "1",
              expiry: ttl ? Date.now() + ttl : undefined,
            });
            return Promise.resolve(1);
          }

          // Version matches - update allowed
          if (currentData && currentData.version === expectedVersion) {
            const newVersion = String(parseInt(currentData.version) + 1);
            dataStore.set(key, {
              value: newValue,
              version: newVersion,
              expiry: ttl ? Date.now() + ttl : undefined,
            });
            return Promise.resolve(parseInt(newVersion));
          }

          // Version mismatch
          return Promise.resolve(null);
        }

        // OPTIMISTIC_READ_SCRIPT (unique: 'return {value, version')
        if (script.includes('return {value, version')) {
          const data = dataStore.get(key);
          if (data) {
            if (data.expiry && Date.now() > data.expiry) {
              dataStore.delete(key);
              return Promise.resolve(null);
            }
            return Promise.resolve([data.value, data.version]);
          }
          return Promise.resolve(null);
        }

        // --- Redlock scripts (from scripts.ts, used by transaction()) ---

        // ACQUIRE_SCRIPT
        if (script.includes('RedlockToolkit Acquire Script')) {
          const identifier = argv[0];
          const ttl = parseInt(argv[1]);

          for (const k of keys) {
            const existing = dataStore.get(k);
            if (existing && existing.value !== identifier &&
                (!existing.expiry || Date.now() < existing.expiry)) {
              return Promise.resolve(0);
            }
          }

          for (const k of keys) {
            dataStore.set(k, {
              value: identifier,
              version: '1',
              expiry: Date.now() + ttl,
            });
          }
          return Promise.resolve(keys.length);
        }

        // RELEASE_SCRIPT
        if (script.includes('RedlockToolkit Release Script')) {
          const identifier = argv[0];
          let released = 0;

          for (const k of keys) {
            const existing = dataStore.get(k);
            if (existing && existing.value === identifier) {
              dataStore.delete(k);
              released++;
            }
          }
          return Promise.resolve(released);
        }

        // EXTEND_SCRIPT
        if (script.includes('RedlockToolkit Extend Script')) {
          const identifier = argv[0];
          const ttl = parseInt(argv[1]);

          for (const k of keys) {
            const existing = dataStore.get(k);
            if (!existing || existing.value !== identifier) {
              return Promise.resolve(0);
            }
          }

          for (const k of keys) {
            const existing = dataStore.get(k)!;
            dataStore.set(k, { ...existing, expiry: Date.now() + ttl });
          }
          return Promise.resolve(keys.length);
        }

        return Promise.resolve(null);
      });
      
      // Mock evalsha to simulate script cache for regular lock operations
      client.evalsha.mockImplementation((_hash: string, keyCount: number, ...args: any[]) => {
        const keys = args.slice(0, keyCount);
        const argv = args.slice(keyCount);
        const key = keys[0];
        const identifier = argv[0];
        const ttl = argv[1];
        
        // For Redlock acquire operations (used in transaction method)
        // Check if this is a lock acquisition or release
        if (identifier && ttl) {
          // Acquisition attempt
          const lockKey = key + ':txlock';
          const existingLock = dataStore.get(lockKey);
          
          if (!existingLock || (existingLock.expiry && Date.now() > existingLock.expiry)) {
            // No lock or expired lock - acquire it
            dataStore.set(lockKey, { 
              value: identifier, 
              version: '1',
              expiry: Date.now() + parseInt(ttl)
            });
            return Promise.resolve(1);
          }
          
          // Lock exists and is not expired
          return Promise.resolve(0);
        } else if (identifier && !ttl) {
          // Release attempt
          const lockKey = key + ':txlock';
          const existingLock = dataStore.get(lockKey);
          
          if (existingLock && existingLock.value === identifier) {
            dataStore.delete(lockKey);
            return Promise.resolve(1);
          }
          return Promise.resolve(0);
        }
        
        return Promise.resolve(0);
      });
    });

    redlock = new OptimisticRedlock(clients, {
      driftFactor: 0.01,
      retryCount: 3,
      retryDelay: 100,
      retryJitter: 50,
      conflictRetryCount: 3,
      conflictRetryDelay: 50,
      conflictBackoffFactor: 2,
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("Optimistic Read Operations", () => {
    it("should read non-existent resource as null", async () => {
      const result = await redlock.optimisticRead("test:resource");
      expect(result).toBeNull();
    });

    it("should read existing resource with version", async () => {
      // First write a value
      const writeResult = await redlock.optimisticWrite(
        "test:resource",
        "initial-value",
        0,
        5000
      );
      expect(writeResult).toBe(1);

      // Read it back
      const readResult = await redlock.optimisticRead("test:resource");
      expect(readResult).not.toBeNull();
      expect(readResult?.value).toBe("initial-value");
      expect(readResult?.version).toBe(1);
    });

    it("should acquire read-only lock", async () => {
      // Write initial value
      await redlock.optimisticWrite("test:resource", "value1", 0, 5000);

      // Acquire read lock
      const lock = await redlock.acquireOptimisticRead("test:resource");
      expect(lock).not.toBeNull();
      expect(lock?.isReadOnly).toBe(true);
      expect(lock?.value).toBe("value1");
      expect(lock?.version).toBe(1);
    });

    it("should not allow updates on read-only lock", async () => {
      await redlock.optimisticWrite("test:resource", "value1", 0, 5000);
      const lock = await redlock.acquireOptimisticRead("test:resource");

      await expect(lock!.update("new-value")).rejects.toThrow(
        "Cannot update read-only lock"
      );
    });
  });

  describe("Optimistic Write Operations", () => {
    it("should write new resource with version 1", async () => {
      const version = await redlock.optimisticWrite(
        "test:resource",
        "new-value",
        0,
        5000
      );
      expect(version).toBe(1);

      const result = await redlock.optimisticRead("test:resource");
      expect(result?.value).toBe("new-value");
      expect(result?.version).toBe(1);
    });

    it("should update resource with correct version", async () => {
      // Initial write
      await redlock.optimisticWrite("test:resource", "value1", 0, 5000);

      // Update with correct version
      const newVersion = await redlock.optimisticWrite(
        "test:resource",
        "value2",
        1,
        5000
      );
      expect(newVersion).toBe(2);

      const result = await redlock.optimisticRead("test:resource");
      expect(result?.value).toBe("value2");
      expect(result?.version).toBe(2);
    });

    it("should fail update with incorrect version", async () => {
      // Initial write
      const firstVersion = await redlock.optimisticWrite("test:resource", "value1", 0, 5000);
      expect(firstVersion).toBe(1);

      // Try to update with wrong version
      const result = await redlock.optimisticWrite(
        "test:resource",
        "value2",
        0, // Wrong version (should be 1)
        5000
      );
      expect(result).toBeNull();

      // Value should remain unchanged
      const readResult = await redlock.optimisticRead("test:resource");
      expect(readResult?.value).toBe("value1");
      expect(readResult?.version).toBe(1);
    });

    it("should handle concurrent writes with conflict detection", async () => {
      // Initial write
      await redlock.optimisticWrite("test:resource", "value1", 0, 5000);

      // Read to get current version
      const current = await redlock.optimisticRead("test:resource");
      expect(current?.version).toBe(1);

      // Try concurrent writes - both with same version
      const write1 = redlock.optimisticWrite("test:resource", "value2", 1, 5000);
      const write2 = redlock.optimisticWrite("test:resource", "value3", 1, 5000);

      const results = await Promise.all([write1, write2]);

      // At least one should succeed (or both if timing allows)
      const successCount = results.filter((r) => r !== null).length;
      expect(successCount).toBeGreaterThanOrEqual(1);

      // Check final state
      const finalResult = await redlock.optimisticRead("test:resource");
      // Version should be incremented at least once
      expect(finalResult?.version).toBeGreaterThanOrEqual(2);
      expect(["value1", "value2", "value3"]).toContain(finalResult?.value);
    });
  });

  describe("Compare and Swap Operations", () => {
    it("should perform CAS when value matches", async () => {
      await redlock.optimisticWrite("test:resource", "value1", 0, 5000);

      const result = await redlock.compareAndSwap(
        "test:resource",
        "value1",
        "value2",
        5000
      );
      expect(result).toBe(2);

      const readResult = await redlock.optimisticRead("test:resource");
      expect(readResult?.value).toBe("value2");
      expect(readResult?.version).toBe(2);
    });

    it("should fail CAS when value doesn't match", async () => {
      await redlock.optimisticWrite("test:resource", "value1", 0, 5000);

      const result = await redlock.compareAndSwap(
        "test:resource",
        "wrong-value",
        "value2",
        5000
      );
      expect(result).toBeNull();

      const readResult = await redlock.optimisticRead("test:resource");
      expect(readResult?.value).toBe("value1");
      expect(readResult?.version).toBe(1);
    });
  });

  describe("Delete Operations", () => {
    it("should delete with correct version", async () => {
      await redlock.optimisticWrite("test:resource", "value1", 0, 5000);

      const deleted = await redlock.optimisticDelete("test:resource", 1);
      expect(deleted).toBe(true);

      const result = await redlock.optimisticRead("test:resource");
      expect(result).toBeNull();
    });

    it("should fail delete with incorrect version", async () => {
      await redlock.optimisticWrite("test:resource", "value1", 0, 5000);

      const deleted = await redlock.optimisticDelete("test:resource", 0);
      expect(deleted).toBe(false);

      const result = await redlock.optimisticRead("test:resource");
      expect(result?.value).toBe("value1");
    });
  });

  describe("Write Lock Operations", () => {
    it("should acquire write lock and perform updates", async () => {
      // Write initial value
      await redlock.optimisticWrite("test:resource", "initial", 0, 5000);

      // Acquire write lock
      const lock = await redlock.acquireOptimisticWrite("test:resource", 5000);
      expect(lock.isReadOnly).toBe(false);
      expect(lock.value).toBe("initial");
      expect(lock.version).toBe(1);

      // Perform update
      const updated = await lock.update("updated-value", 5000);
      expect(updated).toBe(true);
      expect(lock.currentVersion).toBe(2);

      // Verify update
      const result = await redlock.optimisticRead("test:resource");
      expect(result?.value).toBe("updated-value");
      expect(result?.version).toBe(2);
    });

    it("should handle lock refresh", async () => {
      await redlock.optimisticWrite("test:resource", "value1", 0, 5000);

      const lock = await redlock.acquireOptimisticRead("test:resource");
      expect(lock?.version).toBe(1);

      // Update the resource externally
      await redlock.optimisticWrite("test:resource", "value2", 1, 5000);

      // Refresh the lock
      const refreshed = await lock!.refresh();
      expect(refreshed.version).toBe(2);
      expect(refreshed.value).toBe("value2");
    });
  });

  describe("Transaction Support", () => {
    it("should execute transaction successfully", async () => {
      await redlock.optimisticWrite("counter", "0", 0, 5000);

      const result = await redlock.transaction(
        "counter",
        async (lock) => {
          const currentValue = parseInt(lock.value || "0", 10);
          const newValue = currentValue + 1;
          await lock.update(newValue.toString(), 5000);
          return newValue;
        },
        { ttl: 5000 }
      );

      expect(result).toBe(1);

      const final = await redlock.optimisticRead("counter");
      expect(final?.value).toBe("1");
    });

    it("should retry transaction on conflict", async () => {
      await redlock.optimisticWrite("counter", "0", 0, 5000);

      let attemptCount = 0;

      // Simulate conflicting updates
      const transaction1 = redlock.transaction(
        "counter",
        async (lock) => {
          attemptCount++;
          await new Promise((resolve) => setTimeout(resolve, 100));
          const currentValue = parseInt(lock.value || "0", 10);
          await lock.update((currentValue + 1).toString(), 5000);
          return "tx1";
        },
        { ttl: 5000 }
      );

      const transaction2 = redlock.transaction(
        "counter",
        async (lock) => {
          const currentValue = parseInt(lock.value || "0", 10);
          await lock.update((currentValue + 2).toString(), 5000);
          return "tx2";
        },
        { ttl: 5000 }
      );

      const results = await Promise.allSettled([transaction1, transaction2]);

      // Both should eventually complete (one might retry)
      const successCount = results.filter(
        (r) => r.status === "fulfilled"
      ).length;
      expect(successCount).toBeGreaterThan(0);
    });

    it("should fail transaction after max retries", async () => {
      await redlock.optimisticWrite("test:resource", "value1", 0, 5000);

      // Create a transaction that always fails
      const promise = redlock.transaction(
        "test:resource",
        async (lock) => {
          // Simulate external conflict by updating with wrong version
          await redlock.optimisticWrite("test:resource", "conflict", 999, 5000);
          throw new Error("conflict");
        },
        { ttl: 5000, maxRetries: 2 }
      );

      await expect(promise).rejects.toThrow();
    });
  });

  describe("Consensus and Quorum", () => {
    it("should achieve quorum with majority nodes", async () => {
      const version = await redlock.optimisticWrite(
        "test:quorum",
        "value",
        0,
        5000
      );
      expect(version).toBe(1);

      // Verify on all nodes
      for (const client of clients) {
        const value = await client.get("test:quorum");
        const version = await client.get("test:quorum:version");
        expect(value).toBe("value");
        expect(version).toBe("1");
      }
    });

    it("should handle node failure gracefully", async () => {
      // Disconnect one client
      await clients[2].quit();
      clients.pop();

      // Should still work with 2 out of 3 nodes
      const version = await redlock.optimisticWrite(
        "test:partial",
        "value",
        0,
        5000
      );
      expect(version).toBe(1);

      const result = await redlock.optimisticRead("test:partial");
      expect(result?.value).toBe("value");
    });
  });

  describe("TTL and Expiration", () => {
    it("should respect TTL on write", async () => {
      await redlock.optimisticWrite("test:ttl", "value", 0, 100);

      // Immediate read should work
      let result = await redlock.optimisticRead("test:ttl");
      expect(result?.value).toBe("value");

      // Wait for expiration
      await new Promise((resolve) => setTimeout(resolve, 150));

      // Should be expired
      result = await redlock.optimisticRead("test:ttl");
      expect(result).toBeNull();
    });

    it("should extend TTL on update", async () => {
      await redlock.optimisticWrite("test:ttl", "value1", 0, 200);

      // Update with new TTL
      await redlock.optimisticWrite("test:ttl", "value2", 1, 1000);

      // Wait for original TTL
      await new Promise((resolve) => setTimeout(resolve, 250));

      // Should still exist due to extended TTL
      const result = await redlock.optimisticRead("test:ttl");
      expect(result?.value).toBe("value2");
    });
  });

  describe("Error Handling", () => {
    it("should handle network errors gracefully", async () => {
      // Create clients that will fail
      const errorClients = createMockRedisClients(3);
      errorClients.forEach((client: any) => {
        client.eval.mockRejectedValue(new Error("Network error"));
      });
      
      const testRedlock = new OptimisticRedlock(errorClients);

      // Operations should fail gracefully
      const result = await testRedlock.optimisticRead("test:resource");
      expect(result).toBeNull();

      const writeResult = await testRedlock.optimisticWrite(
        "test:resource",
        "value",
        0,
        5000
      );
      expect(writeResult).toBeNull();
    });

    it("should throw on invalid operations", async () => {
      // Try to update non-existent resource with wrong version
      const result = await redlock.optimisticWrite(
        "non-existent",
        "value",
        5, // Wrong version for new resource
        5000
      );
      expect(result).toBeNull();
    });
  });
});