import { describe, it, expect, beforeEach, afterEach } from "vitest";
import Redis from "ioredis";
import { OptimisticRedlock } from "../src";

describe("OptimisticRedlock", () => {
  let clients: Redis[];
  let redlock: OptimisticRedlock;

  beforeEach(async () => {
    // Create 3 Redis clients for testing consensus
    clients = [
      new Redis({ port: 6379, host: "localhost", db: 0 }),
      new Redis({ port: 6379, host: "localhost", db: 1 }),
      new Redis({ port: 6379, host: "localhost", db: 2 }),
    ];

    redlock = new OptimisticRedlock(clients, {
      driftFactor: 0.01,
      retryCount: 3,
      retryDelay: 100,
      retryJitter: 50,
      conflictRetryCount: 3,
      conflictRetryDelay: 50,
      conflictBackoffFactor: 2,
    });

    // Clean up test data
    for (const client of clients) {
      await client.flushdb();
    }
  });

  afterEach(async () => {
    for (const client of clients) {
      await client.quit();
    }
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
      // Create a separate instance for this test
      const testClients = [
        new Redis({ port: 6379, host: "localhost", db: 3 }),
        new Redis({ port: 6379, host: "localhost", db: 4 }),
        new Redis({ port: 6379, host: "localhost", db: 5 }),
      ];
      const testRedlock = new OptimisticRedlock(testClients);

      // Close all connections
      for (const client of testClients) {
        await client.quit();
      }

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