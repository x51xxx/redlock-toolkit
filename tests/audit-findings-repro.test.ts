/**
 * Reproduction tests for algorithm audit findings.
 *
 * Tests marked with `it.fails` describe the desired invariant and currently
 * fail against the implementation. When the corresponding bug is fixed,
 * remove `.fails` so the invariant becomes part of the normal regression suite.
 */

import { describe, expect, it, vi } from "vitest";
import RedlockToolkit from "../src/index";
import { InternalRedlock, InternalRedlockLock } from "../src/algorithms/redlock";
import { OptimisticRedlock, ROLLBACK_WRITE_SCRIPT } from "../src/algorithms/optimistic-redlock";
import { ConsensusError } from "../src/core/errors";
import { RedisClient } from "../src/core/types";
import {
  ACQUIRE_SCRIPT,
  EXTEND_SCRIPT,
  OPTIMISTIC_ACQUIRE_SCRIPT,
  RELEASE_SCRIPT,
  SCRIPT_HASHES,
  SCRIPTS,
} from "../src/utils/scripts";
import {
  createMockRedisClients,
  createTestRedlockToolkitConfig,
  sleep,
} from "./setup";

type StoredValue = { value: string; expiration?: number; version?: number };

function createToolkitClientsWithStores(count = 3): {
  clients: ReturnType<typeof createMockRedisClients>;
  stores: Array<Map<string, StoredValue>>;
} {
  const clients = createMockRedisClients(count);
  const stores = Array.from({ length: count }, () => new Map<string, StoredValue>());

  clients.forEach((client, index) => {
    const store = stores[index];

    client.evalsha.mockImplementation(async (hash: string, keyCount: number, ...args: unknown[]) => {
      const keys = args.slice(0, keyCount) as string[];
      const argv = args.slice(keyCount) as Array<string | number>;

      if (hash === SCRIPT_HASHES.optimisticAcquire) {
        const identifier = String(argv[0]);
        const ttl = Number(argv[1]);
        for (const key of keys) {
          store.set(key, {
            value: identifier,
            expiration: Date.now() + ttl,
            version: 1,
          });
          store.set(`${key}:version`, {
            value: "1",
            expiration: Date.now() + ttl,
          });
        }
        return [1, 1, keys.length];
      }

      if (hash === SCRIPT_HASHES.extend) {
        const identifier = String(argv[0]);
        const ttl = Number(argv[1]);
        const ownsAllKeys = keys.every((key) => store.get(key)?.value === identifier);
        if (!ownsAllKeys) return 0;

        for (const key of keys) {
          store.set(key, {
            value: identifier,
            expiration: Date.now() + ttl,
          });
        }
        return keys.length;
      }

      if (hash === SCRIPT_HASHES.release) {
        const identifier = String(argv[0]);
        let released = 0;
        for (const key of keys) {
          if (store.get(key)?.value === identifier) {
            store.delete(key);
            store.delete(`${key}:version`);
            released++;
          }
        }
        return released;
      }

      return 1;
    });

    client.eval.mockImplementation(client.evalsha);
  });

  return { clients, stores };
}

describe("Audit finding repro: hybrid optimistic locks", () => {
  it("returns a Lock whose identifier matches the Redis owner token", async () => {
    const { clients, stores } = createToolkitClientsWithStores();
    const toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(clients));

    const lock = await toolkit.acquireHybrid("hybrid-resource", {
      primaryStrategy: "optimistic",
      fallbackStrategy: "pessimistic",
      ttl: 5000,
    });

    expect(lock.identifier).toBe(stores[0].get("neolock:hybrid-resource")?.value);

    await toolkit.shutdown();
  });

  it("release() removes the Redis keys acquired by the optimistic branch", async () => {
    const { clients, stores } = createToolkitClientsWithStores();
    const toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(clients));

    const lock = await toolkit.acquireHybrid("hybrid-release-resource", {
      primaryStrategy: "optimistic",
      fallbackStrategy: "pessimistic",
      ttl: 5000,
    });

    await lock.release();

    expect(stores.every((store) => !store.has("neolock:hybrid-release-resource"))).toBe(true);

    await toolkit.shutdown();
  });
});

describe("Audit finding repro: deprecated InternalRedlock ownership safety", () => {
  it("failed extension does not delete a lock that is now owned by another identifier", async () => {
    const stores = Array.from({ length: 3 }, () => new Map<string, string>());
    const clients = stores.map((store) => ({
      options: { host: "localhost", port: 6379 },
      eval: vi.fn(async (script: string, keyCount: number, ...args: string[]) => {
        const keys = args.slice(0, keyCount);
        const argv = args.slice(keyCount);

        if (script === EXTEND_SCRIPT) {
          const identifier = argv[0];
          return keys.every((key) => store.get(key) === identifier) ? keys.length : 0;
        }

        if (script === ACQUIRE_SCRIPT) return keys.length;
        if (script === RELEASE_SCRIPT) return 0;
        return 0;
      }),
      del: vi.fn(async (...keys: string[]) => {
        let removed = 0;
        for (const key of keys) {
          if (store.delete(key)) removed++;
        }
        return removed;
      }),
    })) as unknown as RedisClient[];

    for (const store of stores) {
      store.set("shared-resource", "new-owner");
    }

    const redlock = new InternalRedlock(clients);
    const staleLock = new InternalRedlockLock(redlock, {
      resources: ["shared-resource"],
      value: "old-owner",
      ttl: 1000,
      validity: Date.now() + 1000,
    });

    await expect(redlock.extend(staleLock, 1000)).rejects.toThrow("Failed to extend lock");
    expect(stores.every((store) => store.get("shared-resource") === "new-owner")).toBe(true);
  });
});

describe("Audit finding repro: consensus cleanup with late side effects", () => {
  it("cleans up late successful writes after quorum has already failed", async () => {
    const clients = createMockRedisClients(3);
    const fastFailureClients = new Set<unknown>([clients[0], clients[1]]);
    const lateStore = new Map<string, string>();
    const toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(clients));

    // The cleanup path runs the release script via client.eval on every
    // client; emulate its semantics against the shared store.
    for (const client of clients) {
      client.eval.mockImplementation(async (script: string, keyCount: number, ...args: string[]) => {
        if (String(script).includes("Release Script")) {
          const keys = args.slice(0, keyCount);
          let released = 0;
          for (const key of keys) {
            if (lateStore.delete(key)) released++;
          }
          return released;
        }
        return 1;
      });
    }

    await expect(
      toolkit.executeWithConsensus(
        async (client) => {
          if (fastFailureClients.has(client)) {
            throw new Error("fast failure");
          }

          await sleep(30);
          lateStore.set("neolock:late-side-effect", "owner");
          return 1;
        },
        {
          keys: ["neolock:late-side-effect"],
          identifier: "owner",
          script: SCRIPTS.release,
          args: ["owner"],
        },
      ),
    ).rejects.toThrow(ConsensusError);

    await sleep(60);
    expect(lateStore.has("neolock:late-side-effect")).toBe(false);

    await toolkit.shutdown();
  });
});

describe("Audit finding repro: CountDownLatch quorum semantics", () => {
  it("does not report countDown success when only one Redis node accepts the event", async () => {
    const clients = createMockRedisClients(3);
    clients.forEach((client, index) => {
      client.evalsha.mockImplementation(async (hash: string) => {
        if (hash === SCRIPT_HASHES.latchCountDown) {
          if (index === 0) return [4, 0, 1];
          throw new Error("node unavailable");
        }
        return 1;
      });
    });

    const toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(clients));
    const latch = toolkit.getCountDownLatch("partial-latch");

    await expect(latch.countDown("event-1")).rejects.toThrow();

    await toolkit.shutdown();
  });
});

describe("Audit finding repro: Redis Cluster multi-key compatibility", () => {
  // Still failing by design: switching generateLockKey to hash-tagged keys
  // changes the entire key layout (breaking for existing deployments) and
  // pins every lock to a single cluster slot. Deliberately deferred; the
  // toolkit currently logs a CROSSSLOT warning when a Cluster client is
  // detected instead. Remove `.fails` if/when hash tags are implemented.
  it.fails("generates lock keys with a shared hash tag for multi-resource cluster scripts", () => {
    const clients = createMockRedisClients(1);
    const toolkit = new RedlockToolkit(createTestRedlockToolkitConfig(clients));

    const keys = ["a", "b", "c"].map((resource) => toolkit.generateLockKey(resource));
    expect(keys.every((key) => key.includes("{neolock}"))).toBe(true);
  });
});

describe("Audit finding repro: optimistic multi-resource version guards", () => {
  it("does not special-case only KEYS[1] when expectedVersion is omitted", () => {
    expect(OPTIMISTIC_ACQUIRE_SCRIPT).not.toContain("KEYS[1] .. ':version'");
  });

  it("rolls back successful minority writes when optimisticWrite misses quorum", async () => {
    const stores = Array.from({ length: 3 }, () => new Map<string, string>());
    const clients = stores.map((store, index) => ({
      options: { host: "localhost", port: 6379 + index },
      eval: vi.fn(async (script: string, keyCount: number, ...args: string[]) => {
        const keys = args.slice(0, keyCount);
        const argv = args.slice(keyCount);

        if (script === ROLLBACK_WRITE_SCRIPT) {
          const versionKey = `${keys[0]}:version`;
          const current = store.get(versionKey);
          if (current && Number(current) === Number(argv[0]) + 1) {
            if (Number(argv[0]) === 0) {
              store.delete(keys[0]);
              store.delete(versionKey);
            } else {
              store.set(versionKey, String(Number(current) - 1));
            }
            return 1;
          }
          return 0;
        }

        if (!script.includes("ARGV[2] == \"0\"")) return null;
        if (index !== 0) throw new Error("write failed");

        store.set(keys[0], argv[0]);
        store.set(`${keys[0]}:version`, "1");
        return 1;
      }),
      del: vi.fn(),
      script: vi.fn(),
    })) as unknown as RedisClient[];

    const redlock = new OptimisticRedlock(clients);

    await expect(redlock.optimisticWrite("partial-write-resource", "value", 0)).resolves.toBeNull();
    expect(stores.every((store) => !store.has("partial-write-resource"))).toBe(true);
  });
});
