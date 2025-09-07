/**
 * Optimistic Redlock implementation with version-based conflict detection
 * Extends the base Redlock algorithm with optimistic concurrency control
 */

import { RedisClient } from "../core/types";
import { InternalRedlock as Redlock, RedlockOptions } from "./redlock";

export interface OptimisticLockOptions extends RedlockOptions {
  /**
   * Maximum number of conflict retry attempts
   */
  conflictRetryCount?: number;

  /**
   * Base delay for conflict retries (ms)
   */
  conflictRetryDelay?: number;

  /**
   * Exponential backoff factor for conflict retries
   */
  conflictBackoffFactor?: number;
}

export interface OptimisticResource {
  resource: string;
  value: string | null;
  version: number;
  ttl?: number;
}

export interface OptimisticLockResult extends OptimisticResource {
  validity: number;
  isReadOnly: boolean;
}

/**
 * Lua script for optimistic read with version
 * Returns [value, version] or nil if not exists
 */
const OPTIMISTIC_READ_SCRIPT = `
  local value = redis.call("GET", KEYS[1])
  local version = redis.call("GET", KEYS[1] .. ":version")
  if value then
    return {value, version or "0"}
  else
    return nil
  end
`;

/**
 * Lua script for optimistic write with version check
 * Only updates if version matches, increments version on success
 */
const OPTIMISTIC_WRITE_SCRIPT = `
  local currentVersion = redis.call("GET", KEYS[1] .. ":version")
  
  -- First write (no version exists)
  if not currentVersion and ARGV[2] == "0" then
    redis.call("SET", KEYS[1], ARGV[1])
    redis.call("SET", KEYS[1] .. ":version", "1")
    if ARGV[3] then
      redis.call("PEXPIRE", KEYS[1], ARGV[3])
      redis.call("PEXPIRE", KEYS[1] .. ":version", ARGV[3])
    end
    return 1
  end
  
  -- Version matches - update allowed
  if currentVersion == ARGV[2] then
    local newVersion = tostring(tonumber(currentVersion) + 1)
    redis.call("SET", KEYS[1], ARGV[1])
    redis.call("SET", KEYS[1] .. ":version", newVersion)
    if ARGV[3] then
      redis.call("PEXPIRE", KEYS[1], ARGV[3])
      redis.call("PEXPIRE", KEYS[1] .. ":version", ARGV[3])
    end
    return newVersion
  else
    -- Version mismatch - conflict
    return nil
  end
`;

/**
 * Lua script for compare-and-swap operation
 * Atomically swaps value if current value matches expected
 */
const COMPARE_AND_SWAP_SCRIPT = `
  local current = redis.call("GET", KEYS[1])
  if current == ARGV[1] then
    redis.call("SET", KEYS[1], ARGV[2])
    local version = redis.call("GET", KEYS[1] .. ":version") or "0"
    local newVersion = tostring(tonumber(version) + 1)
    redis.call("SET", KEYS[1] .. ":version", newVersion)
    if ARGV[3] then
      redis.call("PEXPIRE", KEYS[1], ARGV[3])
      redis.call("PEXPIRE", KEYS[1] .. ":version", ARGV[3])
    end
    return newVersion
  else
    return nil
  end
`;

/**
 * Lua script for conditional delete with version check
 */
const OPTIMISTIC_DELETE_SCRIPT = `
  local currentVersion = redis.call("GET", KEYS[1] .. ":version")
  if currentVersion == ARGV[1] then
    redis.call("DEL", KEYS[1])
    redis.call("DEL", KEYS[1] .. ":version")
    return 1
  else
    return 0
  end
`;

export class OptimisticLock {
  public readonly resource: string;
  public readonly value: string | null;
  public readonly version: number;
  public readonly validity: number;
  public readonly isReadOnly: boolean;
  private _currentVersion: number;

  constructor(
    public readonly redlock: OptimisticRedlock,
    result: OptimisticLockResult,
  ) {
    this.resource = result.resource;
    this.value = result.value;
    this.version = result.version;
    this._currentVersion = result.version;
    this.validity = result.validity;
    this.isReadOnly = result.isReadOnly;
  }

  /**
   * Get current version (may be updated after operations)
   */
  get currentVersion(): number {
    return this._currentVersion;
  }

  /**
   * Update value with optimistic concurrency control
   */
  async update(newValue: string, ttl?: number): Promise<boolean> {
    if (this.isReadOnly) {
      throw new Error("Cannot update read-only lock");
    }

    const result = await this.redlock.optimisticWrite(
      this.resource,
      newValue,
      this._currentVersion,
      ttl,
    );

    if (result !== null) {
      this._currentVersion = result;
      return true;
    }

    return false;
  }

  /**
   * Delete resource with version check
   */
  async delete(): Promise<boolean> {
    if (this.isReadOnly) {
      throw new Error("Cannot delete with read-only lock");
    }

    return await this.redlock.optimisticDelete(
      this.resource,
      this._currentVersion,
    );
  }

  /**
   * Compare and swap operation
   */
  async compareAndSwap(
    expectedValue: string,
    newValue: string,
    ttl?: number,
  ): Promise<boolean> {
    if (this.isReadOnly) {
      throw new Error("Cannot perform CAS with read-only lock");
    }

    const result = await this.redlock.compareAndSwap(
      this.resource,
      expectedValue,
      newValue,
      ttl,
    );

    if (result !== null) {
      this._currentVersion = result;
      return true;
    }

    return false;
  }

  /**
   * Refresh read to get latest version
   */
  async refresh(): Promise<OptimisticLock> {
    const result = await this.redlock.optimisticRead(this.resource);
    if (result) {
      this._currentVersion = result.version;
      return new OptimisticLock(this.redlock, {
        ...result,
        validity: Date.now() + 1000, // Read locks are short-lived
        isReadOnly: true,
      });
    }
    throw new Error("Resource no longer exists");
  }
}

export class OptimisticRedlock extends Redlock {
  protected optimisticOptions: OptimisticLockOptions;

  constructor(
    clients: RedisClient[],
    options: Partial<OptimisticLockOptions> = {},
  ) {
    super(clients, options);
    this.optimisticOptions = {
      ...this.options,
      conflictRetryCount: options.conflictRetryCount || 3,
      conflictRetryDelay: options.conflictRetryDelay || 50,
      conflictBackoffFactor: options.conflictBackoffFactor || 2,
    };
  }

  /**
   * Perform optimistic read operation
   */
  async optimisticRead(resource: string): Promise<OptimisticResource | null> {
    const results = await Promise.all(
      this.clients.map(async (client) => {
        try {
          const result = await this.executeOptimisticScript(
            client,
            OPTIMISTIC_READ_SCRIPT,
            [resource],
            [],
          );
          if (result && Array.isArray(result) && result.length >= 2) {
            return {
              value: result[0] as string,
              version: parseInt(result[1] as string, 10),
            };
          }
          return null;
        } catch {
          return null;
        }
      }),
    );

    // Find consensus on value and version
    const validResults = results.filter((r) => r !== null);
    const quorum = Math.floor(this.clients.length / 2) + 1;

    if (validResults.length >= quorum) {
      // Use the most common value/version pair
      const versionMap = new Map<string, number>();
      for (const result of validResults) {
        if (result) {
          const key = `${result.value}:${result.version}`;
          versionMap.set(key, (versionMap.get(key) || 0) + 1);
        }
      }

      let maxCount = 0;
      let consensusResult: { value: string; version: number } | null = null;

      for (const [key, count] of versionMap) {
        if (count >= quorum && count > maxCount) {
          const [value, version] = key.split(":");
          consensusResult = {
            value,
            version: parseInt(version, 10),
          };
          maxCount = count;
        }
      }

      if (consensusResult) {
        return {
          resource,
          value: consensusResult.value,
          version: consensusResult.version,
        };
      }
    }

    return null;
  }

  /**
   * Perform optimistic write with version check
   */
  async optimisticWrite(
    resource: string,
    value: string,
    expectedVersion: number,
    ttl?: number,
  ): Promise<number | null> {
    const results = await Promise.all(
      this.clients.map(async (client) => {
        try {
          const result = await this.executeOptimisticScript(
            client,
            OPTIMISTIC_WRITE_SCRIPT,
            [resource],
            [value, expectedVersion.toString(), ttl?.toString() || ""],
          );
          return result ? parseInt(String(result), 10) : null;
        } catch {
          return null;
        }
      }),
    );

    const successCount = results.filter((r) => r !== null).length;
    const quorum = Math.floor(this.clients.length / 2) + 1;

    if (successCount >= quorum) {
      // Return the new version (all successful writes should have same version)
      return results.find((r) => r !== null) || null;
    }

    return null;
  }

  /**
   * Compare and swap operation
   */
  async compareAndSwap(
    resource: string,
    expectedValue: string,
    newValue: string,
    ttl?: number,
  ): Promise<number | null> {
    const results = await Promise.all(
      this.clients.map(async (client) => {
        try {
          const result = await this.executeOptimisticScript(
            client,
            COMPARE_AND_SWAP_SCRIPT,
            [resource],
            [expectedValue, newValue, ttl?.toString() || ""],
          );
          return result ? parseInt(String(result), 10) : null;
        } catch {
          return null;
        }
      }),
    );

    const successCount = results.filter((r) => r !== null).length;
    const quorum = Math.floor(this.clients.length / 2) + 1;

    if (successCount >= quorum) {
      return results.find((r) => r !== null) || null;
    }

    return null;
  }

  /**
   * Delete with version check
   */
  async optimisticDelete(
    resource: string,
    expectedVersion: number,
  ): Promise<boolean> {
    const results = await Promise.all(
      this.clients.map(async (client) => {
        try {
          const result = await this.executeOptimisticScript(
            client,
            OPTIMISTIC_DELETE_SCRIPT,
            [resource],
            [expectedVersion.toString()],
          );
          return result === 1;
        } catch {
          return false;
        }
      }),
    );

    const successCount = results.filter(Boolean).length;
    const quorum = Math.floor(this.clients.length / 2) + 1;

    return successCount >= quorum;
  }

  /**
   * Acquire optimistic lock for reading
   */
  async acquireOptimisticRead(
    resource: string,
  ): Promise<OptimisticLock | null> {
    const result = await this.optimisticRead(resource);
    if (result) {
      return new OptimisticLock(this, {
        ...result,
        validity: Date.now() + 1000, // Read locks are short-lived
        isReadOnly: true,
      });
    }
    return null;
  }

  /**
   * Acquire optimistic lock for writing
   */
  async acquireOptimisticWrite(
    resource: string,
    ttl: number,
  ): Promise<OptimisticLock> {
    // Read current state first
    const current = await this.optimisticRead(resource);

    return new OptimisticLock(this, {
      resource,
      value: current?.value || null,
      version: current?.version || 0,
      validity: Date.now() + ttl,
      isReadOnly: false,
    });
  }

  /**
   * Execute transaction with automatic conflict retry
   */
  async transaction<T>(
    resource: string,
    operation: (lock: OptimisticLock) => Promise<T>,
    options: { ttl?: number; maxRetries?: number } = {},
  ): Promise<T> {
    const maxRetries =
      options.maxRetries || this.optimisticOptions.conflictRetryCount || 3;
    let attempt = 0;
    let delay = this.optimisticOptions.conflictRetryDelay || 50;

    while (attempt < maxRetries) {
      try {
        // Get exclusive lock for transaction
        const exclusiveLock = await this.acquire(
          resource + ":tx",
          options.ttl || 5000,
        );

        try {
          // Read current state
          const current = await this.optimisticRead(resource);

          const lock = new OptimisticLock(this, {
            resource,
            value: current?.value || null,
            version: current?.version || 0,
            validity: exclusiveLock.validity,
            isReadOnly: false,
          });

          const result = await operation(lock);
          return result;
        } finally {
          await exclusiveLock.release();
        }
      } catch (error) {
        if (
          error instanceof Error &&
          error.message.includes("conflict") &&
          attempt < maxRetries - 1
        ) {
          // Conflict detected, retry with backoff
          await this.optimisticSleep(delay);
          delay *= this.optimisticOptions.conflictBackoffFactor || 2;
          attempt++;
          continue;
        }
        throw error;
      }
    }

    throw new Error(
      `Transaction failed after ${maxRetries} attempts due to conflicts`,
    );
  }

  /**
   * Rollback write operation
   */
  private async rollbackWrite(
    resource: string,
    version: number,
  ): Promise<void> {
    // Attempt to restore previous version on all nodes
    await Promise.all(
      this.clients.map(async (client) => {
        try {
          await client.eval(
            `
            local currentVersion = redis.call("GET", KEYS[1] .. ":version")
            if currentVersion and tonumber(currentVersion) > tonumber(ARGV[1]) then
              redis.call("DECR", KEYS[1] .. ":version")
            end
          `,
            1,
            resource,
            version.toString(),
          );
        } catch {
          // Ignore rollback errors
        }
      }),
    );
  }

  /**
   * Execute Lua script (use client.eval directly)
   */
  private async executeOptimisticScript(
    client: RedisClient,
    script: string,
    keys: string[],
    argv: string[],
  ): Promise<unknown> {
    try {
      return await client.eval(script, keys.length, ...keys, ...argv);
    } catch (error) {
      throw new Error(`Redis script execution failed: ${error}`);
    }
  }

  /**
   * Sleep utility (inherited from base class)
   */
  private optimisticSleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

export default OptimisticRedlock;
