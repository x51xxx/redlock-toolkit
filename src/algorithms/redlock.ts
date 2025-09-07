/**
 * True Redlock algorithm implementation with native AbortController
 * Based on the official Redlock specification
 */

import { EventEmitter } from "events";
import { randomBytes } from "crypto";
import { RedisClient } from "../core/types";
import {
  ACQUIRE_SCRIPT,
  EXTEND_SCRIPT,
  RELEASE_SCRIPT,
} from "../utils/scripts";

export interface RedlockOptions {
  /**
   * Expected clock drift factor
   */
  driftFactor: number;

  /**
   * Number of retry attempts
   */
  retryCount: number;

  /**
   * Delay between retries (ms)
   */
  retryDelay: number;

  /**
   * Random jitter for retry delay (ms)
   */
  retryJitter: number;

  /**
   * Automatically extend locks before expiration
   */
  automaticExtensionThreshold: number;
}

export interface RedlockResource {
  resources: string[];
  value: string;
  ttl: number;
  validity: number;
}

const DEFAULT_OPTIONS: RedlockOptions = {
  driftFactor: 0.01,
  retryCount: 3,
  retryDelay: 200,
  retryJitter: 100,
  automaticExtensionThreshold: 0,
};

/**
 * Simple Redlock-specific Lua scripts
 * These are intentionally separate from the main scripts.ts as they implement
 * the pure Redlock algorithm without additional features
 */

// Using advanced scripts from scripts.ts that support multiple resources

export class RedlockLock {
  public readonly resources: string[];
  public readonly value: string;
  public readonly ttl: number;
  public readonly validity: number;
  private extensionTimer?: NodeJS.Timeout;
  private abortController?: AbortController;

  constructor(
    public readonly redlock: Redlock,
    resource: RedlockResource,
  ) {
    this.resources = resource.resources;
    this.value = resource.value;
    this.ttl = resource.ttl;
    this.validity = resource.validity;
  }

  /**
   * Get remaining validity time
   */
  get remainingTtl(): number {
    return Math.max(0, this.validity - Date.now());
  }

  /**
   * Check if lock is still valid
   */
  get isValid(): boolean {
    return this.remainingTtl > 0;
  }

  /**
   * Extend the lock
   */
  async extend(ttl: number): Promise<RedlockLock> {
    if (!this.isValid) {
      throw new Error("Cannot extend expired lock");
    }

    return await this.redlock.extend(this, ttl);
  }

  /**
   * Release the lock
   */
  async release(): Promise<void> {
    this.stopAutoExtension();
    await this.redlock.release(this);
  }

  /**
   * Execute function with automatic lock extension
   */
  async using<T>(
    routine: (signal: AbortSignal) => Promise<T>,
    options: { extensionTtl?: number } = {},
  ): Promise<T> {
    const controller = new AbortController();
    this.abortController = controller;

    try {
      this.startAutoExtension(options.extensionTtl || this.ttl);
      return await routine(controller.signal);
    } finally {
      this.stopAutoExtension();
      controller.abort();
    }
  }

  private startAutoExtension(extensionTtl: number): void {
    if (this.redlock.options.automaticExtensionThreshold <= 0) {
      return;
    }

    const checkInterval = Math.min(
      this.redlock.options.automaticExtensionThreshold / 2,
      500,
    );

    this.extensionTimer = setInterval(async () => {
      try {
        if (
          this.remainingTtl <=
            this.redlock.options.automaticExtensionThreshold &&
          this.isValid
        ) {
          console.log(
            `   🔄 Auto-extending lock, remaining: ${this.remainingTtl}ms`,
          );
          const extended = await this.extend(extensionTtl);
          // Update this lock instance with extended values
          (this as any).validity = extended.validity;
          console.log(
            `   ✅ Lock extended, new remaining: ${this.remainingTtl}ms`,
          );
        }
      } catch (error) {
        const err = error as Error;
        console.log(`   ❌ Auto-extension failed: ${err.message}`);
        // Extension failed - abort the operation
        this.abortController?.abort(err);
        this.stopAutoExtension();
      }
    }, checkInterval);
  }

  private stopAutoExtension(): void {
    if (this.extensionTimer) {
      clearInterval(this.extensionTimer);
      this.extensionTimer = undefined;
    }
  }
}

export class Redlock extends EventEmitter {
  protected readonly clients: RedisClient[];
  public readonly options: RedlockOptions;
  private scriptsLoaded = false;

  constructor(clients: RedisClient[], options: Partial<RedlockOptions> = {}) {
    super();

    if (!clients || clients.length === 0) {
      throw new Error("At least one Redis client is required");
    }

    this.clients = [...clients];
    this.options = { ...DEFAULT_OPTIONS, ...options };
  }

  /**
   * Acquire a distributed lock using Redlock algorithm
   */
  async acquire(
    resources: string | string[],
    ttl: number,
    options: Partial<RedlockOptions> = {},
  ): Promise<RedlockLock> {
    const resourceList = Array.isArray(resources) ? resources : [resources];
    const opts = { ...this.options, ...options };
    const value = this.generateValue();

    let attempt = 0;
    const maxAttempts = opts.retryCount + 1;

    while (attempt < maxAttempts) {
      const startTime = Date.now();

      try {
        const result = await this.attemptLock(resourceList, value, ttl);

        if (result) {
          const lock = new RedlockLock(this, result);
          this.emit("clientLocked", resourceList, value, ttl);
          return lock;
        }
      } catch (error) {
        this.emit("clientError", error);
      }

      attempt++;

      if (attempt < maxAttempts) {
        const delay = this.calculateRetryDelay(opts);
        await this.sleep(delay);
      }
    }

    throw new Error(
      `Failed to acquire lock on resources "${resourceList.join(", ")}" after ${maxAttempts} attempts`,
    );
  }

  /**
   * Extend an existing lock
   */
  async extend(lock: RedlockLock, ttl: number): Promise<RedlockLock> {
    const startTime = Date.now();
    const drift = Math.round(this.options.driftFactor * ttl) + 2;

    const promises = this.clients.map(async (client) => {
      try {
        const result = await this.executeScript(
          client,
          EXTEND_SCRIPT,
          lock.resources,
          [lock.value, ttl.toString()],
        );
        return result === lock.resources.length;
      } catch (error) {
        return false;
      }
    });

    const results = await Promise.all(promises);
    const successCount = results.filter(Boolean).length;
    const quorum = Math.floor(this.clients.length / 2) + 1;

    const elapsed = Date.now() - startTime;
    const validity = ttl - elapsed - drift;

    if (successCount >= quorum && validity > 0) {
      return new RedlockLock(this, {
        resources: lock.resources,
        value: lock.value,
        ttl,
        validity: startTime + validity,
      });
    }

    // Extension failed - try to release on all clients
    await this.forceRelease(lock);
    throw new Error("Failed to extend lock - quorum not achieved");
  }

  /**
   * Release a lock
   */
  async release(lock: RedlockLock): Promise<void> {
    const promises = this.clients.map(async (client) => {
      try {
        await this.executeScript(client, RELEASE_SCRIPT, lock.resources, [
          lock.value,
        ]);
        return true;
      } catch (error) {
        return false;
      }
    });

    await Promise.all(promises);
    this.emit("clientUnlocked", lock.resources, lock.value);
  }

  /**
   * Force release a lock (without ownership check)
   */
  async forceRelease(lock: RedlockLock): Promise<void> {
    const promises = this.clients.map(async (client) => {
      try {
        await client.del(...lock.resources);
        return true;
      } catch (error) {
        return false;
      }
    });

    await Promise.all(promises);
  }

  /**
   * Execute function with automatic lock management
   */
  async using<T>(
    resources: string | string[],
    ttl: number,
    routine: (signal: AbortSignal) => Promise<T>,
    options: Partial<RedlockOptions> = {},
  ): Promise<T> {
    const lock = await this.acquire(resources, ttl, options);

    try {
      return await lock.using(routine, { extensionTtl: ttl });
    } finally {
      await lock.release();
    }
  }

  /**
   * Attempt to acquire lock with quorum consensus
   */
  private async attemptLock(
    resources: string[],
    value: string,
    ttl: number,
  ): Promise<RedlockResource | null> {
    const startTime = Date.now();
    const drift = Math.round(this.options.driftFactor * ttl) + 2;

    const promises = this.clients.map(async (client) => {
      try {
        const result = await this.executeScript(
          client,
          ACQUIRE_SCRIPT,
          resources,
          [value, ttl.toString()],
        );
        return result === resources.length; // All resources must be acquired
      } catch (error) {
        return false;
      }
    });

    const results = await Promise.all(promises);
    const successCount = results.filter(Boolean).length;
    const quorum = Math.floor(this.clients.length / 2) + 1;

    const elapsed = Date.now() - startTime;
    const validity = ttl - elapsed - drift;

    if (successCount >= quorum && validity > 0) {
      return {
        resources,
        value,
        ttl,
        validity: startTime + validity,
      };
    }

    // Failed to acquire quorum - release locks on successful clients
    const releasePromises = this.clients.map(async (client, index) => {
      if (results[index]) {
        try {
          await this.executeScript(client, RELEASE_SCRIPT, resources, [value]);
        } catch (error) {
          // Ignore release errors
        }
      }
    });

    await Promise.all(releasePromises);
    return null;
  }

  /**
   * Execute Lua script with fallback
   */
  private async executeScript(
    client: RedisClient,
    script: string,
    keys: string[],
    argv: string[],
  ): Promise<any> {
    try {
      return await client.eval(script, keys.length, ...keys, ...argv);
    } catch (error) {
      throw new Error(`Redis script execution failed: ${error}`);
    }
  }

  /**
   * Generate unique lock value
   */
  private generateValue(): string {
    return randomBytes(16).toString("hex");
  }

  /**
   * Calculate retry delay with jitter
   */
  private calculateRetryDelay(options: RedlockOptions): number {
    return options.retryDelay + Math.random() * options.retryJitter;
  }

  /**
   * Sleep utility
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

export default Redlock;
