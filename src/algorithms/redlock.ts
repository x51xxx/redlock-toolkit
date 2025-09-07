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
import { AutoExtensionManager } from "../utils/auto-extension-manager";
import { Logger, SilentLogger } from "../core/logger";

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

/**
 * Internal implementation of Redlock lock instance.
 * @internal This is an internal implementation detail. Use RedlockToolkit instead.
 */
export class InternalRedlockLock {
  public readonly resources: string[];
  public readonly value: string;
  public readonly ttl: number;
  private _validity: number;
  private autoExtensionManager?: AutoExtensionManager;
  private abortController?: AbortController;

  constructor(
    public readonly redlock: InternalRedlock,
    resource: RedlockResource,
  ) {
    this.resources = resource.resources;
    this.value = resource.value;
    this.ttl = resource.ttl;
    this._validity = resource.validity;
  }

  /**
   * Get lock validity expiration timestamp
   */
  get validity(): number {
    return this._validity;
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
  async extend(ttl: number): Promise<InternalRedlockLock> {
    if (!this.isValid) {
      throw new Error("Cannot extend expired lock");
    }

    return await this.redlock.extend(this, ttl);
  }

  /**
   * Release the lock
   */
  async release(): Promise<void> {
    if (this.autoExtensionManager) {
      await this.autoExtensionManager.stop();
      this.autoExtensionManager = undefined;
    }
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
    
    const extensionTtl = options.extensionTtl || this.ttl;
    const logger = this.redlock.logger;
    
    // Create auto-extension manager
    this.autoExtensionManager = new AutoExtensionManager({
      ttl: extensionTtl,
      autoExtendThreshold: this.redlock.options.automaticExtensionThreshold,
      expiration: this._validity,
      isValid: () => this.isValid,
      extend: async (ttl) => {
        logger.debug(`Auto-extending lock, remaining: ${this.remainingTtl}ms`);
        const extended = await this.extend(ttl);
        this._validity = extended.validity;
        logger.debug(`Lock extended, new remaining: ${this.remainingTtl}ms`);
        return { success: true, expiration: extended.validity };
      },
      onError: (error) => {
        logger.error(`Auto-extension failed: ${error.message}`);
        controller.abort(error);
      },
    });
    
    // Start auto-extension
    const extensionSignal = this.autoExtensionManager.start();
    
    // Link extension signal to abort controller
    extensionSignal.on('abort', () => {
      if (extensionSignal.error) {
        controller.abort(extensionSignal.error);
      }
    });
    
    try {
      return await routine(controller.signal);
    } finally {
      await this.autoExtensionManager.stop();
      this.autoExtensionManager = undefined;
      controller.abort();
    }
  }

}

/**
 * Internal implementation of the Redlock algorithm.
 * @internal This is an internal implementation detail. Use RedlockToolkit instead.
 */
export class InternalRedlock extends EventEmitter {
  protected readonly clients: RedisClient[];
  public readonly options: RedlockOptions;
  public readonly logger: Logger;
  private scriptsLoaded = false;

  constructor(clients: RedisClient[], options: Partial<RedlockOptions> = {}, logger?: Logger) {
    super();

    if (!clients || clients.length === 0) {
      throw new Error("At least one Redis client is required");
    }

    this.clients = [...clients];
    this.options = { ...DEFAULT_OPTIONS, ...options };
    this.logger = logger || new SilentLogger();
  }

  /**
   * Acquire a distributed lock using Redlock algorithm
   */
  async acquire(
    resources: string | string[],
    ttl: number,
    options: Partial<RedlockOptions> = {},
  ): Promise<InternalRedlockLock> {
    const resourceList = Array.isArray(resources) ? resources : [resources];
    const opts = { ...this.options, ...options };
    const value = this.generateValue();

    let attempt = 0;
    const maxAttempts = opts.retryCount + 1;

    while (attempt < maxAttempts) {
      try {
        const result = await this.attemptLock(resourceList, value, ttl);

        if (result) {
          const lock = new InternalRedlockLock(this, result);
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
  async extend(lock: InternalRedlockLock, ttl: number): Promise<InternalRedlockLock> {
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
      } catch {
        return false;
      }
    });

    const results = await Promise.all(promises);
    const successCount = results.filter(Boolean).length;
    const quorum = Math.floor(this.clients.length / 2) + 1;

    const elapsed = Date.now() - startTime;
    const validity = ttl - elapsed - drift;

    if (successCount >= quorum && validity > 0) {
      return new InternalRedlockLock(this, {
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
  async release(lock: InternalRedlockLock): Promise<void> {
    const promises = this.clients.map(async (client) => {
      try {
        await this.executeScript(client, RELEASE_SCRIPT, lock.resources, [
          lock.value,
        ]);
        return true;
      } catch {
        return false;
      }
    });

    await Promise.all(promises);
    this.emit("clientUnlocked", lock.resources, lock.value);
  }

  /**
   * Force release a lock (without ownership check)
   */
  async forceRelease(lock: InternalRedlockLock): Promise<void> {
    const promises = this.clients.map(async (client) => {
      try {
        await client.del(...lock.resources);
        return true;
      } catch {
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
      } catch {
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
        } catch {
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
  ): Promise<unknown> {
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

// Note: This export is deprecated. Use RedlockToolkit from the main export instead.
export default InternalRedlock;
