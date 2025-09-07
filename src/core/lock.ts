/**
 * Lock Instance Class
 * Represents an acquired lock with extension and release capabilities
 */

import {
  LockOptions,
  LockExtensionResult,
  LockReleaseResult,
  LockSignal,
  ActiveLockInfo,
} from "./types";
import {
  LockExpiredError,
  LockExtensionError,
  LockReleaseError,
} from "./errors";
import { AutoExtensionManager } from "../utils/auto-extension-manager";


/**
 * Represents an acquired distributed lock
 */
export class Lock {
  private _expiration: number;
  private _extensions = 0;
  private _released = false;
  private readonly acquisitionTime = Date.now();
  private autoExtensionManager?: AutoExtensionManager;

  constructor(
    public readonly resources: string[],
    public readonly identifier: string,
    expiration: number,
    private readonly lockManager: {
      extend: (
        lock: Lock,
        ttl: number,
        options?: Partial<LockOptions>,
      ) => Promise<LockExtensionResult>;
      release: (
        lock: Lock,
        options?: Partial<LockOptions>,
      ) => Promise<LockReleaseResult>;
    },
    private readonly options: Required<LockOptions>,
  ) {
    this._expiration = expiration;
  }

  /**
   * Get current expiration time
   */
  get expiration(): number {
    return this._expiration;
  }

  /**
   * Get number of extensions performed
   */
  get extensions(): number {
    return this._extensions;
  }

  /**
   * Check if lock is released
   */
  get released(): boolean {
    return this._released;
  }

  /**
   * Get lock duration so far
   */
  get duration(): number {
    return Date.now() - this.acquisitionTime;
  }

  /**
   * Get time until expiration
   */
  get timeToExpiration(): number {
    return Math.max(0, this._expiration - Date.now());
  }

  /**
   * Check if lock is expired
   */
  get isExpired(): boolean {
    return Date.now() >= this._expiration;
  }

  /**
   * Check if lock is valid (not expired and not released)
   */
  get isValid(): boolean {
    return !this._released && !this.isExpired;
  }

  /**
   * Get active lock information
   */
  getInfo(): ActiveLockInfo {
    return {
      resources: [...this.resources],
      identifier: this.identifier,
      expiration: this._expiration,
      acquisitionTime: this.acquisitionTime,
      extensions: this._extensions,
    };
  }

  /**
   * Extend the lock with new TTL
   */
  async extend(ttl?: number, options?: Partial<LockOptions>): Promise<Lock> {
    if (this._released) {
      throw new LockReleaseError(this.resources, this.identifier, 0, 1, {
        reason: "Cannot extend released lock",
      });
    }

    if (this.isExpired) {
      throw new LockExpiredError(this._expiration, {
        identifier: this.identifier,
        resources: this.resources,
      });
    }

    const extensionTtl = ttl ?? this.options.ttl;
    const result = await this.lockManager.extend(this, extensionTtl, options);

    if (result.success) {
      this._expiration = result.expiration;
      this._extensions++;
      // Update auto-extension manager if active
      this.autoExtensionManager?.updateExpiration(result.expiration);
      return this;
    } else {
      throw new LockExtensionError(
        this.resources,
        this.identifier,
        this._expiration,
        { ttl: extensionTtl },
      );
    }
  }

  /**
   * Release the lock
   */
  async release(options?: Partial<LockOptions>): Promise<LockReleaseResult> {
    if (this._released) {
      // Idempotent operation - return success if already released
      return {
        success: true,
        releasedCount: 0,
        totalClients: 1,
        stats: Promise.resolve({
          totalClients: 1,
          quorumSize: 1,
          votesFor: new Set(),
          votesAgainst: new Map(),
          startTime: Date.now(),
        }),
      };
    }

    const result = await this.lockManager.release(this, options);
    this._released = true; // Mark as released even if partially successful
    return result;
  }

  /**
   * Create auto-extending lock context
   */
  async using<T>(
    routine: (signal: LockSignal) => Promise<T>,
    options?: Partial<LockOptions>,
  ): Promise<T> {
    if (this._released) {
      throw new LockReleaseError(this.resources, this.identifier, 0, 1, {
        reason: "Cannot use released lock",
      });
    }

    const autoExtendThreshold =
      options?.autoExtendThreshold ?? this.options.autoExtendThreshold;
    const extensionTtl = options?.ttl ?? this.options.ttl;

    if (autoExtendThreshold > 0 && autoExtendThreshold >= extensionTtl - 100) {
      throw new Error(
        `autoExtendThreshold (${autoExtendThreshold}ms) must be at least 100ms less than TTL (${extensionTtl}ms)`,
      );
    }

    // Create auto-extension manager
    this.autoExtensionManager = new AutoExtensionManager({
      ttl: extensionTtl,
      autoExtendThreshold,
      expiration: this._expiration,
      isValid: () => !this._released && !this.isExpired,
      extend: async (ttl) => {
        const result = await this.lockManager.extend(this, ttl, options);
        if (result.success) {
          this._expiration = result.expiration;
          this._extensions++;
        }
        return result;
      },
      onError: (error) => {
        if (error instanceof LockExpiredError) {
          // Already a proper error type
        } else {
          // Convert to LockExtensionError
          error = new LockExtensionError(
            this.resources,
            this.identifier,
            this._expiration,
            { message: error.message },
          );
        }
      },
    });

    // Start auto-extension and get signal
    const signal = this.autoExtensionManager.start() as LockSignal;

    try {
      return await routine(signal);
    } finally {
      // Stop auto-extension
      await this.autoExtensionManager.stop();
      this.autoExtensionManager = undefined;

      // Release the lock
      await this.release(options).catch(() => {
        // Ignore release errors - lock will expire naturally
      });
    }
  }

  /**
   * Wait for lock to expire naturally
   */
  async waitForExpiration(): Promise<void> {
    if (this._released || this.isExpired) {
      return;
    }

    const waitTime = this.timeToExpiration;
    if (waitTime <= 0) {
      return;
    }

    return new Promise((resolve) => {
      setTimeout(resolve, waitTime);
    });
  }

  /**
   * Convert lock to JSON representation
   */
  toJSON(): Record<string, unknown> {
    return {
      resources: this.resources,
      identifier: this.identifier,
      expiration: this._expiration,
      acquisitionTime: this.acquisitionTime,
      extensions: this._extensions,
      released: this._released,
      duration: this.duration,
      timeToExpiration: this.timeToExpiration,
      isExpired: this.isExpired,
      isValid: this.isValid,
    };
  }

  /**
   * String representation of the lock
   */
  toString(): string {
    const status = this._released
      ? "released"
      : this.isExpired
        ? "expired"
        : "active";
    return `Lock(${this.identifier}, ${this.resources.join(",")}, ${status})`;
  }
}
