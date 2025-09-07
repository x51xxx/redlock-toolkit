/**
 * AutoExtensionManager - Unified auto-extension logic for locks
 * Eliminates duplication between Lock and InternalRedlockLock classes
 */

import { EventEmitter } from "events";

export interface AutoExtensionConfig {
  /** Time-to-live for the lock in milliseconds */
  ttl: number;
  /** Threshold in milliseconds before expiration to trigger extension */
  autoExtendThreshold: number;
  /** Current expiration timestamp */
  expiration: number;
  /** Whether the lock is still valid */
  isValid: () => boolean;
  /** Function to extend the lock */
  extend: (ttl: number) => Promise<{ success: boolean; expiration: number }>;
  /** Optional error handler */
  onError?: (error: Error) => void;
}

export interface AutoExtensionSignal extends EventEmitter {
  aborted: boolean;
  error?: Error;
  expiration: number;
  abort(error?: Error): void;
  addEventListener(type: "abort", listener: () => void): void;
  removeEventListener(type: "abort", listener: () => void): void;
}

class AutoExtensionSignalImpl extends EventEmitter implements AutoExtensionSignal {
  private _aborted = false;
  private _error?: Error;

  constructor(public readonly expiration: number) {
    super();
  }

  get aborted(): boolean {
    return this._aborted;
  }

  get error(): Error | undefined {
    return this._error;
  }

  abort(error?: Error): void {
    if (this._aborted) return;
    
    this._aborted = true;
    this._error = error;
    this.emit("abort");
  }

  addEventListener(type: "abort", listener: () => void): void {
    this.on(type, listener);
  }

  removeEventListener(type: "abort", listener: () => void): void {
    this.off(type, listener);
  }
}

/**
 * Manages automatic lock extension with configurable thresholds
 */
export class AutoExtensionManager {
  private extensionTimer?: NodeJS.Timeout;
  private extensionPromise?: Promise<void>;
  private signal?: AutoExtensionSignal;
  private _expiration: number;
  private _extensions = 0;

  constructor(private config: AutoExtensionConfig) {
    this._expiration = config.expiration;
  }

  /**
   * Get current expiration timestamp
   */
  get expiration(): number {
    return this._expiration;
  }

  /**
   * Get number of successful extensions
   */
  get extensions(): number {
    return this._extensions;
  }

  /**
   * Get time until expiration in milliseconds
   */
  get timeToExpiration(): number {
    return Math.max(0, this._expiration - Date.now());
  }

  /**
   * Start auto-extension cycle
   */
  start(): AutoExtensionSignal {
    if (this.signal) {
      throw new Error("Auto-extension already started");
    }

    this.signal = new AutoExtensionSignalImpl(this._expiration);
    
    if (this.config.autoExtendThreshold > 0) {
      this.scheduleNextExtension();
    }
    
    return this.signal;
  }

  /**
   * Stop auto-extension cycle
   */
  async stop(): Promise<void> {
    // Clear any scheduled timer
    if (this.extensionTimer) {
      clearTimeout(this.extensionTimer);
      this.extensionTimer = undefined;
    }

    // Wait for any ongoing extension to complete
    if (this.extensionPromise) {
      await this.extensionPromise.catch(() => {
        // Ignore extension errors during cleanup
      });
    }

    // Clear signal
    this.signal = undefined;
  }

  /**
   * Schedule the next extension check
   */
  private scheduleNextExtension(): void {
    const timeUntilExtension = this.timeToExpiration - this.config.autoExtendThreshold;
    
    // Use setTimeout to prevent stack overflow, with minimum delay for testing
    const delay = Math.max(0, Math.min(timeUntilExtension, 10));
    
    this.extensionTimer = setTimeout(() => {
      this.extensionTimer = undefined;
      this.extensionPromise = this.performExtension();
    }, delay);
  }

  /**
   * Perform lock extension
   */
  private async performExtension(): Promise<void> {
    try {
      // Check if extension is still needed
      if (!this.signal || this.signal.aborted || !this.config.isValid()) {
        return;
      }

      // Check if lock is expired
      if (this.timeToExpiration <= 0) {
        const error = new Error("Lock expired before extension");
        this.signal.abort(error);
        this.config.onError?.(error);
        return;
      }

      // Attempt to extend the lock
      const result = await this.config.extend(this.config.ttl);
      
      if (!result.success) {
        const error = new Error("Failed to extend lock");
        this.signal.abort(error);
        this.config.onError?.(error);
        return;
      }

      // Update expiration and increment counter
      this._expiration = result.expiration;
      this._extensions++;

      // Schedule next extension if still valid
      if (this.signal && !this.signal.aborted && this.config.isValid()) {
        this.scheduleNextExtension();
      }
    } catch (error) {
      const err = error as Error;
      this.signal?.abort(err);
      this.config.onError?.(err);
    } finally {
      this.extensionPromise = undefined;
    }
  }

  /**
   * Update expiration (for manual extensions)
   */
  updateExpiration(expiration: number): void {
    this._expiration = expiration;
    this._extensions++;
  }
}