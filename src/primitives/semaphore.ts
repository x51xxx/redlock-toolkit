/**
 * SemaphorePermit - Represents a held permit in a distributed semaphore.
 * Analogous to Lock for mutex operations.
 */

import { LockSignal } from "../core/types";
import { PermitExtensionError } from "../core/errors";
import { AutoExtensionManager } from "../utils/auto-extension-manager";

export interface SemaphorePermitManager {
  extend: (permit: SemaphorePermit, ttl: number) => Promise<{ success: boolean; expiration: number }>;
  release: (permit: SemaphorePermit) => Promise<{ success: boolean; remainingCount: number }>;
}

export interface RequiredSemaphoreInternalOptions {
  ttl: number;
  autoExtendThreshold: number;
}

export class SemaphorePermit {
  private _expiration: number;
  private _extensions = 0;
  private _released = false;
  private readonly acquisitionTime = Date.now();
  private autoExtensionManager?: AutoExtensionManager;
  private _onRelease?: () => void;

  constructor(
    public readonly resource: string,
    public readonly identifier: string,
    expiration: number,
    private readonly permitManager: SemaphorePermitManager,
    private readonly options: RequiredSemaphoreInternalOptions,
  ) {
    this._expiration = expiration;
  }

  get expiration(): number {
    return this._expiration;
  }

  get extensions(): number {
    return this._extensions;
  }

  get released(): boolean {
    return this._released;
  }

  get duration(): number {
    return Date.now() - this.acquisitionTime;
  }

  get timeToExpiration(): number {
    return Math.max(0, this._expiration - Date.now());
  }

  get isExpired(): boolean {
    return Date.now() >= this._expiration;
  }

  get isValid(): boolean {
    return !this._released && !this.isExpired;
  }

  /** Set a callback to invoke after release */
  set onRelease(callback: (() => void) | undefined) {
    this._onRelease = callback;
  }

  /** Extend the permit with new TTL */
  async extend(ttl?: number): Promise<SemaphorePermit> {
    if (this._released) {
      throw new PermitExtensionError(this.resource, this.identifier, {
        reason: "Cannot extend released permit",
      });
    }

    if (this.isExpired) {
      throw new PermitExtensionError(this.resource, this.identifier, {
        reason: "Cannot extend expired permit",
      });
    }

    const extensionTtl = ttl ?? this.options.ttl;
    const result = await this.permitManager.extend(this, extensionTtl);

    if (result.success) {
      this._expiration = result.expiration;
      this._extensions++;
      this.autoExtensionManager?.updateExpiration(result.expiration);
      return this;
    }

    throw new PermitExtensionError(this.resource, this.identifier, {
      ttl: extensionTtl,
    });
  }

  /** Release the permit */
  async release(): Promise<{ success: boolean; remainingCount: number }> {
    if (this._released) {
      return { success: true, remainingCount: -1 };
    }

    const result = await this.permitManager.release(this);
    this._released = true;
    this._onRelease?.();
    return result;
  }

  /** Auto-extending execution context */
  async using<T>(
    routine: (signal: LockSignal) => Promise<T>,
    options?: { ttl?: number; autoExtendThreshold?: number },
  ): Promise<T> {
    if (this._released) {
      throw new PermitExtensionError(this.resource, this.identifier, {
        reason: "Cannot use released permit",
      });
    }

    const autoExtendThreshold = options?.autoExtendThreshold ?? this.options.autoExtendThreshold;
    const extensionTtl = options?.ttl ?? this.options.ttl;

    this.autoExtensionManager = new AutoExtensionManager({
      ttl: extensionTtl,
      autoExtendThreshold,
      expiration: this._expiration,
      isValid: () => this.isValid,
      extend: async (ttl) => {
        const result = await this.permitManager.extend(this, ttl);
        if (result.success) {
          this._expiration = result.expiration;
          this._extensions++;
        }
        return result;
      },
    });

    const signal = this.autoExtensionManager.start() as LockSignal;

    try {
      return await routine(signal);
    } finally {
      await this.autoExtensionManager.stop();
      this.autoExtensionManager = undefined;
      await this.release().catch(() => {});
    }
  }

  toJSON(): Record<string, unknown> {
    return {
      resource: this.resource,
      identifier: this.identifier,
      expiration: this._expiration,
      extensions: this._extensions,
      released: this._released,
      isValid: this.isValid,
    };
  }
}
