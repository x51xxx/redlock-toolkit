/**
 * SemaphoreStrategy - Implements distributed semaphore with consensus.
 */

import { ILockToolkit, RedisClient, SemaphoreOptions, SemaphoreStatus } from "../core/types";
import { SemaphoreFullError, isRetryableError, getRetryDelay, LockTimeoutError, ConfigurationError } from "../core/errors";
import { SCRIPTS } from "../utils/scripts";
import { SemaphorePermit } from "../primitives/semaphore";

const DEFAULT_SEMAPHORE_OPTIONS = {
  ttl: 30000,
  retryCount: 0,
  retryDelay: 200,
  retryJitter: 100,
  driftFactor: 0.01,
  autoExtendThreshold: 500,
};

export class SemaphoreStrategy {
  constructor(private readonly toolkit: ILockToolkit) {}

  async acquire(resource: string, options: SemaphoreOptions): Promise<SemaphorePermit> {
    if (!options.maxPermits || options.maxPermits <= 0) {
      throw new ConfigurationError("maxPermits", options.maxPermits, "Must be greater than 0");
    }
    if (options.ttl !== undefined && options.ttl <= 0) {
      throw new ConfigurationError("ttl", options.ttl, "Must be greater than 0");
    }

    const opts = { ...DEFAULT_SEMAPHORE_OPTIONS, ...options };
    const identifier = opts.identifier || this.toolkit.generateIdentifier();
    const key = this.toolkit.generateLockKey(`sem:${resource}`);

    const startTime = Date.now();
    let lastError: Error | undefined;
    let attempts = 0;
    const maxAttempts = opts.retryCount === -1 ? Infinity : opts.retryCount + 1;

    let acquired = false;
    let acquiredAt = 0;
    while (attempts < maxAttempts && !acquired) {
      attempts++;

      let semaphoreFullResponse: number[] | null = null;
      try {
        await this.toolkit.executeWithConsensus(async (client: RedisClient) => {
          const response = await this.toolkit.executeScript(
            client,
            SCRIPTS.semaphoreAcquire,
            [key],
            [identifier, opts.ttl, opts.maxPermits],
          ) as number[];

          if (response[0] === 0) {
            semaphoreFullResponse = response;
          }

          return response[0];
        }, { keys: [key], identifier, script: SCRIPTS.semaphoreRelease, args: [identifier] });

        acquired = true;
        acquiredAt = Date.now();
      } catch (error) {
        if (semaphoreFullResponse) {
          lastError = new SemaphoreFullError(resource, semaphoreFullResponse[1], semaphoreFullResponse[2]);
        } else {
          lastError = error as Error;
        }
        if (!isRetryableError(lastError)) break;
        if (attempts < maxAttempts) {
          const delay = getRetryDelay(lastError, opts.retryDelay, opts.retryJitter);
          if (this.toolkit.pubSubWaiter) {
            const maxWait = opts.retryDelay + opts.retryJitter;
            await this.toolkit.pubSubWaiter.waitForRelease("sem", resource, maxWait, delay);
          } else {
            await new Promise((resolve) => setTimeout(resolve, delay));
          }
          this.toolkit.metrics.recordRetry(identifier, attempts);
        }
      }
    }

    if (!acquired) {
      this.toolkit.metrics.recordAcquisitionFailed(
        [resource],
        lastError || new Error("Unknown error"),
        attempts,
        Date.now() - startTime,
      );
      if (lastError) {
        throw lastError;
      }
      throw new LockTimeoutError(Date.now() - startTime, attempts);
    }

    const drift = Math.round(opts.driftFactor * opts.ttl) + 2;
    const expiration = acquiredAt + opts.ttl - drift;

    const permit = new SemaphorePermit(
      resource,
      identifier,
      expiration,
      {
        extend: async (permit, ttl) => this.extend(permit, ttl),
        release: async (permit) => this.release(permit),
      },
      { ttl: opts.ttl, autoExtendThreshold: opts.autoExtendThreshold },
    );

    return permit;
  }

  async extend(permit: SemaphorePermit, ttl: number): Promise<{ success: boolean; expiration: number }> {
    const key = this.toolkit.generateLockKey(`sem:${permit.resource}`);

    const result = await this.toolkit.executeWithConsensus(async (client: RedisClient) => {
      return this.toolkit.executeScript(
        client,
        SCRIPTS.semaphoreExtend,
        [key],
        [permit.identifier, ttl],
      );
    });

    if (result.success) {
      return { success: true, expiration: Date.now() + ttl };
    }

    return { success: false, expiration: permit.expiration };
  }

  async release(permit: SemaphorePermit): Promise<{ success: boolean; remainingCount: number }> {
    const key = this.toolkit.generateLockKey(`sem:${permit.resource}`);

    let capturedRemaining = -1;
    let remainingCaptured = false;

    const result = await this.toolkit.executeWithConsensus(async (client: RedisClient) => {
      const response = await this.toolkit.executeScript(
        client,
        SCRIPTS.semaphoreRelease,
        [key],
        [permit.identifier],
      ) as number[];

      if (!remainingCaptured) {
        capturedRemaining = response[1] ?? -1;
        remainingCaptured = true;
      }

      return response;
    }, undefined, {
      evaluateResult: (res: unknown) => {
        const arr = res as number[];
        return Array.isArray(arr) && arr[0] >= 0;
      },
    });

    return { success: result.success, remainingCount: capturedRemaining };
  }

  async getStatus(resource: string, maxPermits: number): Promise<SemaphoreStatus> {
    const key = this.toolkit.generateLockKey(`sem:${resource}`);

    // Query first client for status (not consensus-critical)
    const response = await this.toolkit.executeScript(
      this.toolkit.getClientForRead(),
      SCRIPTS.semaphoreStatus,
      [key],
      [maxPermits],
    ) as (string | number)[];

    const activePermits = Number(response[0]);
    const holders: SemaphoreStatus["holders"] = [];
    for (let i = 2; i < response.length; i += 2) {
      holders.push({
        identifier: String(response[i]),
        expiresAt: Number(response[i + 1]),
      });
    }

    return {
      resource,
      activePermits,
      maxPermits,
      holders,
    };
  }
}
