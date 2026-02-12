import { Lock } from "../core/lock";
import { ILockToolkit, HybridLockOptions } from "../core/types";
import { HybridLockError, OptimisticLockConflictError } from "../core/errors";

export class HybridLockStrategy {
  constructor(private readonly toolkit: ILockToolkit) {}

  async acquire(resources: string[], options: HybridLockOptions = {}): Promise<Lock> {
    const opts = { ...this.toolkit.defaultOptions, ...options };
    const primaryStrategy = opts.primaryStrategy || 'pessimistic';
    const fallbackStrategy = opts.fallbackStrategy || 'optimistic';

    try {
      if (primaryStrategy === 'pessimistic' || primaryStrategy === 'adaptive') {
        return await this.toolkit.acquire(resources, opts);
      } else if (primaryStrategy === 'optimistic') {
        const result = await this.toolkit.acquireOptimistic(resources, opts);
        if (result.success) {
          return this.toolkit.createLockFromOptimistic(resources, result, opts);
        } else {
          throw new OptimisticLockConflictError(resources, opts.expectedVersion || 0, result.currentVersion || 0, 'version');
        }
      }
    } catch (primaryError) {
      try {
        if (fallbackStrategy === 'pessimistic') {
          return await this.toolkit.acquire(resources, opts);
        } else if (fallbackStrategy === 'optimistic') {
          const result = await this.toolkit.acquireOptimistic(resources, opts);
          if (result.success) {
            return this.toolkit.createLockFromOptimistic(resources, result, opts);
          }
        }
        throw primaryError;
      } catch (fallbackError) {
        throw new HybridLockError(primaryStrategy, fallbackStrategy, primaryError as Error, fallbackError as Error);
      }
    }

    throw new Error("Hybrid locking failed - no strategy succeeded");
  }
}
