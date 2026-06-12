import { ILockToolkit, OptimisticLockOptions, OptimisticLockResult } from "../core/types";
import { OptimisticLockConflictError } from "../core/errors";
import { SCRIPTS } from "../utils/scripts";

/**
 * Parse optimistic Lua script result array.
 * Format: [status, version, countOrReason]
 *   status=0 → conflict, status=1 → success
 */
function parseOptimisticResult(raw: unknown): {
  success: boolean;
  version: number;
  conflict: boolean;
  reason?: string;
  count?: number;
} {
  if (Array.isArray(raw) && raw.length >= 2) {
    const status = Number(raw[0]);
    const version = Number(raw[1]) || 0;
    if (status === 1) {
      return { success: true, version, conflict: false, count: Number(raw[2]) || 0 };
    }
    return { success: false, version, conflict: true, reason: String(raw[2] || 'unknown') };
  }
  // Fallback for unexpected formats
  return { success: false, version: 0, conflict: true, reason: 'invalid_response' };
}

export class OptimisticLockStrategy {
  constructor(private readonly toolkit: ILockToolkit) {}

  async acquire(resources: string[], options: OptimisticLockOptions = {}): Promise<OptimisticLockResult> {
    const opts = { ...this.toolkit.defaultOptions, ...options };
    // An empty identifier (the defaultOptions placeholder) must never reach
    // Redis: every lock written with '' would match every other one in the
    // ownership checks. Treat it as "not provided" and generate a real token.
    const identifier = (options?.identifier && options.identifier !== '')
      ? options.identifier
      : (opts.identifier || this.toolkit.generateIdentifier());
    const keys = resources.map((r) => this.toolkit.generateLockKey(r));

    try {
      await this.toolkit.executeWithConsensus(async (client) => {
        const scriptResult = await this.toolkit.executeScript(client, SCRIPTS.optimisticAcquire, keys, [
          identifier,
          opts.ttl,
          opts.expectedVersion?.toString() || '',
          opts.expectedValue ? JSON.stringify(opts.expectedValue) : '',
        ]);
        const parsed = parseOptimisticResult(scriptResult);
        if (parsed.conflict) {
          throw new OptimisticLockConflictError(
            resources,
            opts.expectedVersion || 0,
            parsed.version,
            (parsed.reason || 'version_mismatch') as 'version' | 'value' | 'locked',
          );
        }
        return scriptResult;
      }, undefined, {
        evaluateResult: (r: unknown) => {
          const parsed = parseOptimisticResult(r);
          return parsed.success;
        },
      });

      this.toolkit.cacheManager.set(identifier, resources, opts.ttl);

      return {
        success: true,
        currentVersion: opts.expectedVersion ? (opts.expectedVersion + 1) : 1,
        retries: 0,
        identifier,
      };
    } catch (error) {
      if (error instanceof OptimisticLockConflictError) {
        this.toolkit.emit("lock:conflict", resources, error.expectedVersion, error.currentVersion);
        return {
          success: false,
          conflict: true,
          currentVersion: error.currentVersion,
        };
      }
      throw error;
    }
  }

  async update(resources: string[], expectedVersion: number, options: OptimisticLockOptions = {}): Promise<OptimisticLockResult> {
    const opts = { ...this.toolkit.defaultOptions, ...options };
    // An empty identifier (the defaultOptions placeholder) must never reach
    // Redis: every lock written with '' would match every other one in the
    // ownership checks. Treat it as "not provided" and generate a real token.
    const identifier = (options?.identifier && options.identifier !== '')
      ? options.identifier
      : (opts.identifier || this.toolkit.generateIdentifier());
    const keys = resources.map((r) => this.toolkit.generateLockKey(r));

    try {
      await this.toolkit.executeWithConsensus(async (client) => {
        const scriptResult = await this.toolkit.executeScript(client, SCRIPTS.optimisticUpdate, keys, [
          identifier,
          opts.ttl,
          expectedVersion.toString(),
          opts.expectedValue ? JSON.stringify(opts.expectedValue) : '',
        ]);
        const parsed = parseOptimisticResult(scriptResult);
        if (parsed.conflict) {
          throw new OptimisticLockConflictError(
            resources,
            expectedVersion,
            parsed.version,
            (parsed.reason || 'version_mismatch') as 'version' | 'value' | 'locked',
          );
        }
        return scriptResult;
      }, undefined, {
        evaluateResult: (r: unknown) => {
          const parsed = parseOptimisticResult(r);
          return parsed.success;
        },
      });

      return {
        success: true,
        currentVersion: expectedVersion + 1,
        retries: 0,
        identifier,
      };
    } catch (error) {
      if (error instanceof OptimisticLockConflictError) {
        this.toolkit.emit("lock:conflict", resources, error.expectedVersion, error.currentVersion);
        return {
          success: false,
          conflict: true,
          currentVersion: error.currentVersion,
        };
      }
      throw error;
    }
  }
}
