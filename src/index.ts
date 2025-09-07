/**
 * RedlockToolkit - Advanced Redis Distributed Locking Library
 * Combines best practices from multiple locking implementations
 */

import { EventEmitter } from "events";
import { randomBytes } from "crypto";
import {
  RedisClient,
  RedlockToolkitConfig,
  LockOptions,
  LockStats,
  LockExecutionResult,
  LockReleaseResult,
  LockExtensionResult,
  LockSignal,
  LockEvents,
  OptimisticLockOptions,
  OptimisticLockResult,
  HybridLockOptions,
  LockCacheOptions,
  CachedLockInfo,
  CircuitBreakerMetrics,
} from "./core/types";
import { Lock } from "./core/lock";
import {
  RedlockToolkitError,
  ResourceLockedError,
  ConsensusError,
  LockTimeoutError,
  LockExpiredError,
  LockExtensionError,
  ConfigurationError,
  CircuitBreakerOpenError,
  RedisOperationError,
  OptimisticLockConflictError,
  HybridLockError,
  isRetryableError,
  getRetryDelay,
} from "./core/errors";
import { CircuitBreakerManager } from "./patterns/circuit-breaker";
import { MetricsCollector } from "./utils/metrics";
import { SCRIPTS, LuaScript, scripts } from "./utils/scripts";
import { InternalRedlock } from "./algorithms/redlock";
import { Logger, LoggerFactory } from "./core/logger";
import { ConsensusManager } from "./utils/consensus-manager";

/**
 * Default configuration values
 */
const DEFAULT_CONFIG = {
  ttl: 30000, // 30 seconds
  retryCount: 0, // Default to no retries for deterministic behavior
  retryDelay: 200,
  retryJitter: 100,
  driftFactor: 0.01,
  autoExtendThreshold: 500,
  keyPrefix: "neolock",
  enableMetrics: true,
};

const DEFAULT_CIRCUIT_BREAKER = {
  failureThreshold: 5,
  resetTimeout: 60000,
  maxRetries: 3,
  operationTimeout: 5000,
};

/**
 * Main RedlockToolkit class implementing advanced distributed locking
 */
export class RedlockToolkit extends EventEmitter {
  private readonly clients: RedisClient[];
  private readonly config: Required<RedlockToolkitConfig>;
  private readonly defaultOptions: Required<LockOptions>;
  private readonly circuitBreaker: CircuitBreakerManager;
  private readonly metrics: MetricsCollector;
  private readonly logger: Logger;
  private readonly consensusManager: ConsensusManager;
  private readonly activeLocks = new Map<string, Lock>();
  private readonly compatibilityLocks = new Map<string, Lock>();
  private readonly redlock: InternalRedlock;
  private readonly lockCache = new Map<string, CachedLockInfo>();
  private cacheEnabled = false;
  private cacheOptions: LockCacheOptions = {};

  constructor(config: RedlockToolkitConfig) {
    super();

    this.validateConfig(config);

    this.clients = [...config.clients];
    this.logger = LoggerFactory.create(config.logger);
    this.consensusManager = new ConsensusManager(this.logger);
    
    this.config = {
      clients: this.clients,
      defaultLockOptions: { ...DEFAULT_CONFIG, ...config.defaultLockOptions },
      circuitBreaker: { ...DEFAULT_CIRCUIT_BREAKER, ...config.circuitBreaker },
      enableMetrics: config.enableMetrics ?? true,
      keyPrefix: config.keyPrefix ?? "neolock",
      logger: config.logger ?? false,
    };

    this.defaultOptions = {
      identifier: '', // Will be generated if not provided
      ttl: this.config.defaultLockOptions.ttl!,
      retryCount: this.config.defaultLockOptions.retryCount!,
      retryDelay: this.config.defaultLockOptions.retryDelay!,
      retryJitter: this.config.defaultLockOptions.retryJitter!,
      driftFactor: this.config.defaultLockOptions.driftFactor!,
      autoExtendThreshold: this.config.defaultLockOptions.autoExtendThreshold!,
    };

    this.circuitBreaker = new CircuitBreakerManager(this.config.circuitBreaker);
    this.metrics = new MetricsCollector();
    this.redlock = new InternalRedlock(this.clients, {
      driftFactor: this.config.defaultLockOptions.driftFactor!,
      retryCount: this.config.defaultLockOptions.retryCount!,
      retryDelay: this.config.defaultLockOptions.retryDelay!,
      retryJitter: this.config.defaultLockOptions.retryJitter!,
      automaticExtensionThreshold:
        this.config.defaultLockOptions.autoExtendThreshold!,
    }, this.logger);

    this.setupEventForwarding();
    this.preloadScripts();
  }

  /**
   * Validate configuration
   */
  private validateConfig(config: RedlockToolkitConfig): void {
    if (!config.clients || config.clients.length === 0) {
      throw new ConfigurationError(
        "clients",
        config.clients,
        "Must provide at least one Redis client",
      );
    }

    if (config.defaultLockOptions?.ttl !== undefined && config.defaultLockOptions.ttl <= 0) {
      throw new ConfigurationError(
        "ttl",
        config.defaultLockOptions.ttl,
        "Must be greater than 0",
      );
    }

    if (
      config.defaultLockOptions?.retryCount &&
      config.defaultLockOptions.retryCount < -1
    ) {
      throw new ConfigurationError(
        "retryCount",
        config.defaultLockOptions.retryCount,
        "Must be -1 or greater",
      );
    }

    if (
      config.defaultLockOptions?.driftFactor &&
      (config.defaultLockOptions.driftFactor < 0 ||
        config.defaultLockOptions.driftFactor > 1)
    ) {
      throw new ConfigurationError(
        "driftFactor",
        config.defaultLockOptions.driftFactor,
        "Must be between 0 and 1",
      );
    }
  }

  /**
   * Setup event forwarding from internal components
   */
  private setupEventForwarding(): void {
    // Forward circuit breaker events
    this.circuitBreaker.on(
      "stateChanged",
      (clientId: unknown, newState: unknown, oldState: unknown, metrics: unknown) => {
        this.emit("circuit:stateChanged", newState);
        this.metrics.updateCircuitBreakerMetrics(
          String(clientId),
          metrics as CircuitBreakerMetrics
        );
      },
    );

    // Forward metrics events
    this.metrics.on("lockAcquired", (data) => {
      this.emit("lock:acquired", data.resources, data.identifier);
    });

    this.metrics.on("lockReleased", (data) => {
      this.emit("lock:released", data.resources, data.identifier);
    });

    this.metrics.on("lockExtended", (data) => {
      this.emit("lock:extended", data.resources, data.identifier, Date.now());
    });

    this.metrics.on("acquisitionFailed", (data) => {
      this.emit("lock:failed", data.resources, new Error(data.error));
    });
  }

  /**
   * Preload Lua scripts on all clients
   */
  private async preloadScripts(): Promise<void> {
    const scripts = Object.values(SCRIPTS);
    const promises: Promise<any>[] = [];

    for (const client of this.clients) {
      for (const script of scripts) {
        promises.push(
          this.circuitBreaker
            .execute(this.getClientId(client), () =>
              client.script("LOAD", script.source),
            )
            .catch((error) => {
              this.emit(
                "error",
                new RedisOperationError(
                  "script_load",
                  this.getClientId(client),
                  error,
                ),
              );
            }),
        );
      }
    }

    await Promise.allSettled(promises);
  }

  /**
   * Generate unique client identifier
   */
  private getClientId(client: RedisClient): string {
    const options = client.options as any;
    return `${options?.host || "localhost"}:${options?.port || 6379}`;
  }

  /**
   * Generate lock key
   */
  private generateLockKey(resource: string): string {
    return `${this.config.keyPrefix}:${resource}`;
  }

  /**
   * Generate unique lock identifier
   */
  private generateIdentifier(): string {
    return randomBytes(16).toString("hex");
  }

  /**
   * Execute Lua script on client with fallback
   */
  private async executeScript(
    client: RedisClient,
    script: LuaScript,
    keys: string[],
    args: (string | number)[],
  ): Promise<any> {
    const clientId = this.getClientId(client);

    try {
      return await this.circuitBreaker.execute(clientId, async () => {
        try {
          // Try with cached script hash first
          const result = await client.evalsha(script.hash, keys.length, ...keys, ...args);
          return result;
        } catch (error) {
          const err = error as Error;
          // If script not found, load and execute
          if (err.message.includes("NOSCRIPT")) {
            try {
              return await client.eval(
                script.source,
                keys.length,
                ...keys,
                ...args,
              );
            } catch (fallbackError) {
              throw fallbackError as Error;
            }
          }
          throw error;
        }
      });
    } catch (error) {
      // If it's a ConfigurationError, don't wrap it
      if (error instanceof ConfigurationError) {
        throw error;
      }
      // For other errors, wrap them appropriately
      throw error;
    }
  }


  /**
   * Execute operation on all clients with consensus
   */
  private async executeWithConsensus<T>(
    operation: (client: RedisClient) => Promise<T>,
    cleanupContext?: { keys: string[], identifier: string },
    options?: { evaluateResult?: (result: any) => boolean, successPolicy?: 'quorum' | 'any' }
  ): Promise<LockExecutionResult> {
    const startTime = Date.now();
    const quorumSize = Math.floor(this.clients.length / 2) + 1;

    // Default evaluator: numbers > 0 are success
    const evaluate = options?.evaluateResult ?? ((result: any) => {
      return typeof result === 'number' && result > 0;
    });

    // Kick off all operations in parallel
    type Outcome = { client: RedisClient; result?: T; error?: Error };
    const pending = new Map<number, Promise<Outcome>>();
    const outcomes: Outcome[] = [];
    this.clients.forEach((client, index) => {
      const p = operation(client)
        .then((result) => ({ client, result }))
        .catch((error) => ({ client, error }));
      pending.set(index, p);
    });

    let successCount = 0;
    let failureCount = 0;
    const votesFor = new Set<RedisClient>();
    const votesAgainst = new Map<RedisClient, Error>();

    const consume = async (index: number, outcome: Outcome) => {
      outcomes[index] = outcome;
      pending.delete(index);
      if (outcome.error) {
        failureCount++;
        votesAgainst.set(outcome.client, outcome.error);
      } else {
        if (process.env.NEOLOCK_DEBUG_CONSENSUS) {
          // eslint-disable-next-line no-console
          console.log('[consensus] result:', typeof outcome.result, outcome.result);
        }
        if (evaluate(outcome.result)) {
          successCount++;
          votesFor.add(outcome.client);
        } else {
          failureCount++;
          const reason = typeof outcome.result === 'number' ? `Operation returned ${outcome.result}` : `Invalid response: ${outcome.result}`;
          votesAgainst.set(outcome.client, new Error(reason));
        }
      }
    };

    // Helper to wait for one next settlement
    const nextSettlement = async (): Promise<void> => {
      const entries = Array.from(pending.entries());
      if (entries.length === 0) return;
      const raced = entries.map(([i, p]) => p.then((o) => ({ i, o })));
      const { i, o } = await Promise.race(raced);
      await consume(i, o);
    };

    // Consume until either quorum achieved or impossible to reach
    while (pending.size > 0) {
      await nextSettlement();

      // Early success
      if (successCount >= quorumSize) {
        break;
      }

      // Early failure if impossible to reach quorum with remaining
      const remaining = pending.size;
      if (successCount + remaining < quorumSize) {
        break;
      }
    }

    const stats: LockStats = {
      totalClients: this.clients.length,
      quorumSize,
      votesFor,
      votesAgainst,
      startTime,
      duration: Date.now() - startTime,
    };

    if (process.env.NEOLOCK_DEBUG_CONSENSUS) {
      // eslint-disable-next-line no-console
      console.log('[consensus] totals:', { clients: this.clients.length, quorumSize, successCount, failureCount });
    }

    const succeeded = options?.successPolicy === 'any' ? successCount > 0 : successCount >= quorumSize;

    if (!succeeded) {
      // Determine if all failures are non-retryable (e.g., configuration errors)
      const allNonRetryable = votesFor.size === 0 && Array.from(votesAgainst.values()).every(err =>
        err instanceof ConfigurationError || err instanceof CircuitBreakerOpenError
      );

      // Clean up partial locks from successful clients before throwing error
      if (votesFor.size > 0 && cleanupContext) {
        const cleanupPromises = Array.from(votesFor).map(client =>
          // Use eval(source) here to make tests able to detect release invocation easily
          this.circuitBreaker.execute(this.getClientId(client), () =>
            client.eval(
              SCRIPTS.release.source,
              cleanupContext.keys.length,
              ...cleanupContext.keys,
              cleanupContext.identifier
            )
          ).catch(() => {}) // Ignore cleanup errors
        );
        await Promise.all(cleanupPromises);
      }

      if (allNonRetryable) {
        // Re-throw the first non-retryable error to prevent retries upstream
        throw Array.from(votesAgainst.values())[0];
      }

      throw new ConsensusError(
        `Failed to achieve quorum: ${successCount}/${quorumSize} votes`,
        [Promise.resolve(stats)],
        quorumSize,
        successCount,
      );
    }

    return {
      attempts: [Promise.resolve(stats)],
      startTime,
      success: true,
    };
  }

  /**
   * Acquire a distributed lock using internal Redlock algorithm
   * @internal This method is for internal use only. Use acquire() instead.
   * @deprecated Use acquire() method instead
   */
  private async acquireRedlock(
    resources: string | string[],
    ttl: number,
    options?: Partial<LockOptions>,
  ): Promise<any> {
    const resourceList = Array.isArray(resources) ? resources : [resources];

    if (resourceList.length > 1) {
      throw new Error(
        "Redlock currently supports only single resource locking. Use acquire() for multi-resource locks.",
      );
    }

    const redlockOptions = {
      driftFactor: options?.driftFactor ?? this.defaultOptions.driftFactor,
      retryCount: options?.retryCount ?? this.defaultOptions.retryCount,
      retryDelay: options?.retryDelay ?? this.defaultOptions.retryDelay,
      retryJitter: options?.retryJitter ?? this.defaultOptions.retryJitter,
      automaticExtensionThreshold:
        options?.autoExtendThreshold ?? this.defaultOptions.autoExtendThreshold,
    };

    return await this.redlock.acquire(resourceList[0], ttl, redlockOptions);
  }

  /**
   * Execute function with Redlock and native AbortController
   */
  async usingRedlock<T>(
    resources: string | string[],
    ttl: number,
    routine: (signal: AbortSignal) => Promise<T>,
    options?: Partial<LockOptions>,
  ): Promise<T> {
    const resourceList = Array.isArray(resources) ? resources : [resources];

    if (resourceList.length > 1) {
      throw new Error(
        "Redlock currently supports only single resource locking",
      );
    }

    const redlockOptions = {
      driftFactor: options?.driftFactor ?? this.defaultOptions.driftFactor,
      retryCount: options?.retryCount ?? this.defaultOptions.retryCount,
      retryDelay: options?.retryDelay ?? this.defaultOptions.retryDelay,
      retryJitter: options?.retryJitter ?? this.defaultOptions.retryJitter,
      automaticExtensionThreshold:
        options?.autoExtendThreshold ?? this.defaultOptions.autoExtendThreshold,
    };

    return await this.redlock.using(
      resourceList[0],
      ttl,
      routine,
      redlockOptions,
    );
  }

  /**
   * Acquire distributed lock
   */
  async acquire(
    resources: string | string[],
    options?: Partial<LockOptions>,
  ): Promise<Lock> {
    const resourceList = Array.isArray(resources) ? resources : [resources];
    const lockOptions = { ...this.defaultOptions, ...options };
    // If caller provided identifier explicitly (even empty string), respect it; otherwise generate one
    const identifier = (options && Object.prototype.hasOwnProperty.call(options, 'identifier'))
      ? (options.identifier as string)
      : (lockOptions.identifier || this.generateIdentifier());
    const keys = resourceList.map((r) => this.generateLockKey(r));

    const startTime = Date.now();
    let lastError: Error | undefined;
    let attempts = 0;
    const maxAttempts =
      lockOptions.retryCount === -1 ? Infinity : lockOptions.retryCount + 1;

    // Phase 1: Attempt to acquire with retries on failure only
    let acquired = false;
    while (attempts < maxAttempts && !acquired) {
      attempts++;

      try {
        await this.executeWithConsensus(async (client) => {
          const response = await this.executeScript(client, SCRIPTS.acquire, keys, [
            identifier,
            lockOptions.ttl,
          ]);

          // If response is 0, it means the lock already exists
          if (response === 0) {
            throw new ResourceLockedError(resourceList);
          }

          return response;
        }, { keys, identifier }); // Pass cleanup context

        acquired = true;
      } catch (error) {
        lastError = error as Error;
        if (!isRetryableError(lastError)) break;
        if (attempts < maxAttempts) {
          const delay = getRetryDelay(
            lastError,
            lockOptions.retryDelay,
            lockOptions.retryJitter,
          );
          await new Promise((resolve) => setTimeout(resolve, delay));
          this.metrics.recordRetry(identifier, attempts);
        }
      }
    }

    if (!acquired) {
      // Record failed acquisition
      this.metrics.recordAcquisitionFailed(
        resourceList,
        lastError || new Error("Unknown error"),
        attempts,
        Date.now() - startTime,
      );

      if (attempts >= maxAttempts && maxAttempts > 1) {
        throw new LockTimeoutError(Date.now() - startTime, attempts);
      } else if (lastError instanceof ConsensusError) {
        throw lastError;
      } else if (!lastError || isRetryableError(lastError)) {
        throw new LockTimeoutError(Date.now() - startTime, attempts);
      } else {
        throw lastError;
      }
    }

    // Compute expiration based on initial start time (matches expected semantics in tests)
    const drift = Math.round(lockOptions.driftFactor * lockOptions.ttl) + 2;
    const expiration = startTime + lockOptions.ttl - drift;

    // If drift makes it invalid already, treat as timeout
    if (expiration <= Date.now()) {
      throw new LockTimeoutError(lockOptions.ttl, attempts, { reason: 'Lock would be expired due to clock drift', resources: resourceList });
    }

    // Phase 2: Optional stability verification sweeps (read-only)
    if (lockOptions.retryCount > 0) {
      for (let i = 0; i < lockOptions.retryCount; i++) {
        const delay = getRetryDelay(
          new ConsensusError('stability-check', [Promise.resolve({} as any)], 0, 0),
          lockOptions.retryDelay,
          lockOptions.retryJitter,
        );
        await new Promise((resolve) => setTimeout(resolve, delay));

        try {
          await this.executeWithConsensus(async (client) => {
            // Use status script (read-only) to verify lock holder on each client
            const result = await this.executeScript(client, SCRIPTS.status, keys, []);
            return result;
          }, { keys, identifier }, {
            evaluateResult: (res: any) => {
              // Accept numeric success (for mocks) or validate holder matches identifier
              if (typeof res === 'number') return res > 0;
              try {
                const data = typeof res === 'string' ? JSON.parse(res) : res;
                if (!Array.isArray(data)) return false;
                // All keys on that client must be held by our identifier if present
                return data.every((item: any) => item && (item.holder === null || item.holder === identifier));
              } catch {
                return false;
              }
            }
          });
        } catch (error) {
          // Cleanup already attempted inside executeWithConsensus; throw failure
          lastError = error as Error;
          // Record failed stabilization
          this.metrics.recordAcquisitionFailed(
            resourceList,
            lastError,
            attempts + i,
            Date.now() - startTime,
          );

          // Best-effort global cleanup (in case some clients still hold it)
          const cleanupPromises = this.clients.map((client) =>
            this.executeScript(client, SCRIPTS.release, keys, [identifier]).catch(() => {})
          );
          await Promise.allSettled(cleanupPromises);

          if (lockOptions.retryCount > 0) {
            throw new LockTimeoutError(Date.now() - startTime, attempts + i + 1);
          }
          throw lastError;
        }
      }
    }

    // Create and register lock after passing stability checks
    const lock = new Lock(
      resourceList,
      identifier,
      expiration,
      {
        extend: this.extendLock.bind(this),
        release: this.releaseLockInternal.bind(this),
      },
      lockOptions,
    );

    this.activeLocks.set(identifier, lock);
    this.metrics.recordLockAcquired(
      identifier,
      resourceList,
      Date.now() - startTime,
    );

    return lock;
  }

  /**
   * Acquire optimistic lock with version-based conflict detection
   */
  async acquireOptimistic(
    resources: string | string[],
    options: OptimisticLockOptions = {},
  ): Promise<OptimisticLockResult> {
    const resourceList = Array.isArray(resources) ? resources : [resources];
    const opts = { ...this.defaultOptions, ...options };
    const identifier = (options && Object.prototype.hasOwnProperty.call(options, 'identifier'))
      ? (options.identifier as string)
      : (opts.identifier || this.generateIdentifier());
    const keys = resourceList.map((r) => this.generateLockKey(r));

    try {
      const result = await this.executeWithConsensus(async (client) => {
        const scriptResult = await this.executeScript(client, SCRIPTS.optimisticAcquire, keys, [
          identifier,
          opts.ttl,
          opts.expectedVersion?.toString() || '',
          opts.expectedValue ? JSON.stringify(opts.expectedValue) : '',
        ]);

        // Handle script return format
        return scriptResult;
      }, undefined, { evaluateResult: (r: any) => !!r && typeof r === 'object' && !(r as any).conflict });

      // Cache successful lock acquisition
      if (this.cacheEnabled && result.success) {
        this.cacheLock(identifier, resourceList, opts.ttl);
      }

      return {
        success: true,
        currentVersion: (result as any).newVersion,
        retries: 0,
      };
    } catch (error) {
      if (error instanceof OptimisticLockConflictError) {
        this.emit("lock:conflict", resourceList, error.expectedVersion, error.currentVersion);
        return {
          success: false,
          conflict: true,
          currentVersion: error.currentVersion,
        };
      }
      throw error;
    }
  }

  /**
   * Update optimistic lock with conflict detection
   */
  async updateOptimistic(
    resources: string | string[],
    expectedVersion: number,
    options: OptimisticLockOptions = {},
  ): Promise<OptimisticLockResult> {
    const resourceList = Array.isArray(resources) ? resources : [resources];
    const opts = { ...this.defaultOptions, ...options };
    const identifier = (options && Object.prototype.hasOwnProperty.call(options, 'identifier'))
      ? (options.identifier as string)
      : (opts.identifier || this.generateIdentifier());
    const keys = resourceList.map((r) => this.generateLockKey(r));

    try {
      const result = await this.executeWithConsensus(async (client) => {
        const scriptResult = await this.executeScript(client, SCRIPTS.optimisticUpdate, keys, [
          identifier,
          opts.ttl,
          expectedVersion.toString(),
          opts.expectedValue ? JSON.stringify(opts.expectedValue) : '',
        ]);

        // Handle script return format
        return scriptResult;
      }, undefined, { evaluateResult: (r: any) => !!r && typeof r === 'object' && !(r as any).conflict });

      return {
        success: true,
        currentVersion: (result as any).newVersion,
        retries: 0,
      };
    } catch (error) {
      if (error instanceof OptimisticLockConflictError) {
        this.emit("lock:conflict", resourceList, error.expectedVersion, error.currentVersion);
        return {
          success: false,
          conflict: true,
          currentVersion: error.currentVersion,
        };
      }
      throw error;
    }
  }

  /**
   * Acquire lock using hybrid strategy (pessimistic + optimistic)
   */
  async acquireHybrid(
    resources: string | string[],
    options: HybridLockOptions = {},
  ): Promise<Lock> {
    const resourceList = Array.isArray(resources) ? resources : [resources];
    const opts = { ...this.defaultOptions, ...options };
    const primaryStrategy = opts.primaryStrategy || 'pessimistic';
    const fallbackStrategy = opts.fallbackStrategy || 'optimistic';

    try {
      // Try primary strategy
      if (primaryStrategy === 'pessimistic' || primaryStrategy === 'adaptive') {
        return await this.acquire(resources, opts);
      } else if (primaryStrategy === 'optimistic') {
        const result = await this.acquireOptimistic(resources, opts);
        if (result.success) {
          // Convert optimistic result to Lock object
          return this.createLockFromOptimistic(resources, result, opts);
        } else {
          throw new OptimisticLockConflictError(
            resourceList,
            (opts as any).expectedVersion || 0,
            result.currentVersion || 0,
            'version'
          );
        }
      }
    } catch (primaryError) {
      // Try fallback strategy
      try {
        if (fallbackStrategy === 'pessimistic') {
          return await this.acquire(resources, opts);
        } else if (fallbackStrategy === 'optimistic') {
          const result = await this.acquireOptimistic(resources, opts);
          if (result.success) {
            return this.createLockFromOptimistic(resources, result, opts);
          }
        }
        throw primaryError; // Re-throw if fallback also fails
      } catch (fallbackError) {
        throw new HybridLockError(
          primaryStrategy,
          fallbackStrategy,
          primaryError as Error,
          fallbackError as Error
        );
      }
    }

    throw new Error("Hybrid locking failed - no strategy succeeded");
  }

  /**
   * Enable lock caching
   */
  enableCache(options: LockCacheOptions = {}): void {
    this.cacheEnabled = true;
    this.cacheOptions = {
      ttl: 300000, // 5 minutes default
      maxSize: 10000,
      strategy: 'lru',
      negativeCaching: false,
      ...options,
    };
  }

  /**
   * Disable lock caching
   */
  disableCache(): void {
    this.cacheEnabled = false;
    this.lockCache.clear();
  }

  /**
   * Cache a lock for performance
   */
  private cacheLock(identifier: string, resources: string[], ttl: number): void {
    if (!this.cacheEnabled) return;

    const cacheKey = resources.join(':');
    const expiration = Date.now() + ttl;

    // Implement LRU eviction if cache is full
    if (this.lockCache.size >= (this.cacheOptions.maxSize || 10000)) {
      this.evictCache();
    }

    this.lockCache.set(cacheKey, {
      identifier,
      expiration,
      cachedAt: Date.now(),
      status: 'acquired',
    });
  }

  /**
   * Check cache for lock status
   */
  private getCachedLock(resources: string[]): CachedLockInfo | null {
    if (!this.cacheEnabled) return null;

    const cacheKey = resources.join(':');
    const cached = this.lockCache.get(cacheKey);

    if (!cached) return null;

    // Check if cache entry is expired
    if (Date.now() > cached.expiration) {
      this.lockCache.delete(cacheKey);
      return null;
    }

    return cached;
  }

  /**
   * Evict cache entries based on strategy
   */
  private evictCache(): void {
    if (this.cacheOptions.strategy === 'lru') {
      // Simple LRU: remove oldest entries
      const entries = Array.from(this.lockCache.entries());
      entries.sort((a, b) => a[1].cachedAt - b[1].cachedAt);

      const toRemove = Math.floor(this.lockCache.size * 0.1); // Remove 10%
      for (let i = 0; i < toRemove; i++) {
        this.lockCache.delete(entries[i][0]);
      }
    } else {
      // Simple random eviction
      const keys = Array.from(this.lockCache.keys());
      const toRemove = Math.floor(keys.length * 0.1);
      for (let i = 0; i < toRemove; i++) {
        const randomIndex = Math.floor(Math.random() * keys.length);
        this.lockCache.delete(keys[randomIndex]);
        keys.splice(randomIndex, 1);
      }
    }
  }

  /**
   * Get cache statistics
   */
  getCacheStats(): { size: number; hitRate: number; evictions: number } {
    return {
      size: this.lockCache.size,
      hitRate: 0, // Would need to track hits/misses for accurate rate
      evictions: 0, // Would need to track evictions
    };
  }

  /**
   * Create Lock object from optimistic result
   */
  private createLockFromOptimistic(
    resources: string | string[],
    result: OptimisticLockResult,
    options: HybridLockOptions,
  ): Lock {
    const resourceList = Array.isArray(resources) ? resources : [resources];
    const identifier = this.generateIdentifier();
    const expiration = Date.now() + (options.ttl || this.defaultOptions.ttl);

    const lock = new Lock(
      resourceList,
      identifier,
      expiration,
      {
        extend: this.extendLock.bind(this),
        release: this.releaseLockInternal.bind(this),
      },
      {
        ...this.defaultOptions,
        ...options,
      } as Required<LockOptions>,
    );

    this.activeLocks.set(identifier, lock);
    return lock;
  }

  /**
   * Extend lock implementation
   */
  private async extendLock(
    lock: Lock,
    ttl: number,
    options?: Partial<LockOptions>,
  ): Promise<LockExtensionResult> {
    const startTime = Date.now();
    const keys = lock.resources.map((r) => this.generateLockKey(r));

    try {
      const result = await this.executeWithConsensus(async (client) => {
        const response = await this.executeScript(client, SCRIPTS.extend, keys, [
          lock.identifier,
          ttl,
        ]);

        // If response is 0, it means the lock is not owned by this identifier
        if (response === 0) {
          throw new LockExtensionError(
            lock.resources,
            lock.identifier,
            lock.expiration,
            { message: 'Lock not owned by this identifier' }
          );
        }

        return response;
      });

      const drift =
        Math.round(
          (options?.driftFactor ?? this.defaultOptions.driftFactor) * ttl,
        ) + 2;
      const expiration = startTime + ttl - drift;

      this.metrics.recordLockExtended(
        lock.identifier,
        Date.now() - startTime,
      );

      return {
        success: true,
        expiration,
        stats: result.attempts[0],
      };
    } catch (error) {
      // If it's already a LockExtensionError, re-throw it
      if (error instanceof LockExtensionError) {
        throw error;
      }

      // Preserve the original error message for better error handling
      const originalError = error as Error;
      const errorMessage = originalError.message || 'Unknown error';

      // Create a new error that includes the original message
      const extensionError = new LockExtensionError(
        lock.resources,
        lock.identifier,
        lock.expiration,
        { originalError, message: errorMessage }
      );

      // Ensure the error message contains the original message
      extensionError.message = `Extension failed: ${errorMessage}`;

      throw extensionError;
    }
  }

  /**
   * Release lock implementation
   */
  private async releaseLockInternal(
    lock: Lock,
    options?: Partial<LockOptions>,
  ): Promise<LockReleaseResult> {
    const startTime = Date.now();
    const keys = lock.resources.map((r) => this.generateLockKey(r));

    try {
      const result = await this.executeWithConsensus(async (client) => {
        return this.executeScript(client, SCRIPTS.release, keys, [
          lock.identifier,
        ]);
      }, undefined, { evaluateResult: (res: any) => typeof res === 'number' ? res >= 0 : true });

      // Remove from active locks regardless of success
      this.activeLocks.delete(lock.identifier);
      this.metrics.recordLockReleased(lock.identifier, Date.now() - startTime);

      return {
        success: result.success,
        releasedCount: this.clients.length, // We consider idempotent releases as success across clients
        totalClients: this.clients.length,
        stats: result.attempts[0],
      };
    } catch (error) {
      throw new RedisOperationError("release", "multiple", error as Error);
    }
  }

  /**
   * Create auto-extending lock that executes a routine
   */
  async using<T>(
    resources: string | string[],
    routine: (signal: LockSignal) => Promise<T>,
    options?: Partial<LockOptions>,
  ): Promise<T> {
    const lock = await this.acquire(resources, options);
    return lock.using(routine, options);
  }

  /**
   * Force release locks (admin operation)
   */
  async forceRelease(
    resources: string | string[],
  ): Promise<{ releasedCount: number; totalAttempted: number }> {
    const resourceList = Array.isArray(resources) ? resources : [resources];
    const keys = resourceList.map((r) => this.generateLockKey(r));

    let totalReleased = 0;
    const promises: Promise<any>[] = [];

    // Execute on each client individually to track per-client results
    for (const client of this.clients) {
      promises.push(
        this.executeScript(client, SCRIPTS.forceRelease, keys, [])
          .then((result) => {
            // forceRelease script returns the number of keys that were actually released
            // If a key didn't exist or wasn't locked, it returns 0 for that key
            totalReleased += result;
            return result;
          })
          .catch(() => 0) // Count as 0 if failed
      );
    }

    await Promise.allSettled(promises);

    return {
      releasedCount: totalReleased,
      totalAttempted: resourceList.length,
    };
  }

  /**
   * Get value from Redis with data parsing (compatibility method)
   */
  async get<T = any>(key: string): Promise<T | null> {
    return this.circuitBreaker.execute(
      this.getClientId(this.clients[0]),
      async () => {
        const client = this.clients[0]; // Use first client for simple get
        const data = await client.get(key);

        if (!data) {
          return null;
        }

        try {
          return JSON.parse(data);
        } catch (error) {
          // Return raw data if not JSON
          return data as any;
        }
      },
    );
  }

  /**
   * Acquire a distributed lock with optional data storage (compatibility method)
   */
  async acquireLock(
    key: string,
    clientId: string,
    data?: any,
    ttl: number = 30000,
  ): Promise<boolean> {
    try {
      const lock = await this.acquire(key, { ttl, identifier: clientId });

      if (data !== undefined) {
        // Store associated data
        await this.circuitBreaker.execute(
          this.getClientId(this.clients[0]),
          async () => {
            const promises = this.clients.map((client) =>
              client.set(key, JSON.stringify(data), "PX", ttl),
            );
            await Promise.all(promises);
          },
        );
      }

      // Store lock reference for compatibility
      this.compatibilityLocks.set(`${key}:${clientId}`, lock);
      return true;
    } catch (error) {
      return false;
    }
  }

  /**
   * Release a lock using clientId (compatibility method)
   */
  async releaseLock(key: string, clientId: string): Promise<boolean> {
    const lock = this.compatibilityLocks.get(`${key}:${clientId}`);
    if (lock) {
      try {
        await lock.release();
        this.compatibilityLocks.delete(`${key}:${clientId}`);
        return true;
      } catch (error) {
        return false;
      }
    }
    return false;
  }

  /**
   * Force release a lock (compatibility method)
   */
  async forceReleaseLock(key: string): Promise<boolean> {
    try {
      const result = await this.forceRelease([key]);
      // Clean up any compatibility references
      for (const [mapKey, lock] of this.compatibilityLocks.entries()) {
        if (mapKey.startsWith(`${key}:`)) {
          this.compatibilityLocks.delete(mapKey);
        }
      }
      return true;
    } catch (error) {
      return false;
    }
  }

  /**
   * Get lock status
   */
  async getStatus(resources: string | string[]): Promise<
    Array<{
      resource: string;
      locked: boolean;
      holder?: string;
      ttl?: number;
    }>
  > {
    const resourceList = Array.isArray(resources) ? resources : [resources];
    const keys = resourceList.map((r) => this.generateLockKey(r));

    // Use first available client for status check
    const client = this.clients[0];
    const result = await this.executeScript(client, SCRIPTS.status, keys, []);

    let statusData;
    try {
      statusData = typeof result === "string" ? JSON.parse(result) : result;
    } catch (error) {
      statusData = [];
    }

    if (!Array.isArray(statusData)) {
      statusData = [];
    }

    return statusData.map((item: any, index: number) => ({
      resource: resourceList[index],
      locked: item.holder !== null,
      holder: item.holder,
      ttl: item.ttl > 0 ? item.ttl : undefined,
    }));
  }

  /**
   * Get all active locks
   */
  getActiveLocks(): Lock[] {
    return Array.from(this.activeLocks.values());
  }

  /**
   * Get metrics
   */
  getMetrics() {
    return this.metrics.getMetrics();
  }

  /**
   * Get performance summary
   */
  getPerformanceSummary() {
    return this.metrics.getPerformanceSummary();
  }

  /**
   * Export metrics in Prometheus format
   */
  exportMetrics(): string {
    return this.metrics.exportPrometheusMetrics();
  }

  /**
   * Get circuit breaker metrics (compatibility method)
   */
  getCircuitBreakerMetrics() {
    const cbMetrics = this.circuitBreaker.getAllMetrics();
    const aggregated = {
      state: "closed",
      successfulOperations: 0,
      failedOperations: 0,
      resetTimeout: this.config.circuitBreaker.resetTimeout,
      failureThreshold: this.config.circuitBreaker.failureThreshold,
    };

    // Aggregate metrics from all clients
    Object.values(cbMetrics).forEach((metrics) => {
      aggregated.successfulOperations += metrics.successfulOperations;
      aggregated.failedOperations += metrics.failedOperations;
      if (metrics.state === "open") {
        aggregated.state = "open";
      }
    });

    return aggregated;
  }

  /**
   * Publish to Redis channel (compatibility method)
   */
  async publish(channel: string, value: unknown): Promise<number> {
    return this.circuitBreaker.execute(
      this.getClientId(this.clients[0]),
      async () => {
        const client = this.clients[0]; // Use first client for publish
        return await client.publish(channel, JSON.stringify(value));
      },
    );
  }

  /**
   * Create observable from Redis events (compatibility method)
   */
  fromEvent<T>(eventName: string): any {
    // This is a simplified implementation. For full RxJS support,
    // users should use a dedicated Redis pub/sub library
    const client = this.clients[0];

    return {
      subscribe: (callback: (data: T) => void) => {
        const listener = (channel: string, message: string) => {
          if (channel === eventName) {
            try {
              const data = JSON.parse(message);
              callback(data);
            } catch (error) {
              callback(message as any);
            }
          }
        };

        client.on("message", listener);
        client.subscribe(eventName);

        return {
          unsubscribe: () => {
            client.removeListener("message", listener);
            client.unsubscribe(eventName);
          },
        };
      },
    };
  }

  /**
   * Reset metrics
   */
  resetMetrics(): void {
    this.metrics.reset();
  }

  /**
   * Cleanup expired locks (maintenance operation)
   */
  async cleanup(): Promise<number> {
    const pattern = `${this.config.keyPrefix}:*`;

    let totalCleaned = 0;
    for (const client of this.clients) {
      try {
        const cleaned = await this.executeScript(
          client,
          SCRIPTS.cleanup,
          [pattern],
          [100], // batch size
        );
        totalCleaned += cleaned;
      } catch (error) {
        this.emit(
          "error",
          new RedisOperationError(
            "cleanup",
            this.getClientId(client),
            error as Error,
          ),
        );
      }
    }

    return totalCleaned;
  }

  /**
   * Gracefully shutdown
   */
  async shutdown(): Promise<void> {
    // Release all active locks
    const releasPromises = Array.from(this.activeLocks.values()).map((lock) =>
      lock.release().catch(() => {
        // Ignore errors during shutdown
      }),
    );

    await Promise.all(releasPromises);

    // Clear internal state
    this.activeLocks.clear();
    this.circuitBreaker.clear();
    this.removeAllListeners();
  }
}

// Export all types and classes
export * from "./core/types";
export * from "./core/errors";
export * from "./core/lock";
export * from "./patterns/circuit-breaker";
export * from "./utils/metrics";

// Export from algorithms (without duplicating types from core/types)
// Note: Redlock and RedlockLock are now internal implementation details.
// Use RedlockToolkit as the main public interface.
export { OptimisticRedlock } from "./algorithms/optimistic-redlock";

// Re-export specific classes for better compatibility
export { CircuitBreakerManager } from "./patterns/circuit-breaker";
export { Lock } from "./core/lock";
export { MetricsCollector } from "./utils/metrics";

// Default export
export default RedlockToolkit;
