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
  LockExecutionResult,
  LockReleaseResult,
  LockExtensionResult,
  LockSignal,
  OptimisticLockOptions,
  OptimisticLockResult,
  HybridLockOptions,
  LockCacheOptions,
  CircuitBreakerMetrics,
  ILockToolkit,
} from "./core/types";
import { Lock } from "./core/lock";
import {
  ResourceLockedError,
  ConsensusError,
  LockTimeoutError,
  LockExtensionError,
  ConfigurationError,
  RedisOperationError,
  OptimisticLockConflictError,
  HybridLockError,
  isRetryableError,
  getRetryDelay,
} from "./core/errors";
import { CircuitBreakerManager } from "./patterns/circuit-breaker";
import { MetricsCollector } from "./utils/metrics";
import { SCRIPTS, LuaScript } from "./utils/scripts";

import { Logger, LoggerFactory } from "./core/logger";
import { ConsensusManager } from "./utils/consensus-manager";
import { CacheManager } from "./managers/cache";

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

import { PessimisticLockStrategy } from "./strategies/pessimistic-strategy";
import { OptimisticLockStrategy } from "./strategies/optimistic-strategy";
import { HybridLockStrategy } from "./strategies/hybrid-strategy";
import { PubSubManager } from "./pubsub/pubsub-manager";
import { PubSubWaiter } from "./pubsub/pubsub-waiter";
import { SemaphoreStrategy } from "./strategies/semaphore-strategy";
import { CountDownLatchStrategy } from "./strategies/countdown-latch-strategy";
import { SemaphorePermit } from "./primitives/semaphore";
import { CountDownLatch } from "./primitives/countdown-latch";
import { SemaphoreOptions, SemaphoreStatus, CountDownLatchOptions, CountDownLatchStatus } from "./core/types";

/**
 * Main RedlockToolkit class implementing advanced distributed locking
 */
export class RedlockToolkit extends EventEmitter implements ILockToolkit {
  private readonly clients: RedisClient[];
  private readonly config: Required<RedlockToolkitConfig>;
  readonly defaultOptions: Required<LockOptions>;
  readonly circuitBreaker: CircuitBreakerManager;
  readonly metrics: MetricsCollector;
  private readonly logger: Logger;
  private readonly consensusManager: ConsensusManager;
  readonly cacheManager: CacheManager;
  private readonly pessimisticStrategy: PessimisticLockStrategy;
  private readonly optimisticStrategy: OptimisticLockStrategy;
  private readonly hybridStrategy: HybridLockStrategy;
  readonly activeLocks = new Map<string, Lock>();
  readonly compatibilityLocks = new Map<string, Lock>();
  private readonly pubSubManager: PubSubManager | null = null;
  readonly pubSubWaiter: PubSubWaiter | null = null;
  private readonly semaphoreStrategy: SemaphoreStrategy;
  private readonly countDownLatchStrategy: CountDownLatchStrategy;
  readonly activeSemaphores = new Map<string, SemaphorePermit>();

  constructor(config: RedlockToolkitConfig) {
    super();

    this.validateConfig(config);

    this.clients = [...config.clients];
    this.logger = LoggerFactory.create(config.logger);
    this.consensusManager = new ConsensusManager(this.logger);
    this.cacheManager = new CacheManager();
    this.pessimisticStrategy = new PessimisticLockStrategy(this);
    this.optimisticStrategy = new OptimisticLockStrategy(this);
    this.hybridStrategy = new HybridLockStrategy(this);
    
    this.config = {
      clients: this.clients,
      defaultLockOptions: { ...DEFAULT_CONFIG, ...config.defaultLockOptions },
      circuitBreaker: { ...DEFAULT_CIRCUIT_BREAKER, ...config.circuitBreaker },
      enableMetrics: config.enableMetrics ?? true,
      keyPrefix: config.keyPrefix ?? "neolock",
      logger: config.logger ?? false,
      pubSub: config.pubSub ?? { enabled: false },
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

    // Initialize pub/sub if enabled
    if (config.pubSub?.enabled) {
      this.pubSubManager = new PubSubManager({
        clients: this.clients,
        subscriberClients: config.pubSub.subscriberClients,
        keyPrefix: this.config.keyPrefix as string,
        logger: this.logger,
      });
      this.pubSubWaiter = new PubSubWaiter(
        this.pubSubManager,
        this.config.keyPrefix as string,
      );
    }

    this.semaphoreStrategy = new SemaphoreStrategy(this);
    this.countDownLatchStrategy = new CountDownLatchStrategy(
      this,
      this.pubSubManager,
      this.config.keyPrefix as string,
    );

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

    // Validate autoExtendThreshold vs TTL compatibility
    const effectiveTtl = config.defaultLockOptions?.ttl ?? DEFAULT_CONFIG.ttl;
    const effectiveThreshold = config.defaultLockOptions?.autoExtendThreshold ?? DEFAULT_CONFIG.autoExtendThreshold;
    if (effectiveThreshold > 0 && effectiveThreshold >= effectiveTtl) {
      throw new ConfigurationError(
        "autoExtendThreshold",
        effectiveThreshold,
        `Must be less than TTL (${effectiveTtl}ms). Auto-extension threshold cannot exceed lock TTL.`,
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
  public getClientId(client: RedisClient): string {
    const options = client.options as any;
    return `${options?.host || "localhost"}:${options?.port || 6379}`;
  }

  /**
   * Get a single Redis client for non-consensus read operations
   */
  public getClientForRead(): RedisClient {
    return this.clients[0];
  }

  /**
   * Generate lock key
   */
  public generateLockKey(resource: string): string {
    return `${this.config.keyPrefix}:${resource}`;
  }

  /**
   * Generate unique lock identifier
   */
  public generateIdentifier(): string {
    return randomBytes(16).toString("hex");
  }

  /**
   * Execute Lua script on client with fallback
   */
  public async executeScript(
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
   * Execute operation on all clients with consensus and cleanup.
   */
  public async executeWithConsensus<T>(
    operation: (client: RedisClient) => Promise<T>,
    cleanupContext?: { keys: string[], identifier: string, script?: LuaScript, args?: (string | number)[] },
    options?: { evaluateResult?: (result: any) => boolean, successPolicy?: 'quorum' | 'any' }
  ): Promise<LockExecutionResult> {
    try {
      const result = await this.consensusManager.execute(
        this.clients,
        operation,
        options
      );

      return {
        attempts: result.attempts,
        startTime: result.startTime,
        success: result.success,
      };
    } catch (error) {
      if (error instanceof ConsensusError && cleanupContext) {
        const stats = await error.attempts[0];
        const votesFor = stats.votesFor;
        if (votesFor.size > 0) {
          const cleanupScript = cleanupContext.script ?? SCRIPTS.release;
          const cleanupArgs = cleanupContext.args ?? [cleanupContext.identifier];
          const cleanupPromises = Array.from(votesFor).map(client =>
            this.circuitBreaker.execute(this.getClientId(client), () =>
              client.eval(
                cleanupScript.source,
                cleanupContext.keys.length,
                ...cleanupContext.keys,
                ...cleanupArgs
              )
            ).catch((err) => {
              this.logger.warn("Consensus cleanup failed", { clientId: this.getClientId(client), error: String(err) });
            })
          );
          await Promise.all(cleanupPromises);
        }
      }
      // Re-throw the original error
      throw error;
    }
  }

  /**
   * Acquire distributed lock
   */
  async acquire(
    resources: string | string[],
    options?: Partial<LockOptions>,
  ): Promise<Lock> {
    const resourceList = Array.isArray(resources) ? resources : [resources];
    
    const hasCustomIdentifier = options && Object.prototype.hasOwnProperty.call(options, 'identifier');
    
    const lockOptions: Required<LockOptions> = { 
      ...this.defaultOptions, 
      ...options,
      identifier: '', // Placeholder, will be overwritten
    };

    if (hasCustomIdentifier) {
      lockOptions.identifier = options.identifier!;
    } else {
      lockOptions.identifier = this.generateIdentifier();
    }

    return this.pessimisticStrategy.acquire(resourceList, lockOptions);
  }

  /**
   * Acquire optimistic lock with version-based conflict detection
   */
  async acquireOptimistic(
    resources: string | string[],
    options: OptimisticLockOptions = {},
  ): Promise<OptimisticLockResult> {
    const resourceList = Array.isArray(resources) ? resources : [resources];
    return this.optimisticStrategy.acquire(resourceList, options);
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
    return this.optimisticStrategy.update(resourceList, expectedVersion, options);
  }

  /**
   * Acquire lock using hybrid strategy (pessimistic + optimistic)
   */
  async acquireHybrid(
    resources: string | string[],
    options: HybridLockOptions = {},
  ): Promise<Lock> {
    const resourceList = Array.isArray(resources) ? resources : [resources];
    return this.hybridStrategy.acquire(resourceList, options);
  }

  /**
   * Enable lock caching
   */
  enableCache(options: LockCacheOptions = {}): void {
    this.cacheManager.enable(options);
  }

  /**
   * Disable lock caching
   */
  disableCache(): void {
    this.cacheManager.disable();
  }

  /**
   * Get cache statistics
   */
  getCacheStats(): { size: number; hitRate: number; evictions: number } {
    return this.cacheManager.getStats();
  }

  /**
   * Create Lock object from optimistic result
   */
  public createLockFromOptimistic(
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
  public async extendLock(
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
  public async releaseLockInternal(
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

      // Notify waiters via pub/sub (fire-and-forget)
      if (this.pubSubManager) {
        for (const resource of lock.resources) {
          const channel = `${this.config.keyPrefix}:notify:lock:${resource}`;
          this.pubSubManager.publish(channel, "released").catch((err) => {
            this.logger.warn("Failed to publish lock release notification", { channel, error: String(err) });
          });
        }
      }

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

    // Result is a flat array of triples: [key, holder_or_empty, ttl, ...]
    const flat = Array.isArray(result) ? result : [];
    const statuses: Array<{
      resource: string;
      locked: boolean;
      holder?: string;
      ttl?: number;
    }> = [];

    for (let i = 0; i < flat.length; i += 3) {
      const holder = flat[i + 1] as string;
      const ttl = Number(flat[i + 2]);
      const idx = Math.floor(i / 3);
      statuses.push({
        resource: resourceList[idx],
        locked: holder !== '',
        holder: holder !== '' ? holder : undefined,
        ttl: ttl > 0 ? ttl : undefined,
      });
    }

    return statuses;
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
    cbMetrics.forEach((metrics) => {
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
   * Acquire a semaphore permit
   */
  async acquireSemaphore(
    resource: string,
    options: SemaphoreOptions,
  ): Promise<SemaphorePermit> {
    const permit = await this.semaphoreStrategy.acquire(resource, options);
    this.activeSemaphores.set(permit.identifier, permit);

    // Clean up tracking and publish notification on release
    permit.onRelease = () => {
      this.activeSemaphores.delete(permit.identifier);
      if (this.pubSubManager) {
        const channel = `${this.config.keyPrefix}:notify:sem:${resource}`;
        this.pubSubManager.publish(channel, "released").catch((err) => {
          this.logger.warn("Failed to publish semaphore release notification", { channel, error: String(err) });
        });
      }
    };

    return permit;
  }

  /**
   * Get semaphore status
   */
  async getSemaphoreStatus(
    resource: string,
    maxPermits: number,
  ): Promise<SemaphoreStatus> {
    return this.semaphoreStrategy.getStatus(resource, maxPermits);
  }

  /**
   * Create a new CountDownLatch
   */
  async createCountDownLatch(
    name: string,
    options: CountDownLatchOptions,
  ): Promise<CountDownLatch> {
    return this.countDownLatchStrategy.create(name, options);
  }

  /**
   * Get a handle to an existing CountDownLatch (e.g. from another process).
   * Does not create any keys in Redis — the latch must already exist.
   */
  getCountDownLatch(
    name: string,
    options?: { awaitTimeout?: number; pollInterval?: number },
  ): CountDownLatch {
    return new CountDownLatch(
      name,
      0, // unknown from handle — caller can use getStatus() to learn the actual count
      {
        countDown: (latch, eventId) => this.countDownLatchStrategy.countDown(latch, eventId),
        getStatus: (n) => this.countDownLatchStrategy.getStatus(n),
        await: (latch, timeoutMs) => this.countDownLatchStrategy.awaitLatch(latch, timeoutMs),
      },
      {
        ttl: 0,
        awaitTimeout: options?.awaitTimeout ?? 30000,
        pollInterval: options?.pollInterval ?? 100,
      },
    );
  }

  /**
   * Get CountDownLatch status
   */
  async getCountDownLatchStatus(name: string): Promise<CountDownLatchStatus> {
    return this.countDownLatchStrategy.getStatus(name);
  }

  /**
   * Reset metrics
   */
  resetMetrics(): void {
    this.metrics.reset();
  }

  /**
   * Cleanup expired locks (maintenance operation).
   * Uses Node.js-side SCAN to avoid blocking Redis's single thread.
   */
  async cleanup(): Promise<number> {
    const pattern = `${this.config.keyPrefix}:*`;
    const batchSize = 100;

    let totalCleaned = 0;
    for (const client of this.clients) {
      try {
        let cursor = "0";
        do {
          const [nextCursor, keys] = await this.circuitBreaker.execute(
            this.getClientId(client),
            () => client.scan(cursor, "MATCH", pattern, "COUNT", batchSize),
          ) as [string, string[]];
          cursor = nextCursor;

          if (keys.length > 0) {
            const cleaned = await this.executeScript(
              client,
              SCRIPTS.cleanupCheck,
              keys,
              [],
            );
            totalCleaned += cleaned;
          }
        } while (cursor !== "0");
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

    // Release all active semaphore permits
    const semaphoreReleases = Array.from(this.activeSemaphores.values()).map((permit) =>
      permit.release().catch((err) => {
        this.logger.warn("Failed to release semaphore permit during shutdown", { identifier: permit.identifier, error: String(err) });
      }),
    );
    await Promise.all(semaphoreReleases);
    this.activeSemaphores.clear();

    // Shut down pub/sub subscribers
    if (this.pubSubManager) {
      await this.pubSubManager.shutdown();
    }

    // Clear internal state
    this.activeLocks.clear();
    this.compatibilityLocks.clear();
    this.circuitBreaker.clear();
    this.metrics.stopEviction();
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

// Primitives exports
export { SemaphorePermit } from "./primitives/semaphore";
export { CountDownLatch } from "./primitives/countdown-latch";

// Pub/Sub exports
export { PubSubManager } from "./pubsub/pubsub-manager";
export { PubSubWaiter } from "./pubsub/pubsub-waiter";

// Default export
export default RedlockToolkit;
