/**
 * RedlockToolkit - Advanced Redis distributed locking library
 * Combines best practices from multiple locking approaches
 */

import { Redis as IORedisClient, Cluster as IORedisCluster } from "ioredis";
import { Logger } from "./logger";

export type RedisClient = IORedisClient | IORedisCluster;

/**
 * Lock configuration options
 */
export interface LockOptions {
  /** Time to live for the lock in milliseconds */
  ttl?: number;
  /** Maximum number of retry attempts */
  retryCount?: number;
  /** Base delay between retries in milliseconds */
  retryDelay?: number;
  /** Maximum jitter to add to retry delay */
  retryJitter?: number;
  /** Clock drift factor for Redlock algorithm */
  driftFactor?: number;
  /** Threshold for automatic lock extension in milliseconds */
  autoExtendThreshold?: number;
  /** Custom lock identifier */
  identifier?: string;
}

/**
 * Circuit breaker configuration
 */
export interface CircuitBreakerOptions {
  /** Number of failures before opening circuit */
  failureThreshold?: number;
  /** Reset timeout in milliseconds */
  resetTimeout?: number;
  /** Maximum number of retries when circuit is half-open */
  maxRetries?: number;
  /** Timeout for individual operations */
  operationTimeout?: number;
}

/**
 * Pub/Sub configuration for notification-based waiting
 */
export interface PubSubConfig {
  /** Enable pub/sub waiting for lock acquisition */
  enabled: boolean;
  /** User-provided subscriber clients (optional; will use client.duplicate() if not provided) */
  subscriberClients?: RedisClient[];
}

/**
 * RedlockToolkit configuration
 */
export interface RedlockToolkitConfig {
  /** Redis clients for distributed consensus */
  clients: RedisClient[];
  /** Default lock options */
  defaultLockOptions?: Partial<LockOptions>;
  /** Circuit breaker configuration */
  circuitBreaker?: CircuitBreakerOptions;
  /** Enable metrics collection */
  enableMetrics?: boolean;
  /** Custom lock key prefix */
  keyPrefix?: string;
  /** Logger instance or boolean (true for console, false for silent) */
  logger?: Logger | boolean;
  /** Pub/Sub configuration for notification-based waiting */
  pubSub?: PubSubConfig;
}

/**
 * Lock execution statistics
 */
export interface LockStats {
  /** Total number of clients */
  totalClients: number;
  /** Required quorum size */
  quorumSize: number;
  /** Clients that voted for the operation */
  votesFor: Set<RedisClient>;
  /** Clients that voted against with their errors */
  votesAgainst: Map<RedisClient, Error>;
  /** Operation start time */
  startTime: number;
  /** Operation duration */
  duration?: number;
}

/**
 * Circuit breaker metrics
 */
export interface CircuitBreakerMetrics {
  /** Current state (closed, open, half-open) */
  state: "closed" | "open" | "half-open";
  /** Total number of operations */
  totalOperations: number;
  /** Number of successful operations */
  successfulOperations: number;
  /** Number of failed operations */
  failedOperations: number;
  /** Last failure time */
  lastFailureTime?: number;
  /** Time when circuit was last opened */
  lastOpenTime?: number;
}

/**
 * Lock metrics
 */
export interface LockMetrics {
  /** Total locks acquired */
  locksAcquired: number;
  /** Total locks released */
  locksReleased: number;
  /** Total lock extensions */
  lockExtensions: number;
  /** Failed lock attempts */
  failedAcquisitions: number;
  /** Average lock duration */
  averageLockDuration: number;
  /** Active locks count */
  activeLocks: number;
  /** Circuit breaker metrics */
  circuitBreaker: CircuitBreakerMetrics;
}

/**
 * Lock attempt result
 */
export type LockAttemptResult =
  | { success: true; value: number; client: RedisClient }
  | { success: false; error: Error; client: RedisClient };

/**
 * Lock execution result
 */
export interface LockExecutionResult {
  /** Array of attempt statistics */
  attempts: Promise<LockStats>[];
  /** Operation start time */
  startTime: number;
  /** Whether operation succeeded */
  success: boolean;
}

/**
 * Active lock information
 */
export interface ActiveLockInfo {
  /** Lock resources */
  resources: string[];
  /** Lock identifier */
  identifier: string;
  /** Expiration time */
  expiration: number;
  /** Acquisition time */
  acquisitionTime: number;
  /** Number of extensions */
  extensions: number;
}

/**
 * Lock release result
 */
export interface LockReleaseResult {
  /** Whether release was successful */
  success: boolean;
  /** Number of clients that released the lock */
  releasedCount: number;
  /** Total number of clients attempted */
  totalClients: number;
  /** Execution statistics */
  stats: Promise<LockStats>;
}

/**
 * Lock extension result
 */
export interface LockExtensionResult {
  /** Whether extension was successful */
  success: boolean;
  /** New expiration time */
  expiration: number;
  /** Execution statistics */
  stats: Promise<LockStats>;
}

/**
 * Auto-extending lock signal
 */
export interface LockSignal {
  /** Whether the lock has been aborted */
  readonly aborted: boolean;
  /** Error that caused abortion (if any) */
  readonly error?: Error;
  /** Current lock expiration time */
  readonly expiration: number;
  /** Add abort listener */
  addEventListener(type: "abort", listener: () => void): void;
  /** Remove abort listener */
  removeEventListener(type: "abort", listener: () => void): void;
}

/**
 * Optimistic locking options
 */
export interface OptimisticLockOptions extends LockOptions {
  /** Expected version for optimistic locking */
  expectedVersion?: number;
  /** Expected value for compare-and-swap */
  expectedValue?: unknown;
  /** Conflict resolution strategy */
  conflictResolution?: "fail" | "retry" | "fallback";
  /** Maximum retries for conflict resolution */
  maxRetries?: number;
}

/**
 * Optimistic lock result
 */
export interface OptimisticLockResult {
  /** Whether operation succeeded */
  success: boolean;
  /** Current version after operation */
  currentVersion?: number;
  /** Conflict detected */
  conflict?: boolean;
  /** Number of retries performed */
  retries?: number;
}

/**
 * Hybrid locking strategy options
 */
export interface HybridLockOptions extends LockOptions, OptimisticLockOptions {
  /** Primary strategy: 'pessimistic' | 'optimistic' | 'adaptive' */
  primaryStrategy?: "pessimistic" | "optimistic" | "adaptive";
  /** Fallback strategy when primary fails */
  fallbackStrategy?: "pessimistic" | "optimistic";
  /** Concurrency threshold for adaptive switching */
  concurrencyThreshold?: number;
  /** Performance metrics window for adaptive decisions */
  metricsWindow?: number;
}

/**
 * Lock caching options
 */
export interface LockCacheOptions {
  /** Cache TTL in milliseconds */
  ttl?: number;
  /** Maximum cache size */
  maxSize?: number;
  /** Cache strategy: 'lru' | 'lfu' | 'ttl' */
  strategy?: "lru" | "lfu" | "ttl";
  /** Enable negative caching */
  negativeCaching?: boolean;
}

/**
 * Cached lock information
 */
export interface CachedLockInfo {
  /** Lock identifier */
  identifier: string;
  /** Expiration timestamp */
  expiration: number;
  /** Cache timestamp */
  cachedAt: number;
  /** Lock status */
  status: "acquired" | "expired" | "released";
}

/**
 * Interface that strategies use to interact with the toolkit.
 * Prevents strategies from depending on the concrete RedlockToolkit class
 * and accessing private members via bracket notation.
 */
export interface ILockToolkit {
  /** Default lock options */
  readonly defaultOptions: Required<LockOptions>;
  /** Active locks map */
  readonly activeLocks: Map<string, import("./lock").Lock>;
  /** Metrics collector */
  readonly metrics: {
    recordLockAcquired(identifier: string, resources: string[], latencyMs: number, ttl?: number): void;
    recordAcquisitionFailed(resources: string[], error: Error, attempts: number, durationMs: number): void;
    recordRetry(identifier: string, attempt: number): void;
  };
  /** Cache manager */
  readonly cacheManager: {
    set(identifier: string, resources: string[], ttl: number): void;
  };
  /** Generate a prefixed lock key */
  generateLockKey(resource: string): string;
  /** Generate unique lock identifier */
  generateIdentifier(): string;
  /** Execute Lua script on a single client */
  executeScript(client: RedisClient, script: import("../utils/scripts").LuaScript, keys: string[], args: (string | number)[]): Promise<unknown>;
  /** Execute operation across all clients with consensus */
  executeWithConsensus<T>(
    operation: (client: RedisClient) => Promise<T>,
    cleanupContext?: { keys: string[]; identifier: string; script?: import("../utils/scripts").LuaScript; args?: (string | number)[] },
    options?: { evaluateResult?: (result: unknown) => boolean; successPolicy?: 'quorum' | 'any' },
  ): Promise<LockExecutionResult>;
  /** Extend a lock */
  extendLock(lock: import("./lock").Lock, ttl: number, options?: Partial<LockOptions>): Promise<LockExtensionResult>;
  /** Release a lock internally */
  releaseLockInternal(lock: import("./lock").Lock, options?: Partial<LockOptions>): Promise<LockReleaseResult>;
  /** Emit an event */
  emit(event: string, ...args: unknown[]): boolean;
  /** Acquire a lock (pessimistic) */
  acquire(resources: string | string[], options?: Partial<LockOptions>): Promise<import("./lock").Lock>;
  /** Acquire an optimistic lock */
  acquireOptimistic(resources: string | string[], options?: OptimisticLockOptions): Promise<OptimisticLockResult>;
  /** Create a Lock from an optimistic result */
  createLockFromOptimistic(resources: string | string[], result: OptimisticLockResult, options: HybridLockOptions): import("./lock").Lock;
  /** Pub/sub waiter (null if not enabled) */
  readonly pubSubWaiter: import("../pubsub/pubsub-waiter").PubSubWaiter | null;
  /** Get a single Redis client for non-consensus read operations */
  getClientForRead(): RedisClient;
}

/**
 * Semaphore configuration options
 */
export interface SemaphoreOptions {
  /** Maximum number of concurrent permit holders */
  maxPermits: number;
  /** Time to live for each permit in milliseconds */
  ttl?: number;
  /** Maximum number of retry attempts when semaphore is full */
  retryCount?: number;
  /** Base delay between retries in milliseconds */
  retryDelay?: number;
  /** Maximum jitter to add to retry delay */
  retryJitter?: number;
  /** Clock drift factor */
  driftFactor?: number;
  /** Threshold for automatic permit extension in milliseconds */
  autoExtendThreshold?: number;
  /** Custom permit identifier */
  identifier?: string;
}

/**
 * Semaphore status
 */
export interface SemaphoreStatus {
  /** Resource name */
  resource: string;
  /** Number of active permits */
  activePermits: number;
  /** Maximum permits */
  maxPermits: number;
  /** Details of current holders */
  holders: Array<{ identifier: string; expiresAt: number }>;
}

/**
 * CountDownLatch options
 */
export interface CountDownLatchOptions {
  /** Number of events to count down from */
  count: number;
  /** Time to live for the latch in milliseconds (expires if not completed) */
  ttl?: number;
  /** Timeout for await in milliseconds */
  awaitTimeout?: number;
  /** Polling interval when waiting (if pub/sub not enabled) */
  pollInterval?: number;
}

/**
 * CountDownLatch status
 */
export interface CountDownLatchStatus {
  /** Whether the latch exists */
  exists: boolean;
  /** Current remaining count */
  remainingCount: number;
  /** Original target count */
  targetCount: number;
  /** Whether the latch has completed (count reached 0) */
  completed: boolean;
  /** Remaining TTL in milliseconds */
  ttl?: number;
}

/**
 * CountDown result
 */
export interface CountDownResult {
  /** Whether the countdown was successful */
  success: boolean;
  /** Remaining count after this countdown */
  remainingCount: number;
  /** Whether this countdown caused the latch to complete */
  justCompleted: boolean;
}

/**
 * Lock events
 */
export interface LockEvents {
  /** Lock acquired successfully */
  "lock:acquired": (resources: string[], identifier: string) => void;
  /** Lock released */
  "lock:released": (resources: string[], identifier: string) => void;
  /** Lock extended */
  "lock:extended": (
    resources: string[],
    identifier: string,
    newExpiration: number,
  ) => void;
  /** Lock acquisition failed */
  "lock:failed": (resources: string[], error: Error) => void;
  /** Lock expired */
  "lock:expired": (resources: string[], identifier: string) => void;
  /** Optimistic lock conflict detected */
  "lock:conflict": (
    resources: string[],
    expectedVersion: number,
    currentVersion: number,
  ) => void;
  /** Circuit breaker state changed */
  "circuit:stateChanged": (state: "closed" | "open" | "half-open") => void;
  /** General error */
  error: (error: Error) => void;
}
