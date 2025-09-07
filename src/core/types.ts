/**
 * RedlockToolkit - Advanced Redis distributed locking library
 * Combines best practices from multiple locking approaches
 */

import { Redis as IORedisClient, Cluster as IORedisCluster } from "ioredis";
import { EventEmitter } from "events";

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
  expectedValue?: any;
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
