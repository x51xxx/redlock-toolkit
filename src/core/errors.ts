/**
 * RedlockToolkit Error Classes
 * Comprehensive error handling with detailed context
 */

import { LockStats } from "./types";

/**
 * Base class for all RedlockToolkit errors
 */
export abstract class RedlockToolkitError extends Error {
  public readonly timestamp: number;
  public readonly context?: Record<string, any>;

  constructor(message: string, context?: Record<string, any>) {
    super(message);
    this.name = this.constructor.name;
    this.timestamp = Date.now();
    this.context = context;

    // Maintain proper stack trace
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, this.constructor);
    }
  }

  /**
   * Get error details as JSON
   */
  toJSON(): Record<string, any> {
    return {
      name: this.name,
      message: this.message,
      timestamp: this.timestamp,
      context: this.context,
      stack: this.stack,
    };
  }
}

/**
 * Thrown when a resource is already locked
 */
export class ResourceLockedError extends RedlockToolkitError {
  public readonly resources: string[];
  public readonly currentHolder?: string;

  constructor(
    resources: string[],
    currentHolder?: string,
    context?: Record<string, any>,
  ) {
    const resourceList = resources.join(", ");
    const holderInfo = currentHolder ? ` (held by: ${currentHolder})` : "";
    super(`Resources already locked: ${resourceList}${holderInfo}`, context);

    this.resources = resources;
    this.currentHolder = currentHolder;
  }
}

/**
 * Thrown when unable to acquire lock due to consensus failure
 */
export class ConsensusError extends RedlockToolkitError {
  public readonly attempts: Promise<LockStats>[];
  public readonly requiredQuorum: number;
  public readonly achievedVotes: number;

  constructor(
    message: string,
    attempts: Promise<LockStats>[],
    requiredQuorum: number,
    achievedVotes: number,
    context?: Record<string, any>,
  ) {
    super(message, context);
    this.attempts = attempts;
    this.requiredQuorum = requiredQuorum;
    this.achievedVotes = achievedVotes;
  }
}

/**
 * Thrown when lock acquisition times out
 */
export class LockTimeoutError extends RedlockToolkitError {
  public readonly timeoutMs: number;
  public readonly attemptsCount: number;

  constructor(
    timeoutMs: number,
    attemptsCount: number,
    context?: Record<string, any>,
  ) {
    super(
      `Lock acquisition timed out after ${timeoutMs}ms (${attemptsCount} attempts)`,
      context,
    );
    this.timeoutMs = timeoutMs;
    this.attemptsCount = attemptsCount;
  }
}

/**
 * Thrown when trying to operate on an expired lock
 */
export class LockExpiredError extends RedlockToolkitError {
  public readonly expiration: number;
  public readonly currentTime: number;

  constructor(expiration: number, context?: Record<string, any>) {
    const currentTime = Date.now();
    super(
      `Lock expired at ${new Date(expiration).toISOString()} (current: ${new Date(currentTime).toISOString()})`,
      context,
    );
    this.expiration = expiration;
    this.currentTime = currentTime;
  }
}

/**
 * Thrown when lock release fails
 */
export class LockReleaseError extends RedlockToolkitError {
  public readonly resources: string[];
  public readonly identifier: string;
  public readonly releasedCount: number;
  public readonly totalClients: number;

  constructor(
    resources: string[],
    identifier: string,
    releasedCount: number,
    totalClients: number,
    context?: Record<string, any>,
  ) {
    super(
      `Failed to release lock on ${resources.join(", ")} (released: ${releasedCount}/${totalClients})`,
      context,
    );
    this.resources = resources;
    this.identifier = identifier;
    this.releasedCount = releasedCount;
    this.totalClients = totalClients;
  }
}

/**
 * Thrown when lock extension fails
 */
export class LockExtensionError extends RedlockToolkitError {
  public readonly resources: string[];
  public readonly identifier: string;
  public readonly currentExpiration: number;

  constructor(
    resources: string[],
    identifier: string,
    currentExpiration: number,
    context?: Record<string, any>,
  ) {
    super(`Failed to extend lock on ${resources.join(", ")}`, context);
    this.resources = resources;
    this.identifier = identifier;
    this.currentExpiration = currentExpiration;
  }
}

/**
 * Thrown when circuit breaker is open
 */
export class CircuitBreakerOpenError extends RedlockToolkitError {
  public readonly lastFailureTime: number;
  public readonly failureCount: number;

  constructor(
    lastFailureTime: number,
    failureCount: number,
    context?: Record<string, any>,
  ) {
    super(
      `Circuit breaker is open (last failure: ${new Date(lastFailureTime).toISOString()}, failures: ${failureCount})`,
      context,
    );
    this.lastFailureTime = lastFailureTime;
    this.failureCount = failureCount;
  }
}

/**
 * Thrown when Redis operation fails
 */
export class RedisOperationError extends RedlockToolkitError {
  public readonly operation: string;
  public readonly client: string;
  public readonly originalError: Error;

  constructor(
    operation: string,
    client: string,
    originalError: Error,
    context?: Record<string, any>,
  ) {
    super(
      `Redis operation '${operation}' failed on client ${client}: ${originalError.message}`,
      context,
    );
    this.operation = operation;
    this.client = client;
    this.originalError = originalError;
  }
}

/**
 * Thrown when configuration is invalid
 */
export class ConfigurationError extends RedlockToolkitError {
  public readonly parameter: string;
  public readonly value: any;

  constructor(
    parameter: string,
    value: any,
    reason: string,
    context?: Record<string, any>,
  ) {
    super(
      `Invalid configuration for '${parameter}': ${reason} (value: ${JSON.stringify(value)})`,
      context,
    );
    this.parameter = parameter;
    this.value = value;
  }
}

/**
 * Thrown when lock validation fails
 */
export class LockValidationError extends RedlockToolkitError {
  public readonly validationType: string;
  public readonly expected: any;
  public readonly actual: any;

  constructor(
    validationType: string,
    expected: any,
    actual: any,
    context?: Record<string, any>,
  ) {
    super(
      `Lock validation failed for '${validationType}': expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`,
      context,
    );
    this.validationType = validationType;
    this.expected = expected;
    this.actual = actual;
  }
}

/**
 * Thrown when optimistic locking detects a conflict
 */
export class OptimisticLockConflictError extends RedlockToolkitError {
  public readonly resources: string[];
  public readonly expectedVersion: number;
  public readonly currentVersion: number;
  public readonly conflictType: "version" | "value" | "locked";

  constructor(
    resources: string[],
    expectedVersion: number,
    currentVersion: number,
    conflictType: "version" | "value" | "locked",
    context?: Record<string, any>,
  ) {
    const resourceList = resources.join(", ");
    super(
      `Optimistic lock conflict on ${resourceList}: expected version ${expectedVersion}, got ${currentVersion} (${conflictType})`,
      context,
    );
    this.resources = resources;
    this.expectedVersion = expectedVersion;
    this.currentVersion = currentVersion;
    this.conflictType = conflictType;
  }
}

/**
 * Thrown when hybrid locking strategy fails
 */
export class HybridLockError extends RedlockToolkitError {
  public readonly primaryStrategy: string;
  public readonly fallbackStrategy: string;
  public readonly primaryError: Error;
  public readonly fallbackError?: Error;

  constructor(
    primaryStrategy: string,
    fallbackStrategy: string,
    primaryError: Error,
    fallbackError?: Error,
    context?: Record<string, any>,
  ) {
    const message = fallbackError
      ? `Hybrid locking failed: ${primaryStrategy} (${primaryError.message}) and ${fallbackStrategy} (${fallbackError.message})`
      : `Hybrid locking failed: ${primaryStrategy} (${primaryError.message}), fallback ${fallbackStrategy} not attempted`;

    super(message, context);
    this.primaryStrategy = primaryStrategy;
    this.fallbackStrategy = fallbackStrategy;
    this.primaryError = primaryError;
    this.fallbackError = fallbackError;
  }
}

/**
 * Utility function to determine if an error is retryable
 */
export function isRetryableError(error: Error): boolean {
  // Circuit breaker open - not retryable until circuit resets
  if (error instanceof CircuitBreakerOpenError) {
    return false;
  }

  // Configuration errors - not retryable
  if (error instanceof ConfigurationError) {
    return false;
  }

  // Lock validation errors - not retryable
  if (error instanceof LockValidationError) {
    return false;
  }

  // Optimistic lock conflicts - retryable with backoff
  if (error instanceof OptimisticLockConflictError) {
    return true;
  }

  // Hybrid lock errors - depends on the underlying errors
  if (error instanceof HybridLockError) {
    // Retry if either primary or fallback error is retryable
    return (
      isRetryableError(error.primaryError) ||
      (error.fallbackError ? isRetryableError(error.fallbackError) : false)
    );
  }

  // Resource locked - retryable with backoff
  if (error instanceof ResourceLockedError) {
    return true;
  }

  // Consensus errors - retryable
  if (error instanceof ConsensusError) {
    return true;
  }

  // Redis operation errors - might be retryable
  if (error instanceof RedisOperationError) {
    // Network errors are typically retryable
    return (
      error.originalError.message.includes("connect") ||
      error.originalError.message.includes("timeout") ||
      error.originalError.message.includes("network")
    );
  }

  // Other errors - generally retryable
  return true;
}

/**
 * Utility function to get retry delay based on error type
 */
export function getRetryDelay(
  error: Error,
  baseDelay: number,
  jitter: number,
): number {
  let multiplier = 1;

  if (error instanceof ResourceLockedError) {
    // Longer delay for resource conflicts
    multiplier = 2;
  } else if (error instanceof ConsensusError) {
    // Moderate delay for consensus failures
    multiplier = 1.5;
  } else if (error instanceof OptimisticLockConflictError) {
    // Shorter delay for optimistic conflicts (they resolve quickly)
    multiplier = 0.5;
  } else if (error instanceof HybridLockError) {
    // Use the delay from the primary error
    return getRetryDelay(error.primaryError, baseDelay, jitter);
  }

  const delay = baseDelay * multiplier;
  // Apply jitter as ±jitter amount around the calculated delay
  const jitterAmount = (Math.random() - 0.5) * 2 * jitter;

  // Ensure delay is never negative
  return Math.max(1, Math.floor(delay + jitterAmount));
}
