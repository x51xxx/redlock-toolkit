/**
 * Circuit Breaker Implementation
 * Protects against cascading failures and provides fault tolerance
 */

import { CircuitBreakerOptions, CircuitBreakerMetrics } from "../core/types";
import { CircuitBreakerOpenError } from "../core/errors";
import { EventEmitter } from "events";

export type CircuitState = "closed" | "open" | "half-open";

/**
 * Circuit Breaker for protecting Redis operations
 */
export class CircuitBreaker extends EventEmitter {
  private state: CircuitState = "closed";
  private failureCount = 0;
  private lastFailureTime?: number;
  private lastOpenTime?: number;
  private successCount = 0;
  private totalOperations = 0;

  private readonly options: Required<CircuitBreakerOptions>;

  constructor(options: CircuitBreakerOptions = {}) {
    super();

    this.options = {
      failureThreshold: options.failureThreshold ?? 5,
      resetTimeout: options.resetTimeout ?? 60000, // 1 minute
      maxRetries: options.maxRetries ?? 3,
      operationTimeout: options.operationTimeout ?? 5000, // 5 seconds
    };

    this.validateOptions();
  }

  /**
   * Validate circuit breaker configuration
   */
  private validateOptions(): void {
    if (this.options.failureThreshold <= 0) {
      throw new Error("failureThreshold must be greater than 0");
    }
    if (this.options.resetTimeout <= 0) {
      throw new Error("resetTimeout must be greater than 0");
    }
    if (this.options.maxRetries < 0) {
      throw new Error("maxRetries must be non-negative");
    }
    if (this.options.operationTimeout <= 0) {
      throw new Error("operationTimeout must be greater than 0");
    }
  }

  /**
   * Execute operation with circuit breaker protection
   */
  async execute<T>(operation: () => Promise<T>): Promise<T> {
    this.checkCircuitState();

    if (this.state === "open") {
      throw new CircuitBreakerOpenError(
        this.lastFailureTime!,
        this.failureCount,
        { state: this.state },
      );
    }

    this.totalOperations++;

    try {
      const result = await this.executeWithTimeout(operation);
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure(error as Error);
      throw error;
    }
  }

  /**
   * Execute operation with timeout
   */
  private async executeWithTimeout<T>(operation: () => Promise<T>): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      let settled = false;

      const timeoutId = setTimeout(() => {
        if (settled) return;
        settled = true;
        const timeoutError = new Error(
          `Operation timed out after ${this.options.operationTimeout}ms`,
        );
        // Mark this as a timeout error for proper handling
        (timeoutError as { isTimeout?: boolean }).isTimeout = true;
        reject(timeoutError);
      }, this.options.operationTimeout);

      operation()
        .then((result) => {
          if (settled) return;
          settled = true;
          clearTimeout(timeoutId);
          resolve(result);
        })
        .catch((error) => {
          if (settled) return;
          settled = true;
          clearTimeout(timeoutId);
          reject(error);
        });
    });
  }

  /**
   * Handle successful operation
   */
  private onSuccess(): void {
    this.successCount++;

    if (this.state === "half-open") {
      // Reset circuit after successful operation in half-open state
      this.reset();
    }
  }

  /**
   * Handle failed operation
   */
  private onFailure(error: Error): void {
    this.failureCount++;
    this.lastFailureTime = Date.now();

    this.emit("failure", error, this.getMetrics());

    if (
      this.state === "closed" &&
      this.failureCount >= this.options.failureThreshold
    ) {
      this.openCircuit();
    } else if (this.state === "half-open") {
      // Failure in half-open state - go back to open
      this.openCircuit();
    }
  }

  /**
   * Open the circuit
   */
  private openCircuit(): void {
    const previousState = this.state;
    this.state = "open";
    this.lastOpenTime = Date.now();

    this.emit("stateChanged", this.state, previousState, this.getMetrics());
  }

  /**
   * Reset the circuit to closed state
   */
  private reset(): void {
    const previousState = this.state;
    this.state = "closed";
    this.failureCount = 0;
    this.successCount = 0;
    this.totalOperations = 0;
    this.lastFailureTime = undefined;
    this.lastOpenTime = undefined;

    this.emit("stateChanged", this.state, previousState, this.getMetrics());
  }

  /**
   * Check if circuit should transition to half-open
   */
  private checkCircuitState(): void {
    if (this.state === "open" && this.shouldAttemptReset()) {
      const previousState = this.state;
      this.state = "half-open";
      this.emit("stateChanged", this.state, previousState, this.getMetrics());
    }
  }

  /**
   * Check if enough time has passed to attempt reset
   */
  private shouldAttemptReset(): boolean {
    if (!this.lastOpenTime) return false;
    return Date.now() - this.lastOpenTime >= this.options.resetTimeout;
  }

  /**
   * Get current circuit breaker metrics
   */
  getMetrics(): CircuitBreakerMetrics {
    return {
      state: this.state,
      totalOperations: this.totalOperations,
      successfulOperations: this.successCount,
      failedOperations: this.failureCount,
      lastFailureTime: this.lastFailureTime,
      lastOpenTime: this.lastOpenTime,
    };
  }

  /**
   * Get current state
   */
  getState(): CircuitState {
    return this.state;
  }

  /**
   * Check if circuit is currently allowing operations
   */
  isOperationAllowed(): boolean {
    this.checkCircuitState();
    return this.state !== "open";
  }

  /**
   * Manually reset the circuit breaker
   */
  manualReset(): void {
    this.reset();
  }

  /**
   * Get health status
   */
  getHealthStatus(): {
    healthy: boolean;
    state: CircuitState;
    failureRate: number;
    timeSinceLastFailure?: number;
  } {
    const failureRate =
      this.totalOperations > 0 ? this.failureCount / this.totalOperations : 0;

    const timeSinceLastFailure = this.lastFailureTime
      ? Date.now() - this.lastFailureTime
      : undefined;

    return {
      healthy: this.state === "closed" && failureRate < 0.5,
      state: this.state,
      failureRate,
      timeSinceLastFailure,
    };
  }
}

/**
 * Circuit Breaker Manager for multiple Redis clients
 */
export class CircuitBreakerManager {
  private breakers = new Map<string, CircuitBreaker>();
  private defaultOptions: CircuitBreakerOptions;

  constructor(defaultOptions: CircuitBreakerOptions = {}) {
    this.defaultOptions = defaultOptions;
  }

  /**
   * Get or create circuit breaker for client
   */
  getBreaker(
    clientId: string,
    options?: CircuitBreakerOptions,
  ): CircuitBreaker {
    if (!this.breakers.has(clientId)) {
      const breakerOptions = { ...this.defaultOptions, ...options };
      const breaker = new CircuitBreaker(breakerOptions);

      // Forward events with client identification
      breaker.on("stateChanged", (newState, oldState, metrics) => {
        this.emit("stateChanged", clientId, newState, oldState, metrics);
      });

      breaker.on("failure", (error, metrics) => {
        this.emit("failure", clientId, error, metrics);
      });

      this.breakers.set(clientId, breaker);
    }

    return this.breakers.get(clientId)!;
  }

  /**
   * Execute operation with circuit breaker for specific client
   */
  async execute<T>(
    clientId: string,
    operation: () => Promise<T>,
    options?: CircuitBreakerOptions,
  ): Promise<T> {
    const breaker = this.getBreaker(clientId, options);
    return breaker.execute(operation);
  }

  /**
   * Get metrics for all circuit breakers
   */
  getAllMetrics(): Map<string, CircuitBreakerMetrics> {
    const metrics = new Map<string, CircuitBreakerMetrics>();

    for (const [clientId, breaker] of this.breakers) {
      metrics.set(clientId, breaker.getMetrics());
    }

    return metrics;
  }

  /**
   * Get health status for all circuit breakers
   */
  getOverallHealthStatus(): {
    healthy: boolean;
    totalClients: number;
    healthyClients: number;
    details: Map<string, ReturnType<CircuitBreaker["getHealthStatus"]>>;
  } {
    const details = new Map();
    let healthyClients = 0;

    for (const [clientId, breaker] of this.breakers) {
      const health = breaker.getHealthStatus();
      details.set(clientId, health);

      if (health.healthy) {
        healthyClients++;
      }
    }

    const totalClients = this.breakers.size;
    const healthy = totalClients > 0 && healthyClients > totalClients / 2;

    return {
      healthy,
      totalClients,
      healthyClients,
      details,
    };
  }

  /**
   * Reset all circuit breakers
   */
  resetAll(): void {
    for (const breaker of this.breakers.values()) {
      breaker.manualReset();
    }
  }

  /**
   * Remove circuit breaker for client
   */
  removeBreaker(clientId: string): void {
    const breaker = this.breakers.get(clientId);
    if (breaker) {
      breaker.removeAllListeners();
      this.breakers.delete(clientId);
    }
  }

  /**
   * Clear all circuit breakers
   */
  clear(): void {
    for (const breaker of this.breakers.values()) {
      breaker.removeAllListeners();
    }
    this.breakers.clear();
  }

  // EventEmitter methods
  private listeners = new Map<string, ((...args: unknown[]) => void)[]>();

  on(event: string, listener: (...args: unknown[]) => void): this {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, []);
    }
    this.listeners.get(event)!.push(listener);
    return this;
  }

  emit(event: string, ...args: unknown[]): boolean {
    const eventListeners = this.listeners.get(event);
    if (eventListeners) {
      eventListeners.forEach((listener) => listener(...args));
      return true;
    }
    return false;
  }
}
