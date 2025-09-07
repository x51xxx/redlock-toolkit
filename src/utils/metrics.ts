/**
 * Metrics Collection and Monitoring
 * Comprehensive observability for lock operations
 */

import { EventEmitter } from "events";
import { LockMetrics, CircuitBreakerMetrics, LockStats } from "../core/types";

// Re-export types that are used by tests
export type { CircuitBreakerMetrics } from "../core/types";

/**
 * Histogram bucket for latency measurements
 */
interface HistogramBucket {
  upperBound: number;
  count: number;
}

/**
 * Histogram for measuring operation latencies
 */
class Histogram {
  private buckets: HistogramBucket[];
  private sum = 0;
  private count = 0;

  constructor(
    buckets: number[] = [
      1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
    ],
  ) {
    this.buckets = buckets.map((upperBound) => ({ upperBound, count: 0 }));
  }

  /**
   * Record a measurement
   */
  observe(value: number): void {
    this.sum += value;
    this.count++;

    for (const bucket of this.buckets) {
      if (value <= bucket.upperBound) {
        bucket.count++;
      }
    }
  }

  /**
   * Get percentile value
   */
  getPercentile(percentile: number): number {
    if (this.count === 0) return 0;

    const targetCount = Math.ceil((percentile / 100) * this.count);

    for (const bucket of this.buckets) {
      if (bucket.count >= targetCount) {
        return bucket.upperBound;
      }
    }

    return this.buckets[this.buckets.length - 1].upperBound;
  }

  /**
   * Get average value
   */
  getAverage(): number {
    return this.count > 0 ? this.sum / this.count : 0;
  }

  /**
   * Get summary statistics
   */
  getSummary(): {
    count: number;
    sum: number;
    average: number;
    p50: number;
    p90: number;
    p95: number;
    p99: number;
  } {
    return {
      count: this.count,
      sum: this.sum,
      average: this.getAverage(),
      p50: this.getPercentile(50),
      p90: this.getPercentile(90),
      p95: this.getPercentile(95),
      p99: this.getPercentile(99),
    };
  }

  /**
   * Reset histogram
   */
  reset(): void {
    this.sum = 0;
    this.count = 0;
    this.buckets.forEach((bucket) => (bucket.count = 0));
  }
}

/**
 * Counter for tracking simple metrics
 */
class Counter {
  private value = 0;

  /**
   * Increment counter
   */
  inc(amount = 1): void {
    this.value += amount;
  }

  /**
   * Get current value
   */
  get(): number {
    return this.value;
  }

  /**
   * Reset counter
   */
  reset(): void {
    this.value = 0;
  }
}

/**
 * Gauge for tracking current values
 */
class Gauge {
  private value = 0;

  /**
   * Set gauge value
   */
  set(value: number): void {
    this.value = value;
  }

  /**
   * Increment gauge
   */
  inc(amount = 1): void {
    this.value += amount;
  }

  /**
   * Decrement gauge
   */
  dec(amount = 1): void {
    this.value -= amount;
  }

  /**
   * Get current value
   */
  get(): number {
    return this.value;
  }

  /**
   * Reset gauge
   */
  reset(): void {
    this.value = 0;
  }
}

/**
 * Comprehensive metrics collector for NeoLock
 */
export class MetricsCollector extends EventEmitter {
  // Counters
  private locksAcquired = new Counter();
  private locksReleased = new Counter();
  private lockExtensions = new Counter();
  private failedAcquisitions = new Counter();
  private retriesTotal = new Counter();

  // Gauges
  private activeLocks = new Gauge();

  // Histograms
  private lockDuration = new Histogram();
  private acquisitionLatency = new Histogram([
    1, 5, 10, 25, 50, 100, 250, 500, 1000,
  ]);
  private releaseLatency = new Histogram([1, 5, 10, 25, 50, 100, 250, 500]);
  private extensionLatency = new Histogram([1, 5, 10, 25, 50, 100, 250, 500]);

  // Circuit breaker metrics
  private circuitBreakerMetrics = new Map<string, CircuitBreakerMetrics>();

  // Active locks tracking
  private activeLocksMap = new Map<
    string,
    {
      resources: string[];
      acquisitionTime: number;
      extensions: number;
    }
  >();

  private startTime = Date.now();

  /**
   * Record lock acquisition
   */
  recordLockAcquired(
    identifier: string,
    resources: string[],
    acquisitionLatencyMs: number,
  ): void {
    this.locksAcquired.inc();
    this.activeLocks.inc();
    this.acquisitionLatency.observe(acquisitionLatencyMs);

    this.activeLocksMap.set(identifier, {
      resources,
      acquisitionTime: Date.now(),
      extensions: 0,
    });

    this.emit("lockAcquired", {
      identifier,
      resources,
      acquisitionLatencyMs,
      timestamp: Date.now(),
    });
  }

  /**
   * Record lock release
   */
  recordLockReleased(identifier: string, releaseLatencyMs: number): void {
    this.locksReleased.inc();
    this.activeLocks.dec();
    this.releaseLatency.observe(releaseLatencyMs);

    const lockInfo = this.activeLocksMap.get(identifier);
    if (lockInfo) {
      const duration = Date.now() - lockInfo.acquisitionTime;
      this.lockDuration.observe(duration);
      this.activeLocksMap.delete(identifier);
    }

    this.emit("lockReleased", {
      identifier,
      releaseLatencyMs,
      timestamp: Date.now(),
    });
  }

  /**
   * Record lock extension
   */
  recordLockExtended(identifier: string, extensionLatencyMs: number): void {
    this.lockExtensions.inc();
    this.extensionLatency.observe(extensionLatencyMs);

    const lockInfo = this.activeLocksMap.get(identifier);
    if (lockInfo) {
      lockInfo.extensions++;
    }

    this.emit("lockExtended", {
      identifier,
      extensionLatencyMs,
      timestamp: Date.now(),
    });
  }

  /**
   * Record failed acquisition
   */
  recordAcquisitionFailed(
    resources: string[],
    error: Error,
    attempts: number,
    totalLatencyMs: number,
  ): void {
    this.failedAcquisitions.inc();
    this.retriesTotal.inc(attempts - 1); // Subtract initial attempt

    this.emit("acquisitionFailed", {
      resources,
      error: error.message,
      attempts,
      totalLatencyMs,
      timestamp: Date.now(),
    });
  }

  /**
   * Record retry attempt
   */
  recordRetry(identifier: string, attempt: number): void {
    this.retriesTotal.inc();

    this.emit("retryAttempt", {
      identifier,
      attempt,
      timestamp: Date.now(),
    });
  }

  /**
   * Update circuit breaker metrics
   */
  updateCircuitBreakerMetrics(
    clientId: string,
    metrics: CircuitBreakerMetrics,
  ): void {
    this.circuitBreakerMetrics.set(clientId, { ...metrics });

    this.emit("circuitBreakerUpdate", {
      clientId,
      metrics,
      timestamp: Date.now(),
    });
  }

  /**
   * Get comprehensive metrics
   */
  getMetrics(): LockMetrics & {
    detailed: {
      lockDurationStats: ReturnType<Histogram["getSummary"]>;
      acquisitionLatencyStats: ReturnType<Histogram["getSummary"]>;
      releaseLatencyStats: ReturnType<Histogram["getSummary"]>;
      extensionLatencyStats: ReturnType<Histogram["getSummary"]>;
      uptimeMs: number;
      activeLockDetails: Array<{
        identifier: string;
        resources: string[];
        durationMs: number;
        extensions: number;
      }>;
    };
  } {
    const now = Date.now();
    const activeLockDetails = Array.from(this.activeLocksMap.entries()).map(
      ([identifier, info]) => ({
        identifier,
        resources: info.resources,
        durationMs: now - info.acquisitionTime,
        extensions: info.extensions,
      }),
    );

    // Aggregate circuit breaker metrics
    const aggregatedCircuitBreaker: CircuitBreakerMetrics = {
      state: "closed", // Will be overridden if any are open
      totalOperations: 0,
      successfulOperations: 0,
      failedOperations: 0,
    };

    for (const metrics of this.circuitBreakerMetrics.values()) {
      aggregatedCircuitBreaker.totalOperations += metrics.totalOperations;
      aggregatedCircuitBreaker.successfulOperations +=
        metrics.successfulOperations;
      aggregatedCircuitBreaker.failedOperations += metrics.failedOperations;

      // If any circuit is open, overall state is open
      if (metrics.state === "open") {
        aggregatedCircuitBreaker.state = "open";
      } else if (
        metrics.state === "half-open" &&
        aggregatedCircuitBreaker.state !== "open"
      ) {
        aggregatedCircuitBreaker.state = "half-open";
      }

      // Track most recent failure
      if (
        metrics.lastFailureTime &&
        (!aggregatedCircuitBreaker.lastFailureTime ||
          metrics.lastFailureTime > aggregatedCircuitBreaker.lastFailureTime)
      ) {
        aggregatedCircuitBreaker.lastFailureTime = metrics.lastFailureTime;
      }
    }

    return {
      locksAcquired: this.locksAcquired.get(),
      locksReleased: this.locksReleased.get(),
      lockExtensions: this.lockExtensions.get(),
      failedAcquisitions: this.failedAcquisitions.get(),
      averageLockDuration: this.lockDuration.getAverage(),
      activeLocks: this.activeLocks.get(),
      circuitBreaker: aggregatedCircuitBreaker,
      detailed: {
        lockDurationStats: this.lockDuration.getSummary(),
        acquisitionLatencyStats: this.acquisitionLatency.getSummary(),
        releaseLatencyStats: this.releaseLatency.getSummary(),
        extensionLatencyStats: this.extensionLatency.getSummary(),
        uptimeMs: now - this.startTime,
        activeLockDetails,
      },
    };
  }

  /**
   * Get circuit breaker metrics for specific client
   */
  getCircuitBreakerMetrics(
    clientId: string,
  ): CircuitBreakerMetrics | undefined {
    return this.circuitBreakerMetrics.get(clientId);
  }

  /**
   * Get all circuit breaker metrics
   */
  getAllCircuitBreakerMetrics(): Map<string, CircuitBreakerMetrics> {
    return new Map(this.circuitBreakerMetrics);
  }

  /**
   * Get performance summary
   */
  getPerformanceSummary(): {
    successRate: number;
    averageAcquisitionLatency: number;
    averageReleaseLatency: number;
    averageLockDuration: number;
    retryRate: number;
    circuitBreakerHealth: {
      overallState: "healthy" | "degraded" | "unhealthy";
      openCircuits: number;
      totalCircuits: number;
    };
  } {
    const totalAttempts =
      this.locksAcquired.get() + this.failedAcquisitions.get();
    const successRate =
      totalAttempts > 0 ? this.locksAcquired.get() / totalAttempts : 1;
    const retryRate =
      totalAttempts > 0 ? this.retriesTotal.get() / totalAttempts : 0;

    let openCircuits = 0;
    for (const metrics of this.circuitBreakerMetrics.values()) {
      if (metrics.state === "open") openCircuits++;
    }

    const totalCircuits = this.circuitBreakerMetrics.size;
    let circuitHealth: "healthy" | "degraded" | "unhealthy" = "healthy";

    if (openCircuits > 0) {
      if (openCircuits >= totalCircuits / 2) {
        circuitHealth = "unhealthy";
      } else {
        circuitHealth = "degraded";
      }
    }

    return {
      successRate,
      averageAcquisitionLatency: this.acquisitionLatency.getAverage(),
      averageReleaseLatency: this.releaseLatency.getAverage(),
      averageLockDuration: this.lockDuration.getAverage(),
      retryRate,
      circuitBreakerHealth: {
        overallState: circuitHealth,
        openCircuits,
        totalCircuits,
      },
    };
  }

  /**
   * Reset all metrics
   */
  reset(): void {
    // Reset counters
    this.locksAcquired.reset();
    this.locksReleased.reset();
    this.lockExtensions.reset();
    this.failedAcquisitions.reset();
    this.retriesTotal.reset();

    // Reset gauges
    this.activeLocks.reset();

    // Reset histograms
    this.lockDuration.reset();
    this.acquisitionLatency.reset();
    this.releaseLatency.reset();
    this.extensionLatency.reset();

    // Clear maps
    this.circuitBreakerMetrics.clear();
    this.activeLocksMap.clear();

    this.startTime = Date.now();

    this.emit("metricsReset", { timestamp: Date.now() });
  }

  /**
   * Export metrics in Prometheus format
   */
  exportPrometheusMetrics(): string {
    const metrics = this.getMetrics();
    const lines: string[] = [];

    // Add help and type information
    lines.push(
      "# HELP neolock_locks_acquired_total Total number of locks acquired",
    );
    lines.push("# TYPE neolock_locks_acquired_total counter");
    lines.push(`neolock_locks_acquired_total ${metrics.locksAcquired}`);

    lines.push(
      "# HELP neolock_locks_released_total Total number of locks released",
    );
    lines.push("# TYPE neolock_locks_released_total counter");
    lines.push(`neolock_locks_released_total ${metrics.locksReleased}`);

    lines.push(
      "# HELP neolock_lock_extensions_total Total number of lock extensions",
    );
    lines.push("# TYPE neolock_lock_extensions_total counter");
    lines.push(`neolock_lock_extensions_total ${metrics.lockExtensions}`);

    lines.push(
      "# HELP neolock_failed_acquisitions_total Total number of failed acquisitions",
    );
    lines.push("# TYPE neolock_failed_acquisitions_total counter");
    lines.push(
      `neolock_failed_acquisitions_total ${metrics.failedAcquisitions}`,
    );

    lines.push(
      "# HELP neolock_active_locks_current Current number of active locks",
    );
    lines.push("# TYPE neolock_active_locks_current gauge");
    lines.push(`neolock_active_locks_current ${metrics.activeLocks}`);

    lines.push(
      "# HELP neolock_lock_duration_average_ms Average lock duration in milliseconds",
    );
    lines.push("# TYPE neolock_lock_duration_average_ms gauge");
    lines.push(
      `neolock_lock_duration_average_ms ${metrics.averageLockDuration}`,
    );

    return lines.join("\n") + "\n";
  }
}
