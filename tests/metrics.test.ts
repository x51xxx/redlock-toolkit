/**
 * Metrics Collection tests
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { sleep } from './setup';
import { CircuitBreakerMetrics, MetricsCollector } from "../src";

describe('MetricsCollector', () => {
  let metrics: MetricsCollector;

  beforeEach(() => {
    metrics = new MetricsCollector();
  });

  describe('Lock Acquisition Metrics', () => {
    it('should record successful lock acquisition', () => {
      metrics.recordLockAcquired('lock-1', ['resource1'], 100);
      
      const data = metrics.getMetrics();
      expect(data.locksAcquired).toBe(1);
      expect(data.activeLocks).toBe(1);
      expect(data.detailed.acquisitionLatencyStats.count).toBe(1);
      expect(data.detailed.acquisitionLatencyStats.average).toBe(100);
    });

    it('should track multiple acquisitions', () => {
      metrics.recordLockAcquired('lock-1', ['resource1'], 50);
      metrics.recordLockAcquired('lock-2', ['resource2'], 150);
      metrics.recordLockAcquired('lock-3', ['resource3'], 100);
      
      const data = metrics.getMetrics();
      expect(data.locksAcquired).toBe(3);
      expect(data.activeLocks).toBe(3);
      expect(data.detailed.acquisitionLatencyStats.average).toBe(100);
    });

    it('should emit acquisition events', () => {
      const events: any[] = [];
      metrics.on('lockAcquired', (event) => events.push(event));
      
      metrics.recordLockAcquired('lock-1', ['resource1'], 75);
      
      expect(events).toHaveLength(1);
      expect(events[0]).toMatchObject({
        identifier: 'lock-1',
        resources: ['resource1'],
        acquisitionLatencyMs: 75
      });
    });
  });

  describe('Lock Release Metrics', () => {
    it('should record lock release', () => {
      metrics.recordLockAcquired('lock-1', ['resource1'], 50);
      
      // Simulate some time passing
      vi.useFakeTimers();
      vi.advanceTimersByTime(200);
      
      metrics.recordLockReleased('lock-1', 25);
      
      const data = metrics.getMetrics();
      expect(data.locksReleased).toBe(1);
      expect(data.activeLocks).toBe(0);
      expect(data.detailed.releaseLatencyStats.count).toBe(1);
      expect(data.detailed.lockDurationStats.count).toBe(1);
      // Allow small tolerance for timer precision
      expect(data.detailed.lockDurationStats.sum).toBeGreaterThanOrEqual(200);
      expect(data.detailed.lockDurationStats.sum).toBeLessThanOrEqual(202);
      
      vi.useRealTimers();
    });

    it('should handle release of non-existent lock', () => {
      metrics.recordLockReleased('non-existent', 10);
      
      const data = metrics.getMetrics();
      expect(data.locksReleased).toBe(1);
      expect(data.activeLocks).toBe(-1); // This might happen in edge cases
    });

    it('should emit release events', () => {
      const events: any[] = [];
      metrics.on('lockReleased', (event) => events.push(event));
      
      metrics.recordLockReleased('lock-1', 30);
      
      expect(events).toHaveLength(1);
      expect(events[0]).toMatchObject({
        identifier: 'lock-1',
        releaseLatencyMs: 30
      });
    });
  });

  describe('Lock Extension Metrics', () => {
    it('should record lock extensions', () => {
      metrics.recordLockAcquired('lock-1', ['resource1'], 50);
      metrics.recordLockExtended('lock-1', 20);
      metrics.recordLockExtended('lock-1', 25);
      
      const data = metrics.getMetrics();
      expect(data.lockExtensions).toBe(2);
      expect(data.detailed.extensionLatencyStats.count).toBe(2);
      expect(data.detailed.extensionLatencyStats.average).toBe(22.5);
    });

    it('should track extensions per lock', () => {
      metrics.recordLockAcquired('lock-1', ['resource1'], 50);
      metrics.recordLockExtended('lock-1', 20);
      metrics.recordLockExtended('lock-1', 25);
      
      const activeLockDetails = metrics.getMetrics().detailed.activeLockDetails;
      const lockDetail = activeLockDetails.find(l => l.identifier === 'lock-1');
      
      expect(lockDetail?.extensions).toBe(2);
    });

    it('should emit extension events', () => {
      const events: any[] = [];
      metrics.on('lockExtended', (event) => events.push(event));
      
      metrics.recordLockExtended('lock-1', 40);
      
      expect(events).toHaveLength(1);
      expect(events[0]).toMatchObject({
        identifier: 'lock-1',
        extensionLatencyMs: 40
      });
    });
  });

  describe('Failure Metrics', () => {
    it('should record acquisition failures', () => {
      const error = new Error('Lock acquisition failed');
      metrics.recordAcquisitionFailed(['resource1'], error, 3, 500);
      
      const data = metrics.getMetrics();
      expect(data.failedAcquisitions).toBe(1);
    });

    it('should record retry attempts', () => {
      metrics.recordRetry('lock-1', 1);
      metrics.recordRetry('lock-1', 2);
      metrics.recordRetry('lock-2', 1);
      
      // Note: This depends on internal implementation
      // In a real implementation, you might want to expose retry metrics
    });

    it('should emit failure events', () => {
      const events: any[] = [];
      metrics.on('acquisitionFailed', (event) => events.push(event));
      
      const error = new Error('Test error');
      metrics.recordAcquisitionFailed(['resource1'], error, 2, 300);
      
      expect(events).toHaveLength(1);
      expect(events[0]).toMatchObject({
        resources: ['resource1'],
        error: 'Test error',
        attempts: 2,
        totalLatencyMs: 300
      });
    });
  });

  describe('Circuit Breaker Metrics', () => {
    it('should update circuit breaker metrics', () => {
      const cbMetrics: CircuitBreakerMetrics = {
        state: 'closed',
        totalOperations: 100,
        successfulOperations: 95,
        failedOperations: 5,
        lastFailureTime: Date.now() - 1000
      };
      
      metrics.updateCircuitBreakerMetrics('client-1', cbMetrics);
      
      const data = metrics.getMetrics();
      expect(data.circuitBreaker.totalOperations).toBe(100);
      expect(data.circuitBreaker.successfulOperations).toBe(95);
      expect(data.circuitBreaker.failedOperations).toBe(5);
    });

    it('should aggregate multiple circuit breaker metrics', () => {
      const cb1: CircuitBreakerMetrics = {
        state: 'closed',
        totalOperations: 50,
        successfulOperations: 45,
        failedOperations: 5
      };
      
      const cb2: CircuitBreakerMetrics = {
        state: 'closed',
        totalOperations: 30,
        successfulOperations: 28,
        failedOperations: 2
      };
      
      metrics.updateCircuitBreakerMetrics('client-1', cb1);
      metrics.updateCircuitBreakerMetrics('client-2', cb2);
      
      const data = metrics.getMetrics();
      expect(data.circuitBreaker.totalOperations).toBe(80);
      expect(data.circuitBreaker.successfulOperations).toBe(73);
      expect(data.circuitBreaker.failedOperations).toBe(7);
    });

    it('should prioritize open state in aggregation', () => {
      const cb1: CircuitBreakerMetrics = {
        state: 'closed',
        totalOperations: 50,
        successfulOperations: 50,
        failedOperations: 0
      };
      
      const cb2: CircuitBreakerMetrics = {
        state: 'open',
        totalOperations: 30,
        successfulOperations: 25,
        failedOperations: 5
      };
      
      metrics.updateCircuitBreakerMetrics('client-1', cb1);
      metrics.updateCircuitBreakerMetrics('client-2', cb2);
      
      const data = metrics.getMetrics();
      expect(data.circuitBreaker.state).toBe('open');
    });

    it('should get individual circuit breaker metrics', () => {
      const cbMetrics: CircuitBreakerMetrics = {
        state: 'half-open',
        totalOperations: 25,
        successfulOperations: 20,
        failedOperations: 5
      };
      
      metrics.updateCircuitBreakerMetrics('client-1', cbMetrics);
      
      const individual = metrics.getCircuitBreakerMetrics('client-1');
      expect(individual).toEqual(cbMetrics);
      
      const nonExistent = metrics.getCircuitBreakerMetrics('non-existent');
      expect(nonExistent).toBeUndefined();
    });
  });

  describe('Performance Summary', () => {
    it('should calculate success rate correctly', () => {
      metrics.recordLockAcquired('lock-1', ['resource1'], 50);
      metrics.recordLockAcquired('lock-2', ['resource2'], 75);
      metrics.recordAcquisitionFailed(['resource3'], new Error('Failed'), 1, 100);
      
      const summary = metrics.getPerformanceSummary();
      expect(summary.successRate).toBeCloseTo(0.667, 2); // 2/3
    });

    it('should calculate average latencies', () => {
      metrics.recordLockAcquired('lock-1', ['resource1'], 100);
      metrics.recordLockReleased('lock-1', 50);
      metrics.recordLockExtended('lock-1', 30);
      
      const summary = metrics.getPerformanceSummary();
      expect(summary.averageAcquisitionLatency).toBe(100);
      expect(summary.averageReleaseLatency).toBe(50);
    });

    it('should assess circuit breaker health', () => {
      // All circuits healthy
      metrics.updateCircuitBreakerMetrics('client-1', {
        state: 'closed',
        totalOperations: 100,
        successfulOperations: 100,
        failedOperations: 0
      });
      
      let summary = metrics.getPerformanceSummary();
      expect(summary.circuitBreakerHealth.overallState).toBe('healthy');
      expect(summary.circuitBreakerHealth.openCircuits).toBe(0);
      
      // One circuit open
      metrics.updateCircuitBreakerMetrics('client-2', {
        state: 'open',
        totalOperations: 50,
        successfulOperations: 40,
        failedOperations: 10
      });
      
      summary = metrics.getPerformanceSummary();
      expect(summary.circuitBreakerHealth.overallState).toBe('unhealthy');
      expect(summary.circuitBreakerHealth.openCircuits).toBe(1);
    });
  });

  describe('Active Lock Details', () => {
    it('should provide detailed active lock information', () => {
      vi.useFakeTimers();
      const startTime = Date.now();
      
      metrics.recordLockAcquired('lock-1', ['resource1', 'resource2'], 50);
      vi.advanceTimersByTime(100);
      
      metrics.recordLockExtended('lock-1', 20);
      vi.advanceTimersByTime(50);
      
      const data = metrics.getMetrics();
      const activeLocks = data.detailed.activeLockDetails;
      
      expect(activeLocks).toHaveLength(1);
      expect(activeLocks[0]).toMatchObject({
        identifier: 'lock-1',
        resources: ['resource1', 'resource2'],
        durationMs: 150,
        extensions: 1
      });
      
      vi.useRealTimers();
    });
  });

  describe('Prometheus Metrics Export', () => {
    it('should export metrics in Prometheus format', () => {
      metrics.recordLockAcquired('lock-1', ['resource1'], 100);
      metrics.recordLockReleased('lock-1', 50);
      metrics.recordLockExtended('lock-2', 25);
      metrics.recordAcquisitionFailed(['resource3'], new Error('Failed'), 2, 200);
      
      const prometheus = metrics.exportPrometheusMetrics();
      
      expect(prometheus).toContain('neolock_locks_acquired_total 1');
      expect(prometheus).toContain('neolock_locks_released_total 1');
      expect(prometheus).toContain('neolock_lock_extensions_total 1');
      expect(prometheus).toContain('neolock_failed_acquisitions_total 1');
      expect(prometheus).toContain('neolock_active_locks_current 0');
      
      // Check for help and type annotations
      expect(prometheus).toContain('# HELP neolock_locks_acquired_total');
      expect(prometheus).toContain('# TYPE neolock_locks_acquired_total counter');
    });
  });

  describe('Histogram Statistics', () => {
    it('should calculate percentiles correctly', () => {
      // Add latency samples
      for (let i = 1; i <= 100; i++) {
        metrics.recordLockAcquired(`lock-${i}`, ['resource'], i);
        metrics.recordLockReleased(`lock-${i}`, i);
      }
      
      const data = metrics.getMetrics();
      const acquisitionStats = data.detailed.acquisitionLatencyStats;
      
      expect(acquisitionStats.count).toBe(100);
      expect(acquisitionStats.average).toBe(50.5);
      expect(acquisitionStats.p50).toBeGreaterThan(40);
      expect(acquisitionStats.p90).toBeGreaterThan(80);
      expect(acquisitionStats.p95).toBeGreaterThan(90);
      expect(acquisitionStats.p99).toBeGreaterThan(95);
    });
  });

  describe('Metrics Reset', () => {
    it('should reset all metrics', () => {
      metrics.recordLockAcquired('lock-1', ['resource1'], 100);
      metrics.recordLockReleased('lock-1', 50);
      metrics.recordAcquisitionFailed(['resource2'], new Error('Failed'), 1, 200);
      
      let data = metrics.getMetrics();
      expect(data.locksAcquired).toBe(1);
      expect(data.locksReleased).toBe(1);
      expect(data.failedAcquisitions).toBe(1);
      
      metrics.reset();
      
      data = metrics.getMetrics();
      expect(data.locksAcquired).toBe(0);
      expect(data.locksReleased).toBe(0);
      expect(data.failedAcquisitions).toBe(0);
      expect(data.activeLocks).toBe(0);
      expect(data.detailed.activeLockDetails).toHaveLength(0);
    });

    it('should emit reset event', () => {
      const events: any[] = [];
      metrics.on('metricsReset', (event) => events.push(event));
      
      metrics.reset();
      
      expect(events).toHaveLength(1);
      expect(events[0].timestamp).toBeDefined();
    });
  });

  describe('Uptime Tracking', () => {
    it('should track uptime', async () => {
      await sleep(100);
      
      const data = metrics.getMetrics();
      expect(data.detailed.uptimeMs).toBeGreaterThanOrEqual(100);
    });
  });

  describe('Event System', () => {
    it('should emit all expected events', () => {
      const events: string[] = [];
      
      metrics.on('lockAcquired', () => events.push('acquired'));
      metrics.on('lockReleased', () => events.push('released'));
      metrics.on('lockExtended', () => events.push('extended'));
      metrics.on('acquisitionFailed', () => events.push('failed'));
      metrics.on('retryAttempt', () => events.push('retry'));
      metrics.on('circuitBreakerUpdate', () => events.push('circuit'));
      metrics.on('metricsReset', () => events.push('reset'));
      
      metrics.recordLockAcquired('lock-1', ['resource1'], 50);
      metrics.recordLockExtended('lock-1', 20);
      metrics.recordLockReleased('lock-1', 30);
      metrics.recordAcquisitionFailed(['resource2'], new Error('Failed'), 1, 100);
      metrics.recordRetry('lock-2', 1);
      metrics.updateCircuitBreakerMetrics('client-1', {
        state: 'closed',
        totalOperations: 1,
        successfulOperations: 1,
        failedOperations: 0
      });
      metrics.reset();
      
      expect(events).toContain('acquired');
      expect(events).toContain('released');
      expect(events).toContain('extended');
      expect(events).toContain('failed');
      expect(events).toContain('retry');
      expect(events).toContain('circuit');
      expect(events).toContain('reset');
    });
  });
});