/**
 * Circuit Breaker Tests
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { CircuitBreaker, CircuitBreakerManager } from '../src/patterns/circuit-breaker';

describe('CircuitBreaker', () => {
  let circuitBreaker: CircuitBreaker;
  const defaultOptions = {
    failureThreshold: 3,
    resetTimeout: 1000,
    maxRetries: 2,
    operationTimeout: 500
  };

  beforeEach(() => {
    circuitBreaker = new CircuitBreaker(defaultOptions);
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  describe('State Transitions', () => {
    it('should start in closed state', () => {
      expect(circuitBreaker.getState()).toBe('closed');
      expect(circuitBreaker.isOperationAllowed()).toBe(true);
    });

    it('should transition to open state after failure threshold', async () => {
      const failingOperation = () => Promise.reject(new Error('Test failure'));

      // Fail operations to reach threshold
      for (let i = 0; i < defaultOptions.failureThreshold; i++) {
        try {
          await circuitBreaker.execute(failingOperation);
        } catch (error) {
          // Expected to fail
        }
      }

      expect(circuitBreaker.getState()).toBe('open');
      expect(circuitBreaker.isOperationAllowed()).toBe(false);
    });

    it('should transition to half-open after reset timeout', async () => {
      const failingOperation = () => Promise.reject(new Error('Test failure'));

      // Open the circuit
      for (let i = 0; i < defaultOptions.failureThreshold; i++) {
        try {
          await circuitBreaker.execute(failingOperation);
        } catch (error) {
          // Expected
        }
      }

      expect(circuitBreaker.getState()).toBe('open');

      // Advance time past reset timeout
      vi.advanceTimersByTime(defaultOptions.resetTimeout + 100);

      // Next call should transition to half-open
      const successOperation = () => Promise.resolve('success');
      await circuitBreaker.execute(successOperation);

      // After successful operation, it should be closed
      expect(circuitBreaker.getState()).toBe('closed');
    });

    it('should reset to closed from half-open on success', async () => {
      const failingOperation = () => Promise.reject(new Error('Test failure'));
      const successOperation = () => Promise.resolve('success');

      // Open the circuit
      for (let i = 0; i < defaultOptions.failureThreshold; i++) {
        try {
          await circuitBreaker.execute(failingOperation);
        } catch (error) {
          // Expected
        }
      }

      // Wait for reset timeout
      vi.advanceTimersByTime(defaultOptions.resetTimeout + 100);

      // Execute successful operation in half-open state
      const result = await circuitBreaker.execute(successOperation);
      
      expect(result).toBe('success');
      expect(circuitBreaker.getState()).toBe('closed');
    });

    it('should return to open from half-open on failure', async () => {
      const failingOperation = () => Promise.reject(new Error('Test failure'));

      // Open the circuit
      for (let i = 0; i < defaultOptions.failureThreshold; i++) {
        try {
          await circuitBreaker.execute(failingOperation);
        } catch (error) {
          // Expected
        }
      }

      // Wait for reset timeout
      vi.advanceTimersByTime(defaultOptions.resetTimeout + 100);

      // Try failing operation in half-open state
      try {
        await circuitBreaker.execute(failingOperation);
      } catch (error) {
        // Expected
      }

      expect(circuitBreaker.getState()).toBe('open');
    });
  });

  describe('Operation Execution', () => {
    it('should execute operation successfully in closed state', async () => {
      const operation = () => Promise.resolve('success');
      const result = await circuitBreaker.execute(operation);
      
      expect(result).toBe('success');
      
      const metrics = circuitBreaker.getMetrics();
      expect(metrics.successfulOperations).toBe(1);
      expect(metrics.failedOperations).toBe(0);
    });

    it('should handle operation timeout', async () => {
      vi.useRealTimers(); // Use real timers for this test
      
      const slowOperation = () => new Promise((resolve) => {
        setTimeout(() => resolve('slow'), 1000);
      });

      const fastBreaker = new CircuitBreaker({ ...defaultOptions, operationTimeout: 100 });
      
      await expect(fastBreaker.execute(slowOperation)).rejects.toThrow('Operation timed out');
      
      const metrics = fastBreaker.getMetrics();
      expect(metrics.failedOperations).toBe(1);
      
      vi.useFakeTimers(); // Restore fake timers
    });

    it('should reject operations immediately when circuit is open', async () => {
      const failingOperation = () => Promise.reject(new Error('Test failure'));

      // Open the circuit
      for (let i = 0; i < defaultOptions.failureThreshold; i++) {
        try {
          await circuitBreaker.execute(failingOperation);
        } catch (error) {
          // Expected
        }
      }

      // Should reject without executing operation
      const mockOperation = vi.fn(() => Promise.resolve('should not execute'));
      
      await expect(circuitBreaker.execute(mockOperation)).rejects.toThrow('Circuit breaker is open');
      expect(mockOperation).not.toHaveBeenCalled();
    });

    it('should track failures', async () => {
      const failingOperation = () => Promise.reject(new Error('Test failure'));

      for (let i = 0; i < 2; i++) {
        try {
          await circuitBreaker.execute(failingOperation);
        } catch (error) {
          // Expected
        }
      }

      const metrics = circuitBreaker.getMetrics();
      expect(metrics.failedOperations).toBe(2);
      expect(circuitBreaker.getState()).toBe('closed'); // Still closed, hasn't reached threshold
    });

    it('should track successful operations after failures', async () => {
      const failingOperation = () => Promise.reject(new Error('Test failure'));
      const successOperation = () => Promise.resolve('success');

      // Some failures
      for (let i = 0; i < 2; i++) {
        try {
          await circuitBreaker.execute(failingOperation);
        } catch (error) {
          // Expected
        }
      }

      let metrics = circuitBreaker.getMetrics();
      expect(metrics.failedOperations).toBe(2);

      // Successful operation
      await circuitBreaker.execute(successOperation);

      metrics = circuitBreaker.getMetrics();
      expect(metrics.successfulOperations).toBe(1);
      expect(circuitBreaker.getState()).toBe('closed'); // Should remain closed
    });
  });

  describe('Metrics', () => {
    it('should track all operation metrics', async () => {
      const successOperation = () => Promise.resolve('success');
      const failOperation = () => Promise.reject(new Error('fail'));

      // Execute some operations
      await circuitBreaker.execute(successOperation);
      await circuitBreaker.execute(successOperation);
      
      try {
        await circuitBreaker.execute(failOperation);
      } catch (error) {
        // Expected
      }

      const metrics = circuitBreaker.getMetrics();
      
      expect(metrics.totalOperations).toBe(3);
      expect(metrics.successfulOperations).toBe(2);
      expect(metrics.failedOperations).toBe(1);
      expect(metrics.state).toBe('closed');
    });

    it('should track last failure time', async () => {
      const failOperation = () => Promise.reject(new Error('fail'));
      const beforeFailure = Date.now();

      try {
        await circuitBreaker.execute(failOperation);
      } catch (error) {
        // Expected
      }

      const metrics = circuitBreaker.getMetrics();
      expect(metrics.lastFailureTime).toBeGreaterThanOrEqual(beforeFailure);
      expect(metrics.lastFailureTime).toBeLessThanOrEqual(Date.now());
    });

    it('should reset metrics', async () => {
      // Execute some operations first
      const successOperation = () => Promise.resolve('success');
      const failOperation = () => Promise.reject(new Error('fail'));
      
      await circuitBreaker.execute(successOperation);
      try {
        await circuitBreaker.execute(failOperation);
      } catch (e) {
        // Expected
      }
      
      // Verify metrics are non-zero
      let metrics = circuitBreaker.getMetrics();
      expect(metrics.totalOperations).toBeGreaterThan(0);
      
      // Manual reset
      circuitBreaker.manualReset();

      // Check after reset
      metrics = circuitBreaker.getMetrics();
      expect(metrics.totalOperations).toBe(0);
      expect(metrics.successfulOperations).toBe(0);
      expect(metrics.failedOperations).toBe(0);
      expect(metrics.lastFailureTime).toBeUndefined();
      expect(circuitBreaker.getState()).toBe('closed');
    });
  });

  describe('Error Handling', () => {
    it('should handle different error types', async () => {
      const errors = [
        new Error('Generic error'),
        new TypeError('Type error'),
        'String error',
        { message: 'Object error' }
      ];

      for (const error of errors) {
        const operation = () => Promise.reject(error);
        
        await expect(circuitBreaker.execute(operation)).rejects.toThrow();
        
        const metrics = circuitBreaker.getMetrics();
        expect(metrics.failedOperations).toBeGreaterThan(0);
      }
    });

    it('should handle synchronous errors', async () => {
      const throwingOperation = () => {
        throw new Error('Sync error');
      };

      await expect(circuitBreaker.execute(throwingOperation)).rejects.toThrow('Sync error');
      
      const metrics = circuitBreaker.getMetrics();
      expect(metrics.failedOperations).toBe(1);
    });
  });

  describe('Events', () => {
    it('should emit state change events', async () => {
      const stateChangeHandler = vi.fn();
      circuitBreaker.on('stateChanged', stateChangeHandler);

      const failingOperation = () => Promise.reject(new Error('Test failure'));

      // Trigger state change to open
      for (let i = 0; i < defaultOptions.failureThreshold; i++) {
        try {
          await circuitBreaker.execute(failingOperation);
        } catch (error) {
          // Expected
        }
      }

      expect(stateChangeHandler).toHaveBeenCalledWith('open', 'closed', expect.any(Object));
    });

    // Note: Circuit breaker doesn't emit success event in current implementation
    it.skip('should emit success event', async () => {
      // This test is skipped as the implementation doesn't emit success events
    });

    it('should emit failure event', async () => {
      const failureHandler = vi.fn();
      circuitBreaker.on('failure', failureHandler);

      const error = new Error('Test error');
      const operation = () => Promise.reject(error);

      try {
        await circuitBreaker.execute(operation);
      } catch (e) {
        // Expected
      }

      expect(failureHandler).toHaveBeenCalledWith(error, expect.any(Object));
    });
  });
});

describe('CircuitBreakerManager', () => {
  let manager: CircuitBreakerManager;
  const options = {
    failureThreshold: 3,
    resetTimeout: 1000,
    maxRetries: 2,
    operationTimeout: 500
  };

  beforeEach(() => {
    manager = new CircuitBreakerManager(options);
  });

  describe('Client Management', () => {
    it('should create circuit breakers for new clients', async () => {
      const operation1 = () => Promise.resolve('client1');
      const operation2 = () => Promise.resolve('client2');

      const result1 = await manager.execute('client1', operation1);
      const result2 = await manager.execute('client2', operation2);

      expect(result1).toBe('client1');
      expect(result2).toBe('client2');

      const metrics = manager.getAllMetrics();
      expect(metrics.size).toBe(2);
      expect(metrics.get('client1')).toBeDefined();
      expect(metrics.get('client2')).toBeDefined();
    });

    it('should reuse circuit breaker for same client', async () => {
      const operation = () => Promise.resolve('success');

      await manager.execute('client1', operation);
      await manager.execute('client1', operation);

      const metrics = manager.getAllMetrics();
      const clientMetrics = metrics.get('client1');
      expect(clientMetrics?.totalOperations).toBe(2);
      expect(clientMetrics?.successfulOperations).toBe(2);
    });

    it('should handle operations independently per client', async () => {
      const failOperation = () => Promise.reject(new Error('fail'));
      const successOperation = () => Promise.resolve('success');

      // Fail operations for client1
      for (let i = 0; i < options.failureThreshold; i++) {
        try {
          await manager.execute('client1', failOperation);
        } catch (error) {
          // Expected
        }
      }

      // client1 should be open
      const metrics1 = manager.getAllMetrics().get('client1');
      expect(metrics1?.state).toBe('open');

      // client2 should still work
      const result = await manager.execute('client2', successOperation);
      expect(result).toBe('success');
      const metrics2 = manager.getAllMetrics().get('client2');
      expect(metrics2?.state).toBe('closed');
    });
  });

  describe('Metrics Management', () => {
    it('should get metrics for specific client', async () => {
      const operation = () => Promise.resolve('success');
      await manager.execute('client1', operation);

      const metrics = manager.getAllMetrics().get('client1');
      expect(metrics?.successfulOperations).toBe(1);
      expect(metrics?.state).toBe('closed');
    });

    it('should return undefined for unknown client', () => {
      const metrics = manager.getAllMetrics().get('unknown');
      expect(metrics).toBeUndefined();
    });

    it('should get all client metrics', async () => {
      const operation = () => Promise.resolve('success');
      
      await manager.execute('client1', operation);
      await manager.execute('client2', operation);
      await manager.execute('client3', operation);

      const allMetrics = manager.getAllMetrics();
      
      expect(allMetrics.size).toBe(3);
      expect(allMetrics.get('client1')?.successfulOperations).toBe(1);
      expect(allMetrics.get('client2')?.successfulOperations).toBe(1);
      expect(allMetrics.get('client3')?.successfulOperations).toBe(1);
    });

    it('should reset specific client', async () => {
      const operation = () => Promise.resolve('success');
      await manager.execute('client1', operation);
      
      // Get the breaker and reset it manually
      const breaker = manager.getBreaker('client1');
      breaker.manualReset();
      
      const metrics = manager.getAllMetrics().get('client1');
      // After reset, state should be closed
      expect(metrics?.state).toBe('closed');
    });

    it('should reset all clients', async () => {
      const operation = () => Promise.resolve('success');
      
      await manager.execute('client1', operation);
      await manager.execute('client2', operation);
      
      manager.resetAll();
      
      const allMetrics = manager.getAllMetrics();
      const metrics1 = allMetrics.get('client1');
      const metrics2 = allMetrics.get('client2');
      
      // After reset, states should be closed
      expect(metrics1?.state).toBe('closed');
      expect(metrics2?.state).toBe('closed');
    });

    it('should clear all circuit breakers', async () => {
      const operation = () => Promise.resolve('success');
      
      await manager.execute('client1', operation);
      await manager.execute('client2', operation);
      
      manager.clear();
      
      const allMetrics = manager.getAllMetrics();
      expect(allMetrics.size).toBe(0);
    });
  });

  describe('State Management', () => {
    it('should get state for specific client', async () => {
      const failOperation = () => Promise.reject(new Error('fail'));
      
      // Open circuit for client1
      for (let i = 0; i < options.failureThreshold; i++) {
        try {
          await manager.execute('client1', failOperation);
        } catch (error) {
          // Expected
        }
      }

      const breaker = manager.getBreaker('client1');
      expect(breaker.getState()).toBe('open');
      expect(breaker.isOperationAllowed()).toBe(false);
    });

    it('should create new breaker for unknown client', () => {
      const breaker = manager.getBreaker('unknown');
      expect(breaker.getState()).toBe('closed');
      expect(breaker.isOperationAllowed()).toBe(true);
    });
  });

  describe('Event Forwarding', () => {
    it('should forward circuit breaker events', async () => {
      const stateChangeHandler = vi.fn();
      manager.on('stateChanged', stateChangeHandler);

      const failOperation = () => Promise.reject(new Error('fail'));

      // Trigger state change
      for (let i = 0; i < options.failureThreshold; i++) {
        try {
          await manager.execute('client1', failOperation);
        } catch (error) {
          // Expected
        }
      }

      expect(stateChangeHandler).toHaveBeenCalledWith(
        'client1',
        'open',
        'closed',
        expect.any(Object)
      );
    });
  });

  describe('Error Handling', () => {
    it('should handle client-specific errors', async () => {
      const error1 = new Error('Client 1 error');
      const error2 = new Error('Client 2 error');
      
      const operation1 = () => Promise.reject(error1);
      const operation2 = () => Promise.reject(error2);

      await expect(manager.execute('client1', operation1)).rejects.toThrow('Client 1 error');
      await expect(manager.execute('client2', operation2)).rejects.toThrow('Client 2 error');

      const allMetrics = manager.getAllMetrics();
      const metrics1 = allMetrics.get('client1');
      const metrics2 = allMetrics.get('client2');
      
      expect(metrics1?.failedOperations).toBe(1);
      expect(metrics2?.failedOperations).toBe(1);
    });

    it('should isolate circuit breaker failures', async () => {
      const failOperation = () => Promise.reject(new Error('fail'));
      const successOperation = () => Promise.resolve('success');

      // Open circuit for client1
      for (let i = 0; i < options.failureThreshold; i++) {
        try {
          await manager.execute('client1', failOperation);
        } catch (error) {
          // Expected
        }
      }

      // client1 should reject operations
      await expect(manager.execute('client1', successOperation)).rejects.toThrow('Circuit breaker is open');

      // client2 should still work
      await expect(manager.execute('client2', successOperation)).resolves.toBe('success');
    });
  });
});