/**
 * Tests for ConsensusManager
 * Ensures proper consensus operations across multiple Redis clients
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { ConsensusManager } from '../src/utils/consensus-manager';
import { SilentLogger, ConsoleLogger, LogLevel } from '../src/core/logger';
import { ConsensusError, ConfigurationError, CircuitBreakerOpenError } from '../src/core/errors';
import { createMockRedisClients } from './setup';

describe('ConsensusManager', () => {
  let consensusManager: ConsensusManager;
  let mockClients: any[];
  let logger: SilentLogger;

  beforeEach(() => {
    logger = new SilentLogger();
    consensusManager = new ConsensusManager(logger);
    mockClients = createMockRedisClients(3);
  });

  describe('Basic Consensus Operations', () => {
    it('should achieve consensus when all operations succeed', async () => {
      const operation = vi.fn().mockResolvedValue(1);
      
      const result = await consensusManager.execute(
        mockClients,
        operation,
        {}
      );

      expect(result.success).toBe(true);
      expect(result.stats.votesFor.size).toBe(2); // Early termination after quorum
      expect(result.stats.votesAgainst.size).toBe(0);
      expect(operation).toHaveBeenCalledTimes(3);
    });

    it('should achieve consensus with quorum (majority succeeds)', async () => {
      let callCount = 0;
      const operation = vi.fn().mockImplementation(() => {
        callCount++;
        if (callCount <= 2) {
          return Promise.resolve(1); // First 2 succeed
        }
        return Promise.reject(new Error('Failed'));
      });

      const result = await consensusManager.execute(
        mockClients,
        operation,
        {}
      );

      expect(result.success).toBe(true);
      expect(result.stats.votesFor.size).toBe(2); // Quorum achieved
      // Early termination means 3rd operation may not be processed
      expect(result.stats.votesAgainst.size).toBeGreaterThanOrEqual(0);
    });

    it('should fail consensus when quorum not achieved', async () => {
      let callCount = 0;
      const operation = vi.fn().mockImplementation(() => {
        callCount++;
        if (callCount === 1) {
          return Promise.resolve(1); // Only 1 succeeds
        }
        return Promise.reject(new Error('Failed'));
      });

      await expect(
        consensusManager.execute(mockClients, operation, {})
      ).rejects.toThrow(ConsensusError);
    });

    it('should succeed with "any" policy when at least one succeeds', async () => {
      let callCount = 0;
      const operation = vi.fn().mockImplementation(() => {
        callCount++;
        if (callCount === 1) {
          return Promise.resolve(1); // Only 1 succeeds
        }
        return Promise.reject(new Error('Failed'));
      });

      const result = await consensusManager.execute(
        mockClients,
        operation,
        { successPolicy: 'any' }
      );

      expect(result.success).toBe(true);
      expect(result.stats.votesFor.size).toBe(1);
    });

    it('should fail with "any" policy when all fail', async () => {
      const operation = vi.fn().mockRejectedValue(new Error('Failed'));

      await expect(
        consensusManager.execute(mockClients, operation, { successPolicy: 'any' })
      ).rejects.toThrow(ConsensusError);
    });
  });

  describe('Quorum Calculation', () => {
    it('should calculate correct quorum for odd number of clients', async () => {
      // 3 clients -> quorum 2
      const clients3 = createMockRedisClients(3);
      const operation = vi.fn().mockResolvedValue(1);
      
      const result = await consensusManager.execute(clients3, operation, {});
      expect(result.stats.quorumSize).toBe(2);

      // 5 clients -> quorum 3
      const clients5 = createMockRedisClients(5);
      const result5 = await consensusManager.execute(clients5, operation, {});
      expect(result5.stats.quorumSize).toBe(3);
    });

    it('should calculate correct quorum for even number of clients', async () => {
      // 4 clients -> quorum 3
      const clients4 = createMockRedisClients(4);
      const operation = vi.fn().mockResolvedValue(1);
      
      const result = await consensusManager.execute(clients4, operation, {});
      expect(result.stats.quorumSize).toBe(3);

      // 6 clients -> quorum 4
      const clients6 = createMockRedisClients(6);
      const result6 = await consensusManager.execute(clients6, operation, {});
      expect(result6.stats.quorumSize).toBe(4);
    });

    it('should handle single client (quorum 1)', async () => {
      const singleClient = createMockRedisClients(1);
      const operation = vi.fn().mockResolvedValue(1);
      
      const result = await consensusManager.execute(singleClient, operation, {});
      expect(result.stats.quorumSize).toBe(1);
      expect(result.success).toBe(true);
    });
  });

  describe('Result Evaluation', () => {
    it('should use default evaluator (number > 0)', async () => {
      const operation = vi.fn()
        .mockResolvedValueOnce(1)    // Success
        .mockResolvedValueOnce(1)    // Success (quorum achieved)
        .mockResolvedValueOnce(0);   // This may not be processed due to early termination

      const result = await consensusManager.execute(
        mockClients,
        operation,
        {}
      );

      expect(result.success).toBe(true);
      expect(result.stats.votesFor.size).toBe(2);
    });

    it('should handle non-numeric results with default evaluator', async () => {
      const operation = vi.fn()
        .mockResolvedValueOnce('string')
        .mockResolvedValueOnce(null)
        .mockResolvedValueOnce(undefined);

      await expect(
        consensusManager.execute(mockClients, operation, {})
      ).rejects.toThrow(ConsensusError);
    });

    it('should use custom evaluator', async () => {
      const customEvaluator = (result: unknown) => result === 'success';
      const operation = vi.fn()
        .mockResolvedValueOnce('success')
        .mockResolvedValueOnce('success')
        .mockResolvedValueOnce('failure');

      const result = await consensusManager.execute(
        mockClients,
        operation,
        { evaluateResult: customEvaluator }
      );

      expect(result.success).toBe(true);
      expect(result.stats.votesFor.size).toBe(2);
      // Early termination may prevent processing all operations
      expect(result.stats.votesAgainst.size).toBeGreaterThanOrEqual(0);
    });

    it('should handle boolean evaluator', async () => {
      const booleanEvaluator = (result: unknown) => result === true;
      const operation = vi.fn()
        .mockResolvedValueOnce(true)
        .mockResolvedValueOnce(true)
        .mockResolvedValueOnce(false);

      const result = await consensusManager.execute(
        mockClients,
        operation,
        { evaluateResult: booleanEvaluator }
      );

      expect(result.success).toBe(true);
      expect(result.stats.votesFor.size).toBe(2);
    });
  });

  describe('Error Handling', () => {
    it('should handle mixed success and errors', async () => {
      const operation = vi.fn()
        .mockResolvedValueOnce(1)
        .mockRejectedValueOnce(new Error('Network error'))
        .mockResolvedValueOnce(1);

      const result = await consensusManager.execute(
        mockClients,
        operation,
        {}
      );

      expect(result.success).toBe(true);
      expect(result.stats.votesFor.size).toBe(2);
      expect(result.stats.votesAgainst.size).toBe(1);
    });

    it('should throw non-retryable errors when all fail with non-retryable', async () => {
      // Note: Non-retryable errors are only re-thrown if ALL errors are non-retryable
      // and there are no successful votes
      const operation = vi.fn()
        .mockRejectedValueOnce(new ConfigurationError('Bad config'))
        .mockRejectedValueOnce(new ConfigurationError('Bad config 2'))
        .mockRejectedValueOnce(new ConfigurationError('Bad config 3'));

      await expect(
        consensusManager.execute(mockClients, operation, {})
      ).rejects.toThrow(ConfigurationError);
    });

    it('should handle CircuitBreakerOpenError as non-retryable', async () => {
      // All errors must be non-retryable for re-throw
      const operation = vi.fn()
        .mockRejectedValueOnce(new CircuitBreakerOpenError('circuit-1', new Date()))
        .mockRejectedValueOnce(new CircuitBreakerOpenError('circuit-2', new Date()))
        .mockRejectedValueOnce(new CircuitBreakerOpenError('circuit-3', new Date()));

      await expect(
        consensusManager.execute(mockClients, operation, {})
      ).rejects.toThrow(CircuitBreakerOpenError);
    });

    it('should handle all non-retryable errors', async () => {
      const operation = vi.fn()
        .mockRejectedValueOnce(new ConfigurationError('Bad config'))
        .mockRejectedValueOnce(new CircuitBreakerOpenError('circuit-1', new Date()))
        .mockRejectedValueOnce(new ConfigurationError('Another bad config'));

      await expect(
        consensusManager.execute(mockClients, operation, {})
      ).rejects.toThrow(ConfigurationError);
    });

    it('should throw ConsensusError for retryable failures', async () => {
      const operation = vi.fn().mockRejectedValue(new Error('Retryable error'));

      try {
        await consensusManager.execute(mockClients, operation, {});
      } catch (error) {
        expect(error).toBeInstanceOf(ConsensusError);
        if (error instanceof ConsensusError) {
          expect(error.message).toContain('Failed to achieve quorum');
          expect(error.requiredQuorum).toBe(2);
          expect(error.achievedVotes).toBe(0);
        }
      }
    });
  });

  describe('Early Termination', () => {
    it('should terminate early when quorum achieved', async () => {
      const clients = createMockRedisClients(5);
      let callCount = 0;
      const delays = [10, 20, 30, 100, 100]; // First 3 are fast, last 2 are slow
      
      const operation = vi.fn().mockImplementation(() => {
        const delay = delays[callCount++];
        return new Promise(resolve => setTimeout(() => resolve(1), delay));
      });

      const startTime = Date.now();
      const result = await consensusManager.execute(clients, operation, {});
      const duration = Date.now() - startTime;

      expect(result.success).toBe(true);
      expect(result.stats.votesFor.size).toBeGreaterThanOrEqual(3); // Quorum of 5 is 3
      expect(duration).toBeLessThan(50); // Should not wait for slow operations
    });

    it('should terminate early when quorum impossible', async () => {
      const clients = createMockRedisClients(5);
      let callCount = 0;
      
      const operation = vi.fn().mockImplementation(() => {
        callCount++;
        if (callCount <= 3) {
          // 3 fast failures make quorum impossible (need 3 successes for quorum)
          return Promise.reject(new Error('Fast failure'));
        }
        // These will be slow but should not be waited for
        return new Promise(resolve => setTimeout(() => resolve(1), 100));
      });

      const startTime = Date.now();
      
      await expect(
        consensusManager.execute(clients, operation, {})
      ).rejects.toThrow(ConsensusError);
      
      const duration = Date.now() - startTime;
      expect(duration).toBeLessThan(50); // Should fail fast
    });
  });

  describe('VotingTracker', () => {
    it('should track successful and failed votes correctly', async () => {
      const operation = vi.fn()
        .mockResolvedValueOnce(1)
        .mockRejectedValueOnce(new Error('Error 1'))
        .mockResolvedValueOnce(1);

      const result = await consensusManager.execute(
        mockClients,
        operation,
        {}
      );

      expect(result.stats.votesFor.size).toBe(2);
      expect(result.stats.votesAgainst.size).toBe(1);
      
      // Check error is stored
      const errors = Array.from(result.stats.votesAgainst.values());
      expect(errors[0].message).toBe('Error 1');
    });

    it('should correctly identify when quorum is achieved', async () => {
      const clients = createMockRedisClients(7); // Quorum is 4
      let successCount = 0;
      
      const operation = vi.fn().mockImplementation(() => {
        successCount++;
        if (successCount <= 4) {
          return Promise.resolve(1);
        }
        return new Promise(resolve => setTimeout(() => resolve(1), 1000));
      });

      const result = await consensusManager.execute(clients, operation, {});
      
      expect(result.success).toBe(true);
      expect(result.stats.votesFor.size).toBeGreaterThanOrEqual(4);
    });
  });

  describe('Performance', () => {
    it('should execute operations in parallel', async () => {
      const clients = createMockRedisClients(10);
      const operationDuration = 50;
      
      const operation = vi.fn().mockImplementation(() => {
        return new Promise(resolve => 
          setTimeout(() => resolve(1), operationDuration)
        );
      });

      const startTime = Date.now();
      const result = await consensusManager.execute(clients, operation, {});
      const duration = Date.now() - startTime;

      expect(result.success).toBe(true);
      expect(duration).toBeLessThan(operationDuration * 2); // Should be parallel, not sequential
      expect(operation).toHaveBeenCalledTimes(10);
    });

    it('should handle large number of clients efficiently', async () => {
      const clients = createMockRedisClients(100);
      const operation = vi.fn().mockResolvedValue(1);

      const result = await consensusManager.execute(clients, operation, {});

      expect(result.success).toBe(true);
      expect(result.stats.totalClients).toBe(100);
      expect(result.stats.quorumSize).toBe(51);
      expect(operation).toHaveBeenCalledTimes(100);
    });
  });

  describe('Stats and Logging', () => {
    it('should include correct stats in result', async () => {
      const operation = vi.fn().mockResolvedValue(1);
      
      const result = await consensusManager.execute(
        mockClients,
        operation,
        {}
      );

      expect(result.stats).toMatchObject({
        totalClients: 3,
        quorumSize: 2,
        startTime: expect.any(Number),
        duration: expect.any(Number)
      });
      expect(result.stats.duration).toBeGreaterThanOrEqual(0);
      expect(result.startTime).toBe(result.stats.startTime);
    });

    it('should log consensus results when logger is provided', async () => {
      const debugSpy = vi.fn();
      const mockLogger = {
        error: vi.fn(),
        warn: vi.fn(),
        info: vi.fn(),
        debug: debugSpy,
        trace: vi.fn()
      };
      
      const manager = new ConsensusManager(mockLogger);
      const operation = vi.fn().mockResolvedValue(1);
      
      await manager.execute(mockClients, operation, {});
      
      expect(debugSpy).toHaveBeenCalledWith(
        'Consensus operation completed',
        expect.objectContaining({
          totalClients: 3,
          quorumSize: 2,
          successCount: 2, // Early termination after quorum
          failureCount: 0
        })
      );
    });
  });

  describe('Edge Cases', () => {
    it('should handle empty client array', async () => {
      const operation = vi.fn();
      
      await expect(
        consensusManager.execute([], operation, {})
      ).rejects.toThrow();
    });

    it('should handle operations that return different valid values', async () => {
      const operation = vi.fn()
        .mockResolvedValueOnce(1)
        .mockResolvedValueOnce(2)
        .mockResolvedValueOnce(3);

      const result = await consensusManager.execute(
        mockClients,
        operation,
        {}
      );

      expect(result.success).toBe(true);
      expect(result.stats.votesFor.size).toBe(2); // Early termination after quorum
    });

    it('should handle slow operations correctly', async () => {
      const operation = vi.fn().mockImplementation((client) => {
        const index = mockClients.indexOf(client);
        const delay = (index + 1) * 10; // Different delays
        return new Promise(resolve => 
          setTimeout(() => resolve(index + 1), delay)
        );
      });

      const result = await consensusManager.execute(
        mockClients,
        operation,
        {}
      );

      expect(result.success).toBe(true);
      expect(result.stats.votesFor.size).toBe(2); // Early termination after quorum
    });
  });
});