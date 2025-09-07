/**
 * Error handling tests
 */

import { describe, it, expect } from 'vitest';
import {
  RedlockToolkitError,
  ResourceLockedError,
  ConsensusError,
  LockTimeoutError,
  LockExpiredError,
  LockReleaseError,
  LockExtensionError,
  CircuitBreakerOpenError,
  RedisOperationError,
  ConfigurationError,
  LockValidationError,
  isRetryableError,
  getRetryDelay
} from "../src";

describe('Error Classes', () => {
  describe('RedlockToolkitError Base Class', () => {
    class TestError extends RedlockToolkitError {
      constructor(message: string, context?: Record<string, any>) {
        super(message, context);
      }
    }

    it('should create error with message and context', () => {
      const context = { resource: 'test', attempt: 1 };
      const error = new TestError('Test error', context);
      
      expect(error.message).toBe('Test error');
      expect(error.context).toEqual(context);
      expect(error.timestamp).toBeDefined();
      expect(error.name).toBe('TestError');
    });

    it('should provide JSON representation', () => {
      const context = { key: 'value' };
      const error = new TestError('Test message', context);
      const json = error.toJSON();
      
      expect(json.name).toBe('TestError');
      expect(json.message).toBe('Test message');
      expect(json.context).toEqual(context);
      expect(json.timestamp).toBe(error.timestamp);
      expect(json.stack).toBeDefined();
    });

    it('should maintain proper stack trace', () => {
      const error = new TestError('Test error');
      expect(error.stack).toBeDefined();
      expect(error.stack).toContain('TestError');
    });
  });

  describe('ResourceLockedError', () => {
    it('should create with resources and holder', () => {
      const resources = ['user:123', 'account:456'];
      const holder = 'process-abc';
      const error = new ResourceLockedError(resources, holder);
      
      expect(error.resources).toEqual(resources);
      expect(error.currentHolder).toBe(holder);
      expect(error.message).toContain('user:123, account:456');
      expect(error.message).toContain('process-abc');
    });

    it('should create without holder information', () => {
      const resources = ['resource1'];
      const error = new ResourceLockedError(resources);
      
      expect(error.resources).toEqual(resources);
      expect(error.currentHolder).toBeUndefined();
      expect(error.message).toContain('resource1');
      expect(error.message).not.toContain('held by');
    });

    it('should include context information', () => {
      const context = { attemptNumber: 3 };
      const error = new ResourceLockedError(['resource1'], 'holder', context);
      
      expect(error.context).toEqual(context);
    });
  });

  describe('ConsensusError', () => {
    it('should create with consensus information', () => {
      const attempts = [Promise.resolve({
        totalClients: 3,
        quorumSize: 2,
        votesFor: new Set<any>(),
        votesAgainst: new Map<any, any>(),
        startTime: Date.now(),
        duration: 0
      })];
      
      const error = new ConsensusError(
        'Failed to achieve consensus',
        attempts,
        2,
        1
      );
      
      expect(error.attempts).toBe(attempts);
      expect(error.requiredQuorum).toBe(2);
      expect(error.achievedVotes).toBe(1);
      expect(error.message).toBe('Failed to achieve consensus');
    });
  });

  describe('LockTimeoutError', () => {
    it('should create with timeout information', () => {
      const error = new LockTimeoutError(5000, 10);
      
      expect(error.timeoutMs).toBe(5000);
      expect(error.attemptsCount).toBe(10);
      expect(error.message).toContain('5000ms');
      expect(error.message).toContain('10 attempts');
    });
  });

  describe('LockExpiredError', () => {
    it('should create with expiration information', () => {
      const expiration = Date.now() - 1000;
      const error = new LockExpiredError(expiration);
      
      expect(error.expiration).toBe(expiration);
      expect(error.currentTime).toBeCloseTo(Date.now(), -2);
      expect(error.message).toContain(new Date(expiration).toISOString());
    });
  });

  describe('LockReleaseError', () => {
    it('should create with release information', () => {
      const resources = ['resource1', 'resource2'];
      const identifier = 'lock-123';
      const error = new LockReleaseError(resources, identifier, 1, 3);
      
      expect(error.resources).toEqual(resources);
      expect(error.identifier).toBe(identifier);
      expect(error.releasedCount).toBe(1);
      expect(error.totalClients).toBe(3);
      expect(error.message).toContain('resource1, resource2');
      expect(error.message).toContain('1/3');
    });
  });

  describe('LockExtensionError', () => {
    it('should create with extension information', () => {
      const resources = ['resource1'];
      const identifier = 'lock-123';
      const expiration = Date.now() + 5000;
      const error = new LockExtensionError(resources, identifier, expiration);
      
      expect(error.resources).toEqual(resources);
      expect(error.identifier).toBe(identifier);
      expect(error.currentExpiration).toBe(expiration);
      expect(error.message).toContain('resource1');
    });
  });

  describe('CircuitBreakerOpenError', () => {
    it('should create with circuit breaker information', () => {
      const lastFailure = Date.now() - 2000;
      const failureCount = 5;
      const error = new CircuitBreakerOpenError(lastFailure, failureCount);
      
      expect(error.lastFailureTime).toBe(lastFailure);
      expect(error.failureCount).toBe(failureCount);
      expect(error.message).toContain('Circuit breaker is open');
      expect(error.message).toContain('failures: 5');
    });
  });

  describe('RedisOperationError', () => {
    it('should create with Redis operation information', () => {
      const originalError = new Error('Connection failed');
      const error = new RedisOperationError('SET', 'redis-1', originalError);
      
      expect(error.operation).toBe('SET');
      expect(error.client).toBe('redis-1');
      expect(error.originalError).toBe(originalError);
      expect(error.message).toContain('SET');
      expect(error.message).toContain('redis-1');
      expect(error.message).toContain('Connection failed');
    });
  });

  describe('ConfigurationError', () => {
    it('should create with configuration information', () => {
      const error = new ConfigurationError('ttl', -1, 'must be positive');
      
      expect(error.parameter).toBe('ttl');
      expect(error.value).toBe(-1);
      expect(error.message).toContain('ttl');
      expect(error.message).toContain('must be positive');
      expect(error.message).toContain('-1');
    });
  });

  describe('LockValidationError', () => {
    it('should create with validation information', () => {
      const error = new LockValidationError('ownership', 'lock-123', 'lock-456');
      
      expect(error.validationType).toBe('ownership');
      expect(error.expected).toBe('lock-123');
      expect(error.actual).toBe('lock-456');
      expect(error.message).toContain('ownership');
      expect(error.message).toContain('lock-123');
      expect(error.message).toContain('lock-456');
    });
  });
});

describe('Error Utilities', () => {
  describe('isRetryableError', () => {
    it('should identify non-retryable errors', () => {
      expect(isRetryableError(new CircuitBreakerOpenError(Date.now(), 3))).toBe(false);
      expect(isRetryableError(new ConfigurationError('ttl', -1, 'invalid'))).toBe(false);
      expect(isRetryableError(new LockValidationError('test', 'a', 'b'))).toBe(false);
    });

    it('should identify retryable errors', () => {
      expect(isRetryableError(new ResourceLockedError(['resource1']))).toBe(true);
      expect(isRetryableError(new ConsensusError('msg', [], 2, 1))).toBe(true);
    });

    it('should handle Redis operation errors based on message', () => {
      const networkError = new RedisOperationError(
        'GET',
        'redis-1',
        new Error('connect ECONNREFUSED')
      );
      expect(isRetryableError(networkError)).toBe(true);

      const timeoutError = new RedisOperationError(
        'SET',
        'redis-1',
        new Error('operation timeout')
      );
      expect(isRetryableError(timeoutError)).toBe(true);

      const syntaxError = new RedisOperationError(
        'EVAL',
        'redis-1',
        new Error('syntax error in script')
      );
      expect(isRetryableError(syntaxError)).toBe(false);
    });

    it('should default to retryable for unknown errors', () => {
      expect(isRetryableError(new Error('Unknown error'))).toBe(true);
    });
  });

  describe('getRetryDelay', () => {
    it('should apply multipliers for different error types', () => {
      const baseDelay = 100;
      const jitter = 50;

      const resourceLockedDelay = getRetryDelay(
        new ResourceLockedError(['resource1']),
        baseDelay,
        jitter
      );
      expect(resourceLockedDelay).toBeGreaterThanOrEqual(150); // 2x multiplier (with possible jitter)
      expect(resourceLockedDelay).toBeLessThanOrEqual(300); // + jitter range

      const consensusDelay = getRetryDelay(
        new ConsensusError('msg', [], 2, 1),
        baseDelay,
        jitter
      );
      expect(consensusDelay).toBeGreaterThanOrEqual(100); // 1.5x multiplier (with possible jitter)
      expect(consensusDelay).toBeLessThanOrEqual(250); // + jitter range

      const generalDelay = getRetryDelay(
        new Error('General error'),
        baseDelay,
        jitter
      );
      expect(generalDelay).toBeGreaterThanOrEqual(50); // 1x multiplier (with possible jitter)
      expect(generalDelay).toBeLessThanOrEqual(200); // + jitter range
    });

    it('should add random jitter', () => {
      const delays = new Set();
      
      // Generate multiple delays to check for randomness
      for (let i = 0; i < 20; i++) {
        const delay = getRetryDelay(new Error('test'), 100, 50);
        delays.add(delay);
      }

      // Should have multiple different values due to jitter
      expect(delays.size).toBeGreaterThan(1);
    });

    it('should handle zero jitter', () => {
      const delay = getRetryDelay(new Error('test'), 100, 0);
      expect(delay).toBe(100);
    });

    it('should floor the result', () => {
      const delay = getRetryDelay(new Error('test'), 100, 10);
      expect(delay).toBe(Math.floor(delay));
      expect(Number.isInteger(delay)).toBe(true);
    });
  });
});

describe('Error Inheritance', () => {
  it('should maintain proper inheritance chain', () => {
    const error = new ResourceLockedError(['resource1']);
    
    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(RedlockToolkitError);
    expect(error).toBeInstanceOf(ResourceLockedError);
  });

  it('should work with instanceof checks', () => {
    const errors = [
      new ResourceLockedError(['resource1']),
      new LockTimeoutError(1000, 5),
      new ConfigurationError('ttl', -1, 'invalid'),
      new CircuitBreakerOpenError(Date.now(), 3)
    ];

    for (const error of errors) {
      expect(error).toBeInstanceOf(RedlockToolkitError);
      expect(error).toBeInstanceOf(Error);
    }
  });

  it('should preserve error names for logging', () => {
    const errors = [
      { error: new ResourceLockedError(['resource1']), name: 'ResourceLockedError' },
      { error: new LockTimeoutError(1000, 5), name: 'LockTimeoutError' },
      { error: new ConfigurationError('ttl', -1, 'invalid'), name: 'ConfigurationError' },
      { error: new CircuitBreakerOpenError(Date.now(), 3), name: 'CircuitBreakerOpenError' }
    ];

    for (const { error, name } of errors) {
      expect(error.name).toBe(name);
    }
  });
});

describe('Error Context and Debugging', () => {
  it('should provide rich context for debugging', () => {
    const context = {
      clientId: 'redis-1',
      operation: 'acquire',
      resources: ['user:123'],
      attempt: 3,
      totalAttempts: 10
    };

    const error = new ResourceLockedError(['user:123'], 'other-process', context);
    const json = error.toJSON();

    expect(json.context).toEqual(context);
    expect(json.timestamp).toBeDefined();
    expect(json.stack).toBeDefined();
  });

  it('should handle errors without context', () => {
    const error = new LockTimeoutError(5000, 10);
    const json = error.toJSON();

    expect(json.context).toBeUndefined();
    expect(json.name).toBe('LockTimeoutError');
    expect(json.message).toBeDefined();
  });

  it('should serialize complex context objects', () => {
    const context = {
      nested: { key: 'value' },
      array: [1, 2, 3],
      date: new Date(),
      undefined: undefined,
      null: null
    };

    const error = new ResourceLockedError(['resource1'], undefined, context);
    const json = error.toJSON();

    expect(json.context).toEqual(context);
  });
});