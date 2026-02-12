/**
 * Test setup and utilities with enhanced failure injection
 */

import { vi } from 'vitest';

// Mock console methods for cleaner test output
const originalConsole = { ...console };

export function mockConsole() {
  console.log = vi.fn();
  console.warn = vi.fn();
  console.error = vi.fn();
  console.info = vi.fn();
}

export function restoreConsole() {
  Object.assign(console, originalConsole);
}

// Global test utilities
export const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Failure injection utilities
export interface FailureConfig {
  failureRate?: number; // 0-1 probability of failure
  latency?: { min: number; max: number }; // Response latency range
  errorType?: 'network' | 'timeout' | 'corruption' | 'byzantine';
  partialFailure?: boolean; // Only fail for some operations
  progressiveFailure?: boolean; // Increase failure rate over time
}

export class ChaosClient {
  private operationCount = 0;
  private failureConfig: FailureConfig;
  private baseClient: any;

  constructor(baseClient: any, config: FailureConfig = {}) {
    this.baseClient = baseClient;
    this.failureConfig = config;
  }

  async executeWithChaos(operation: () => Promise<any>): Promise<any> {
    this.operationCount++;
    
    // Add latency
    if (this.failureConfig.latency) {
      const delay = Math.random() * 
        (this.failureConfig.latency.max - this.failureConfig.latency.min) + 
        this.failureConfig.latency.min;
      await sleep(delay);
    }

    // Progressive failure
    let failureRate = this.failureConfig.failureRate || 0;
    if (this.failureConfig.progressiveFailure) {
      failureRate = Math.min(1, failureRate + (this.operationCount * 0.01));
    }

    // Inject failure
    if (Math.random() < failureRate) {
      switch (this.failureConfig.errorType) {
        case 'network':
          throw new Error('ECONNREFUSED');
        case 'timeout':
          await sleep(10000); // Cause timeout
          throw new Error('Operation timeout');
        case 'corruption':
          return Math.random() > 0.5 ? null : 'corrupted_data';
        case 'byzantine':
          // Return incorrect but valid-looking data
          return Math.random() > 0.5 ? 1 : 0;
        default:
          throw new Error('Chaos injection failure');
      }
    }

    return operation();
  }

  // Proxy all methods through chaos injection
  evalsha(...args: any[]) {
    return this.executeWithChaos(() => this.baseClient.evalsha(...args));
  }

  eval(...args: any[]) {
    return this.executeWithChaos(() => this.baseClient.eval(...args));
  }

  get options() {
    return this.baseClient.options;
  }
}

// Create chaos-enabled mock clients
export const createChaosClients = (
  count: number, 
  chaosConfig: FailureConfig | FailureConfig[] = {}
): any[] => {
  const configs = Array.isArray(chaosConfig) ? chaosConfig : 
    Array(count).fill(chaosConfig);
  
  return createMockRedisClients(count).map((client, i) => 
    new ChaosClient(client, configs[i] || {})
  );
};

export const createMockRedisClient = () => {
  const storage = new Map<string, { value: string; expiration?: number }>();
  
  return {
    options: { host: 'localhost', port: 6379 },
    storage,
    
    // Basic Redis commands
    set: vi.fn().mockImplementation((key: string, value: string, ...args: any[]) => {
      const entry = { value };
      
      // Handle PX (milliseconds) expiration
      if (args.includes('PX')) {
        const pxIndex = args.indexOf('PX');
        const ttl = args[pxIndex + 1];
        (entry as any).expiration = Date.now() + ttl;
      }
      
      // Handle NX (set if not exists)
      if (args.includes('NX') && storage.has(key)) {
        return null;
      }
      
      storage.set(key, entry);
      return 'OK';
    }),
    
    get: vi.fn().mockImplementation((key: string) => {
      const entry = storage.get(key);
      if (!entry) return null;
      
      // Check expiration
      if (entry.expiration && Date.now() > entry.expiration) {
        storage.delete(key);
        return null;
      }
      
      return entry.value;
    }),
    
    del: vi.fn().mockImplementation((...keys: string[]) => {
      let count = 0;
      for (const key of keys) {
        if (storage.delete(key)) count++;
      }
      return count;
    }),
    
    exists: vi.fn().mockImplementation((key: string) => {
      const entry = storage.get(key);
      if (!entry) return 0;
      
      // Check expiration
      if (entry.expiration && Date.now() > entry.expiration) {
        storage.delete(key);
        return 0;
      }
      
      return 1;
    }),
    
    pttl: vi.fn().mockImplementation((key: string) => {
      const entry = storage.get(key);
      if (!entry) return -2; // Key doesn't exist
      if (!entry.expiration) return -1; // No expiration
      
      const remaining = entry.expiration - Date.now();
      return remaining > 0 ? remaining : -2;
    }),
    
    // Script commands
    eval: vi.fn(),
    evalsha: vi.fn(),
    script: vi.fn().mockImplementation((command: string) => {
      if (command === 'LOAD') return 'sha1hash';
      return 'OK';
    }),

    // SCAN command for iterative key scanning
    scan: vi.fn().mockImplementation(() => {
      return ['0', []]; // Return cursor "0" (done) and empty keys
    }),
    
    // Utility methods
    flushall: vi.fn().mockImplementation(() => {
      storage.clear();
      return 'OK';
    }),
    
    // Pub/Sub commands
    publish: vi.fn().mockResolvedValue(0),
    subscribe: vi.fn().mockResolvedValue(undefined),
    unsubscribe: vi.fn().mockResolvedValue(undefined),
    on: vi.fn(),
    removeListener: vi.fn(),
    duplicate: vi.fn().mockImplementation(function (this: ReturnType<typeof createMockRedisClient>) {
      const dup = createMockRedisClient();
      dup.options = { ...this.options };
      return dup;
    }),

    disconnect: vi.fn(),
    quit: vi.fn()
  };
};

// Mock Redis clients for tests
export const createMockRedisClients = (count: number = 3) => {
  return Array.from({ length: count }, (_, i) => {
    const client = createMockRedisClient();
    client.options.port = 6379 + i;
    
    // Setup default successful responses for evalsha
    client.evalsha.mockImplementation(async () => {
      await sleep(1); // Minimal latency
      return 1; // Success
    });
    
    return client;
  });
};

// Default test-friendly RedlockToolkit config
export const createTestRedlockToolkitConfig = (clients: any[], overrides: any = {}) => ({
  clients,
  circuitBreaker: {
    failureThreshold: 1000, // Very high threshold to avoid circuit breaking in tests
    resetTimeout: 60000,
    maxRetries: 3,
    operationTimeout: 5000
  },
  enableMetrics: true,
  ...overrides
});

// Global state tracking for cleanup
const globalRedlockToolkitInstances: any[] = [];

export function trackRedlockToolkitInstance(instance: any) {
  globalRedlockToolkitInstances.push(instance);
}

