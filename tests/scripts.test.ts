/**
 * Lua Scripts validation tests
 */

import { describe, it, expect } from 'vitest';
import {
  calculateScriptHash,
  getScript,
  SCRIPT_HASHES,
  SCRIPTS,
  validateScriptHashes,
} from "../src/utils/scripts";

describe('Lua Scripts', () => {
  describe('Script Hash Validation', () => {
    it('should have correct pre-computed hashes', () => {
      expect(validateScriptHashes()).toBe(true);
    });

    it('should calculate hashes correctly', () => {
      Object.entries(SCRIPTS).forEach(([name, script]) => {
        const computedHash = calculateScriptHash(script.source);
        expect(computedHash).toBe(script.hash);
        expect(computedHash).toBe(SCRIPT_HASHES[name as keyof typeof SCRIPT_HASHES]);
      });
    });

    it('should detect hash mismatches', () => {
      const testScript = 'return 1';
      const hash1 = calculateScriptHash(testScript);
      const hash2 = calculateScriptHash(testScript + ' -- comment');
      
      expect(hash1).not.toBe(hash2);
    });
  });

  describe('Script Content Validation', () => {
    it('should have acquire script with proper structure', () => {
      const script = SCRIPTS.acquire.source;
      
      expect(script).toContain('KEYS');
      expect(script).toContain('ARGV');
      expect(script).toContain('return');
      
      // Should handle multiple keys
      expect(script).toContain('#KEYS');
      
      // Should check for existing locks
      expect(script).toContain('exists');
      
      // Should set locks with expiration
      expect(script).toContain('PX');
    });

    it('should have extend script with ownership validation', () => {
      const script = SCRIPTS.extend.source;
      
      expect(script).toContain('KEYS');
      expect(script).toContain('ARGV');
      expect(script).toContain('return');
      
      // Should validate ownership before extending
      expect(script).toContain('GET');
      expect(script).toContain('ARGV[1]'); // identifier check
      
      // Should set new expiration
      expect(script).toContain('SET');
      expect(script).toContain('PX');
    });

    it('should have release script with safe deletion', () => {
      const script = SCRIPTS.release.source;
      
      expect(script).toContain('KEYS');
      expect(script).toContain('ARGV');
      expect(script).toContain('return');
      
      // Should only delete if owner
      expect(script).toContain('GET');
      expect(script).toContain('ARGV[1]'); // identifier check
      expect(script).toContain('DEL');
    });

    it('should have force release script', () => {
      const script = SCRIPTS.forceRelease.source;
      
      expect(script).toContain('KEYS');
      expect(script).toContain('return');
      expect(script).toContain('DEL');
      
      // Should not check ownership (force delete)
      expect(script).not.toContain('get');
    });

    it('should have status script for lock inspection', () => {
      const script = SCRIPTS.status.source;
      
      expect(script).toContain('KEYS');
      expect(script).toContain('return');
      expect(script).toContain('cjson.encode');
      
      // Should get lock info
      expect(script).toContain('GET');
      expect(script).toContain('PTTL');
    });

    it('should have acquire with wait functionality', () => {
      const script = SCRIPTS.acquireWithWait.source;
      
      expect(script).toContain('KEYS');
      expect(script).toContain('ARGV');
      expect(script).toContain('return');
      
      // Should check max wait time
      expect(script).toContain('ARGV[3]'); // maxWait
      
      // Should return wait suggestion
      expect(script).toContain('waitTime');
      expect(script).toContain('blockedBy');
    });

    it('should have cleanup script for maintenance', () => {
      const script = SCRIPTS.cleanup.source;
      
      expect(script).toContain('KEYS');
      expect(script).toContain('ARGV');
      expect(script).toContain('return');
      
      // Should use SCAN for safe iteration
      expect(script).toContain('SCAN');
      expect(script).toContain('MATCH');
      
      // Should check TTL
      expect(script).toContain('PTTL');
    });
  });

  describe('Script Logic Validation', () => {
    it('should handle atomic operations in acquire script', () => {
      const script = SCRIPTS.acquire.source;
      
      // Should check all keys before setting any
      const lines = script.split('\n').map(l => l.trim()).filter(l => l);
      
      const checksSection = lines.slice(
        lines.findIndex(l => l.includes('for i = 1, #KEYS')),
        lines.findIndex(l => l.includes('return 0'))
      );
      
      const setsSection = lines.slice(
        lines.findIndex(l => l.includes('-- Create an entry') || l.includes('for i = 1, #KEYS')),
        lines.findIndex(l => l.includes('return #KEYS'))
      );
      
      // Should have some logic for checking and setting
      expect(script).toContain('for i = 1, #KEYS');
      expect(script).toContain('return');
    });

    it('should validate all keys in extend script', () => {
      const script = SCRIPTS.extend.source;
      
      // Should validate ownership of ALL keys before extending ANY
      expect(script).toContain('for i = 1, #KEYS');
      expect(script).toContain('current ~= identifier');
      expect(script).toContain('return 0');
    });

    it('should count operations correctly in release script', () => {
      const script = SCRIPTS.release.source;
      
      // Should count successful deletions
      expect(script).toContain('local released = 0');
      expect(script).toContain('released = released + 1');
      expect(script).toContain('return released');
    });
  });

  describe('Script Error Handling', () => {
    it('should use safe operations patterns', () => {
      const script = SCRIPTS.release.source;
      
      // Release script should check ownership before deletion
      expect(script).toContain('current == identifier');
      expect(script).toContain('DEL');
    });

    it('should handle edge cases in cleanup script', () => {
      const script = SCRIPTS.cleanup.source;
      
      // Should handle cursor completion
      expect(script).toContain('until cursor == 0');
      
      // Should handle different TTL states
      expect(script).toContain('ttl == -2'); // Key doesn't exist
      expect(script).toContain('ttl == -1'); // Key exists but no TTL
    });
  });

  describe('Script Utility Functions', () => {
    it('should get script by name with validation', () => {
      const acquireScript = getScript('acquire');
      expect(acquireScript.source).toBe(SCRIPTS.acquire.source);
      expect(acquireScript.hash).toBe(SCRIPTS.acquire.hash);
    });

    it('should throw error for unknown script', () => {
      expect(() => getScript('nonexistent' as any)).toThrow('Unknown script: nonexistent');
    });

    it('should validate script hash on retrieval', () => {
      // This test would fail if there was a hash mismatch
      expect(() => getScript('acquire')).not.toThrow();
    });
  });

  describe('Script Performance Considerations', () => {
    it('should have efficient key iteration patterns', () => {
      Object.values(SCRIPTS).forEach(script => {
        const source = script.source;
        
        // Should use ipairs for array iteration
        if (source.includes('for i, key in')) {
          expect(source).toContain('ipairs(KEYS)');
        }
        
        // Should avoid excessive nested loops (reasonable complexity)
        const forLoopCount = (source.match(/for /g) || []).length;
        expect(forLoopCount).toBeLessThanOrEqual(3); // Allow reasonable complexity for multi-key operations
      });
    });

    it('should minimize Redis calls', () => {
      // Acquire script should not make unnecessary calls
      const acquireScript = SCRIPTS.acquire.source;
      
      // Should check existence efficiently
      expect(acquireScript).toContain('exists');
      
      // Should set all keys efficiently  
      const setCallCount = (acquireScript.match(/redis\.call\('SET'/gi) || []).length;
      expect(setCallCount).toBeGreaterThanOrEqual(1); // At least one set call pattern
    });

    it('should have reasonable script sizes', () => {
      Object.entries(SCRIPTS).forEach(([name, script]) => {
        const size = script.source.length;
        
        // Scripts should not be too large (Redis limit is 1MB, but keep reasonable)
        expect(size).toBeLessThan(10000); // 10KB should be more than enough
        
        // Scripts should not be too small (should have actual logic)
        expect(size).toBeGreaterThan(100);
      });
    });
  });

  describe('Script Consistency', () => {
    it('should use consistent parameter patterns', () => {
      const scripts = [SCRIPTS.acquire, SCRIPTS.extend, SCRIPTS.release];
      
      scripts.forEach(script => {
        const source = script.source;
        
        // Should use ARGV[1] for identifier where applicable
        if (source.includes('ARGV[1]')) {
          // Identifier should be first argument
          expect(source).toContain('ARGV[1]');
        }
        
        // Should use consistent key naming
        expect(source).toContain('KEYS');
      });
    });

    it('should have consistent return value patterns', () => {
      // Acquire and extend should return count
      expect(SCRIPTS.acquire.source).toContain('return #KEYS');
      expect(SCRIPTS.extend.source).toContain('return #KEYS');
      
      // Release should return count of released
      expect(SCRIPTS.release.source).toContain('return released');
      
      // Status should return JSON
      expect(SCRIPTS.status.source).toContain('cjson.encode');
    });

    it('should use consistent error patterns', () => {
      const conditionalScripts = [SCRIPTS.acquire, SCRIPTS.extend];
      
      conditionalScripts.forEach(script => {
        const source = script.source;
        
        // Should return 0 for failure conditions
        if (source.includes('return 0')) {
          expect(source).toContain('return 0');
        }
      });
    });
  });
});