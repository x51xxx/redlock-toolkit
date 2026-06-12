/**
 * Manages lock caching for performance optimization.
 */
import { LockCacheOptions, CachedLockInfo } from "../core/types";

export class CacheManager {
  private readonly lockCache = new Map<string, CachedLockInfo>();
  private cacheEnabled = false;
  private cacheOptions: LockCacheOptions = {};

  /**
   * Enable lock caching
   */
  enable(options: LockCacheOptions = {}): void {
    this.cacheEnabled = true;
    this.cacheOptions = {
      ttl: 300000, // 5 minutes default
      maxSize: 10000,
      strategy: 'lru',
      negativeCaching: false,
      ...options,
    };
  }

  /**
   * Disable lock caching
   */
  disable(): void {
    this.cacheEnabled = false;
    this.lockCache.clear();
  }

  /**
   * Cache a lock for performance
   */
  set(identifier: string, resources: string[], ttl: number): void {
    if (!this.cacheEnabled) return;

    const cacheKey = resources.join(':');
    const expiration = Date.now() + ttl;

    // Implement LRU eviction if cache is full
    if (this.lockCache.size >= (this.cacheOptions.maxSize || 10000)) {
      this.evict();
    }

    this.lockCache.set(cacheKey, {
      identifier,
      expiration,
      cachedAt: Date.now(),
      status: 'acquired',
    });
  }

  /**
   * Check cache for lock status
   */
  get(resources: string[]): CachedLockInfo | null {
    if (!this.cacheEnabled) return null;

    const cacheKey = resources.join(':');
    const cached = this.lockCache.get(cacheKey);

    if (!cached) return null;

    // Check if cache entry is expired
    if (Date.now() > cached.expiration) {
      this.lockCache.delete(cacheKey);
      return null;
    }

    return cached;
  }

  /**
   * Invalidate the cache entry for a resource set. Must be called whenever
   * the lock is released, otherwise a stale 'acquired' entry survives until
   * its TTL and misreports the lock as held.
   */
  invalidate(resources: string[]): void {
    if (!this.cacheEnabled) return;
    this.lockCache.delete(resources.join(':'));
  }

  /**
   * Evict cache entries based on strategy
   */
  private evict(): void {
    if (this.cacheOptions.strategy === 'lru') {
      // Simple LRU: remove oldest entries
      const entries = Array.from(this.lockCache.entries());
      entries.sort((a, b) => a[1].cachedAt - b[1].cachedAt);

      const toRemove = Math.floor(this.lockCache.size * 0.1); // Remove 10%
      for (let i = 0; i < toRemove; i++) {
        this.lockCache.delete(entries[i][0]);
      }
    } else {
      // Simple random eviction
      const keys = Array.from(this.lockCache.keys());
      const toRemove = Math.floor(keys.length * 0.1);
      for (let i = 0; i < toRemove; i++) {
        const randomIndex = Math.floor(Math.random() * keys.length);
        this.lockCache.delete(keys[randomIndex]);
        keys.splice(randomIndex, 1);
      }
    }
  }

  /**
   * Get cache statistics
   */
  getStats(): { size: number; hitRate: number; evictions: number } {
    return {
      size: this.lockCache.size,
      hitRate: 0, // Would need to track hits/misses for accurate rate
      evictions: 0, // Would need to track evictions
    };
  }
}
