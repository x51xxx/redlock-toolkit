/**
 * Lua Scripts for Atomic Redis Operations
 * Optimized scripts based on analysis of multiple implementations
 */

import { createHash } from "crypto";

/**
 * Script for acquiring locks with optional data storage
 */
export const ACQUIRE_SCRIPT = `
-- RedlockToolkit Acquire Script
-- KEYS[1..n]: lock keys
-- ARGV[1]: lock identifier (UUID)
-- ARGV[2]: TTL in milliseconds
-- ARGV[3]: data to store (JSON string, optional)

local identifier = ARGV[1]
local ttl = tonumber(ARGV[2])
local data = ARGV[3]

-- Check if any of the resources are already locked
for i = 1, #KEYS do
    local current = redis.call('GET', KEYS[i])
    if current and current ~= identifier then
        return 0  -- Lock exists and is held by different client
    end
end

-- Acquire all locks atomically
for i = 1, #KEYS do
    redis.call('SET', KEYS[i], identifier, 'PX', ttl)
    
    -- Store data if provided
    if data and data ~= '' then
        local dataKey = KEYS[i] .. ':data'
        redis.call('SET', dataKey, data, 'PX', ttl)
    end
end

return #KEYS  -- Return number of locks acquired
`;

/**
 * Script for extending locks
 */
export const EXTEND_SCRIPT = `
-- RedlockToolkit Extend Script
-- KEYS[1..n]: lock keys
-- ARGV[1]: lock identifier (UUID)
-- ARGV[2]: new TTL in milliseconds

local identifier = ARGV[1]
local ttl = tonumber(ARGV[2])

-- Verify ownership of all locks before extending
for i = 1, #KEYS do
    local current = redis.call('GET', KEYS[i])
    if current ~= identifier then
        return 0  -- Not owner of all locks
    end
end

-- Extend all locks atomically
for i = 1, #KEYS do
    redis.call('SET', KEYS[i], identifier, 'PX', ttl)
    
    -- Extend data TTL if exists (use PEXPIRE for millisecond precision)
    local dataKey = KEYS[i] .. ':data'
    if redis.call('EXISTS', dataKey) == 1 then
        redis.call('PEXPIRE', dataKey, ttl)
    end
end

return #KEYS  -- Return number of locks extended
`;

/**
 * Script for releasing locks
 */
export const RELEASE_SCRIPT = `
-- RedlockToolkit Release Script
-- KEYS[1..n]: lock keys
-- ARGV[1]: lock identifier (UUID)

local identifier = ARGV[1]
local released = 0

-- Release only locks owned by this identifier
for i = 1, #KEYS do
    local current = redis.call('GET', KEYS[i])
    if current == identifier then
        redis.call('DEL', KEYS[i])
        
        -- Clean up data if exists
        local dataKey = KEYS[i] .. ':data'
        redis.call('DEL', dataKey)
        
        released = released + 1
    end
end

return released  -- Return number of locks released
`;

/**
 * Script for force releasing locks (admin operation)
 */
export const FORCE_RELEASE_SCRIPT = `
-- RedlockToolkit Force Release Script
-- KEYS[1..n]: lock keys

local released = 0

for i = 1, #KEYS do
    if redis.call('EXISTS', KEYS[i]) == 1 then
        redis.call('DEL', KEYS[i])
        
        -- Clean up data if exists
        local dataKey = KEYS[i] .. ':data'
        redis.call('DEL', dataKey)
        
        released = released + 1
    end
end

return released  -- Return number of locks released
`;

/**
 * Script for checking lock status.
 * Returns a flat array: [key1, holder1_or_empty, ttl1, key2, holder2_or_empty, ttl2, ...]
 * Empty string for holder means the key is not locked.
 */
export const STATUS_SCRIPT = `
-- RedlockToolkit Status Script
-- KEYS[1..n]: lock keys
--
-- Returns flat array of triples: {key, holder_or_empty_string, ttl, ...}

local result = {}

for i = 1, #KEYS do
    local key = KEYS[i]
    local value = redis.call('GET', key)
    local ttl = redis.call('PTTL', key)

    result[#result + 1] = key
    if value then
        result[#result + 1] = value
    else
        result[#result + 1] = ''
    end
    result[#result + 1] = ttl
end

return result
`;

/**
 * Script for acquiring locks with wait-if-locked functionality.
 * Returns a flat array:
 *   [acquiredCount, waitTime, blockerCount, key1, holder1, ttl1, key2, holder2, ttl2, ...]
 */
export const ACQUIRE_WITH_WAIT_SCRIPT = `
-- RedlockToolkit Acquire with Wait Script
-- KEYS[1..n]: lock keys
-- ARGV[1]: lock identifier (UUID)
-- ARGV[2]: TTL in milliseconds
-- ARGV[3]: max wait time in milliseconds
-- ARGV[4]: data to store (JSON string, optional)
--
-- Returns flat array:
--   {acquiredCount, waitTime, blockerCount, key1, holder1, ttl1, ...}

local identifier = ARGV[1]
local ttl = tonumber(ARGV[2])
local maxWait = tonumber(ARGV[3])
local data = ARGV[4]

-- Check current lock status
local blockers = {}
local minTtl = maxWait

for i = 1, #KEYS do
    local current = redis.call('GET', KEYS[i])
    if current and current ~= identifier then
        local keyTtl = redis.call('PTTL', KEYS[i])
        if keyTtl > 0 and keyTtl < minTtl then
            minTtl = keyTtl
        end
        blockers[#blockers + 1] = KEYS[i]
        blockers[#blockers + 1] = current
        blockers[#blockers + 1] = keyTtl
    end
end

local blockerCount = #blockers / 3

-- If blocked and min TTL is within wait time, return wait suggestion
if blockerCount > 0 and minTtl <= maxWait then
    local result = {0, minTtl, blockerCount}
    for i = 1, #blockers do
        result[#result + 1] = blockers[i]
    end
    return result
end

-- If blocked but TTL too long, fail immediately
if blockerCount > 0 then
    local result = {0, -1, blockerCount}
    for i = 1, #blockers do
        result[#result + 1] = blockers[i]
    end
    return result
end

-- Acquire all locks
for i = 1, #KEYS do
    redis.call('SET', KEYS[i], identifier, 'PX', ttl)

    if data and data ~= '' then
        local dataKey = KEYS[i] .. ':data'
        redis.call('SET', dataKey, data, 'PX', ttl)
    end
end

return {#KEYS, 0, 0}
`;

/**
 * Script for cleaning up specific keys that have no TTL (persistent locks).
 * Called from Node.js after scanning keys in batches.
 * KEYS[1..n]: specific keys to check
 * Returns: number of keys deleted
 */
export const CLEANUP_CHECK_SCRIPT = `
-- RedlockToolkit Cleanup Check Script
-- KEYS[1..n]: specific keys to check and clean
local cleaned = 0

for i = 1, #KEYS do
    local ttl = redis.call('PTTL', KEYS[i])
    if ttl == -1 then
        redis.call('DEL', KEYS[i])
        cleaned = cleaned + 1
    end
end

return cleaned
`;

/**
 * Script for optimistic locking with version control
 */
export const OPTIMISTIC_ACQUIRE_SCRIPT = `
-- RedlockToolkit Optimistic Acquire Script
-- KEYS[1..n]: lock keys
-- ARGV[1]: lock identifier (UUID)
-- ARGV[2]: TTL in milliseconds
-- ARGV[3]: expected version (optional)
-- ARGV[4]: expected value (JSON, optional)
--
-- Returns array:
--   On conflict: {0, currentVersion, 'reason'}
--   On success:  {1, newVersion, acquiredCount}

local identifier = ARGV[1]
local ttl = tonumber(ARGV[2])
local expectedVersion = (ARGV[3] ~= nil and ARGV[3] ~= '') and tonumber(ARGV[3]) or nil
local expectedValue = ARGV[4]

-- Check version and value constraints for optimistic locking
for i = 1, #KEYS do
    local key = KEYS[i]
    local current = redis.call('GET', key)
    local versionKey = key .. ':version'
    local currentVersion = redis.call('GET', versionKey)
    local currentVersionNum = currentVersion and tonumber(currentVersion) or 0

    -- If key exists and is locked by someone else
    if current and current ~= identifier then
        return {0, currentVersionNum, 'locked'}
    end

    -- Check version constraint
    if expectedVersion and currentVersion then
        local ver = tonumber(currentVersion)
        if ver and ver ~= expectedVersion then
            return {0, ver, 'version_mismatch'}
        end
    end

    -- Check value constraint
    if expectedValue and expectedValue ~= '' then
        local valueKey = key .. ':value'
        local currentValue = redis.call('GET', valueKey)
        if currentValue and currentValue ~= expectedValue then
            return {0, currentVersionNum, 'value_mismatch'}
        end
    end
end

-- Acquire all locks with version increment
-- Use current server-side version as base to prevent rollback
local baseVersion = 0
if expectedVersion then
    baseVersion = expectedVersion
else
    -- If version already exists but no expectedVersion was provided,
    -- this is a conflict - the caller must provide expectedVersion for existing resources
    local firstVersionKey = KEYS[1] .. ':version'
    local existingVersion = redis.call('GET', firstVersionKey)
    if existingVersion then
        return {0, tonumber(existingVersion) or 0, 'version_required'}
    end
end
local newVersion = baseVersion + 1

for i = 1, #KEYS do
    local key = KEYS[i]
    redis.call('SET', KEYS[i], identifier, 'PX', ttl)

    -- Update version
    local versionKey = key .. ':version'
    redis.call('SET', versionKey, tostring(newVersion), 'PX', ttl)

    -- Store expected value if provided
    if expectedValue and expectedValue ~= '' then
        local valueKey = key .. ':value'
        redis.call('SET', valueKey, expectedValue, 'PX', ttl)
    end
end

return {1, newVersion, #KEYS}
`;

/**
 * Script for optimistic update with conflict detection
 */
export const OPTIMISTIC_UPDATE_SCRIPT = `
-- RedlockToolkit Optimistic Update Script
-- KEYS[1..n]: lock keys
-- ARGV[1]: lock identifier (UUID)
-- ARGV[2]: new TTL in milliseconds
-- ARGV[3]: expected version
-- ARGV[4]: new value (JSON, optional)
--
-- Returns array:
--   On conflict: {0, currentVersion, 'reason'}
--   On success:  {1, newVersion, updatedCount}

local identifier = ARGV[1]
local ttl = tonumber(ARGV[2])
local expectedVersion = tonumber(ARGV[3])
local newValue = ARGV[4]

-- Verify ownership and version
for i = 1, #KEYS do
    local key = KEYS[i]
    local current = redis.call('GET', key)
    local versionKey = key .. ':version'
    local currentVersion = redis.call('GET', versionKey)

    -- Check ownership
    if not current or current ~= identifier then
        local currentVersionNum = currentVersion and tonumber(currentVersion) or 0
        return {0, currentVersionNum, 'not_owner'}
    end

    -- Check version
    if currentVersion then
        local ver = tonumber(currentVersion)
        if ver and ver ~= expectedVersion then
            return {0, ver, 'version_mismatch'}
        end
    end
end

-- Update all locks with new version
local newVersion = expectedVersion + 1
for i = 1, #KEYS do
    local key = KEYS[i]
    redis.call('SET', KEYS[i], identifier, 'PX', ttl)

    -- Update version
    local versionKey = key .. ':version'
    redis.call('SET', versionKey, tostring(newVersion), 'PX', ttl)

    -- Update value if provided
    if newValue and newValue ~= '' then
        local valueKey = key .. ':value'
        redis.call('SET', valueKey, newValue, 'PX', ttl)
    end
end

return {1, newVersion, #KEYS}
`;

/**
 * Script for acquiring a semaphore permit.
 * Uses a Sorted Set where member=identifier, score=expirationTimestamp.
 */
export const SEMAPHORE_ACQUIRE_SCRIPT = `
-- RedlockToolkit Semaphore Acquire Script
-- KEYS[1]: semaphore key (ZSET)
-- ARGV[1]: identifier (UUID for this permit holder)
-- ARGV[2]: TTL in milliseconds
-- ARGV[3]: max permits

local key = KEYS[1]
local identifier = ARGV[1]
local ttl = tonumber(ARGV[2])
local maxPermits = tonumber(ARGV[3])
-- Use Redis server time to avoid client clock skew
local timeResult = redis.call('TIME')
local now = tonumber(timeResult[1]) * 1000 + math.floor(tonumber(timeResult[2]) / 1000)

-- Remove expired permits
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)

-- Check if already holding a permit (idempotent re-acquire)
local existingScore = redis.call('ZSCORE', key, identifier)
if existingScore then
    redis.call('ZADD', key, now + ttl, identifier)
    local count = redis.call('ZCARD', key)
    return {1, count, maxPermits}
end

-- Check available permits
local currentCount = redis.call('ZCARD', key)
if currentCount < maxPermits then
    redis.call('ZADD', key, now + ttl, identifier)
    redis.call('PEXPIRE', key, ttl * 2)
    return {1, currentCount + 1, maxPermits}
end

return {0, currentCount, maxPermits}
`;

/**
 * Script for releasing a semaphore permit.
 */
export const SEMAPHORE_RELEASE_SCRIPT = `
-- RedlockToolkit Semaphore Release Script
-- KEYS[1]: semaphore key (ZSET)
-- ARGV[1]: identifier

local key = KEYS[1]
local identifier = ARGV[1]
-- Use Redis server time to avoid client clock skew
local timeResult = redis.call('TIME')
local now = tonumber(timeResult[1]) * 1000 + math.floor(tonumber(timeResult[2]) / 1000)

redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
local removed = redis.call('ZREM', key, identifier)
local remaining = redis.call('ZCARD', key)

return {removed, remaining}
`;

/**
 * Script for extending a semaphore permit.
 */
export const SEMAPHORE_EXTEND_SCRIPT = `
-- RedlockToolkit Semaphore Extend Script
-- KEYS[1]: semaphore key (ZSET)
-- ARGV[1]: identifier
-- ARGV[2]: TTL in milliseconds

local key = KEYS[1]
local identifier = ARGV[1]
local ttl = tonumber(ARGV[2])
-- Use Redis server time to avoid client clock skew
local timeResult = redis.call('TIME')
local now = tonumber(timeResult[1]) * 1000 + math.floor(tonumber(timeResult[2]) / 1000)

local existingScore = redis.call('ZSCORE', key, identifier)
if not existingScore then
    return 0
end

if tonumber(existingScore) < now then
    redis.call('ZREM', key, identifier)
    return 0
end

redis.call('ZADD', key, now + ttl, identifier)
redis.call('PEXPIRE', key, ttl * 2)
return 1
`;

/**
 * Script for checking semaphore status.
 */
export const SEMAPHORE_STATUS_SCRIPT = `
-- RedlockToolkit Semaphore Status Script
-- KEYS[1]: semaphore key (ZSET)
-- ARGV[1]: max permits

local key = KEYS[1]
-- Use Redis server time to avoid client clock skew
local timeResult = redis.call('TIME')
local now = tonumber(timeResult[1]) * 1000 + math.floor(tonumber(timeResult[2]) / 1000)
local maxPermits = tonumber(ARGV[1])

redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
local members = redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
local count = #members / 2

local result = {count, maxPermits}
for i = 1, #members, 2 do
    result[#result + 1] = members[i]
    result[#result + 1] = members[i + 1]
end
return result
`;

/**
 * Script for creating a CountDownLatch.
 */
export const LATCH_CREATE_SCRIPT = `
-- RedlockToolkit Latch Create Script
-- KEYS[1]: latch count key
-- KEYS[2]: latch target key
-- ARGV[1]: count (N)
-- ARGV[2]: TTL in milliseconds

local countKey = KEYS[1]
local targetKey = KEYS[2]
local count = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])

local existing = redis.call('GET', countKey)
if existing then
    return 0
end

redis.call('SET', countKey, count, 'PX', ttl)
redis.call('SET', targetKey, count, 'PX', ttl)
return 1
`;

/**
 * Script for counting down a latch (idempotent per eventId).
 */
export const LATCH_COUNTDOWN_SCRIPT = `
-- RedlockToolkit Latch CountDown Script
-- KEYS[1]: latch count key
-- KEYS[2]: latch events set key
-- ARGV[1]: event identifier (for idempotency)
-- ARGV[2]: pub/sub channel name

local countKey = KEYS[1]
local eventsKey = KEYS[2]
local eventId = ARGV[1]
local channel = ARGV[2]

local current = redis.call('GET', countKey)
if not current then
    return {-1, 0, 0}
end

local currentCount = tonumber(current)
if currentCount <= 0 then
    return {0, 0, -1}
end

-- Idempotency check using SET (O(1) vs O(n) with LPOS)
if redis.call('SISMEMBER', eventsKey, eventId) == 1 then
    return {currentCount, 0, -2}
end

local newCount = redis.call('DECR', countKey)

redis.call('SADD', eventsKey, eventId)
local ttl = redis.call('PTTL', countKey)
if ttl > 0 then
    redis.call('PEXPIRE', eventsKey, ttl)
end

if newCount <= 0 then
    if channel and channel ~= '' then
        redis.call('PUBLISH', channel, 'latch_complete')
    end
    return {0, 0, 2}
end

return {newCount, 0, 1}
`;

/**
 * Script for checking latch status.
 */
export const LATCH_STATUS_SCRIPT = `
-- RedlockToolkit Latch Status Script
-- KEYS[1]: latch count key
-- KEYS[2]: latch target key

local countKey = KEYS[1]
local targetKey = KEYS[2]

local current = redis.call('GET', countKey)
if not current then
    return {-1, -1, -1}
end

local target = redis.call('GET', targetKey)
local ttl = redis.call('PTTL', countKey)

return {tonumber(current), tonumber(target or 0), ttl}
`;

/**
 * Pre-computed script hashes for performance
 */
export const SCRIPT_HASHES = {
  acquire: createHash("sha1").update(ACQUIRE_SCRIPT).digest("hex"),
  extend: createHash("sha1").update(EXTEND_SCRIPT).digest("hex"),
  release: createHash("sha1").update(RELEASE_SCRIPT).digest("hex"),
  forceRelease: createHash("sha1").update(FORCE_RELEASE_SCRIPT).digest("hex"),
  status: createHash("sha1").update(STATUS_SCRIPT).digest("hex"),
  acquireWithWait: createHash("sha1")
    .update(ACQUIRE_WITH_WAIT_SCRIPT)
    .digest("hex"),
  cleanupCheck: createHash("sha1").update(CLEANUP_CHECK_SCRIPT).digest("hex"),
  optimisticAcquire: createHash("sha1")
    .update(OPTIMISTIC_ACQUIRE_SCRIPT)
    .digest("hex"),
  optimisticUpdate: createHash("sha1")
    .update(OPTIMISTIC_UPDATE_SCRIPT)
    .digest("hex"),
  semaphoreAcquire: createHash("sha1").update(SEMAPHORE_ACQUIRE_SCRIPT).digest("hex"),
  semaphoreRelease: createHash("sha1").update(SEMAPHORE_RELEASE_SCRIPT).digest("hex"),
  semaphoreExtend: createHash("sha1").update(SEMAPHORE_EXTEND_SCRIPT).digest("hex"),
  semaphoreStatus: createHash("sha1").update(SEMAPHORE_STATUS_SCRIPT).digest("hex"),
  latchCreate: createHash("sha1").update(LATCH_CREATE_SCRIPT).digest("hex"),
  latchCountDown: createHash("sha1").update(LATCH_COUNTDOWN_SCRIPT).digest("hex"),
  latchStatus: createHash("sha1").update(LATCH_STATUS_SCRIPT).digest("hex"),
};

/**
 * Script definition with hash
 */
export interface LuaScript {
  source: string;
  hash: string;
}

/**
 * All available scripts
 */
export const SCRIPTS: Record<string, LuaScript> = {
  acquire: {
    source: ACQUIRE_SCRIPT,
    hash: SCRIPT_HASHES.acquire,
  },
  extend: {
    source: EXTEND_SCRIPT,
    hash: SCRIPT_HASHES.extend,
  },
  release: {
    source: RELEASE_SCRIPT,
    hash: SCRIPT_HASHES.release,
  },
  forceRelease: {
    source: FORCE_RELEASE_SCRIPT,
    hash: SCRIPT_HASHES.forceRelease,
  },
  status: {
    source: STATUS_SCRIPT,
    hash: SCRIPT_HASHES.status,
  },
  acquireWithWait: {
    source: ACQUIRE_WITH_WAIT_SCRIPT,
    hash: SCRIPT_HASHES.acquireWithWait,
  },
  cleanupCheck: {
    source: CLEANUP_CHECK_SCRIPT,
    hash: SCRIPT_HASHES.cleanupCheck,
  },
  optimisticAcquire: {
    source: OPTIMISTIC_ACQUIRE_SCRIPT,
    hash: SCRIPT_HASHES.optimisticAcquire,
  },
  optimisticUpdate: {
    source: OPTIMISTIC_UPDATE_SCRIPT,
    hash: SCRIPT_HASHES.optimisticUpdate,
  },
  semaphoreAcquire: {
    source: SEMAPHORE_ACQUIRE_SCRIPT,
    hash: SCRIPT_HASHES.semaphoreAcquire,
  },
  semaphoreRelease: {
    source: SEMAPHORE_RELEASE_SCRIPT,
    hash: SCRIPT_HASHES.semaphoreRelease,
  },
  semaphoreExtend: {
    source: SEMAPHORE_EXTEND_SCRIPT,
    hash: SCRIPT_HASHES.semaphoreExtend,
  },
  semaphoreStatus: {
    source: SEMAPHORE_STATUS_SCRIPT,
    hash: SCRIPT_HASHES.semaphoreStatus,
  },
  latchCreate: {
    source: LATCH_CREATE_SCRIPT,
    hash: SCRIPT_HASHES.latchCreate,
  },
  latchCountDown: {
    source: LATCH_COUNTDOWN_SCRIPT,
    hash: SCRIPT_HASHES.latchCountDown,
  },
  latchStatus: {
    source: LATCH_STATUS_SCRIPT,
    hash: SCRIPT_HASHES.latchStatus,
  },
};

/**
 * Export raw scripts for compatibility
 */
export const scripts = {
  acquire: ACQUIRE_SCRIPT,
  extend: EXTEND_SCRIPT,
  release: RELEASE_SCRIPT,
  forceRelease: FORCE_RELEASE_SCRIPT,
  status: STATUS_SCRIPT,
  acquireWithWait: ACQUIRE_WITH_WAIT_SCRIPT,
  cleanupCheck: CLEANUP_CHECK_SCRIPT,
  optimisticAcquire: OPTIMISTIC_ACQUIRE_SCRIPT,
  optimisticUpdate: OPTIMISTIC_UPDATE_SCRIPT,
  semaphoreAcquire: SEMAPHORE_ACQUIRE_SCRIPT,
  semaphoreRelease: SEMAPHORE_RELEASE_SCRIPT,
  semaphoreExtend: SEMAPHORE_EXTEND_SCRIPT,
  semaphoreStatus: SEMAPHORE_STATUS_SCRIPT,
  latchCreate: LATCH_CREATE_SCRIPT,
  latchCountDown: LATCH_COUNTDOWN_SCRIPT,
  latchStatus: LATCH_STATUS_SCRIPT,
};

/**
 * Utility function to calculate script hash
 */
export function calculateScriptHash(script: string): string {
  return createHash("sha1").update(script).digest("hex");
}

/**
 * Validate that pre-computed hashes are correct
 */
export function validateScriptHashes(): boolean {
  return Object.entries(SCRIPTS).every(([, script]) => {
    const computedHash = calculateScriptHash(script.source);
    return computedHash === script.hash;
  });
}

/**
 * Get script by name with runtime validation
 */
export function getScript(name: keyof typeof SCRIPTS): LuaScript {
  const script = SCRIPTS[name];
  if (!script) {
    throw new Error(`Unknown script: ${name}`);
  }

  // Validate hash matches source
  const computedHash = calculateScriptHash(script.source);
  if (computedHash !== script.hash) {
    throw new Error(
      `Script hash mismatch for ${name}: expected ${script.hash}, got ${computedHash}`,
    );
  }

  return script;
}
