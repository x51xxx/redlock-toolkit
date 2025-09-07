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
    
    -- Extend data TTL if exists
    local dataKey = KEYS[i] .. ':data'
    if redis.call('EXISTS', dataKey) == 1 then
        redis.call('EXPIRE', dataKey, math.floor(ttl / 1000))
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
 * Script for checking lock status
 */
export const STATUS_SCRIPT = `
-- RedlockToolkit Status Script
-- KEYS[1..n]: lock keys

local result = {}

for i = 1, #KEYS do
    local key = KEYS[i]
    local value = redis.call('GET', key)
    local ttl = redis.call('PTTL', key)
    
    if value then
        result[#result + 1] = {
            key = key,
            holder = value,
            ttl = ttl
        }
    else
        result[#result + 1] = {
            key = key,
            holder = nil,
            ttl = -2  -- Key doesn't exist
        }
    end
end

return cjson.encode(result)
`;

/**
 * Script for acquiring locks with wait-if-locked functionality
 */
export const ACQUIRE_WITH_WAIT_SCRIPT = `
-- RedlockToolkit Acquire with Wait Script
-- KEYS[1..n]: lock keys
-- ARGV[1]: lock identifier (UUID)
-- ARGV[2]: TTL in milliseconds
-- ARGV[3]: max wait time in milliseconds
-- ARGV[4]: data to store (JSON string, optional)

local identifier = ARGV[1]
local ttl = tonumber(ARGV[2])
local maxWait = tonumber(ARGV[3])
local data = ARGV[4]

-- Check current lock status
local blockedBy = {}
local minTtl = maxWait

for i = 1, #KEYS do
    local current = redis.call('GET', KEYS[i])
    if current and current ~= identifier then
        local keyTtl = redis.call('PTTL', KEYS[i])
        if keyTtl > 0 and keyTtl < minTtl then
            minTtl = keyTtl
        end
        blockedBy[#blockedBy + 1] = {
            key = KEYS[i],
            holder = current,
            ttl = keyTtl
        }
    end
end

-- If blocked and min TTL is within wait time, return wait suggestion
if #blockedBy > 0 and minTtl <= maxWait then
    return {
        acquired = 0,
        waitTime = minTtl,
        blockedBy = blockedBy
    }
end

-- If blocked but TTL too long, fail immediately
if #blockedBy > 0 then
    return {
        acquired = 0,
        waitTime = -1,
        blockedBy = blockedBy
    }
end

-- Acquire all locks
for i = 1, #KEYS do
    redis.call('SET', KEYS[i], identifier, 'PX', ttl)
    
    if data and data ~= '' then
        local dataKey = KEYS[i] .. ':data'
        redis.call('SET', dataKey, data, 'PX', ttl)
    end
end

return {
    acquired = #KEYS,
    waitTime = 0,
    blockedBy = {}
}
`;

/**
 * Script for cleaning up expired locks (maintenance)
 */
export const CLEANUP_SCRIPT = `
-- RedlockToolkit Cleanup Script
-- KEYS[1]: pattern for lock keys (e.g., "locks:*")
-- ARGV[1]: batch size (default 100)

local pattern = KEYS[1]
local batchSize = tonumber(ARGV[1]) or 100
local cursor = 0
local cleaned = 0

repeat
    local result = redis.call('SCAN', cursor, 'MATCH', pattern, 'COUNT', batchSize)
    cursor = tonumber(result[1])
    local keys = result[2]

    for i = 1, #keys do
        local key = keys[i]
        local ttl = redis.call('PTTL', key)

        -- Clean up expired or invalid locks
        if ttl == -2 then  -- Key doesn't exist
            -- Already cleaned
        elseif ttl == -1 then  -- Key exists but no TTL
            redis.call('DEL', key)  -- Remove persistent locks
            cleaned = cleaned + 1
        end
    end

until cursor == 0

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

local identifier = ARGV[1]
local ttl = tonumber(ARGV[2])
local expectedVersion = ARGV[3] and tonumber(ARGV[3]) or nil
local expectedValue = ARGV[4]

-- Check version and value constraints for optimistic locking
for i = 1, #KEYS do
    local key = KEYS[i]
    local current = redis.call('GET', key)
    local versionKey = key .. ':version'
    local currentVersion = redis.call('GET', versionKey)

    -- If key exists and is locked by someone else
    if current and current ~= identifier then
        return { conflict = true, currentVersion = currentVersion, reason = 'locked' }
    end

    -- Check version constraint
    if expectedVersion and currentVersion then
        local ver = tonumber(currentVersion)
        if ver and ver ~= expectedVersion then
            return { conflict = true, currentVersion = ver, reason = 'version_mismatch' }
        end
    end

    -- Check value constraint
    if expectedValue and expectedValue ~= '' then
        local valueKey = key .. ':value'
        local currentValue = redis.call('GET', valueKey)
        if currentValue and currentValue ~= expectedValue then
            return { conflict = true, currentVersion = currentVersion, reason = 'value_mismatch' }
        end
    end
end

-- Acquire all locks with version increment
local newVersion = (expectedVersion or 0) + 1
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

return { success = true, newVersion = newVersion, acquired = #KEYS }
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
        return { conflict = true, reason = 'not_owner' }
    end

    -- Check version
    if currentVersion then
        local ver = tonumber(currentVersion)
        if ver and ver ~= expectedVersion then
            return { conflict = true, currentVersion = ver, reason = 'version_mismatch' }
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

return { success = true, newVersion = newVersion, updated = #KEYS }
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
  cleanup: createHash("sha1").update(CLEANUP_SCRIPT).digest("hex"),
  optimisticAcquire: createHash("sha1")
    .update(OPTIMISTIC_ACQUIRE_SCRIPT)
    .digest("hex"),
  optimisticUpdate: createHash("sha1")
    .update(OPTIMISTIC_UPDATE_SCRIPT)
    .digest("hex"),
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
  cleanup: {
    source: CLEANUP_SCRIPT,
    hash: SCRIPT_HASHES.cleanup,
  },
  optimisticAcquire: {
    source: OPTIMISTIC_ACQUIRE_SCRIPT,
    hash: SCRIPT_HASHES.optimisticAcquire,
  },
  optimisticUpdate: {
    source: OPTIMISTIC_UPDATE_SCRIPT,
    hash: SCRIPT_HASHES.optimisticUpdate,
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
  cleanup: CLEANUP_SCRIPT,
  optimisticAcquire: OPTIMISTIC_ACQUIRE_SCRIPT,
  optimisticUpdate: OPTIMISTIC_UPDATE_SCRIPT,
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
