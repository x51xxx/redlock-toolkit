# Redlock Algorithm Implementation

## Overview

Redlock is a distributed locking algorithm for Redis, designed to provide reliable mutual exclusion guarantees in multi-process and multi-machine environments.

## Theoretical Foundations

### Redlock Algorithm Principles

1. **Consensus-based** - uses a quorum of Redis instances
2. **Clock drift tolerance** - accounts for time discrepancies between servers
3. **Fault tolerance** - operates even when a minority of Redis servers fail
4. **Retry mechanism** - automatic retries with jitter

### Core Guarantees

- Mutual exclusion
- Deadlock freedom
- Fault tolerance

## Implementation Architecture

### Lua Scripts

#### 1. ACQUIRE_SCRIPT
```lua
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
```

**Logic:**
- Checks whether any of the resources are already locked by a different client.
- If all resources are free or belong to the same client, acquires them atomically.
- Sets TTL and optionally stores associated data.
- Returns the number of acquired locks.

#### 2. EXTEND_SCRIPT
```lua
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
        redis.call('PEXPIRE', dataKey, ttl)
    end
end

return #KEYS  -- Return number of locks extended
```

**Logic:**
- Verifies ownership of all locks.
- Extends TTL only if all locks belong to the current client.
- Atomically updates the expiration time for all resources and associated data.

#### 3. RELEASE_SCRIPT
```lua
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
```

**Logic:**
- Deletes only locks belonging to the current client (compare-and-delete).
- Also deletes associated data.
- Returns the number of released resources.

## Configuration

### Default Parameters
```typescript
{
  driftFactor: 0.01,                    // 1% of TTL for clock drift
  retryCount: 0,                        // no retries by default (deterministic)
  retryDelay: 200,                      // 200ms between attempts
  retryJitter: 100,                     // +/-100ms random delay
  autoExtendThreshold: 500              // auto-extension 500ms before expiration
}
```

### Drift Calculation
```typescript
const drift = Math.round(driftFactor * duration) + 2;
const expiration = start + duration - drift;
```

- `+2ms` for Redis expires precision (1ms) + safety margin
- Accounts for possible time discrepancies between servers

## Operation Execution Algorithm

### Quorum System

```typescript
const quorumSize = Math.floor(clientsCount / 2) + 1;
```

**Success Conditions:**
- The operation succeeds if a quorum of clients voted "for"
- The operation fails if a quorum of clients voted "against"

### Retry Logic

```typescript
const maxAttempts = retryCount === -1 ? Infinity : retryCount + 1;

// Random delay between attempts
const delay = retryDelay + Math.floor((Math.random() * 2 - 1) * retryJitter);
```

**Features:**
- Jittered retry delay with error-based scaling to reduce contention
- Option for unlimited retries (-1)
- Fail-fast upon reaching a quorum "against"

## Lock Class API

### Core Methods

```typescript
// Release the lock
await lock.release(): Promise<LockReleaseResult>

// Extend the lock
await lock.extend(ttl?: number, options?: Partial<LockOptions>): Promise<Lock>
```

### Properties
- `resources: string[]` - locked resources
- `identifier: string` - unique lock identifier
- `expiration: number` - expiration timestamp
- `isValid: boolean` - whether the lock is valid (not expired and not released)
- `isExpired: boolean` - whether the lock has expired
- `released: boolean` - whether the lock has been released
- `timeToExpiration: number` - time until expiration (milliseconds)
- `extensions: number` - number of lock extensions performed

## Using API (Auto-extending Locks)

### Usage Example
```typescript
await toolkit.using([senderId, recipientId], async (signal) => {
  // Perform an operation under the lock
  await performOperation();

  // Check if the lock is still active
  if (signal.aborted) {
    throw signal.error;
  }

  // Continue the operation
  await performAnotherOperation();
}, { ttl: 5000, autoExtendThreshold: 1000 });
```

### Automatic Extension
- Automatically extends the lock until the routine completes
- Uses `LockSignal` to notify about problems (check `signal.aborted`)
- Uses recursive `setTimeout` (not `setInterval`) to prevent request accumulation
- Triggers extension when `remainingTTL <= autoExtendThreshold`

## Error Handling

### Error Types

1. **ResourceLockedError** - the resource is already locked
2. **ConsensusError** - failed to reach quorum
3. **LockTimeoutError** - lock acquisition timed out after all retries
4. **LockExtensionError** - failed to extend the lock
5. **CircuitBreakerOpenError** - circuit breaker is open due to failures
6. **Standard Errors** - network and other system errors

### Event Emitter
```typescript
toolkit.on("error", (error) => {
  // Log errors for monitoring
  if (!(error instanceof ResourceLockedError)) {
    console.error(error);
  }
});
```

## Optimizations

### Script Caching
- Uses `EVALSHA` for cached scripts
- Falls back to `EVAL` if the script is not found
- SHA1 hashing for script identification

### Parallel Execution
- Parallel execution across all Redis clients
- Early return upon reaching quorum
- Asynchronous statistics for non-blocking data collection

## Usage Recommendations

### High Availability Setup
- Use a minimum of 3 independent Redis instances
- An odd number of servers is preferred
- Distribute servers across different physical machines

### Redis Cluster Considerations
- Use hash tags for multiple resources: `{redlock}resource1`, `{redlock}resource2`
- All keys must map to the same cluster node
- Account for asynchronous replication during failover

### Monitoring
- Track ExecutionStats for performance analysis
- Monitor error events
- Analyze quorum and retry metrics

### Security
- Use cryptographically strong random values
- Secure Redis connections (TLS, AUTH)
- Restrict access to Redis servers
