/**
 * ConsensusManager - Handles consensus operations for distributed locking
 * Breaks down complex consensus logic into manageable pieces
 */

import { RedisClient } from "../core/types";
import { ConsensusError, ConfigurationError, CircuitBreakerOpenError } from "../core/errors";
import { Logger } from "../core/logger";

export interface ConsensusOperation<T> {
  client: RedisClient;
  execute: () => Promise<T>;
}

export interface ConsensusOptions {
  evaluateResult?: (result: unknown) => boolean;
  successPolicy?: 'quorum' | 'any';
  logger?: Logger;
}

export interface ConsensusOutcome<T> {
  client: RedisClient;
  result?: T;
  error?: Error;
}

export interface ConsensusStats {
  totalClients: number;
  quorumSize: number;
  votesFor: Set<RedisClient>;
  votesAgainst: Map<RedisClient, Error>;
  startTime: number;
  duration: number;
}

export interface ConsensusResult {
  success: boolean;
  stats: ConsensusStats;
  attempts: Promise<ConsensusStats>[];
  startTime: number;
}

/**
 * Manages consensus operations across multiple Redis clients
 */
export class ConsensusManager {
  private readonly logger: Logger;
  
  constructor(logger: Logger) {
    this.logger = logger;
  }

  /**
   * Execute an operation across multiple clients and achieve consensus
   */
  async execute<T>(
    clients: RedisClient[],
    operation: (client: RedisClient) => Promise<T>,
    options?: ConsensusOptions
  ): Promise<ConsensusResult> {
    const startTime = Date.now();
    const quorumSize = this.calculateQuorum(clients.length);
    const evaluator = this.createEvaluator(options?.evaluateResult);
    
    // Start all operations in parallel
    const pendingOps = this.startOperations(clients, operation);
    
    // Track voting results
    const votingTracker = new VotingTracker(clients.length, quorumSize);
    
    // Process outcomes until consensus achieved or impossible
    await this.processOutcomes(pendingOps, votingTracker, evaluator);
    
    // Create final stats
    const stats = this.createStats(
      clients.length,
      quorumSize,
      votingTracker,
      startTime
    );
    
    // Log consensus results
    this.logConsensusResult(stats);
    
    // Determine success based on policy
    const success = this.determineSuccess(
      votingTracker,
      quorumSize,
      options?.successPolicy
    );
    
    if (!success) {
      this.handleConsensusFailure(votingTracker, quorumSize, stats);
    }
    
    return {
      success,
      stats,
      attempts: [Promise.resolve(stats)],
      startTime,
    };
  }

  /**
   * Calculate quorum size (majority)
   */
  private calculateQuorum(clientCount: number): number {
    return Math.floor(clientCount / 2) + 1;
  }

  /**
   * Create result evaluator function
   */
  private createEvaluator(customEvaluator?: (result: unknown) => boolean): (result: unknown) => boolean {
    return customEvaluator ?? ((result: unknown) => {
      return typeof result === 'number' && result > 0;
    });
  }

  /**
   * Start operations on all clients in parallel
   */
  private startOperations<T>(
    clients: RedisClient[],
    operation: (client: RedisClient) => Promise<T>
  ): Map<number, Promise<ConsensusOutcome<T>>> {
    const pending = new Map<number, Promise<ConsensusOutcome<T>>>();
    
    clients.forEach((client, index) => {
      const promise = operation(client)
        .then((result) => ({ client, result }))
        .catch((error) => ({ client, error }));
      pending.set(index, promise);
    });
    
    return pending;
  }

  /**
   * Process outcomes until consensus is achieved or impossible
   */
  private async processOutcomes<T>(
    pendingOps: Map<number, Promise<ConsensusOutcome<T>>>,
    votingTracker: VotingTracker,
    evaluator: (result: unknown) => boolean
  ): Promise<void> {
    while (pendingOps.size > 0) {
      // Wait for next operation to complete
      const { index, outcome } = await this.waitForNextOutcome(pendingOps);
      
      // Process the outcome
      this.processOutcome(outcome, votingTracker, evaluator);
      pendingOps.delete(index);
      
      // Check for early termination conditions
      if (this.canTerminateEarly(votingTracker, pendingOps.size)) {
        break;
      }
    }
  }

  /**
   * Wait for the next operation to complete
   */
  private async waitForNextOutcome<T>(
    pendingOps: Map<number, Promise<ConsensusOutcome<T>>>
  ): Promise<{ index: number; outcome: ConsensusOutcome<T> }> {
    const entries = Array.from(pendingOps.entries());
    const raced = entries.map(([i, p]) => p.then((o) => ({ index: i, outcome: o })));
    return await Promise.race(raced);
  }

  /**
   * Process a single outcome and update voting tracker
   */
  private processOutcome<T>(
    outcome: ConsensusOutcome<T>,
    votingTracker: VotingTracker,
    evaluator: (result: unknown) => boolean
  ): void {
    if (outcome.error) {
      votingTracker.recordFailure(outcome.client, outcome.error);
    } else if (evaluator(outcome.result)) {
      votingTracker.recordSuccess(outcome.client);
    } else {
      const reason = this.createFailureReason(outcome.result);
      votingTracker.recordFailure(outcome.client, new Error(reason));
    }
    
    this.logger.trace("Consensus outcome processed", {
      success: votingTracker.successCount,
      failure: votingTracker.failureCount,
    });
  }

  /**
   * Create failure reason message
   */
  private createFailureReason(result: unknown): string {
    if (typeof result === 'number') {
      return `Operation returned ${result}`;
    }
    return `Invalid response: ${result}`;
  }

  /**
   * Check if we can terminate early
   */
  private canTerminateEarly(votingTracker: VotingTracker, pendingCount: number): boolean {
    // Success: achieved quorum
    if (votingTracker.hasQuorum()) {
      return true;
    }
    
    // Failure: impossible to achieve quorum with remaining operations
    if (!votingTracker.canAchieveQuorum(pendingCount)) {
      return true;
    }
    
    return false;
  }

  /**
   * Create consensus statistics
   */
  private createStats(
    totalClients: number,
    quorumSize: number,
    votingTracker: VotingTracker,
    startTime: number
  ): ConsensusStats {
    return {
      totalClients,
      quorumSize,
      votesFor: new Set(votingTracker.votesFor),
      votesAgainst: new Map(votingTracker.votesAgainst),
      startTime,
      duration: Date.now() - startTime,
    };
  }

  /**
   * Log consensus result
   */
  private logConsensusResult(stats: ConsensusStats): void {
    this.logger.debug("Consensus operation completed", {
      totalClients: stats.totalClients,
      quorumSize: stats.quorumSize,
      successCount: stats.votesFor.size,
      failureCount: stats.votesAgainst.size,
      duration: stats.duration,
    });
  }

  /**
   * Determine if consensus was successful
   */
  private determineSuccess(
    votingTracker: VotingTracker,
    quorumSize: number,
    policy?: 'quorum' | 'any'
  ): boolean {
    if (policy === 'any') {
      return votingTracker.successCount > 0;
    }
    return votingTracker.successCount >= quorumSize;
  }

  /**
   * Handle consensus failure
   */
  private handleConsensusFailure(
    votingTracker: VotingTracker,
    quorumSize: number,
    stats: ConsensusStats
  ): never {
    // Check if all failures are non-retryable
    const allNonRetryable = this.areAllFailuresNonRetryable(votingTracker);
    
    if (allNonRetryable && votingTracker.votesAgainst.size > 0) {
      // Re-throw the first non-retryable error
      throw votingTracker.votesAgainst.values().next().value;
    }
    
    throw new ConsensusError(
      `Failed to achieve quorum: ${votingTracker.successCount}/${quorumSize} votes`,
      [Promise.resolve(stats)],
      quorumSize,
      votingTracker.successCount,
    );
  }

  /**
   * Check if all failures are non-retryable
   */
  private areAllFailuresNonRetryable(votingTracker: VotingTracker): boolean {
    if (votingTracker.votesFor.length > 0) {
      return false;
    }
    
    return Array.from(votingTracker.votesAgainst.values()).every(err =>
      err instanceof ConfigurationError || err instanceof CircuitBreakerOpenError
    );
  }
}

/**
 * Tracks voting during consensus operations
 */
class VotingTracker {
  public readonly votesFor: RedisClient[] = [];
  public readonly votesAgainst: Map<RedisClient, Error> = new Map();
  public successCount = 0;
  public failureCount = 0;
  
  constructor(
    private readonly totalClients: number,
    private readonly quorumSize: number
  ) {}
  
  recordSuccess(client: RedisClient): void {
    this.successCount++;
    this.votesFor.push(client);
  }
  
  recordFailure(client: RedisClient, error: Error): void {
    this.failureCount++;
    this.votesAgainst.set(client, error);
  }
  
  hasQuorum(): boolean {
    return this.successCount >= this.quorumSize;
  }
  
  canAchieveQuorum(pendingCount: number): boolean {
    return this.successCount + pendingCount >= this.quorumSize;
  }
  
  get size(): number {
    return this.votesFor.length;
  }
}