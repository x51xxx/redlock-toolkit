/**
 * ConsensusManager - Handles consensus operations for distributed locking
 */

import { RedisClient, LockStats } from "../core/types";
import { ConsensusError, ConfigurationError, isRetryableError } from "../core/errors";
import { Logger } from "../core/logger";

// This interface defines the structure tests are expecting.
export interface ConsensusResult {
  success: boolean;
  stats: LockStats;
  attempts: Promise<LockStats>[];
  startTime: number;
}

export class ConsensusManager {
  constructor(private readonly logger: Logger) {}

  async execute<T>(
    clients: RedisClient[],
    operation: (client: RedisClient) => Promise<T>,
    options?: { evaluateResult?: (result: unknown) => boolean, successPolicy?: 'quorum' | 'any' }
  ): Promise<ConsensusResult> {
    if (!clients || clients.length === 0) {
      throw new ConfigurationError('clients', clients, 'Must provide at least one Redis client');
    }

    const startTime = Date.now();
    const quorumSize = Math.floor(clients.length / 2) + 1;

    const evaluate = options?.evaluateResult ?? ((result: unknown) => {
      return typeof result === 'number' && result > 0;
    });

    type Outcome = { client: RedisClient; result?: T; error?: Error };
    const pending = new Map<number, Promise<Outcome>>();
    clients.forEach((client, index) => {
      const p = operation(client)
        .then((result) => ({ client, result }))
        .catch((error) => ({ client, error }));
      pending.set(index, p);
    });

    const votesFor = new Set<RedisClient>();
    const votesAgainst = new Map<RedisClient, Error>();

    const consume = (outcome: Outcome) => {
      if (outcome.error) {
        votesAgainst.set(outcome.client, outcome.error);
      } else {
        if (evaluate(outcome.result)) {
          votesFor.add(outcome.client);
        } else {
          const reason = typeof outcome.result === 'number' ? `Operation returned ${outcome.result}` : `Invalid response: ${outcome.result}`;
          votesAgainst.set(outcome.client, new Error(reason));
        }
      }
    };

    const nextSettlement = async (): Promise<void> => {
      const entries = Array.from(pending.entries());
      if (entries.length === 0) return;
      const raced = entries.map(([i, p]) => p.then((o) => ({ i, o })));
      const { i, o } = await Promise.race(raced);
      consume(o);
      pending.delete(i);
    };

    while (pending.size > 0) {
      await nextSettlement();
      if (votesFor.size >= quorumSize) break;
      if (votesFor.size + pending.size < quorumSize) break;
    }

    const failed = options?.successPolicy === 'any'
      ? votesFor.size === 0
      : votesFor.size < quorumSize;

    if (failed) {
      // On failure every in-flight operation must settle before we report
      // stats: an abandoned operation may still succeed on its node after
      // the quorum decision, and the caller's cleanup needs to run after
      // that side effect has landed, not before. (On success the early exit
      // stays — the released stragglers are covered by the eventual release,
      // which always targets all clients.)
      while (pending.size > 0) {
        await nextSettlement();
      }
    } else {
      // Consume remaining pending promises to prevent unhandled rejections
      for (const [, pendingOp] of pending) {
        pendingOp.catch(() => {
          // Silently consume errors from abandoned operations after early consensus
        });
      }
    }

    const stats: LockStats = {
      totalClients: clients.length,
      quorumSize,
      votesFor,
      votesAgainst,
      startTime,
      duration: Date.now() - startTime,
    };
    
    this.logger.debug("Consensus operation completed", {
      totalClients: stats.totalClients,
      quorumSize: stats.quorumSize,
      successCount: stats.votesFor.size,
      failureCount: stats.votesAgainst.size,
      duration: stats.duration,
    });

    const succeeded = (options?.successPolicy === 'any' ? votesFor.size > 0 : votesFor.size >= quorumSize);

    if (!succeeded) {
      const allNonRetryable = votesFor.size === 0 && Array.from(votesAgainst.values()).every(err => !isRetryableError(err));
      if (allNonRetryable && votesAgainst.size > 0) {
        throw Array.from(votesAgainst.values())[0];
      }

      throw new ConsensusError(
        `Failed to achieve quorum: ${votesFor.size}/${quorumSize} votes`,
        [Promise.resolve(stats)],
        quorumSize,
        votesFor.size,
      );
    }

    return {
      attempts: [Promise.resolve(stats)],
      startTime,
      success: true,
      stats: stats, // Include stats directly for tests
    };
  }
}