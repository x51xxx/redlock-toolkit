/**
 * CountDownLatchStrategy - Implements distributed countdown latch with consensus.
 */

import { randomBytes } from "crypto";
import {
  ILockToolkit,
  RedisClient,
  CountDownLatchOptions,
  CountDownResult,
  CountDownLatchStatus,
} from "../core/types";
import { LatchNotFoundError, LatchExistsError, LatchTimeoutError, ConfigurationError } from "../core/errors";
import { SCRIPTS } from "../utils/scripts";
import { CountDownLatch } from "../primitives/countdown-latch";
import { PubSubManager } from "../pubsub/pubsub-manager";

const DEFAULT_LATCH_OPTIONS = {
  ttl: 60000,
  awaitTimeout: 30000,
  pollInterval: 100,
};

export class CountDownLatchStrategy {
  constructor(
    private readonly toolkit: ILockToolkit,
    private readonly pubSubManager: PubSubManager | null,
    private readonly keyPrefix: string,
  ) {}

  async create(name: string, options: CountDownLatchOptions): Promise<CountDownLatch> {
    if (!options.count || options.count <= 0) {
      throw new ConfigurationError("count", options.count, "Must be greater than 0");
    }
    if (options.ttl !== undefined && options.ttl <= 0) {
      throw new ConfigurationError("ttl", options.ttl, "Must be greater than 0");
    }

    const opts = { ...DEFAULT_LATCH_OPTIONS, ...options };
    const countKey = this.toolkit.generateLockKey(`latch:${name}`);
    const targetKey = this.toolkit.generateLockKey(`latch:${name}:target`);

    let createdOnAny = false;

    await this.toolkit.executeWithConsensus(async (client: RedisClient) => {
      const res = await this.toolkit.executeScript(
        client,
        SCRIPTS.latchCreate,
        [countKey, targetKey],
        [opts.count, opts.ttl],
      );
      if (res === 1) createdOnAny = true;
      return res;
    }, undefined, {
      successPolicy: "quorum" as const,
      evaluateResult: (res: unknown) => typeof res === "number" && res >= 0,
    });

    if (!createdOnAny) {
      throw new LatchExistsError(name);
    }

    return new CountDownLatch(
      name,
      opts.count,
      {
        countDown: (latch, eventId) => this.countDown(latch, eventId),
        getStatus: (name) => this.getStatus(name),
        await: (latch, timeoutMs) => this.awaitLatch(latch, timeoutMs),
      },
      { ttl: opts.ttl, awaitTimeout: opts.awaitTimeout, pollInterval: opts.pollInterval },
    );
  }

  async countDown(latch: CountDownLatch, eventId?: string): Promise<CountDownResult> {
    const id = eventId ?? randomBytes(8).toString("hex");
    const countKey = this.toolkit.generateLockKey(`latch:${latch.name}`);
    const eventsKey = this.toolkit.generateLockKey(`latch:${latch.name}:events`);
    const channel = `${this.keyPrefix}:notify:latch:${latch.name}`;

    let responseData: number[] = [-1, 0, 0];
    let responseDataCaptured = false;

    await this.toolkit.executeWithConsensus(async (client: RedisClient) => {
      const response = await this.toolkit.executeScript(
        client,
        SCRIPTS.latchCountDown,
        [countKey, eventsKey],
        [id, channel],
      ) as number[];

      if (!responseDataCaptured) {
        responseData = response;
        responseDataCaptured = true;
      }

      if (response[0] === -1 && response[2] === 0) {
        throw new LatchNotFoundError(latch.name);
      }

      return response[2]; // status code
    }, undefined, {
      successPolicy: "any" as const,
      evaluateResult: (res: unknown) => typeof res === "number" && res !== 0,
    });

    const status = responseData[2];
    return {
      success: status === 1 || status === 2 || status === -2,
      remainingCount: Math.max(0, responseData[0]),
      justCompleted: status === 2,
    };
  }

  async getStatus(name: string): Promise<CountDownLatchStatus> {
    const countKey = this.toolkit.generateLockKey(`latch:${name}`);
    const targetKey = this.toolkit.generateLockKey(`latch:${name}:target`);

    const response = await this.toolkit.executeScript(
      this.toolkit.getClientForRead(),
      SCRIPTS.latchStatus,
      [countKey, targetKey],
      [],
    ) as number[];

    if (response[0] === -1) {
      return {
        exists: false,
        remainingCount: 0,
        targetCount: 0,
        completed: false,
      };
    }

    return {
      exists: true,
      remainingCount: response[0],
      targetCount: response[1],
      completed: response[0] <= 0,
      ttl: response[2] > 0 ? response[2] : undefined,
    };
  }

  async awaitLatch(latch: CountDownLatch, timeoutMs?: number): Promise<boolean> {
    const timeout = timeoutMs ?? DEFAULT_LATCH_OPTIONS.awaitTimeout;
    const deadline = Date.now() + timeout;

    // Check if already completed
    const initial = await this.getStatus(latch.name);
    if (initial.completed) return true;
    if (!initial.exists) throw new LatchNotFoundError(latch.name);

    // Try pub/sub waiting if available
    if (this.pubSubManager) {
      const channel = `${this.keyPrefix}:notify:latch:${latch.name}`;
      const remaining = deadline - Date.now();
      if (remaining <= 0) {
        throw new LatchTimeoutError(latch.name, initial.remainingCount, timeout);
      }

      const message = await this.pubSubManager.waitForMessage(channel, remaining);
      if (message !== null) {
        // Verify the latch is actually complete (don't trust arbitrary pub/sub messages)
        const verified = await this.getStatus(latch.name);
        if (verified.completed) return true;
        // If not actually complete, fall through to final status check
      }

      // Double-check status after timeout (message may have been missed)
      const final = await this.getStatus(latch.name);
      if (final.completed) return true;

      throw new LatchTimeoutError(latch.name, final.remainingCount, timeout);
    }

    // Polling mode
    const pollInterval = latch.pollInterval ?? DEFAULT_LATCH_OPTIONS.pollInterval;
    while (Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, pollInterval));
      const status = await this.getStatus(latch.name);
      if (status.completed) return true;
      if (!status.exists) throw new LatchNotFoundError(latch.name);
    }

    const finalStatus = await this.getStatus(latch.name);
    if (finalStatus.completed) return true;

    throw new LatchTimeoutError(latch.name, finalStatus.remainingCount, timeout);
  }
}
