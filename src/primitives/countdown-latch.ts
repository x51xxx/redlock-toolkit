/**
 * CountDownLatch - Distributed countdown synchronization primitive.
 * Allows waiting until N events occur across distributed processes.
 */

import { CountDownResult, CountDownLatchStatus } from "../core/types";

export interface CountDownLatchManager {
  countDown: (latch: CountDownLatch, eventId?: string) => Promise<CountDownResult>;
  getStatus: (name: string) => Promise<CountDownLatchStatus>;
  await: (latch: CountDownLatch, timeoutMs?: number) => Promise<boolean>;
}

export class CountDownLatch {
  constructor(
    public readonly name: string,
    public readonly targetCount: number,
    private readonly latchManager: CountDownLatchManager,
    private readonly options: { ttl: number; awaitTimeout: number; pollInterval: number },
  ) {}

  /** Get the configured poll interval */
  get pollInterval(): number {
    return this.options.pollInterval;
  }

  /** Count down by 1. Returns result with remaining count. */
  async countDown(eventId?: string): Promise<CountDownResult> {
    return this.latchManager.countDown(this, eventId);
  }

  /** Wait until count reaches 0 or timeout. Returns true if completed. */
  async await(timeoutMs?: number): Promise<boolean> {
    return this.latchManager.await(this, timeoutMs ?? this.options.awaitTimeout);
  }

  /** Get current status */
  async getStatus(): Promise<CountDownLatchStatus> {
    return this.latchManager.getStatus(this.name);
  }
}
