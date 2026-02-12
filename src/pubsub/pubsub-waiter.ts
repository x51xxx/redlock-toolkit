/**
 * PubSubWaiter - Integrates pub/sub notifications into retry loops.
 *
 * When pub/sub is enabled, waits for a release notification instead of
 * a fixed timer delay. Falls back to timer delay if pub/sub is not available.
 */

import { PubSubManager } from "./pubsub-manager";

export type WaitResult = "notified" | "timeout" | "fallback";

export class PubSubWaiter {
  constructor(
    private readonly pubSubManager: PubSubManager | null,
    private readonly keyPrefix: string,
  ) {}

  /**
   * Wait for a resource to become available.
   * Uses pub/sub if available, otherwise falls back to timer delay.
   */
  async waitForRelease(
    resourceType: "lock" | "sem" | "latch",
    resource: string,
    maxWaitMs: number,
    fallbackDelayMs: number,
  ): Promise<WaitResult> {
    if (!this.pubSubManager) {
      await new Promise((resolve) => setTimeout(resolve, fallbackDelayMs));
      return "fallback";
    }

    const channel = `${this.keyPrefix}:notify:${resourceType}:${resource}`;
    const result = await this.pubSubManager.waitForMessage(channel, maxWaitMs);
    return result !== null ? "notified" : "timeout";
  }

  /** Build a notification channel name */
  getChannel(resourceType: "lock" | "sem" | "latch", resource: string): string {
    return `${this.keyPrefix}:notify:${resourceType}:${resource}`;
  }
}
