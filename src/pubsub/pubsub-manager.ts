/**
 * PubSubManager - Manages dedicated subscriber connections for pub/sub waiting.
 *
 * ioredis clients in subscribe mode cannot run regular commands,
 * so we lazily create a duplicate connection for subscriptions.
 */

import { EventEmitter } from "events";
import { RedisClient } from "../core/types";
import { ConfigurationError } from "../core/errors";
import { Logger } from "../core/logger";

/** Subset of ioredis methods used for pub/sub operations */
interface PubSubCapableClient {
  subscribe(channel: string): Promise<unknown>;
  unsubscribe(channel: string): Promise<unknown>;
  publish(channel: string, message: string): Promise<number>;
  disconnect(): Promise<void>;
}

export interface PubSubManagerConfig {
  /** Original Redis clients (used for publish only) */
  clients: RedisClient[];
  /** Optional user-provided subscriber clients */
  subscriberClients?: RedisClient[];
  /** Key prefix for channel names */
  keyPrefix: string;
  /** Logger instance */
  logger: Logger;
}

export class PubSubManager extends EventEmitter {
  private subscriber: RedisClient | null = null;
  private initialized = false;
  private initPromise: Promise<void> | null = null;
  private readonly channelListeners = new Map<string, Set<(message: string) => void>>();
  private messageHandler: ((channel: string, message: string) => void) | null = null;
  private ownsSubscriber = false;

  constructor(private readonly config: PubSubManagerConfig) {
    super();
  }

  /** Lazily initialize subscriber connection */
  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return;
    if (this.initPromise) return this.initPromise;

    this.initPromise = this.doInitialize();
    try {
      await this.initPromise;
    } catch (err) {
      this.initPromise = null;
      throw err;
    }
  }

  private async doInitialize(): Promise<void> {
    if (this.config.subscriberClients?.length) {
      this.subscriber = this.config.subscriberClients[0];
      this.ownsSubscriber = false;
    } else {
      const firstClient = this.config.clients[0] as unknown as Record<string, unknown>;
      if (typeof firstClient.duplicate === "function") {
        this.subscriber = (firstClient.duplicate as () => RedisClient)();
        this.ownsSubscriber = true;
      } else {
        throw new ConfigurationError(
          "subscriberClients",
          undefined,
          "Client does not support duplicate(). Provide subscriberClients in config.",
        );
      }
    }

    this.messageHandler = (channel: string, message: string) => {
      const listeners = this.channelListeners.get(channel);
      if (listeners) {
        for (const listener of listeners) {
          try {
            listener(message);
          } catch {
            // Listener errors should not break pub/sub routing
          }
        }
      }
    };

    (this.subscriber as EventEmitter).on("message", this.messageHandler);
    this.initialized = true;
    this.config.logger.debug("PubSubManager initialized");
  }

  /** Subscribe to a channel with a callback */
  async subscribe(channel: string, listener: (message: string) => void): Promise<void> {
    await this.ensureInitialized();

    let listeners = this.channelListeners.get(channel);
    if (!listeners) {
      listeners = new Set();
      this.channelListeners.set(channel, listeners);
      // First listener for this channel — subscribe on Redis
      try {
        await (this.subscriber as unknown as PubSubCapableClient).subscribe(channel);
      } catch (err) {
        this.channelListeners.delete(channel);
        throw err;
      }
    }
    listeners.add(listener);
  }

  /** Unsubscribe a listener from a channel */
  async unsubscribe(channel: string, listener?: (message: string) => void): Promise<void> {
    const listeners = this.channelListeners.get(channel);
    if (!listeners) return;

    if (listener) {
      listeners.delete(listener);
      if (listeners.size === 0) {
        this.channelListeners.delete(channel);
        if (this.subscriber) {
          await (this.subscriber as unknown as PubSubCapableClient).unsubscribe(channel).catch((err) => {
            this.config.logger.warn("Failed to unsubscribe from channel", { channel, error: String(err) });
          });
        }
      }
    } else {
      // Remove all listeners for this channel
      this.channelListeners.delete(channel);
      if (this.subscriber) {
        await (this.subscriber as unknown as PubSubCapableClient).unsubscribe(channel).catch((err) => {
          this.config.logger.warn("Failed to unsubscribe from channel", { channel, error: String(err) });
        });
      }
    }
  }

  /** Publish to a channel (uses original client, NOT subscriber) */
  async publish(channel: string, message: string): Promise<number> {
    const client = this.config.clients[0];
    return (client as unknown as PubSubCapableClient).publish(channel, message);
  }

  /** Wait for a message on a channel with timeout */
  async waitForMessage(channel: string, timeoutMs: number): Promise<string | null> {
    await this.ensureInitialized();

    return new Promise<string | null>((resolve) => {
      let resolved = false;
      const onMessage: ((message: string) => void) | undefined;

      // Remove listener from local Set synchronously to prevent memory leak
      // even if the async Redis UNSUBSCRIBE call fails.
      const localCleanup = () => {
        if (onMessage) {
          const listeners = this.channelListeners.get(channel);
          if (listeners) {
            listeners.delete(onMessage);
            if (listeners.size === 0) {
              this.channelListeners.delete(channel);
            }
          }
        }
        // Best-effort Redis unsubscribe (fire-and-forget)
        if (this.subscriber) {
          (this.subscriber as unknown as PubSubCapableClient).unsubscribe(channel).catch((err) => {
            this.config.logger.warn("Failed to unsubscribe", { channel, error: String(err) });
          });
        }
      };

      const timer = setTimeout(() => {
        if (!resolved) {
          resolved = true;
          localCleanup();
          resolve(null);
        }
      }, timeoutMs);

      onMessage = (message: string) => {
        if (!resolved) {
          resolved = true;
          clearTimeout(timer);
          localCleanup();
          resolve(message);
        }
      };

      // subscribe is called synchronously after ensureInitialized
      this.subscribeSync(channel, onMessage);
    });
  }

  /** Synchronous subscribe (after ensureInitialized) — adds listener and triggers Redis SUBSCRIBE */
  private subscribeSync(channel: string, listener: (message: string) => void): void {
    let listeners = this.channelListeners.get(channel);
    if (!listeners) {
      listeners = new Set();
      this.channelListeners.set(channel, listeners);
      (this.subscriber as unknown as PubSubCapableClient).subscribe(channel).catch((err) => {
        this.channelListeners.delete(channel);
        this.config.logger.warn("subscribeSync failed, cleaned stale channel", { channel, error: String(err) });
      });
    }
    listeners.add(listener);
  }

  /** Get number of active channel subscriptions */
  getSubscriptionCount(): number {
    return this.channelListeners.size;
  }

  /** Shut down subscriber connections */
  async shutdown(): Promise<void> {
    if (!this.initialized || !this.subscriber) return;

    // Unsubscribe from all channels
    for (const channel of this.channelListeners.keys()) {
      await (this.subscriber as unknown as PubSubCapableClient).unsubscribe(channel).catch((err) => {
        this.config.logger.warn("Failed to unsubscribe during shutdown", { channel, error: String(err) });
      });
    }
    this.channelListeners.clear();

    // Remove message handler
    if (this.messageHandler) {
      (this.subscriber as EventEmitter).removeListener("message", this.messageHandler);
      this.messageHandler = null;
    }

    // Disconnect only if we created the subscriber via duplicate()
    if (this.ownsSubscriber) {
      await (this.subscriber as unknown as PubSubCapableClient).disconnect().catch((err) => {
        this.config.logger.warn("Failed to disconnect subscriber during shutdown", { error: String(err) });
      });
    }

    this.subscriber = null;
    this.initialized = false;
    this.config.logger.debug("PubSubManager shut down");
  }
}
