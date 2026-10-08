import { ConsumeMessage } from 'amqplib';

// Records which messages were already processed, so a redelivered or duplicated
// message is acked without running the business logic twice.
// Async on purpose: a production store (database unique key, Redis SET NX) would be.
export interface ProcessedStore {
  has(id: string): Promise<boolean>;
  add(id: string): Promise<void>;
}

// Per-process memory, bounded by maxSize (oldest ids are evicted first).
// Lost on restart and not shared between replicas: enough for the demo, not for production.
export class InMemoryProcessedStore implements ProcessedStore {
  private readonly ids = new Set<string>();

  constructor(private readonly maxSize = 10_000) {}

  async has(id: string): Promise<boolean> {
    return this.ids.has(id);
  }

  async add(id: string): Promise<void> {
    this.ids.add(id);
    if (this.ids.size > this.maxSize) {
      const oldest = this.ids.values().next().value as string;
      this.ids.delete(oldest);
    }
  }
}

// Producers stamp each message with a UUID messageId; messages published without one
// (e.g. via the management API) fall back to the orderId.
export function dedupKey(msg: ConsumeMessage, orderId: string): string {
  return msg.properties.messageId ?? orderId;
}
