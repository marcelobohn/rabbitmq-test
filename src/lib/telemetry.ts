import { Channel, ChannelModel } from 'amqplib';
import { EXCHANGES } from './config';

export type TelemetryStage =
  | 'created'
  | 'processing'
  | 'processed'
  | 'failed'
  | 'retry-scheduled'
  | 'dlq'
  | 'duplicate-skipped'
  | 'event-published'
  | 'inventory-done'
  | 'notification-done';

export type TelemetryComponent =
  | 'dashboard'
  | 'producer'
  | 'publisher'
  | 'worker'
  | 'inventory-consumer'
  | 'notification-consumer';

export interface TelemetryEvent {
  orderId: string;
  messageId?: string;
  component: TelemetryComponent;
  stage: TelemetryStage;
  attempt?: number;
  at: string;
  detail?: Record<string, unknown>;
}

export type TelemetryInput = Omit<TelemetryEvent, 'component' | 'at'>;
export type Emit = (event: TelemetryInput) => void;

export const noopEmit: Emit = () => {};

export async function assertTelemetryExchange(channel: Channel): Promise<void> {
  await channel.assertExchange(EXCHANGES.ORDERS_TELEMETRY, 'topic', { durable: true });
}

// Fire-and-forget: telemetry is not persistent, not confirmed, and a failure here
// is only logged — it must never affect the order being processed.
export function emitter(channel: Channel, component: TelemetryComponent): Emit {
  return (input) => {
    const event: TelemetryEvent = { ...input, component, at: new Date().toISOString() };
    try {
      channel.publish(
        EXCHANGES.ORDERS_TELEMETRY,
        `${component}.${input.stage}`,
        Buffer.from(JSON.stringify(event)),
        { persistent: false, contentType: 'application/json' }
      );
    } catch (err) {
      console.warn(`[telemetry] Could not emit ${input.stage} for ${input.orderId}: ${(err as Error).message}`);
    }
  };
}

// Telemetry gets its own channel: if anything goes wrong with it (e.g. the broker
// closes it), the channel that carries the orders is not affected.
export async function openTelemetry(connection: ChannelModel, component: TelemetryComponent): Promise<Emit> {
  const channel = await connection.createChannel();
  channel.on('error', (err: Error) => console.warn(`[telemetry] Channel error: ${err.message}`));
  await assertTelemetryExchange(channel);
  return emitter(channel, component);
}
