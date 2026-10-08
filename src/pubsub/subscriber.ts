import { Channel, ConsumeMessage } from 'amqplib';
import { getConnection } from '../lib/connection';
import { EXCHANGES, OrderMessage } from '../lib/config';
import { ProcessedStore, InMemoryProcessedStore, dedupKey } from '../lib/idempotency';
import { Emit, TelemetryComponent, TelemetryStage, openTelemetry } from '../lib/telemetry';

export type EventHandler = (order: OrderMessage) => void | Promise<void>;

export interface EventHandlingOptions {
  handler: EventHandler;
  store: ProcessedStore;
  label: string;
  emit: Emit;
  doneStage: TelemetryStage;
}

export async function handleEvent(
  channel: Channel,
  msg: ConsumeMessage,
  { handler, store, label, emit, doneStage }: EventHandlingOptions
): Promise<void> {
  const messageId = msg.properties.messageId as string | undefined;
  // OrderCreated comes from the standalone publisher, OrderProcessed from the worker
  const eventType = (msg.properties.type as string | undefined) ?? 'OrderCreated';
  let orderId = messageId ?? '(invalid JSON)';

  try {
    const order: OrderMessage = JSON.parse(msg.content.toString());
    orderId = order.orderId;
    const key = dedupKey(msg, order.orderId);

    if (await store.has(key)) {
      channel.ack(msg);
      console.log(`[${label}] Duplicate ${eventType} ${key} for order ${order.orderId} — skipped`);
      emit({ orderId, messageId, stage: 'duplicate-skipped', detail: { eventType } });
      return;
    }

    console.log(`[${label}] Received ${eventType} for order ${order.orderId}`);
    await handler(order);
    await store.add(key);
    channel.ack(msg);
    emit({ orderId, messageId, stage: doneStage, detail: { eventType } });
  } catch (err) {
    const error = (err as Error).message;
    console.error(`[${label}] Failed to process message:`, error);
    channel.nack(msg, false, false);
    emit({ orderId, messageId, stage: 'failed', detail: { eventType, error } });
  }
}

// Subscribes a durable, named queue to the orders.events fanout exchange.
// Unlike an exclusive server-named queue, it outlives the consumer: events
// published while the subscriber is down are delivered when it comes back.
export async function startSubscriber(
  queue: string,
  handler: EventHandler,
  component: TelemetryComponent,
  doneStage: TelemetryStage
): Promise<void> {
  const connection = await getConnection();
  const channel = await connection.createChannel();
  const emit = await openTelemetry(connection, component);
  const store = new InMemoryProcessedStore();
  const label = component;

  await channel.assertExchange(EXCHANGES.ORDERS_EVENTS, 'fanout', { durable: true });
  await channel.assertQueue(queue, { durable: true });
  await channel.bindQueue(queue, EXCHANGES.ORDERS_EVENTS, '');
  channel.prefetch(1);

  console.log(`[${label}] Waiting for order events on ${queue}...`);

  await channel.consume(queue, (msg) => {
    if (msg) handleEvent(channel, msg, { handler, store, label, emit, doneStage }).catch(console.error);
  });

  process.on('SIGTERM', async () => {
    await channel.close();
    process.exit(0);
  });
}
