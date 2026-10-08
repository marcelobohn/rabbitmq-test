import { Channel, ConsumeMessage } from 'amqplib';
import { getConnection } from '../lib/connection';
import { EXCHANGES, OrderMessage } from '../lib/config';
import { ProcessedStore, InMemoryProcessedStore, dedupKey } from '../lib/idempotency';

export type EventHandler = (order: OrderMessage) => void | Promise<void>;

export async function handleEvent(
  channel: Channel,
  msg: ConsumeMessage,
  handler: EventHandler,
  store: ProcessedStore,
  label: string
): Promise<void> {
  try {
    const order: OrderMessage = JSON.parse(msg.content.toString());
    const key = dedupKey(msg, order.orderId);

    if (await store.has(key)) {
      channel.ack(msg);
      console.log(`[${label}] Duplicate event ${key} for order ${order.orderId} — skipped`);
      return;
    }

    await handler(order);
    await store.add(key);
    channel.ack(msg);
  } catch (err) {
    console.error(`[${label}] Failed to process message:`, (err as Error).message);
    channel.nack(msg, false, false);
  }
}

// Subscribes a durable, named queue to the orders.events fanout exchange.
// Unlike an exclusive server-named queue, it outlives the consumer: events
// published while the subscriber is down are delivered when it comes back.
export async function startSubscriber(queue: string, handler: EventHandler, label: string): Promise<void> {
  const connection = await getConnection();
  const channel = await connection.createChannel();
  const store = new InMemoryProcessedStore();

  await channel.assertExchange(EXCHANGES.ORDERS_EVENTS, 'fanout', { durable: true });
  await channel.assertQueue(queue, { durable: true });
  await channel.bindQueue(queue, EXCHANGES.ORDERS_EVENTS, '');
  channel.prefetch(1);

  console.log(`[${label}] Waiting for OrderCreated events on ${queue}...`);

  await channel.consume(queue, (msg) => {
    if (msg) handleEvent(channel, msg, handler, store, label).catch(console.error);
  });

  process.on('SIGTERM', async () => {
    await channel.close();
    process.exit(0);
  });
}
