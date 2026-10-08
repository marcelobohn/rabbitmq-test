import { ConfirmChannel, ConsumeMessage, Options } from 'amqplib';
import { getConnection, closeConnection } from '../lib/connection';
import { QUEUES, MAX_RETRIES, RETRY_DELAYS, OrderMessage } from '../lib/config';
import { ProcessedStore, InMemoryProcessedStore, dedupKey } from '../lib/idempotency';

type OrderProcessor = (order: OrderMessage) => Promise<void>;

const defaultProcessor: OrderProcessor = async (order) => {
  const delay = 500 + Math.random() * 1500;
  await new Promise(r => setTimeout(r, delay));
  console.log(`[worker] Processed order ${order.orderId} — total: $${order.total}`);
};

const defaultStore = new InMemoryProcessedStore();

// Moves the message to another queue without ever losing it: the copy is published
// and confirmed by the broker first, and only then the original is acked. If the
// broker does not confirm, the original goes back to the queue instead.
async function forward(
  channel: ConfirmChannel,
  msg: ConsumeMessage,
  queue: string,
  options: Options.Publish
): Promise<boolean> {
  try {
    channel.sendToQueue(queue, msg.content, {
      ...options,
      messageId: msg.properties.messageId,
      persistent: true,
    });
    await channel.waitForConfirms();
  } catch (err) {
    console.error(`[worker] Publish to ${queue} not confirmed, requeueing: ${(err as Error).message}`);
    channel.nack(msg, false, true);
    return false;
  }
  channel.ack(msg);
  return true;
}

export async function handleMessage(
  channel: ConfirmChannel,
  msg: ConsumeMessage,
  processor: OrderProcessor = defaultProcessor,
  store: ProcessedStore = defaultStore
): Promise<void> {
  let order: OrderMessage;
  try {
    order = JSON.parse(msg.content.toString());
  } catch (parseErr) {
    // Malformed message — straight to DLQ, no retry
    const error = `Parse error: ${(parseErr as Error).message}`;
    if (await forward(channel, msg, QUEUES.ORDERS_DLQ, { headers: { 'x-error': error } })) {
      console.error(`[worker] Malformed message sent to DLQ: ${(parseErr as Error).message}`);
    }
    return;
  }

  const key = dedupKey(msg, order.orderId);
  if (await store.has(key)) {
    channel.ack(msg);
    console.log(`[worker] Duplicate message ${key} for order ${order.orderId} — skipped`);
    return;
  }

  const retryCount: number = (msg.properties.headers?.['x-retry-count'] as number) ?? 0;

  try {
    await processor(order);
  } catch (err) {
    if (retryCount < MAX_RETRIES) {
      const retryQueue = retryCount === 0 ? QUEUES.ORDERS_RETRY_5S : QUEUES.ORDERS_RETRY_30S;
      const headers = { ...msg.properties.headers, 'x-retry-count': retryCount + 1 };
      if (await forward(channel, msg, retryQueue, { headers })) {
        console.log(`[worker] Retry ${retryCount + 1}/${MAX_RETRIES} for order ${order.orderId} via ${retryQueue}`);
      }
    } else {
      const headers = { ...msg.properties.headers, 'x-error': (err as Error).message };
      if (await forward(channel, msg, QUEUES.ORDERS_DLQ, { headers })) {
        console.log(`[worker] Max retries reached for order ${order.orderId} — sent to DLQ`);
      }
    }
    return;
  }

  // Recorded before the ack: with a persistent store, a crash in between causes a
  // redelivery that is skipped as duplicate instead of processed a second time.
  await store.add(key);
  channel.ack(msg);
}

async function main(): Promise<void> {
  const connection = await getConnection();
  const channel = await connection.createConfirmChannel();

  await channel.assertQueue(QUEUES.ORDERS_PROCESSING, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': QUEUES.ORDERS_DLQ,
    },
  });
  await channel.assertQueue(QUEUES.ORDERS_DLQ, { durable: true });
  await channel.assertQueue(QUEUES.ORDERS_RETRY_5S, {
    durable: true,
    arguments: {
      'x-message-ttl': RETRY_DELAYS[0],
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': QUEUES.ORDERS_PROCESSING,
    },
  });
  await channel.assertQueue(QUEUES.ORDERS_RETRY_30S, {
    durable: true,
    arguments: {
      'x-message-ttl': RETRY_DELAYS[1],
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': QUEUES.ORDERS_PROCESSING,
    },
  });

  channel.prefetch(1);
  console.log('[worker] Waiting for orders...');

  channel.consume(QUEUES.ORDERS_PROCESSING, (msg) => {
    if (msg) handleMessage(channel, msg).catch(console.error);
  });

  process.on('SIGTERM', async () => {
    await channel.close();
    await closeConnection();
    process.exit(0);
  });
}

if (require.main === module) {
  main().catch(console.error);
}
