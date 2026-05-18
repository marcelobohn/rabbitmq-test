import { Channel, ConsumeMessage } from 'amqplib';
import { getConnection, closeConnection } from '../lib/connection';
import { QUEUES, MAX_RETRIES, RETRY_DELAYS, OrderMessage } from '../lib/config';

type OrderProcessor = (order: OrderMessage) => Promise<void>;

const defaultProcessor: OrderProcessor = async (order) => {
  const delay = 500 + Math.random() * 1500;
  await new Promise(r => setTimeout(r, delay));
  console.log(`[worker] Processed order ${order.orderId} — total: $${order.total}`);
};

export async function handleMessage(
  channel: Channel,
  msg: ConsumeMessage,
  processor: OrderProcessor = defaultProcessor
): Promise<void> {
  const order: OrderMessage = JSON.parse(msg.content.toString());
  const retryCount: number = (msg.properties.headers?.['x-retry-count'] as number) ?? 0;

  try {
    await processor(order);
    channel.ack(msg);
  } catch (err) {
    channel.ack(msg);

    if (retryCount < MAX_RETRIES) {
      const retryQueue = retryCount === 0 ? QUEUES.ORDERS_RETRY_5S : QUEUES.ORDERS_RETRY_30S;
      channel.sendToQueue(retryQueue, msg.content, {
        headers: { ...msg.properties.headers, 'x-retry-count': retryCount + 1 },
        persistent: true,
      });
      console.log(`[worker] Retry ${retryCount + 1}/${MAX_RETRIES} for order ${order.orderId} via ${retryQueue}`);
    } else {
      channel.sendToQueue(QUEUES.ORDERS_DLQ, msg.content, {
        headers: { ...msg.properties.headers, 'x-error': (err as Error).message },
        persistent: true,
      });
      console.log(`[worker] Max retries reached for order ${order.orderId} — sent to DLQ`);
    }
  }
}

async function main(): Promise<void> {
  const connection = await getConnection();
  const channel = await connection.createChannel();

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

main().catch(console.error);
