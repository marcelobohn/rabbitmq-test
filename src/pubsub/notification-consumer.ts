import { getConnection } from '../lib/connection';
import { EXCHANGES, OrderMessage } from '../lib/config';

export function handleNotification(order: OrderMessage): void {
  console.log(
    `[notification] Sending order confirmation to customer ${order.customerId} — order ${order.orderId}, total: $${order.total}`
  );
}

async function main(): Promise<void> {
  const connection = await getConnection();
  const channel = await connection.createChannel();

  await channel.assertExchange(EXCHANGES.ORDERS_EVENTS, 'fanout', { durable: true });

  const { queue } = await channel.assertQueue('', { exclusive: true });
  await channel.bindQueue(queue, EXCHANGES.ORDERS_EVENTS, '');

  console.log('[notification-consumer] Waiting for OrderCreated events...');

  await channel.consume(queue, (msg) => {
    if (!msg) return;
    try {
      const order: OrderMessage = JSON.parse(msg.content.toString());
      handleNotification(order);
      channel.ack(msg);
    } catch (err) {
      console.error('[notification-consumer] Failed to process message:', (err as Error).message);
      channel.nack(msg, false, false);
    }
  });
}

if (require.main === module) {
  main().catch(console.error);
}
