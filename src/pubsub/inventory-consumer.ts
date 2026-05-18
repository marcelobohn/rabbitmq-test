import { getConnection } from '../lib/connection';
import { EXCHANGES, OrderMessage } from '../lib/config';

export function handleInventoryUpdate(order: OrderMessage): void {
  const totalItems = order.items.reduce((sum, item) => sum + item.quantity, 0);
  console.log(
    `[inventory] Deducting ${totalItems} item(s) for order ${order.orderId} (customer: ${order.customerId})`
  );
}

async function main(): Promise<void> {
  const connection = await getConnection();
  const channel = await connection.createChannel();

  await channel.assertExchange(EXCHANGES.ORDERS_EVENTS, 'fanout', { durable: true });

  const { queue } = await channel.assertQueue('', { exclusive: true });
  await channel.bindQueue(queue, EXCHANGES.ORDERS_EVENTS, '');

  console.log('[inventory-consumer] Waiting for OrderCreated events...');

  await channel.consume(queue, (msg) => {
    if (!msg) return;
    try {
      const order: OrderMessage = JSON.parse(msg.content.toString());
      handleInventoryUpdate(order);
      channel.ack(msg);
    } catch (err) {
      console.error('[inventory-consumer] Failed to process message:', (err as Error).message);
      channel.nack(msg, false, false);
    }
  });
}

if (require.main === module) {
  main().catch(console.error);
}
