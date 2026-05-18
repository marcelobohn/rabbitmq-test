import { Channel } from 'amqplib';
import { getConnection, closeConnection } from '../lib/connection';
import { EXCHANGES, OrderMessage } from '../lib/config';

export function publishOrderCreated(channel: Channel, order: OrderMessage): void {
  channel.publish(EXCHANGES.ORDERS_EVENTS, '', Buffer.from(JSON.stringify(order)), {
    persistent: true,
  });
}

async function main(): Promise<void> {
  const connection = await getConnection();
  const channel = await connection.createChannel();

  await channel.assertExchange(EXCHANGES.ORDERS_EVENTS, 'fanout', { durable: true });

  for (let i = 1; i <= 3; i++) {
    const order: OrderMessage = {
      orderId: `ord-pub-${String(i).padStart(3, '0')}`,
      customerId: `cust-${i * 10}`,
      items: [{ productId: `prod-${i}`, quantity: i, price: 29.99 }],
      total: parseFloat((i * 29.99).toFixed(2)),
      createdAt: new Date().toISOString(),
    };
    publishOrderCreated(channel, order);
    console.log(`[publisher] Published OrderCreated: ${order.orderId}`);
  }

  await channel.close();
  await closeConnection();
}

if (require.main === module) {
  main().catch(console.error);
}
