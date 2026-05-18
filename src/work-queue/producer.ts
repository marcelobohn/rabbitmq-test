import { getConnection, closeConnection } from '../lib/connection';
import { QUEUES, OrderMessage } from '../lib/config';

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

  for (let i = 1; i <= 10; i++) {
    const order: OrderMessage = {
      orderId: `ord-${String(i).padStart(3, '0')}`,
      customerId: `cust-${Math.floor(Math.random() * 100)}`,
      items: [
        {
          productId: `prod-${Math.floor(Math.random() * 50)}`,
          quantity: Math.ceil(Math.random() * 5),
          price: parseFloat((Math.random() * 100).toFixed(2)),
        },
      ],
      total: parseFloat((Math.random() * 500).toFixed(2)),
      createdAt: new Date().toISOString(),
    };

    channel.sendToQueue(QUEUES.ORDERS_PROCESSING, Buffer.from(JSON.stringify(order)), {
      persistent: true,
      headers: { 'x-retry-count': 0 },
    });
    console.log(`[producer] Sent order ${order.orderId} — total: $${order.total}`);
  }

  await channel.close();
  await closeConnection();
}

main().catch(console.error);
