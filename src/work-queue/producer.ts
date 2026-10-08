import { v4 as uuidv4 } from 'uuid';
import { getConnection, closeConnection } from '../lib/connection';
import { QUEUES, OrderMessage } from '../lib/config';
import { openTelemetry } from '../lib/telemetry';

async function main(): Promise<void> {
  const connection = await getConnection();
  const channel = await connection.createConfirmChannel();
  const emit = await openTelemetry(connection, 'producer');

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

    const messageId = uuidv4();
    channel.sendToQueue(QUEUES.ORDERS_PROCESSING, Buffer.from(JSON.stringify(order)), {
      persistent: true,
      messageId,
      headers: { 'x-retry-count': 0 },
    });
    emit({
      orderId: order.orderId,
      messageId,
      stage: 'created',
      detail: { total: order.total, customerId: order.customerId, outcome: 'success' },
    });
    console.log(`[producer] Sent order ${order.orderId} — total: $${order.total}`);
  }

  // Only exit after the broker confirms every message was stored
  await channel.waitForConfirms();
  console.log('[producer] All orders confirmed by the broker');

  await channel.close();
  await closeConnection();
}

if (require.main === module) {
  main().catch(console.error);
}
