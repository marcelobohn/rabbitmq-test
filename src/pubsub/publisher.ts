import { Channel } from 'amqplib';
import { v4 as uuidv4 } from 'uuid';
import { getConnection, closeConnection } from '../lib/connection';
import { EXCHANGES, OrderMessage } from '../lib/config';
import { openTelemetry } from '../lib/telemetry';

// Returns the messageId stamped on the event
export function publishOrderCreated(channel: Channel, order: OrderMessage): string {
  const messageId = uuidv4();
  channel.publish(EXCHANGES.ORDERS_EVENTS, '', Buffer.from(JSON.stringify(order)), {
    persistent: true,
    messageId,
    type: 'OrderCreated',
    contentType: 'application/json',
  });
  return messageId;
}

async function main(): Promise<void> {
  const connection = await getConnection();
  const channel = await connection.createConfirmChannel();
  const emit = await openTelemetry(connection, 'publisher');

  await channel.assertExchange(EXCHANGES.ORDERS_EVENTS, 'fanout', { durable: true });

  for (let i = 1; i <= 3; i++) {
    const order: OrderMessage = {
      orderId: `ord-pub-${String(i).padStart(3, '0')}`,
      customerId: `cust-${i * 10}`,
      items: [{ productId: `prod-${i}`, quantity: i, price: 29.99 }],
      total: parseFloat((i * 29.99).toFixed(2)),
      createdAt: new Date().toISOString(),
    };
    const messageId = publishOrderCreated(channel, order);
    emit({
      orderId: order.orderId,
      messageId,
      stage: 'created',
      detail: { total: order.total, customerId: order.customerId },
    });
    console.log(`[publisher] Published OrderCreated: ${order.orderId}`);
  }

  await channel.waitForConfirms();
  console.log('[publisher] All events confirmed by the broker');

  await channel.close();
  await closeConnection();
}

if (require.main === module) {
  main().catch(console.error);
}
