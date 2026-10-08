import { QUEUES, OrderMessage } from '../lib/config';
import { startSubscriber } from './subscriber';

export function handleNotification(order: OrderMessage): void {
  console.log(
    `[notification] Sending order confirmation to customer ${order.customerId} — order ${order.orderId}, total: $${order.total}`
  );
}

async function main(): Promise<void> {
  await startSubscriber(QUEUES.NOTIFICATION_EVENTS, handleNotification, 'notification-consumer');
}

if (require.main === module) {
  main().catch(console.error);
}
