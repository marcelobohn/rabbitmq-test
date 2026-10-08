import { QUEUES, OrderMessage } from '../lib/config';
import { startSubscriber } from './subscriber';

export function handleInventoryUpdate(order: OrderMessage): void {
  const totalItems = order.items.reduce((sum, item) => sum + item.quantity, 0);
  console.log(
    `[inventory] Deducting ${totalItems} item(s) for order ${order.orderId} (customer: ${order.customerId})`
  );
}

async function main(): Promise<void> {
  await startSubscriber(QUEUES.INVENTORY_EVENTS, handleInventoryUpdate, 'inventory-consumer', 'inventory-done');
}

if (require.main === module) {
  main().catch(console.error);
}
