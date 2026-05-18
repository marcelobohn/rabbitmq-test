import { handleInventoryUpdate } from '../../src/pubsub/inventory-consumer';
import { handleNotification } from '../../src/pubsub/notification-consumer';
import { OrderMessage } from '../../src/lib/config';

const sampleOrder: OrderMessage = {
  orderId: 'ord-001',
  customerId: 'cust-42',
  items: [{ productId: 'prod-10', quantity: 3, price: 15.00 }],
  total: 45.00,
  createdAt: '2026-05-18T00:00:00.000Z',
};

describe('handleInventoryUpdate', () => {
  it('logs stock deduction without throwing', () => {
    expect(() => handleInventoryUpdate(sampleOrder)).not.toThrow();
  });
});

describe('handleNotification', () => {
  it('logs email notification without throwing', () => {
    expect(() => handleNotification(sampleOrder)).not.toThrow();
  });
});
