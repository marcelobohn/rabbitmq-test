import { Channel } from 'amqplib';
import { publishOrderCreated } from '../../src/pubsub/publisher';
import { EXCHANGES, OrderMessage } from '../../src/lib/config';

describe('publishOrderCreated', () => {
  it('publishes to orders.events exchange with empty routing key', () => {
    const mockChannel = { publish: jest.fn().mockReturnValue(true) } as unknown as Channel;
    const order: OrderMessage = {
      orderId: 'ord-001',
      customerId: 'cust-1',
      items: [{ productId: 'prod-1', quantity: 2, price: 15.00 }],
      total: 30.00,
      createdAt: '2026-05-18T00:00:00.000Z',
    };

    publishOrderCreated(mockChannel, order);

    expect(mockChannel.publish).toHaveBeenCalledWith(
      EXCHANGES.ORDERS_EVENTS,
      '',
      expect.any(Buffer),
      expect.objectContaining({ persistent: true, type: 'OrderCreated' })
    );
  });

  it('stamps each event with a unique messageId for consumer deduplication', () => {
    const mockChannel = { publish: jest.fn().mockReturnValue(true) } as unknown as Channel;
    const order: OrderMessage = {
      orderId: 'ord-003',
      customerId: 'cust-1',
      items: [{ productId: 'prod-1', quantity: 1, price: 10.00 }],
      total: 10.00,
      createdAt: '2026-05-18T00:00:00.000Z',
    };

    publishOrderCreated(mockChannel, order);
    publishOrderCreated(mockChannel, order);

    const [first, second] = (mockChannel.publish as jest.Mock).mock.calls.map(c => c[3].messageId);
    expect(first).toEqual(expect.any(String));
    expect(first).not.toEqual(second);
  });

  it('serializes the full order payload in the message body', () => {
    const mockChannel = { publish: jest.fn().mockReturnValue(true) } as unknown as Channel;
    const order: OrderMessage = {
      orderId: 'ord-002',
      customerId: 'cust-7',
      items: [{ productId: 'prod-5', quantity: 1, price: 99.90 }],
      total: 99.90,
      createdAt: '2026-05-18T00:00:00.000Z',
    };

    publishOrderCreated(mockChannel, order);

    const [, , bodyBuffer] = (mockChannel.publish as jest.Mock).mock.calls[0];
    const published = JSON.parse(bodyBuffer.toString());
    expect(published).toEqual(order);
  });
});
