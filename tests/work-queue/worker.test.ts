import { Channel, ConsumeMessage } from 'amqplib';
import { handleMessage } from '../../src/work-queue/worker';
import { QUEUES, MAX_RETRIES, OrderMessage } from '../../src/lib/config';

function makeMsg(body: OrderMessage, headers: Record<string, unknown> = {}): ConsumeMessage {
  return {
    content: Buffer.from(JSON.stringify(body)),
    properties: { headers, correlationId: undefined, replyTo: undefined, contentType: undefined, contentEncoding: undefined, deliveryMode: undefined, priority: undefined, expiration: undefined, messageId: undefined, timestamp: undefined, type: undefined, userId: undefined, appId: undefined, clusterId: undefined },
    fields: { deliveryTag: 1, redelivered: false, exchange: '', routingKey: QUEUES.ORDERS_PROCESSING, consumerTag: 'tag' },
  } as unknown as ConsumeMessage;
}

const sampleOrder: OrderMessage = {
  orderId: 'ord-001',
  customerId: 'cust-42',
  items: [{ productId: 'prod-1', quantity: 2, price: 15.00 }],
  total: 30.00,
  createdAt: '2026-05-18T00:00:00.000Z',
};

describe('handleMessage', () => {
  let mockChannel: jest.Mocked<Pick<Channel, 'ack' | 'sendToQueue'>>;

  beforeEach(() => {
    mockChannel = {
      ack: jest.fn(),
      sendToQueue: jest.fn().mockReturnValue(true),
    };
  });

  it('acks the message after successful processing', async () => {
    const processor = jest.fn().mockResolvedValue(undefined);
    const msg = makeMsg(sampleOrder);

    await handleMessage(mockChannel as unknown as Channel, msg, processor);

    expect(mockChannel.ack).toHaveBeenCalledWith(msg);
    expect(mockChannel.sendToQueue).not.toHaveBeenCalled();
  });

  it('sends to first retry queue (5s) on first failure', async () => {
    const processor = jest.fn().mockRejectedValue(new Error('timeout'));
    const msg = makeMsg(sampleOrder, { 'x-retry-count': 0 });

    await handleMessage(mockChannel as unknown as Channel, msg, processor);

    expect(mockChannel.ack).toHaveBeenCalledWith(msg);
    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      QUEUES.ORDERS_RETRY_5S,
      msg.content,
      expect.objectContaining({
        headers: expect.objectContaining({ 'x-retry-count': 1 }),
        persistent: true,
      })
    );
  });

  it('sends to second retry queue (30s) on second failure', async () => {
    const processor = jest.fn().mockRejectedValue(new Error('timeout'));
    const msg = makeMsg(sampleOrder, { 'x-retry-count': 1 });

    await handleMessage(mockChannel as unknown as Channel, msg, processor);

    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      QUEUES.ORDERS_RETRY_30S,
      msg.content,
      expect.objectContaining({
        headers: expect.objectContaining({ 'x-retry-count': 2 }),
      })
    );
  });

  it('sends to DLQ after max retries are exhausted', async () => {
    const processor = jest.fn().mockRejectedValue(new Error('permanent failure'));
    const msg = makeMsg(sampleOrder, { 'x-retry-count': MAX_RETRIES });

    await handleMessage(mockChannel as unknown as Channel, msg, processor);

    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      QUEUES.ORDERS_DLQ,
      msg.content,
      expect.objectContaining({
        headers: expect.objectContaining({ 'x-error': 'permanent failure' }),
        persistent: true,
      })
    );
  });
});
