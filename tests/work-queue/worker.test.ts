import { ConfirmChannel, ConsumeMessage } from 'amqplib';
import { handleMessage } from '../../src/work-queue/worker';
import { QUEUES, MAX_RETRIES, OrderMessage } from '../../src/lib/config';
import { InMemoryProcessedStore } from '../../src/lib/idempotency';

function makeMsg(
  body: OrderMessage | string,
  headers: Record<string, unknown> = {},
  messageId: string | undefined = 'msg-001'
): ConsumeMessage {
  const content = typeof body === 'string' ? body : JSON.stringify(body);
  return {
    content: Buffer.from(content),
    properties: { headers, messageId, correlationId: undefined, replyTo: undefined, contentType: undefined, contentEncoding: undefined, deliveryMode: undefined, priority: undefined, expiration: undefined, timestamp: undefined, type: undefined, userId: undefined, appId: undefined, clusterId: undefined },
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
  let mockChannel: jest.Mocked<Pick<ConfirmChannel, 'ack' | 'nack' | 'sendToQueue' | 'waitForConfirms'>>;
  let store: InMemoryProcessedStore;
  // Records the order of channel calls, to assert "confirm before ack".
  let calls: string[];

  beforeEach(() => {
    calls = [];
    mockChannel = {
      ack: jest.fn().mockImplementation(() => { calls.push('ack'); }),
      nack: jest.fn().mockImplementation(() => { calls.push('nack'); }),
      sendToQueue: jest.fn().mockImplementation(() => { calls.push('sendToQueue'); return true; }),
      waitForConfirms: jest.fn().mockImplementation(async () => { calls.push('waitForConfirms'); }),
    };
    store = new InMemoryProcessedStore();
  });

  const run = (msg: ConsumeMessage, processor: jest.Mock) =>
    handleMessage(mockChannel as unknown as ConfirmChannel, msg, processor, store);

  it('acks the message after successful processing', async () => {
    const processor = jest.fn().mockResolvedValue(undefined);
    const msg = makeMsg(sampleOrder);

    await run(msg, processor);

    expect(mockChannel.ack).toHaveBeenCalledWith(msg);
    expect(mockChannel.sendToQueue).not.toHaveBeenCalled();
  });

  it('sends to first retry queue (5s) on first failure', async () => {
    const processor = jest.fn().mockRejectedValue(new Error('timeout'));
    const msg = makeMsg(sampleOrder, { 'x-retry-count': 0 });

    await run(msg, processor);

    expect(mockChannel.ack).toHaveBeenCalledWith(msg);
    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      QUEUES.ORDERS_RETRY_5S,
      msg.content,
      expect.objectContaining({
        headers: expect.objectContaining({ 'x-retry-count': 1 }),
        persistent: true,
        messageId: 'msg-001',
      })
    );
  });

  it('sends to second retry queue (30s) on second failure', async () => {
    const processor = jest.fn().mockRejectedValue(new Error('timeout'));
    const msg = makeMsg(sampleOrder, { 'x-retry-count': 1 });

    await run(msg, processor);

    expect(mockChannel.ack).toHaveBeenCalledWith(msg);
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

    await run(msg, processor);

    expect(mockChannel.ack).toHaveBeenCalledWith(msg);
    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      QUEUES.ORDERS_DLQ,
      msg.content,
      expect.objectContaining({
        headers: expect.objectContaining({ 'x-error': 'permanent failure' }),
        persistent: true,
      })
    );
  });

  describe('publish-then-ack ordering', () => {
    it('acks the original only after the broker confirms the retry copy', async () => {
      const processor = jest.fn().mockRejectedValue(new Error('timeout'));

      await run(makeMsg(sampleOrder), processor);

      expect(calls).toEqual(['sendToQueue', 'waitForConfirms', 'ack']);
    });

    it('requeues the original instead of acking when the retry copy is not confirmed', async () => {
      mockChannel.waitForConfirms.mockRejectedValueOnce(new Error('nacked by broker'));
      const processor = jest.fn().mockRejectedValue(new Error('timeout'));
      const msg = makeMsg(sampleOrder);

      await run(msg, processor);

      expect(mockChannel.ack).not.toHaveBeenCalled();
      expect(mockChannel.nack).toHaveBeenCalledWith(msg, false, true);
    });

    it('sends malformed messages to the DLQ and acks only after confirmation', async () => {
      const processor = jest.fn();
      const msg = makeMsg('not json');

      await run(msg, processor);

      expect(processor).not.toHaveBeenCalled();
      expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
        QUEUES.ORDERS_DLQ,
        msg.content,
        expect.objectContaining({
          headers: expect.objectContaining({ 'x-error': expect.stringContaining('Parse error') }),
        })
      );
      expect(calls).toEqual(['sendToQueue', 'waitForConfirms', 'ack']);
    });
  });

  describe('idempotency', () => {
    it('skips the processor and acks when the same messageId arrives twice', async () => {
      const processor = jest.fn().mockResolvedValue(undefined);
      const first = makeMsg(sampleOrder);
      const duplicate = makeMsg(sampleOrder);

      await run(first, processor);
      await run(duplicate, processor);

      expect(processor).toHaveBeenCalledTimes(1);
      expect(mockChannel.ack).toHaveBeenCalledWith(duplicate);
    });

    it('processes a retried message whose earlier attempt failed', async () => {
      const processor = jest.fn()
        .mockRejectedValueOnce(new Error('timeout'))
        .mockResolvedValueOnce(undefined);

      await run(makeMsg(sampleOrder, { 'x-retry-count': 0 }), processor);
      await run(makeMsg(sampleOrder, { 'x-retry-count': 1 }), processor);

      expect(processor).toHaveBeenCalledTimes(2);
    });

    it('treats different messageIds as different messages', async () => {
      const processor = jest.fn().mockResolvedValue(undefined);

      await run(makeMsg(sampleOrder, {}, 'msg-A'), processor);
      await run(makeMsg(sampleOrder, {}, 'msg-B'), processor);

      expect(processor).toHaveBeenCalledTimes(2);
    });
  });
});
