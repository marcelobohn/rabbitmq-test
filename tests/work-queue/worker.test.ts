import { ConfirmChannel, ConsumeMessage } from 'amqplib';
import { handleMessage, createSimulatedProcessor } from '../../src/work-queue/worker';
import { QUEUES, EXCHANGES, MAX_RETRIES, RETRY_DELAYS, OrderMessage } from '../../src/lib/config';
import { TelemetryInput } from '../../src/lib/telemetry';
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
  let mockChannel: jest.Mocked<Pick<ConfirmChannel, 'ack' | 'nack' | 'sendToQueue' | 'publish' | 'waitForConfirms'>>;
  let emitted: TelemetryInput[];
  let store: InMemoryProcessedStore;
  // Records the order of channel calls, to assert "confirm before ack".
  let calls: string[];

  beforeEach(() => {
    calls = [];
    mockChannel = {
      ack: jest.fn().mockImplementation(() => { calls.push('ack'); }),
      nack: jest.fn().mockImplementation(() => { calls.push('nack'); }),
      sendToQueue: jest.fn().mockImplementation(() => { calls.push('sendToQueue'); return true; }),
      publish: jest.fn().mockImplementation(() => { calls.push('publish'); return true; }),
      waitForConfirms: jest.fn().mockImplementation(async () => { calls.push('waitForConfirms'); }),
    };
    store = new InMemoryProcessedStore();
    emitted = [];
  });

  const stages = () => emitted.map(e => e.stage);

  const run = (msg: ConsumeMessage, processor: jest.Mock) =>
    handleMessage(mockChannel as unknown as ConfirmChannel, msg, processor, store, (e) => { emitted.push(e); });

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

describe('handleMessage — OrderProcessed and telemetry', () => {
  let channel: jest.Mocked<Pick<ConfirmChannel, 'ack' | 'nack' | 'sendToQueue' | 'publish' | 'waitForConfirms'>>;
  let store: InMemoryProcessedStore;
  let emitted: TelemetryInput[];
  let calls: string[];

  beforeEach(() => {
    calls = [];
    emitted = [];
    store = new InMemoryProcessedStore();
    channel = {
      ack: jest.fn().mockImplementation(() => { calls.push('ack'); }),
      nack: jest.fn().mockImplementation(() => { calls.push('nack'); }),
      sendToQueue: jest.fn().mockImplementation(() => { calls.push('sendToQueue'); return true; }),
      publish: jest.fn().mockImplementation(() => { calls.push('publish'); return true; }),
      waitForConfirms: jest.fn().mockImplementation(async () => { calls.push('waitForConfirms'); }),
    };
  });

  const run = (msg: ConsumeMessage, processor: jest.Mock) =>
    handleMessage(channel as unknown as ConfirmChannel, msg, processor, store, (e) => { emitted.push(e); });
  const stages = () => emitted.map(e => e.stage);

  it('publishes OrderProcessed with a derived messageId, confirmed before the ack', async () => {
    const msg = makeMsg(sampleOrder);

    await run(msg, jest.fn().mockResolvedValue(undefined));

    expect(channel.publish).toHaveBeenCalledWith(
      EXCHANGES.ORDERS_EVENTS,
      '',
      msg.content,
      expect.objectContaining({ type: 'OrderProcessed', messageId: 'msg-001:processed', persistent: true })
    );
    expect(calls).toEqual(['publish', 'waitForConfirms', 'ack']);
    expect(stages()).toEqual(['processing', 'processed', 'event-published']);
    expect(emitted[0]).toEqual(expect.objectContaining({ orderId: 'ord-001', messageId: 'msg-001', attempt: 1 }));
  });

  it('requeues the order and does not mark it processed when OrderProcessed is not confirmed', async () => {
    channel.waitForConfirms.mockRejectedValueOnce(new Error('nacked'));
    const processor = jest.fn().mockResolvedValue(undefined);
    const msg = makeMsg(sampleOrder);
    jest.spyOn(console, 'error').mockImplementation(() => {});

    await run(msg, processor);
    expect(channel.nack).toHaveBeenCalledWith(msg, false, true);
    expect(channel.ack).not.toHaveBeenCalled();

    await run(makeMsg(sampleOrder), processor);
    expect(processor).toHaveBeenCalledTimes(2);
  });

  it('emits failed and retry-scheduled with queue and delay on a first failure', async () => {
    await run(makeMsg(sampleOrder, { 'x-retry-count': 0 }), jest.fn().mockRejectedValue(new Error('boom')));

    expect(stages()).toEqual(['processing', 'failed', 'retry-scheduled']);
    expect(emitted[1].detail).toEqual({ error: 'boom' });
    expect(emitted[2].detail).toEqual({ queue: QUEUES.ORDERS_RETRY_5S, delayMs: RETRY_DELAYS[0] });
  });

  it('reports the attempt number from x-retry-count', async () => {
    await run(makeMsg(sampleOrder, { 'x-retry-count': 1 }), jest.fn().mockRejectedValue(new Error('boom')));

    expect(emitted[0].attempt).toBe(2);
    expect(emitted[2].detail).toEqual({ queue: QUEUES.ORDERS_RETRY_30S, delayMs: RETRY_DELAYS[1] });
  });

  it('emits dlq when retries are exhausted', async () => {
    await run(makeMsg(sampleOrder, { 'x-retry-count': MAX_RETRIES }), jest.fn().mockRejectedValue(new Error('boom')));

    expect(stages()).toEqual(['processing', 'failed', 'dlq']);
  });

  it('emits duplicate-skipped for a duplicate', async () => {
    const processor = jest.fn().mockResolvedValue(undefined);
    await run(makeMsg(sampleOrder), processor);
    emitted = [];

    await run(makeMsg(sampleOrder), processor);

    expect(stages()).toEqual(['duplicate-skipped']);
  });

  it('emits dlq for malformed JSON', async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    await run(makeMsg('not json'), jest.fn());

    expect(stages()).toEqual(['dlq']);
    expect(emitted[0].detail).toEqual(expect.objectContaining({ error: expect.stringContaining('Parse error') }));
  });
});

describe('createSimulatedProcessor', () => {
  const processor = createSimulatedProcessor(async () => {});
  const msgWith = (headers: Record<string, unknown>) => makeMsg(sampleOrder, headers);

  beforeEach(() => { jest.spyOn(console, 'log').mockImplementation(() => {}); });

  it('succeeds without an x-simulate header', async () => {
    await expect(processor(sampleOrder, msgWith({}))).resolves.toBeUndefined();
  });

  it('succeeds with x-simulate success', async () => {
    await expect(processor(sampleOrder, msgWith({ 'x-simulate': 'success' }))).resolves.toBeUndefined();
  });

  it('fail-once fails on the first attempt and succeeds on the retry', async () => {
    await expect(processor(sampleOrder, msgWith({ 'x-simulate': 'fail-once', 'x-retry-count': 0 }))).rejects.toThrow('fail-once');
    await expect(processor(sampleOrder, msgWith({ 'x-simulate': 'fail-once', 'x-retry-count': 1 }))).resolves.toBeUndefined();
  });

  it('fail-always fails on every attempt', async () => {
    for (const retry of [0, 1, 2]) {
      await expect(processor(sampleOrder, msgWith({ 'x-simulate': 'fail-always', 'x-retry-count': retry }))).rejects.toThrow('fail-always');
    }
  });
});
