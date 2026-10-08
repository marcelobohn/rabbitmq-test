import { Channel, ConsumeMessage } from 'amqplib';
import { handleEvent } from '../../src/pubsub/subscriber';
import { OrderMessage } from '../../src/lib/config';
import { InMemoryProcessedStore } from '../../src/lib/idempotency';

const sampleOrder: OrderMessage = {
  orderId: 'ord-001',
  customerId: 'cust-42',
  items: [{ productId: 'prod-10', quantity: 3, price: 15.00 }],
  total: 45.00,
  createdAt: '2026-05-18T00:00:00.000Z',
};

function makeMsg(content: string, messageId: string | undefined = 'evt-001'): ConsumeMessage {
  return { content: Buffer.from(content), properties: { messageId, headers: {} }, fields: {} } as unknown as ConsumeMessage;
}

describe('handleEvent', () => {
  let channel: jest.Mocked<Pick<Channel, 'ack' | 'nack'>>;
  let store: InMemoryProcessedStore;

  beforeEach(() => {
    channel = { ack: jest.fn(), nack: jest.fn() };
    store = new InMemoryProcessedStore();
  });

  const run = (msg: ConsumeMessage, handler: jest.Mock) =>
    handleEvent(channel as unknown as Channel, msg, handler, store, 'test');

  it('runs the handler and acks', async () => {
    const handler = jest.fn();
    const msg = makeMsg(JSON.stringify(sampleOrder));

    await run(msg, handler);

    expect(handler).toHaveBeenCalledWith(sampleOrder);
    expect(channel.ack).toHaveBeenCalledWith(msg);
  });

  it('acks a duplicate event without running the handler again', async () => {
    const handler = jest.fn();

    await run(makeMsg(JSON.stringify(sampleOrder)), handler);
    const duplicate = makeMsg(JSON.stringify(sampleOrder));
    await run(duplicate, handler);

    expect(handler).toHaveBeenCalledTimes(1);
    expect(channel.ack).toHaveBeenCalledWith(duplicate);
  });

  it('does not mark the event as processed when the handler fails', async () => {
    const handler = jest.fn()
      .mockImplementationOnce(() => { throw new Error('boom'); })
      .mockImplementationOnce(() => undefined);

    const failed = makeMsg(JSON.stringify(sampleOrder));
    await run(failed, handler);
    await run(makeMsg(JSON.stringify(sampleOrder)), handler);

    expect(channel.nack).toHaveBeenCalledWith(failed, false, false);
    expect(handler).toHaveBeenCalledTimes(2);
  });

  it('nacks malformed messages without requeue', async () => {
    const handler = jest.fn();
    const msg = makeMsg('not json');

    await run(msg, handler);

    expect(handler).not.toHaveBeenCalled();
    expect(channel.nack).toHaveBeenCalledWith(msg, false, false);
  });
});
