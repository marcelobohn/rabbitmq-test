import { Channel, ConsumeMessage } from 'amqplib';
import { handleBankEvent } from '../../src/bank/alerts-consumer';
import { InMemoryProcessedStore } from '../../src/lib/idempotency';

function withdrawal(amount: number, version = 3, messageId: string | undefined = `conta-1:${version}`): ConsumeMessage {
  const body = {
    streamId: 'conta-1', version, position: 10, type: 'MoneyWithdrawn',
    payload: { accountId: 'conta-1', amount }, recordedAt: '2026-10-09T12:00:00+00:00',
  };
  return { content: Buffer.from(JSON.stringify(body)), properties: { messageId, type: 'MoneyWithdrawn' }, fields: {} } as unknown as ConsumeMessage;
}

describe('handleBankEvent', () => {
  let channel: jest.Mocked<Pick<Channel, 'ack' | 'nack'>>;
  let store: InMemoryProcessedStore;

  beforeEach(() => {
    channel = { ack: jest.fn(), nack: jest.fn() };
    store = new InMemoryProcessedStore();
  });

  const run = (msg: ConsumeMessage) =>
    handleBankEvent(channel as unknown as Channel, msg, { store, threshold: 100_000 });

  it('alerts on a withdrawal at or above the threshold', async () => {
    const msg = withdrawal(100_000);

    const result = await run(msg);

    expect(result.kind).toBe('alert');
    expect(result.message).toContain('R$ 1.000,00');
    expect(result.message).toContain('conta-1');
    expect(channel.ack).toHaveBeenCalledWith(msg);
  });

  it('only logs a withdrawal below the threshold', async () => {
    const result = await run(withdrawal(99_999));

    expect(result.kind).toBe('info');
    expect(channel.ack).toHaveBeenCalled();
  });

  it('ignores a republished event with the same messageId', async () => {
    await run(withdrawal(200_000));
    const duplicate = withdrawal(200_000);

    const result = await run(duplicate);

    expect(result.kind).toBe('duplicate');
    expect(channel.ack).toHaveBeenCalledWith(duplicate);
  });

  it('falls back to <stream>:<version> when there is no messageId', async () => {
    await run(withdrawal(200_000, 4, undefined));

    expect((await run(withdrawal(200_000, 4, undefined))).kind).toBe('duplicate');
  });

  it('nacks malformed messages without requeue', async () => {
    const msg = { content: Buffer.from('not json'), properties: {}, fields: {} } as unknown as ConsumeMessage;
    jest.spyOn(console, 'error').mockImplementation(() => {});

    const result = await run(msg);

    expect(result.kind).toBe('invalid');
    expect(channel.nack).toHaveBeenCalledWith(msg, false, false);
  });
});
