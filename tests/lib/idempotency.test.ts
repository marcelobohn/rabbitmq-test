import { ConsumeMessage } from 'amqplib';
import { InMemoryProcessedStore, dedupKey } from '../../src/lib/idempotency';

describe('InMemoryProcessedStore', () => {
  it('reports ids only after they are added', async () => {
    const store = new InMemoryProcessedStore();

    expect(await store.has('msg-1')).toBe(false);
    await store.add('msg-1');
    expect(await store.has('msg-1')).toBe(true);
  });

  it('evicts the oldest id when maxSize is exceeded', async () => {
    const store = new InMemoryProcessedStore(2);

    await store.add('a');
    await store.add('b');
    await store.add('c');

    expect(await store.has('a')).toBe(false);
    expect(await store.has('b')).toBe(true);
    expect(await store.has('c')).toBe(true);
  });
});

describe('dedupKey', () => {
  const withMessageId = (messageId: string | undefined) =>
    ({ properties: { messageId } } as unknown as ConsumeMessage);

  it('prefers the AMQP messageId', () => {
    expect(dedupKey(withMessageId('uuid-1'), 'ord-001')).toBe('uuid-1');
  });

  it('falls back to the orderId when the message has no messageId', () => {
    expect(dedupKey(withMessageId(undefined), 'ord-001')).toBe('ord-001');
  });
});
