jest.mock('../../src/lib/connection');
import { getConnection } from '../../src/lib/connection';
import { queryOrderStatus } from '../../src/rpc/client';

describe('queryOrderStatus', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('resolves with status when reply arrives with matching correlationId', async () => {
    let consumeCallback: ((msg: any) => void) | null = null;

    const mockChannel = {
      assertQueue: jest.fn().mockResolvedValue({ queue: 'amq.gen-test' }),
      consume: jest.fn().mockImplementation((_queue: string, cb: (msg: any) => void) => {
        consumeCallback = cb;
        return Promise.resolve({ consumerTag: 'tag' });
      }),
      sendToQueue: jest.fn().mockImplementation(
        (_queue: string, _buf: Buffer, opts: { correlationId: string }) => {
          setTimeout(() => {
            consumeCallback?.({
              content: Buffer.from(
                JSON.stringify({ orderId: 'ord-001', status: 'processing', updatedAt: '2026-05-18T00:00:00.000Z' })
              ),
              properties: { correlationId: opts.correlationId },
            });
          }, 100);
          return true;
        }
      ),
      close: jest.fn().mockResolvedValue(undefined),
    };

    (getConnection as jest.Mock).mockResolvedValue({
      createChannel: jest.fn().mockResolvedValue(mockChannel),
    });

    const resultPromise = queryOrderStatus('ord-001');
    await jest.runAllTimersAsync();
    const result = await resultPromise;

    expect(result.orderId).toBe('ord-001');
    expect(result.status).toBe('processing');
  });

  it('rejects with timeout error when no reply arrives within 5s', async () => {
    const mockChannel = {
      assertQueue: jest.fn().mockResolvedValue({ queue: 'amq.gen-test' }),
      consume: jest.fn().mockResolvedValue({ consumerTag: 'tag' }),
      sendToQueue: jest.fn().mockReturnValue(true),
      close: jest.fn().mockResolvedValue(undefined),
    };

    (getConnection as jest.Mock).mockResolvedValue({
      createChannel: jest.fn().mockResolvedValue(mockChannel),
    });

    const resultPromise = queryOrderStatus('ord-999');
    jest.advanceTimersByTime(5001);

    await expect(resultPromise).rejects.toThrow('RPC timeout');
  });

  it('ignores replies with a non-matching correlationId', async () => {
    let consumeCallback: ((msg: any) => void) | null = null;

    const mockChannel = {
      assertQueue: jest.fn().mockResolvedValue({ queue: 'amq.gen-test' }),
      consume: jest.fn().mockImplementation((_queue: string, cb: (msg: any) => void) => {
        consumeCallback = cb;
        return Promise.resolve({ consumerTag: 'tag' });
      }),
      sendToQueue: jest.fn().mockImplementation(() => {
        setTimeout(() => {
          consumeCallback?.({
            content: Buffer.from(JSON.stringify({ orderId: 'ord-001', status: 'shipped', updatedAt: '' })),
            properties: { correlationId: 'wrong-id' },
          });
        }, 100);
        return true;
      }),
      close: jest.fn().mockResolvedValue(undefined),
    };

    (getConnection as jest.Mock).mockResolvedValue({
      createChannel: jest.fn().mockResolvedValue(mockChannel),
    });

    const resultPromise = queryOrderStatus('ord-001');
    await jest.runAllTimersAsync();

    await expect(resultPromise).rejects.toThrow('RPC timeout');
  });
});
