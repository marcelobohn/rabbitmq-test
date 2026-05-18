jest.mock('amqplib');
import amqplib from 'amqplib';
import { connectWithRetry } from '../../src/lib/connection';

const mockConnect = amqplib.connect as jest.MockedFunction<typeof amqplib.connect>;

describe('connectWithRetry', () => {
  beforeEach(() => {
    jest.useFakeTimers();
    mockConnect.mockReset();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('resolves immediately on first successful attempt', async () => {
    const mockConn = { on: jest.fn(), close: jest.fn() } as any;
    mockConnect.mockResolvedValueOnce(mockConn);

    const conn = await connectWithRetry('amqp://localhost', 3);

    expect(conn).toBe(mockConn);
    expect(mockConnect).toHaveBeenCalledTimes(1);
  });

  it('retries on failure and resolves on second attempt', async () => {
    const mockConn = { on: jest.fn(), close: jest.fn() } as any;
    mockConnect
      .mockRejectedValueOnce(new Error('ECONNREFUSED'))
      .mockResolvedValueOnce(mockConn);

    const connPromise = connectWithRetry('amqp://localhost', 3);
    await jest.runAllTimersAsync();
    const conn = await connPromise;

    expect(conn).toBe(mockConn);
    expect(mockConnect).toHaveBeenCalledTimes(2);
  });

  it('throws after exhausting all attempts', async () => {
    mockConnect.mockRejectedValue(new Error('ECONNREFUSED'));

    const connPromise = connectWithRetry('amqp://localhost', 3);
    await Promise.all([
      jest.runAllTimersAsync(),
      expect(connPromise).rejects.toThrow('ECONNREFUSED'),
    ]);

    expect(mockConnect).toHaveBeenCalledTimes(3);
  });
});
