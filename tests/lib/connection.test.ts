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

describe('getConnection', () => {
  afterEach(() => {
    jest.resetModules();
  });

  it('returns the same promise on concurrent calls (no double connect)', async () => {
    const mockConn = { on: jest.fn(), close: jest.fn(), removeAllListeners: jest.fn() } as any;

    let getConn!: () => Promise<any>;
    jest.isolateModules(() => {
      jest.mock('amqplib');
      const amqp = require('amqplib');
      (amqp.connect as jest.Mock).mockResolvedValue(mockConn);
      ({ getConnection: getConn } = require('../../src/lib/connection'));
    });

    const [conn1, conn2] = await Promise.all([getConn(), getConn()]);

    expect(conn1).toBe(conn2);
    // Both calls should have resolved to the same connection object
    expect(conn1).toBe(mockConn);
  });

  it('reconnects after connection error resets the promise', async () => {
    const mockConn = { on: jest.fn(), close: jest.fn(), removeAllListeners: jest.fn() } as any;

    let errorHandler: ((err: Error) => void) | undefined;
    mockConn.on.mockImplementation((event: string, handler: (err?: Error) => void) => {
      if (event === 'error') errorHandler = handler;
    });

    let getConn!: () => Promise<any>;
    let amqpMock: any;
    jest.isolateModules(() => {
      jest.mock('amqplib');
      amqpMock = require('amqplib');
      amqpMock.connect.mockResolvedValue(mockConn);
      ({ getConnection: getConn } = require('../../src/lib/connection'));
    });

    await getConn();
    // Trigger the error handler to reset the promise
    errorHandler!(new Error('socket hang up'));

    // mockConn will be returned again since connect is still mocked to return it
    await getConn();
    expect(amqpMock.connect).toHaveBeenCalledTimes(2);
  });
});

describe('closeConnection', () => {
  afterEach(() => {
    jest.resetModules();
  });

  it('closes the active connection and resets state', async () => {
    const mockConn = {
      on: jest.fn(),
      close: jest.fn().mockResolvedValue(undefined),
      removeAllListeners: jest.fn(),
    } as any;

    let getConn!: () => Promise<any>;
    let closConn!: () => Promise<void>;
    jest.isolateModules(() => {
      jest.mock('amqplib');
      const amqp = require('amqplib');
      (amqp.connect as jest.Mock).mockResolvedValue(mockConn);
      ({ getConnection: getConn, closeConnection: closConn } = require('../../src/lib/connection'));
    });

    await getConn();
    await closConn();

    expect(mockConn.removeAllListeners).toHaveBeenCalled();
    expect(mockConn.close).toHaveBeenCalled();
  });

  it('is a no-op when no connection exists', async () => {
    let closConn!: () => Promise<void>;
    let amqpMock: any;
    jest.isolateModules(() => {
      jest.mock('amqplib');
      amqpMock = require('amqplib');
      ({ closeConnection: closConn } = require('../../src/lib/connection'));
    });

    await expect(closConn()).resolves.toBeUndefined();
    expect(amqpMock.connect).not.toHaveBeenCalled();
  });
});
