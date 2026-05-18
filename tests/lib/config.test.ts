describe('config', () => {
  const originalUrl = process.env.RABBITMQ_URL;

  afterEach(() => {
    if (originalUrl === undefined) {
      delete process.env.RABBITMQ_URL;
    } else {
      process.env.RABBITMQ_URL = originalUrl;
    }
    jest.resetModules();
  });

  it('uses default URL when RABBITMQ_URL is not set', () => {
    delete process.env.RABBITMQ_URL;
    jest.resetModules();
    const { RABBITMQ_URL } = require('../../src/lib/config');
    expect(RABBITMQ_URL).toBe('amqp://guest:guest@localhost:5672');
  });

  it('uses RABBITMQ_URL env var when set', () => {
    process.env.RABBITMQ_URL = 'amqp://user:pass@custom:5672';
    jest.resetModules();
    const { RABBITMQ_URL } = require('../../src/lib/config');
    expect(RABBITMQ_URL).toBe('amqp://user:pass@custom:5672');
  });

  it('exports expected queue names', () => {
    const { QUEUES } = require('../../src/lib/config');
    expect(QUEUES.ORDERS_PROCESSING).toBe('orders.processing');
    expect(QUEUES.ORDERS_DLQ).toBe('orders.dlq');
    expect(QUEUES.ORDERS_RETRY_5S).toBe('orders.retry.5s');
    expect(QUEUES.ORDERS_RETRY_30S).toBe('orders.retry.30s');
    expect(QUEUES.ORDERS_STATUS_RPC).toBe('orders.status.rpc');
  });

  it('exports orders.events exchange name', () => {
    const { EXCHANGES } = require('../../src/lib/config');
    expect(EXCHANGES.ORDERS_EVENTS).toBe('orders.events');
  });

  it('MAX_RETRIES equals the number of RETRY_DELAYS entries', () => {
    const { MAX_RETRIES, RETRY_DELAYS } = require('../../src/lib/config');
    expect(MAX_RETRIES).toBe(RETRY_DELAYS.length);
  });
});
