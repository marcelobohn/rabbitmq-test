import { summarizeQueues } from '../../src/dashboard/queue-stats';

describe('summarizeQueues', () => {
  it('keeps orders.* queues, sorted by name, with depth and rates', () => {
    const api = [
      { name: 'orders.processing', messages: 3, consumers: 1,
        message_stats: { publish_details: { rate: 2.1 }, ack_details: { rate: 1.8 } } },
      { name: 'amq.gen-abc', messages: 0, consumers: 1 },
      { name: 'orders.dlq', messages: 2, consumers: 0 },
      { name: 'other', messages: 9, consumers: 0 },
    ];

    expect(summarizeQueues(api)).toEqual([
      { name: 'orders.dlq', messages: 2, consumers: 0, publishRate: 0, ackRate: 0 },
      { name: 'orders.processing', messages: 3, consumers: 1, publishRate: 2.1, ackRate: 1.8 },
    ]);
  });

  it('treats missing counters as 0 and non-arrays as empty', () => {
    expect(summarizeQueues([{ name: 'orders.retry.5s' }])).toEqual([
      { name: 'orders.retry.5s', messages: 0, consumers: 0, publishRate: 0, ackRate: 0 },
    ]);
    expect(summarizeQueues({ error: 'x' })).toEqual([]);
  });
});
