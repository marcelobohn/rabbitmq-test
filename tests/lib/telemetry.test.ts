import { Channel } from 'amqplib';
import { emitter } from '../../src/lib/telemetry';
import { EXCHANGES } from '../../src/lib/config';

describe('emitter', () => {
  it('publishes to orders.telemetry with a <component>.<stage> routing key', () => {
    const channel = { publish: jest.fn().mockReturnValue(true) };
    const emit = emitter(channel as unknown as Channel, 'worker');

    emit({ orderId: 'ord-001', messageId: 'm-1', stage: 'processing', attempt: 1 });

    const [exchange, routingKey, body, options] = channel.publish.mock.calls[0];
    expect(exchange).toBe(EXCHANGES.ORDERS_TELEMETRY);
    expect(routingKey).toBe('worker.processing');
    expect(options).toEqual(expect.objectContaining({ persistent: false }));

    const event = JSON.parse(body.toString());
    expect(event).toEqual(expect.objectContaining({
      orderId: 'ord-001', messageId: 'm-1', component: 'worker', stage: 'processing', attempt: 1,
    }));
    expect(new Date(event.at).toISOString()).toBe(event.at);
  });

  it('never throws, even when publish throws', () => {
    const channel = { publish: jest.fn(() => { throw new Error('Channel closed'); }) };
    const emit = emitter(channel as unknown as Channel, 'worker');
    const warn = jest.spyOn(console, 'warn').mockImplementation(() => {});

    expect(() => emit({ orderId: 'ord-001', stage: 'processed' })).not.toThrow();
    expect(warn).toHaveBeenCalled();
    warn.mockRestore();
  });
});
