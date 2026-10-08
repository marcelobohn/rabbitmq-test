import { OrderStore } from '../../src/dashboard/order-store';
import { TelemetryEvent, TelemetryStage, TelemetryComponent } from '../../src/lib/telemetry';

let clock = 0;
function ev(
  stage: TelemetryStage,
  extra: Partial<TelemetryEvent> = {},
  component: TelemetryComponent = 'worker'
): TelemetryEvent {
  clock += 1000;
  return { orderId: 'ord-1', component, stage, at: new Date(clock).toISOString(), ...extra };
}

const created = () => ev('created', { detail: { total: 10.5, customerId: 'c1', outcome: 'fail-once' } }, 'dashboard');

describe('OrderStore', () => {
  beforeEach(() => { clock = 0; });

  it('follows a successful order to completed', () => {
    const store = new OrderStore();
    store.apply(created());
    expect(store.list()[0].status).toBe('queued');

    store.apply(ev('processing', { attempt: 1 }));
    expect(store.list()[0].status).toBe('processing');

    store.apply(ev('processed', { attempt: 1 }));
    store.apply(ev('event-published', { attempt: 1 }));
    expect(store.list()[0].status).toBe('processed');

    store.apply(ev('inventory-done', {}, 'inventory-consumer'));
    const view = store.apply(ev('notification-done', {}, 'notification-consumer'));

    expect(view).toEqual(expect.objectContaining({
      orderId: 'ord-1', status: 'completed', attempt: 1, total: 10.5, outcome: 'fail-once', source: 'dashboard',
      delivered: { inventory: true, notification: true },
    }));
    expect(view.steps).toHaveLength(6);
  });

  it('shows retrying while waiting in a retry queue, then the second attempt', () => {
    const store = new OrderStore();
    store.apply(created());
    store.apply(ev('processing', { attempt: 1 }));
    store.apply(ev('failed', { attempt: 1, detail: { error: 'x' } }));
    const retrying = store.apply(ev('retry-scheduled', { attempt: 1, detail: { queue: 'orders.retry.5s', delayMs: 5000 } }));
    expect(retrying.status).toBe('retrying');
    expect(store.totals().retrying).toBe(1);

    const second = store.apply(ev('processing', { attempt: 2 }));
    expect(second.status).toBe('processing');
    expect(second.attempt).toBe(2);
    expect(store.totals().retrying).toBe(0);
  });

  it('ends in dlq', () => {
    const store = new OrderStore();
    store.apply(created());
    store.apply(ev('processing', { attempt: 3 }));
    store.apply(ev('failed', { attempt: 3 }));
    const view = store.apply(ev('dlq', { attempt: 3 }));

    expect(view.status).toBe('dlq');
    expect(store.totals().dlq).toBe(1);
  });

  it('derives the same state when events arrive out of order', () => {
    const events = [
      created(),
      ev('processing', { attempt: 1 }),
      ev('processed', { attempt: 1 }),
      ev('event-published', { attempt: 1 }),
      ev('inventory-done', {}, 'inventory-consumer'),
      ev('notification-done', {}, 'notification-consumer'),
    ];
    const inOrder = new OrderStore();
    events.forEach(e => inOrder.apply(e));

    const shuffled = new OrderStore();
    [4, 0, 5, 2, 1, 3].forEach(i => shuffled.apply(events[i]));

    expect(shuffled.list()[0]).toEqual(inOrder.list()[0]);
  });

  it('orders steps with the same timestamp by their position in the flow', () => {
    const store = new OrderStore();
    const at = new Date(5000).toISOString();
    store.apply({ orderId: 'ord-1', component: 'worker', stage: 'processed', attempt: 1, at });
    store.apply({ orderId: 'ord-1', component: 'worker', stage: 'processing', attempt: 1, at });

    expect(store.list()[0].steps.map(s => s.stage)).toEqual(['processing', 'processed']);
    expect(store.list()[0].status).toBe('processed');
  });

  it('completes an OrderCreated event from the standalone publisher once both subscribers handled it', () => {
    const store = new OrderStore();
    store.apply(ev('created', {}, 'publisher'));
    store.apply(ev('inventory-done', {}, 'inventory-consumer'));
    const view = store.apply(ev('notification-done', {}, 'notification-consumer'));

    expect(view.status).toBe('completed');
  });

  it('keeps only the most recent orders and lists newest first', () => {
    const store = new OrderStore(2);
    ['a', 'b', 'c'].forEach(id => store.apply(ev('created', { orderId: id }, 'dashboard')));

    expect(store.list().map(o => o.orderId)).toEqual(['c', 'b']);
  });

  it('counts session totals, including duplicates', () => {
    const store = new OrderStore();
    store.apply(created());
    store.apply(ev('processing', { attempt: 1 }));
    store.apply(ev('processed', { attempt: 1 }));
    store.apply(ev('duplicate-skipped'));

    expect(store.totals()).toEqual({ created: 1, processed: 1, retrying: 0, dlq: 0, duplicates: 1 });
  });

  it('clear() empties the list and resets the session totals', () => {
    const store = new OrderStore();
    store.apply(created());
    store.apply(ev('processing', { attempt: 1 }));
    store.apply(ev('processed', { attempt: 1 }));

    store.clear();

    expect(store.list()).toEqual([]);
    expect(store.totals()).toEqual({ created: 0, processed: 0, retrying: 0, dlq: 0, duplicates: 0 });

    // An order still in flight reappears with the steps from the clear onwards
    const view = store.apply(ev('event-published', { attempt: 1 }));
    expect(view.steps.map(s => s.stage)).toEqual(['event-published']);
  });
});
