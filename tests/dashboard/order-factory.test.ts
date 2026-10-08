import { parseCreateOrders, pickOutcome, buildOrder, ValidationError } from '../../src/dashboard/order-factory';

describe('parseCreateOrders', () => {
  it('accepts a valid request', () => {
    expect(parseCreateOrders({ count: 5, failureRate: 30, outcome: 'fail-once' }))
      .toEqual({ count: 5, failureRate: 30, outcome: 'fail-once' });
  });

  it('defaults failureRate to 0 and outcome to random', () => {
    expect(parseCreateOrders({ count: 1 })).toEqual({ count: 1, failureRate: 0, outcome: 'random' });
  });

  it.each([
    [{ count: 0 }, 'count'],
    [{ count: 51 }, 'count'],
    [{ count: 2.5 }, 'count'],
    [{ count: '5' }, 'count'],
    [{}, 'count'],
    [{ count: 1, failureRate: -1 }, 'failureRate'],
    [{ count: 1, failureRate: 101 }, 'failureRate'],
    [{ count: 1, failureRate: 'x' }, 'failureRate'],
    [{ count: 1, outcome: 'explode' }, 'outcome'],
  ])('rejects %j (%s)', (body, field) => {
    expect(() => parseCreateOrders(body)).toThrow(ValidationError);
    expect(() => parseCreateOrders(body)).toThrow(field);
  });

  it.each([null, 'text', 42, []])('rejects a body that is not an object: %j', (body) => {
    expect(() => parseCreateOrders(body)).toThrow(ValidationError);
  });
});

describe('pickOutcome', () => {
  const sequence = (...values: number[]) => () => values.shift() ?? 0;

  it('never fails at 0%', () => {
    expect(pickOutcome(0, sequence(0))).toBe('success');
  });

  it('always fails at 100%, split between fail-once and fail-always', () => {
    expect(pickOutcome(100, sequence(0.99, 0.2))).toBe('fail-once');
    expect(pickOutcome(100, sequence(0.99, 0.7))).toBe('fail-always');
  });

  it('fails when the draw is below the rate', () => {
    expect(pickOutcome(30, sequence(0.29, 0.1))).toBe('fail-once');
    expect(pickOutcome(30, sequence(0.30))).toBe('success');
  });
});

describe('buildOrder', () => {
  it('builds an order whose total matches its items', () => {
    const order = buildOrder();
    const sum = order.items.reduce((acc, i) => acc + i.quantity * i.price, 0);

    expect(order.orderId).toMatch(/^ord-[a-z0-9]{6}$/);
    expect(order.total).toBeCloseTo(sum, 2);
    expect(order.items.length).toBeGreaterThanOrEqual(1);
  });
});
