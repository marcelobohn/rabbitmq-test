import { OrderMessage, SimulatedOutcome, SIMULATED_OUTCOMES } from '../lib/config';

export type RequestedOutcome = 'random' | SimulatedOutcome;

export interface CreateOrdersRequest {
  count: number;
  failureRate: number;
  outcome: RequestedOutcome;
}

export class ValidationError extends Error {}

const MAX_ORDERS_PER_REQUEST = 50;

export function parseCreateOrders(body: unknown): CreateOrdersRequest {
  if (typeof body !== 'object' || body === null || Array.isArray(body)) {
    throw new ValidationError('body must be a JSON object');
  }
  const { count, failureRate = 0, outcome = 'random' } = body as Record<string, unknown>;

  if (typeof count !== 'number' || !Number.isInteger(count) || count < 1 || count > MAX_ORDERS_PER_REQUEST) {
    throw new ValidationError(`count must be an integer from 1 to ${MAX_ORDERS_PER_REQUEST}`);
  }
  if (typeof failureRate !== 'number' || !Number.isFinite(failureRate) || failureRate < 0 || failureRate > 100) {
    throw new ValidationError('failureRate must be a number from 0 to 100');
  }
  if (outcome !== 'random' && !SIMULATED_OUTCOMES.includes(outcome as SimulatedOutcome)) {
    throw new ValidationError(`outcome must be one of: random, ${SIMULATED_OUTCOMES.join(', ')}`);
  }
  return { count, failureRate, outcome: outcome as RequestedOutcome };
}

// failureRate% of the orders fail; of those, half recover on the retry (fail-once)
// and half end up in the DLQ (fail-always)
export function pickOutcome(failureRate: number, rand: () => number = Math.random): SimulatedOutcome {
  if (rand() * 100 >= failureRate) return 'success';
  return rand() < 0.5 ? 'fail-once' : 'fail-always';
}

export function buildOrder(rand: () => number = Math.random): OrderMessage {
  const id = Array.from({ length: 6 }, () => '0123456789abcdefghijklmnopqrstuvwxyz'[Math.floor(rand() * 36)]).join('');
  const items = Array.from({ length: 1 + Math.floor(rand() * 3) }, () => ({
    productId: `prod-${Math.floor(rand() * 50)}`,
    quantity: 1 + Math.floor(rand() * 5),
    price: Math.round((5 + rand() * 95) * 100) / 100,
  }));
  const total = Math.round(items.reduce((sum, i) => sum + i.quantity * i.price, 0) * 100) / 100;

  return {
    orderId: `ord-${id}`,
    customerId: `cust-${Math.floor(rand() * 100)}`,
    items,
    total,
    createdAt: new Date().toISOString(),
  };
}
