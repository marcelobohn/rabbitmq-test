export const RABBITMQ_URL = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672';

export const QUEUES = {
  ORDERS_PROCESSING: 'orders.processing',
  ORDERS_DLQ: 'orders.dlq',
  ORDERS_RETRY_5S: 'orders.retry.5s',
  ORDERS_RETRY_30S: 'orders.retry.30s',
  ORDERS_STATUS_RPC: 'orders.status.rpc',
  // One durable queue per pub/sub subscriber: events published while the
  // subscriber is down wait in its queue instead of being dropped.
  INVENTORY_EVENTS: 'orders.events.inventory',
  NOTIFICATION_EVENTS: 'orders.events.notification',
} as const;

// Every declaration of orders.processing must use exactly these options: the broker
// rejects (and closes the channel on) a redeclaration with different arguments.
export const ORDERS_PROCESSING_OPTIONS = {
  durable: true,
  arguments: {
    'x-dead-letter-exchange': '',
    'x-dead-letter-routing-key': QUEUES.ORDERS_DLQ,
  },
};

export const EXCHANGES = {
  ORDERS_EVENTS: 'orders.events',
  ORDERS_TELEMETRY: 'orders.telemetry',
} as const;

// Outcome the simulated worker processor applies, read from the x-simulate header
export type SimulatedOutcome = 'success' | 'fail-once' | 'fail-always';
export const SIMULATED_OUTCOMES: readonly SimulatedOutcome[] = ['success', 'fail-once', 'fail-always'];

export const RETRY_DELAYS: readonly number[] = [5000, 30000];
export const MAX_RETRIES = RETRY_DELAYS.length;

export interface OrderMessage {
  orderId: string;
  customerId: string;
  items: Array<{ productId: string; quantity: number; price: number }>;
  total: number;
  createdAt: string;
}

export interface StatusRequest {
  orderId: string;
}

export interface StatusResponse {
  orderId: string;
  status: 'pending' | 'processing' | 'shipped' | 'delivered';
  updatedAt: string;
}
