import { TelemetryComponent, TelemetryEvent, TelemetryStage } from '../lib/telemetry';

export type OrderStatus = 'queued' | 'processing' | 'retrying' | 'processed' | 'completed' | 'dlq';

export interface OrderView {
  orderId: string;
  total?: number;
  outcome?: string;
  source?: TelemetryComponent;
  status: OrderStatus;
  attempt: number;
  steps: TelemetryEvent[];
  delivered: { inventory: boolean; notification: boolean };
  updatedAt: string;
}

export interface Totals {
  created: number;
  processed: number;
  retrying: number;
  dlq: number;
  duplicates: number;
}

// Position of each stage in the flow: breaks ties between events with the same timestamp
const STAGE_RANK: Record<TelemetryStage, number> = {
  'created': 0,
  'processing': 1,
  'failed': 2,
  'retry-scheduled': 3,
  'dlq': 4,
  'processed': 5,
  'event-published': 6,
  'inventory-done': 7,
  'notification-done': 7,
  'duplicate-skipped': 8,
};

function compareSteps(a: TelemetryEvent, b: TelemetryEvent): number {
  return a.at.localeCompare(b.at)
    || (a.attempt ?? 0) - (b.attempt ?? 0)
    || STAGE_RANK[a.stage] - STAGE_RANK[b.stage];
}

// Telemetry from different components can arrive out of order, so the view is
// always rebuilt from the sorted steps instead of patched event by event.
function derive(orderId: string, steps: TelemetryEvent[]): OrderView {
  const view: OrderView = {
    orderId,
    status: 'queued',
    attempt: 0,
    steps,
    delivered: { inventory: false, notification: false },
    updatedAt: steps[steps.length - 1].at,
  };

  for (const step of steps) {
    if (step.attempt) view.attempt = Math.max(view.attempt, step.attempt);

    switch (step.stage) {
      case 'created':
        view.source = step.component;
        view.total = step.detail?.total as number | undefined;
        view.outcome = step.detail?.outcome as string | undefined;
        break;
      case 'processing': view.status = 'processing'; break;
      case 'retry-scheduled': view.status = 'retrying'; break;
      case 'dlq': view.status = 'dlq'; break;
      case 'processed':
      case 'event-published': view.status = 'processed'; break;
      case 'inventory-done': view.delivered.inventory = true; break;
      case 'notification-done': view.delivered.notification = true; break;
    }
  }

  if (view.status !== 'dlq' && view.delivered.inventory && view.delivered.notification) {
    view.status = 'completed';
  }
  return view;
}

export class OrderStore {
  private readonly orders = new Map<string, OrderView>();
  private counters = { created: 0, processed: 0, dlq: 0, duplicates: 0 };

  constructor(private readonly maxOrders = 200) {}

  apply(event: TelemetryEvent): OrderView {
    this.count(event.stage);

    const previous = this.orders.get(event.orderId);
    const steps = [...(previous?.steps ?? []), event].sort(compareSteps);
    const view = derive(event.orderId, steps);

    this.orders.set(event.orderId, view);
    if (this.orders.size > this.maxOrders) {
      const oldest = this.orders.keys().next().value as string;
      this.orders.delete(oldest);
    }
    return view;
  }

  // Newest first, by when the order was first seen
  list(): OrderView[] {
    return [...this.orders.values()].reverse();
  }

  totals(): Totals {
    const retrying = [...this.orders.values()].filter(o => o.status === 'retrying').length;
    return { ...this.counters, retrying };
  }

  // Forgets every order and resets the session totals. Orders still in flight
  // reappear on their next telemetry event, with the steps from then on.
  clear(): void {
    this.orders.clear();
    this.counters = { created: 0, processed: 0, dlq: 0, duplicates: 0 };
  }

  private count(stage: TelemetryStage): void {
    if (stage === 'created') this.counters.created++;
    else if (stage === 'processed') this.counters.processed++;
    else if (stage === 'dlq') this.counters.dlq++;
    else if (stage === 'duplicate-skipped') this.counters.duplicates++;
  }
}
