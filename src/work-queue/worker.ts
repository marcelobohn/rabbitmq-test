import { ConfirmChannel, ConsumeMessage, Options } from 'amqplib';
import { getConnection, closeConnection } from '../lib/connection';
import { QUEUES, EXCHANGES, MAX_RETRIES, RETRY_DELAYS, OrderMessage, SimulatedOutcome } from '../lib/config';
import { ProcessedStore, InMemoryProcessedStore, dedupKey } from '../lib/idempotency';
import { Emit, noopEmit, openTelemetry } from '../lib/telemetry';

export type OrderProcessor = (order: OrderMessage, msg: ConsumeMessage) => Promise<void>;

const sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms));

// Simulated work: waits 0.5–2 s, then succeeds or fails according to the
// x-simulate header (success when absent). fail-once fails only on the first
// attempt, so the order recovers through the 5 s retry queue.
export function createSimulatedProcessor(wait: (ms: number) => Promise<void> = sleep): OrderProcessor {
  return async (order, msg) => {
    await wait(500 + Math.random() * 1500);

    const outcome = (msg.properties.headers?.['x-simulate'] as SimulatedOutcome | undefined) ?? 'success';
    const retryCount = (msg.properties.headers?.['x-retry-count'] as number) ?? 0;
    if (outcome === 'fail-always' || (outcome === 'fail-once' && retryCount === 0)) {
      throw new Error(`Simulated failure (${outcome})`);
    }

    console.log(`[worker] Processed order ${order.orderId} — total: $${order.total}`);
  };
}

const defaultProcessor = createSimulatedProcessor();
const defaultStore = new InMemoryProcessedStore();

// Moves the message to another queue without ever losing it: the copy is published
// and confirmed by the broker first, and only then the original is acked. If the
// broker does not confirm, the original goes back to the queue instead.
async function forward(
  channel: ConfirmChannel,
  msg: ConsumeMessage,
  queue: string,
  options: Options.Publish
): Promise<boolean> {
  try {
    channel.sendToQueue(queue, msg.content, {
      ...options,
      messageId: msg.properties.messageId,
      persistent: true,
    });
    await channel.waitForConfirms();
  } catch (err) {
    console.error(`[worker] Publish to ${queue} not confirmed, requeueing: ${(err as Error).message}`);
    channel.nack(msg, false, true);
    return false;
  }
  channel.ack(msg);
  return true;
}

// Announces the processed order on orders.events (fanout) so the pub/sub
// subscribers react to it. The messageId is derived from the order's, so a
// re-published event after a reprocessing is recognised as a duplicate.
async function publishOrderProcessed(channel: ConfirmChannel, msg: ConsumeMessage, key: string): Promise<boolean> {
  try {
    channel.publish(EXCHANGES.ORDERS_EVENTS, '', msg.content, {
      persistent: true,
      messageId: `${key}:processed`,
      type: 'OrderProcessed',
      contentType: 'application/json',
    });
    await channel.waitForConfirms();
    return true;
  } catch (err) {
    console.error(`[worker] OrderProcessed not confirmed, requeueing: ${(err as Error).message}`);
    channel.nack(msg, false, true);
    return false;
  }
}

export async function handleMessage(
  channel: ConfirmChannel,
  msg: ConsumeMessage,
  processor: OrderProcessor = defaultProcessor,
  store: ProcessedStore = defaultStore,
  emit: Emit = noopEmit
): Promise<void> {
  const messageId = msg.properties.messageId as string | undefined;

  let order: OrderMessage;
  try {
    order = JSON.parse(msg.content.toString());
  } catch (parseErr) {
    // Malformed message — straight to DLQ, no retry
    const error = `Parse error: ${(parseErr as Error).message}`;
    if (await forward(channel, msg, QUEUES.ORDERS_DLQ, { headers: { 'x-error': error } })) {
      console.error(`[worker] Malformed message sent to DLQ: ${(parseErr as Error).message}`);
      emit({ orderId: messageId ?? '(invalid JSON)', messageId, stage: 'dlq', detail: { error } });
    }
    return;
  }

  const key = dedupKey(msg, order.orderId);
  const base = { orderId: order.orderId, messageId };

  if (await store.has(key)) {
    channel.ack(msg);
    console.log(`[worker] Duplicate message ${key} for order ${order.orderId} — skipped`);
    emit({ ...base, stage: 'duplicate-skipped' });
    return;
  }

  const retryCount: number = (msg.properties.headers?.['x-retry-count'] as number) ?? 0;
  const attempt = retryCount + 1;
  emit({ ...base, stage: 'processing', attempt });

  try {
    await processor(order, msg);
  } catch (err) {
    const error = (err as Error).message;
    emit({ ...base, stage: 'failed', attempt, detail: { error } });

    if (retryCount < MAX_RETRIES) {
      const retryQueue = retryCount === 0 ? QUEUES.ORDERS_RETRY_5S : QUEUES.ORDERS_RETRY_30S;
      const headers = { ...msg.properties.headers, 'x-retry-count': attempt };
      if (await forward(channel, msg, retryQueue, { headers })) {
        console.log(`[worker] Retry ${attempt}/${MAX_RETRIES} for order ${order.orderId} via ${retryQueue}`);
        emit({ ...base, stage: 'retry-scheduled', attempt, detail: { queue: retryQueue, delayMs: RETRY_DELAYS[retryCount] } });
      }
    } else {
      const headers = { ...msg.properties.headers, 'x-error': error };
      if (await forward(channel, msg, QUEUES.ORDERS_DLQ, { headers })) {
        console.log(`[worker] Max retries reached for order ${order.orderId} — sent to DLQ`);
        emit({ ...base, stage: 'dlq', attempt, detail: { error } });
      }
    }
    return;
  }

  emit({ ...base, stage: 'processed', attempt });
  if (!(await publishOrderProcessed(channel, msg, key))) return;
  emit({ ...base, stage: 'event-published', attempt });

  // Recorded before the ack: with a persistent store, a crash in between causes a
  // redelivery that is skipped as duplicate instead of processed a second time.
  await store.add(key);
  channel.ack(msg);
}

async function main(): Promise<void> {
  const connection = await getConnection();
  const channel = await connection.createConfirmChannel();
  const emit = await openTelemetry(connection, 'worker');

  await channel.assertExchange(EXCHANGES.ORDERS_EVENTS, 'fanout', { durable: true });

  await channel.assertQueue(QUEUES.ORDERS_PROCESSING, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': QUEUES.ORDERS_DLQ,
    },
  });
  await channel.assertQueue(QUEUES.ORDERS_DLQ, { durable: true });
  await channel.assertQueue(QUEUES.ORDERS_RETRY_5S, {
    durable: true,
    arguments: {
      'x-message-ttl': RETRY_DELAYS[0],
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': QUEUES.ORDERS_PROCESSING,
    },
  });
  await channel.assertQueue(QUEUES.ORDERS_RETRY_30S, {
    durable: true,
    arguments: {
      'x-message-ttl': RETRY_DELAYS[1],
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': QUEUES.ORDERS_PROCESSING,
    },
  });

  channel.prefetch(1);
  console.log('[worker] Waiting for orders...');

  channel.consume(QUEUES.ORDERS_PROCESSING, (msg) => {
    if (msg) handleMessage(channel, msg, defaultProcessor, defaultStore, emit).catch(console.error);
  });

  process.on('SIGTERM', async () => {
    await channel.close();
    await closeConnection();
    process.exit(0);
  });
}

if (require.main === module) {
  main().catch(console.error);
}
