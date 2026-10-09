import { Channel, ConsumeMessage } from 'amqplib';
import { getConnection } from '../lib/connection';
import { ProcessedStore, InMemoryProcessedStore } from '../lib/idempotency';

// Consumes events published by the event-sourcing-php relay. The topic exchange
// routes only withdrawals to this queue (binding account.MoneyWithdrawn): the
// other event types never reach it.
export const BANK_EVENTS_EXCHANGE = 'bank.events';
export const ALERTS_QUEUE = 'bank.events.alerts';
export const WITHDRAWALS_ROUTING_KEY = 'account.MoneyWithdrawn';

export interface BankEvent {
  streamId: string;
  version: number;
  position?: number;
  type: string;
  payload: { accountId?: string; amount?: number };
  recordedAt: string;
}

export interface AlertResult {
  kind: 'alert' | 'info' | 'duplicate' | 'invalid';
  message: string;
}

const money = new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' });
const formatCents = (cents: number) => money.format(cents / 100).replace(/ /g, ' ');

function parse(content: Buffer): BankEvent {
  const event = JSON.parse(content.toString()) as BankEvent;
  if (typeof event.streamId !== 'string' || typeof event.version !== 'number'
      || typeof event.payload?.amount !== 'number') {
    throw new Error('missing streamId, version or payload.amount');
  }
  return event;
}

export async function handleBankEvent(
  channel: Channel,
  msg: ConsumeMessage,
  { store, threshold }: { store: ProcessedStore; threshold: number }
): Promise<AlertResult> {
  let event: BankEvent;
  try {
    event = parse(msg.content);
  } catch (err) {
    channel.nack(msg, false, false);
    return { kind: 'invalid', message: `Mensagem inválida descartada: ${(err as Error).message}` };
  }

  // The relay may republish after a crash; <stream>:<version> identifies the event
  const key = (msg.properties.messageId as string | undefined) ?? `${event.streamId}:${event.version}`;
  if (await store.has(key)) {
    channel.ack(msg);
    return { kind: 'duplicate', message: `Evento ${key} repetido — ignorado` };
  }

  const amount = event.payload.amount as number;
  const result: AlertResult = amount >= threshold
    ? { kind: 'alert', message: `🚨 Saque alto: ${formatCents(amount)} na conta ${event.streamId} (v${event.version})` }
    : { kind: 'info', message: `Saque de ${formatCents(amount)} na conta ${event.streamId} (v${event.version})` };

  await store.add(key);
  channel.ack(msg);
  return result;
}

async function main(): Promise<void> {
  const threshold = Number(process.env.BANK_ALERT_THRESHOLD ?? 100_000);
  const connection = await getConnection();
  const channel = await connection.createChannel();
  const store = new InMemoryProcessedStore();

  await channel.assertExchange(BANK_EVENTS_EXCHANGE, 'topic', { durable: true });
  await channel.assertQueue(ALERTS_QUEUE, { durable: true });
  await channel.bindQueue(ALERTS_QUEUE, BANK_EVENTS_EXCHANGE, WITHDRAWALS_ROUTING_KEY);
  channel.prefetch(1);

  console.log(`[bank-alerts] Waiting for withdrawals on ${ALERTS_QUEUE} (alert at ${formatCents(threshold)} or more)...`);

  await channel.consume(ALERTS_QUEUE, (msg) => {
    if (!msg) return;
    handleBankEvent(channel, msg, { store, threshold })
      .then(({ kind, message }) => (kind === 'invalid' ? console.error : console.log)(`[bank-alerts] ${message}`))
      .catch(console.error);
  });

  process.on('SIGINT', async () => {
    await channel.close();
    process.exit(0);
  });
}

if (require.main === module) {
  main().catch(console.error);
}
