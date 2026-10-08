import http from 'node:http';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { ConfirmChannel } from 'amqplib';
import { v4 as uuidv4 } from 'uuid';
import { getConnection, closeConnection } from '../lib/connection';
import { EXCHANGES, QUEUES, ORDERS_PROCESSING_OPTIONS } from '../lib/config';
import { Emit, noopEmit, openTelemetry, assertTelemetryExchange, TelemetryEvent } from '../lib/telemetry';
import { queryOrderStatus } from '../rpc/client';
import { OrderStore } from './order-store';
import { parseCreateOrders, pickOutcome, buildOrder, ValidationError } from './order-factory';
import { fetchQueueStats, QueueStat } from './queue-stats';

const PORT = Number(process.env.PORT ?? 3000);
const HOST = process.env.HOST ?? '127.0.0.1';
const MGMT_URL = process.env.RABBITMQ_MGMT_URL ?? 'http://localhost:15672';
const MGMT_USER = process.env.RABBITMQ_MGMT_USER ?? 'guest';
const MGMT_PASS = process.env.RABBITMQ_MGMT_PASS ?? 'guest';

const RECONNECT_DELAY_MS = 3000;
const STATS_INTERVAL_MS = 2000;
const HEARTBEAT_INTERVAL_MS = 15000;
const MAX_BODY_BYTES = 10 * 1024;

const PUBLIC_DIR = path.join(__dirname, 'public');
const STATIC_FILES: Record<string, { file: string; type: string }> = {
  '/': { file: 'index.html', type: 'text/html; charset=utf-8' },
  '/app.js': { file: 'app.js', type: 'text/javascript; charset=utf-8' },
  '/style.css': { file: 'style.css', type: 'text/css; charset=utf-8' },
};

const store = new OrderStore();
const clients = new Set<http.ServerResponse>();
let brokerUp = false;
let queueStats: QueueStat[] | null = null;
let publishChannel: ConfirmChannel | null = null;
let emit: Emit = noopEmit;

// --- Server-Sent Events -----------------------------------------------------

function send(res: http.ServerResponse, type: string, data: unknown): void {
  res.write(`event: ${type}\ndata: ${JSON.stringify(data)}\n\n`);
}

function broadcast(type: string, data: unknown): void {
  for (const client of clients) send(client, type, data);
}

function setBroker(up: boolean): void {
  if (brokerUp === up) return;
  brokerUp = up;
  broadcast('broker', { up });
}

// --- RabbitMQ ----------------------------------------------------------------

let connecting = false;

// Consumes all telemetry through an exclusive, server-named queue: it exists only
// while the dashboard is running, which is fine — with no one watching, there is
// nothing to show. Contrast with the durable queues of the pub/sub subscribers.
async function connectBroker(): Promise<void> {
  if (connecting) return;
  connecting = true;
  try {
    const connection = await getConnection();
    connection.once('close', () => {
      publishChannel = null;
      emit = noopEmit;
      setBroker(false);
      setTimeout(connectBroker, RECONNECT_DELAY_MS);
    });

    const telemetryChannel = await connection.createChannel();
    await assertTelemetryExchange(telemetryChannel);
    const { queue } = await telemetryChannel.assertQueue('', { exclusive: true, autoDelete: true });
    await telemetryChannel.bindQueue(queue, EXCHANGES.ORDERS_TELEMETRY, '#');
    await telemetryChannel.consume(queue, (msg) => {
      if (!msg) return;
      try {
        const event: TelemetryEvent = JSON.parse(msg.content.toString());
        const order = store.apply(event);
        broadcast('order', { order, totals: store.totals() });
      } catch (err) {
        console.warn(`[dashboard] Ignoring malformed telemetry: ${(err as Error).message}`);
      }
    }, { noAck: true });

    const channel = await connection.createConfirmChannel();
    await channel.assertQueue(QUEUES.ORDERS_PROCESSING, ORDERS_PROCESSING_OPTIONS);
    publishChannel = channel;
    emit = await openTelemetry(connection, 'dashboard');

    setBroker(true);
    console.log('[dashboard] Connected to RabbitMQ');
  } catch (err) {
    console.error(`[dashboard] RabbitMQ unavailable, retrying in ${RECONNECT_DELAY_MS}ms: ${(err as Error).message}`);
    setBroker(false);
    setTimeout(connectBroker, RECONNECT_DELAY_MS);
  } finally {
    connecting = false;
  }
}

async function refreshStats(): Promise<void> {
  try {
    queueStats = await fetchQueueStats(MGMT_URL, MGMT_USER, MGMT_PASS);
    broadcast('stats', { queues: queueStats });
  } catch (err) {
    queueStats = null;
    broadcast('stats', { queues: null, error: (err as Error).message });
  }
}

// --- HTTP --------------------------------------------------------------------

function json(res: http.ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(body));
}

function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        reject(new ValidationError(`body larger than ${MAX_BODY_BYTES} bytes`));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

async function createOrders(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
  let request;
  try {
    const raw = await readBody(req);
    let body: unknown;
    try {
      body = JSON.parse(raw);
    } catch {
      throw new ValidationError('body must be valid JSON');
    }
    request = parseCreateOrders(body);
  } catch (err) {
    if (err instanceof ValidationError) return json(res, 400, { error: err.message });
    throw err;
  }

  const channel = publishChannel;
  if (!channel) return json(res, 503, { error: 'RabbitMQ unavailable' });

  const created = [];
  for (let i = 0; i < request.count; i++) {
    const order = buildOrder();
    const outcome = request.outcome === 'random' ? pickOutcome(request.failureRate) : request.outcome;
    const messageId = uuidv4();

    channel.sendToQueue(QUEUES.ORDERS_PROCESSING, Buffer.from(JSON.stringify(order)), {
      persistent: true,
      messageId,
      contentType: 'application/json',
      headers: { 'x-retry-count': 0, 'x-simulate': outcome },
    });
    emit({
      orderId: order.orderId,
      messageId,
      stage: 'created',
      detail: { total: order.total, customerId: order.customerId, outcome },
    });
    created.push({ orderId: order.orderId, outcome });
  }

  try {
    await channel.waitForConfirms();
  } catch (err) {
    return json(res, 502, { error: `broker did not confirm the orders: ${(err as Error).message}` });
  }
  json(res, 201, { orders: created });
}

async function orderStatus(orderId: string, res: http.ServerResponse): Promise<void> {
  if (!brokerUp) return json(res, 503, { error: 'RabbitMQ unavailable' });
  const started = Date.now();
  try {
    const response = await queryOrderStatus(orderId);
    json(res, 200, { ...response, latencyMs: Date.now() - started });
  } catch (err) {
    const message = (err as Error).message;
    json(res, message.startsWith('RPC timeout') ? 504 : 502, { error: message });
  }
}

function openEventStream(req: http.IncomingMessage, res: http.ServerResponse): void {
  res.writeHead(200, {
    'content-type': 'text/event-stream; charset=utf-8',
    'cache-control': 'no-cache',
    connection: 'keep-alive',
  });
  send(res, 'snapshot', { orders: store.list(), totals: store.totals() });
  send(res, 'broker', { up: brokerUp });
  send(res, 'stats', { queues: queueStats });
  clients.add(res);
  req.on('close', () => clients.delete(res));
}

async function serveStatic(pathname: string, res: http.ServerResponse): Promise<boolean> {
  const entry = STATIC_FILES[pathname];
  if (!entry) return false;
  const content = await readFile(path.join(PUBLIC_DIR, entry.file));
  res.writeHead(200, { 'content-type': entry.type, 'cache-control': 'no-cache' });
  res.end(content);
  return true;
}

const STATUS_ROUTE = /^\/api\/orders\/([^/]+)\/status$/;

const server = http.createServer(async (req, res) => {
  try {
    const { pathname } = new URL(req.url ?? '/', 'http://localhost');

    if (req.method === 'GET' && pathname === '/events') return openEventStream(req, res);
    if (req.method === 'POST' && pathname === '/api/orders') return await createOrders(req, res);

    const statusMatch = req.method === 'POST' ? STATUS_ROUTE.exec(pathname) : null;
    if (statusMatch) return await orderStatus(decodeURIComponent(statusMatch[1]), res);

    if (req.method === 'GET' && (await serveStatic(pathname, res))) return;

    json(res, 404, { error: 'not found' });
  } catch (err) {
    console.error('[dashboard] Request failed:', err);
    if (!res.headersSent) json(res, 500, { error: 'internal error' });
    else res.end();
  }
});

function main(): void {
  server.listen(PORT, HOST, () => console.log(`[dashboard] Listening on http://${HOST}:${PORT}`));
  connectBroker();
  refreshStats();
  setInterval(refreshStats, STATS_INTERVAL_MS);
  setInterval(() => { for (const client of clients) client.write(': ping\n\n'); }, HEARTBEAT_INTERVAL_MS);

  process.on('SIGTERM', async () => {
    for (const client of clients) client.end();
    server.close();
    await closeConnection();
    process.exit(0);
  });
}

if (require.main === module) {
  main();
}
