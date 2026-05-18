import { getConnection, closeConnection } from '../lib/connection';
import { QUEUES, StatusRequest, StatusResponse } from '../lib/config';

const STATUSES: StatusResponse['status'][] = ['pending', 'processing', 'shipped', 'delivered'];

export function getOrderStatus(orderId: string): StatusResponse {
  const status = STATUSES[orderId.length % STATUSES.length];
  return { orderId, status, updatedAt: new Date().toISOString() };
}

async function main(): Promise<void> {
  const connection = await getConnection();
  const channel = await connection.createChannel();

  await channel.assertQueue(QUEUES.ORDERS_STATUS_RPC, { durable: true });
  channel.prefetch(1);

  console.log('[rpc-server] Waiting for status requests...');

  await channel.consume(QUEUES.ORDERS_STATUS_RPC, (msg) => {
    if (!msg) return;

    const request: StatusRequest = JSON.parse(msg.content.toString());
    const response = getOrderStatus(request.orderId);

    channel.sendToQueue(
      msg.properties.replyTo,
      Buffer.from(JSON.stringify(response)),
      { correlationId: msg.properties.correlationId }
    );
    channel.ack(msg);
    console.log(`[rpc-server] Replied to ${request.orderId}: ${response.status}`);
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
