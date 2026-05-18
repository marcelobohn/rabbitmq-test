import { v4 as uuidv4 } from 'uuid';
import { getConnection, closeConnection } from '../lib/connection';
import { QUEUES, StatusRequest, StatusResponse } from '../lib/config';

export function queryOrderStatus(orderId: string): Promise<StatusResponse> {
  const correlationId = uuidv4();

  // eslint-disable-next-line prefer-const
  let resolvePromise!: (value: StatusResponse) => void;
  // eslint-disable-next-line prefer-const
  let rejectPromise!: (reason: unknown) => void;

  const result = new Promise<StatusResponse>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });

  const timer = setTimeout(() => {
    rejectPromise(new Error(`RPC timeout: no reply received for order ${orderId} within 5s`));
  }, 5000);

  (async () => {
    try {
      const connection = await getConnection();
      const channel = await connection.createChannel();
      const { queue: replyQueue } = await channel.assertQueue('', { exclusive: true, autoDelete: true });

      channel.consume(
        replyQueue,
        (msg) => {
          if (!msg) return;
          if (msg.properties.correlationId !== correlationId) return;

          clearTimeout(timer);
          const response: StatusResponse = JSON.parse(msg.content.toString());
          channel.close().catch(() => {});
          resolvePromise(response);
        },
        { noAck: true }
      );

      const request: StatusRequest = { orderId };
      channel.sendToQueue(QUEUES.ORDERS_STATUS_RPC, Buffer.from(JSON.stringify(request)), {
        correlationId,
        replyTo: replyQueue,
        persistent: false,
      });

      result.catch(() => {
        channel.close().catch(() => {});
      });
    } catch (err) {
      clearTimeout(timer);
      rejectPromise(err);
    }
  })();

  return result;
}

async function main(): Promise<void> {
  const orderId = process.argv[2] ?? 'ord-001';
  console.log(`[rpc-client] Querying status for order ${orderId}...`);

  const response = await queryOrderStatus(orderId);
  console.log(`[rpc-client] Status: ${response.status} (updated: ${response.updatedAt})`);

  await closeConnection();
}

if (require.main === module) {
  main().catch(console.error);
}
