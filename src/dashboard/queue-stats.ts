export interface QueueStat {
  name: string;
  messages: number;
  consumers: number;
  publishRate: number;
  ackRate: number;
}

interface ApiQueue {
  name: string;
  messages?: number;
  consumers?: number;
  message_stats?: {
    publish_details?: { rate?: number };
    ack_details?: { rate?: number };
  };
}

// Keeps the project's queues (orders.*); server-named amq.gen-* queues are left out
export function summarizeQueues(json: unknown): QueueStat[] {
  if (!Array.isArray(json)) return [];
  return (json as ApiQueue[])
    .filter(q => typeof q.name === 'string' && q.name.startsWith('orders.'))
    .map(q => ({
      name: q.name,
      messages: q.messages ?? 0,
      consumers: q.consumers ?? 0,
      publishRate: q.message_stats?.publish_details?.rate ?? 0,
      ackRate: q.message_stats?.ack_details?.rate ?? 0,
    }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

export async function fetchQueueStats(baseUrl: string, user: string, pass: string): Promise<QueueStat[]> {
  const res = await fetch(`${baseUrl}/api/queues/%2F`, {
    headers: { authorization: `Basic ${Buffer.from(`${user}:${pass}`).toString('base64')}` },
    signal: AbortSignal.timeout(3000),
  });
  if (!res.ok) throw new Error(`management API returned ${res.status}`);
  return summarizeQueues(await res.json());
}
