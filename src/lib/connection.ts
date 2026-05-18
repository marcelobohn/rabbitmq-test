import amqplib, { ChannelModel } from 'amqplib';
import { RABBITMQ_URL } from './config';

let _connection: ChannelModel | null = null;

export async function connectWithRetry(url: string, maxAttempts = 10): Promise<ChannelModel> {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await amqplib.connect(url);
    } catch (err) {
      if (attempt >= maxAttempts) throw err;
      const delay = Math.min(500 * 2 ** attempt, 30000);
      console.log(`[connection] Attempt ${attempt}/${maxAttempts} failed. Retrying in ${delay}ms...`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
  throw new Error('unreachable');
}

export async function getConnection(): Promise<ChannelModel> {
  if (!_connection) {
    _connection = await connectWithRetry(RABBITMQ_URL);
    _connection.on('error', (err: Error) => {
      console.error('[connection] Error:', err.message);
      _connection = null;
    });
    _connection.on('close', () => {
      _connection = null;
    });
  }
  return _connection;
}

export async function closeConnection(): Promise<void> {
  if (_connection) {
    await _connection.close();
    _connection = null;
  }
}
