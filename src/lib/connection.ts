import amqplib, { ChannelModel } from 'amqplib';
import { RABBITMQ_URL } from './config';

let _connectionPromise: Promise<ChannelModel> | null = null;

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

export function getConnection(): Promise<ChannelModel> {
  if (!_connectionPromise) {
    _connectionPromise = connectWithRetry(RABBITMQ_URL).then(conn => {
      conn.on('error', (err: Error) => {
        console.error('[connection] Error:', err.message);
        _connectionPromise = null;
      });
      conn.on('close', () => {
        console.warn('[connection] Connection closed');
        _connectionPromise = null;
      });
      return conn;
    });
  }
  return _connectionPromise;
}

export async function closeConnection(): Promise<void> {
  if (_connectionPromise) {
    const conn = await _connectionPromise;
    _connectionPromise = null;
    conn.removeAllListeners();
    await conn.close();
  }
}
