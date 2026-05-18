import { getOrderStatus } from '../../src/rpc/server';

describe('getOrderStatus', () => {
  it('returns a StatusResponse with the correct orderId', () => {
    const result = getOrderStatus('ord-001');
    expect(result.orderId).toBe('ord-001');
  });

  it('returns a valid status value', () => {
    const result = getOrderStatus('ord-001');
    expect(['pending', 'processing', 'shipped', 'delivered']).toContain(result.status);
  });

  it('returns a valid ISO date string for updatedAt', () => {
    const result = getOrderStatus('ord-001');
    expect(new Date(result.updatedAt).getTime()).not.toBeNaN();
  });

  it('returns consistent status for the same orderId', () => {
    const a = getOrderStatus('ord-abc');
    const b = getOrderStatus('ord-abc');
    expect(a.status).toBe(b.status);
  });
});
