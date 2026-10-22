import { describe, expect, it } from 'vitest';
import { TokenBucket } from '../../src/ratelimit.js';

describe('TokenBucket', () => {
  it('allows the burst immediately, then paces to the rate', async () => {
    const bucket = new TokenBucket(100, 10);
    const start = performance.now();
    await Promise.all(Array.from({ length: 30 }, () => bucket.take()));
    const elapsed = performance.now() - start;
    // 10 from the burst + 20 more at 100/s ≈ 200ms.
    expect(elapsed).toBeGreaterThanOrEqual(180);
    expect(elapsed).toBeLessThan(400);
  });

  it('serves waiters in order', async () => {
    const bucket = new TokenBucket(50, 1);
    const order: number[] = [];
    await Promise.all([1, 2, 3].map((i) => bucket.take().then(() => order.push(i))));
    expect(order).toEqual([1, 2, 3]);
  });
});
