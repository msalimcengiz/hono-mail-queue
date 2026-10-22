import { describe, expect, it } from 'vitest';
import { decide, isPermanent, RETRY_DELAYS_MS, retrySuffix } from '../../src/retry.js';

const smtp = (code: number) => Object.assign(new Error('smtp'), { responseCode: code });

describe('retry policy', () => {
  it('treats 5xx as permanent and 4xx/network errors as temporary', () => {
    expect(isPermanent(smtp(550))).toBe(true);
    expect(isPermanent(smtp(451))).toBe(false);
    expect(isPermanent(new Error('ECONNRESET'))).toBe(false);
  });

  it('walks the delay ladder, then gives up', () => {
    const delays = RETRY_DELAYS_MS.map((_, i) => decide(i + 1, smtp(451)));
    expect(delays.map((d) => (d.kind === 'retry' ? d.queueSuffix : d.kind))).toEqual(['5s', '30s', '5m']);
    const last = decide(RETRY_DELAYS_MS.length + 1, smtp(451));
    expect(last.kind).toBe('dead');
  });

  it('dead-letters permanent failures immediately', () => {
    const d = decide(1, smtp(550));
    expect(d).toMatchObject({ kind: 'dead' });
    expect(d.kind === 'dead' && d.reason).toMatch(/^permanent: 550/);
  });

  it('formats suffixes', () => {
    expect(retrySuffix(5_000)).toBe('5s');
    expect(retrySuffix(300_000)).toBe('5m');
  });
});
