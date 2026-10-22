import { pino } from 'pino';
import { describe, expect, it, vi } from 'vitest';
import { type ConsumerDeps, handle } from '../../src/consumer.js';
import type { EmailRow } from '../../src/db.js';

const row = (attempts: number): EmailRow => ({
  id: '00000000-0000-4000-8000-000000000001',
  class: 'transactional',
  to_address: 'a@example.com',
  subject: 's',
  html: 'h',
  status: 'sending',
  attempts,
  created_at: new Date(),
});

function deps(over: Partial<Omit<ConsumerDeps, 'repo'>> = {}, attempts = 1) {
  const d = {
    repo: {
      claim: vi.fn().mockResolvedValue(row(attempts)),
      markSent: vi.fn().mockResolvedValue(undefined),
      markRetrying: vi.fn().mockResolvedValue(undefined),
      markFailed: vi.fn().mockResolvedValue(undefined),
    },
    send: vi.fn().mockResolvedValue(undefined),
    publishRetry: vi.fn().mockResolvedValue(undefined),
    publishDead: vi.fn().mockResolvedValue(undefined),
    throttle: vi.fn().mockResolvedValue(undefined),
    logger: pino({ level: 'silent' }),
    ...over,
  };
  return d;
}

const id = row(1).id;
const smtp = (code: number) => Object.assign(new Error('smtp'), { responseCode: code });

describe('handle', () => {
  it('sends and marks sent', async () => {
    const d = deps();
    expect(await handle(d, 'transactional', id, false)).toBe('sent');
    expect(d.throttle).toHaveBeenCalledOnce();
    expect(d.repo.markSent).toHaveBeenCalledWith(id);
  });

  it('skips duplicates without sending', async () => {
    const d = deps();
    d.repo.claim.mockResolvedValue(null);
    expect(await handle(d, 'transactional', id, true)).toBe('skipped');
    expect(d.send).not.toHaveBeenCalled();
  });

  it('schedules a retry on a temporary failure, publishing before updating the row', async () => {
    const calls: string[] = [];
    const d = deps({
      send: vi.fn().mockRejectedValue(smtp(451)),
      publishRetry: vi.fn(async () => {
        calls.push('publish');
      }),
    });
    d.repo.markRetrying.mockImplementation(async () => {
      calls.push('mark');
    });
    expect(await handle(d, 'bulk', id, false)).toBe('retry');
    expect(d.publishRetry).toHaveBeenCalledWith('bulk', '5s', id);
    expect(calls).toEqual(['publish', 'mark']);
  });

  it('dead-letters permanent failures', async () => {
    const d = deps({ send: vi.fn().mockRejectedValue(smtp(550)) });
    expect(await handle(d, 'transactional', id, false)).toBe('dead');
    expect(d.publishDead).toHaveBeenCalled();
    expect(d.repo.markFailed).toHaveBeenCalledWith(id, expect.stringMatching(/^permanent/));
  });

  it('dead-letters after the last retry', async () => {
    const d = deps({ send: vi.fn().mockRejectedValue(smtp(421)) }, 4);
    expect(await handle(d, 'transactional', id, false)).toBe('dead');
    expect(d.publishRetry).not.toHaveBeenCalled();
  });
});
