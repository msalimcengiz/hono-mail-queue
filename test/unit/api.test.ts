import { pino } from 'pino';
import { describe, expect, it, vi } from 'vitest';
import { type ApiDeps, createApp } from '../../src/api.js';

function setup(over: Partial<ApiDeps['repo']> = {}, publish = vi.fn().mockResolvedValue(undefined)) {
  const repo = {
    insertOne: vi.fn().mockResolvedValue({ id: 'id-1', status: 'queued', duplicate: false }),
    insertMany: vi.fn(async (rows: unknown[]) => rows.map((_, i) => `id-${i}`)),
    markEnqueued: vi.fn().mockResolvedValue(undefined),
    get: vi.fn().mockResolvedValue(null),
    batchStats: vi.fn().mockResolvedValue({}),
  };
  Object.assign(repo, over);
  const app = createApp({ repo, publish, ready: async () => {}, logger: pino({ level: 'silent' }) });
  const post = (path: string, body: unknown, headers: Record<string, string> = {}) =>
    app.request(path, {
      method: 'POST',
      body: JSON.stringify(body),
      headers: { 'Content-Type': 'application/json', ...headers },
    });
  return { app, repo, publish, post };
}

const reset = { template: 'password_reset', to: 'a@example.com', vars: { name: 'A', reset_url: 'https://x' } };

describe('POST /v1/emails', () => {
  it('stores, publishes to the transactional class and marks enqueued', async () => {
    const { post, repo, publish } = setup();
    const res = await post('/v1/emails', reset);
    expect(res.status).toBe(202);
    expect(publish).toHaveBeenCalledWith('transactional', ['id-1']);
    expect(repo.markEnqueued).toHaveBeenCalledWith(['id-1']);
    expect(repo.insertOne.mock.calls[0]?.[0].html).toContain('href="https://x"');
  });

  it('validates the body and template vars', async () => {
    const { post } = setup();
    expect((await post('/v1/emails', { ...reset, to: 'nope' })).status).toBe(422);
    expect((await post('/v1/emails', { ...reset, template: 'missing' })).status).toBe(422);
    const res = await post('/v1/emails', { ...reset, vars: { name: 'A' } });
    expect(res.status).toBe(422);
    expect(await res.json()).toMatchObject({ missing: ['reset_url'] });
  });

  it('returns the original email for a repeated Idempotency-Key', async () => {
    const { post, repo, publish } = setup({
      insertOne: vi.fn().mockResolvedValue({ id: 'orig', status: 'sent', duplicate: true }),
    });
    const res = await post('/v1/emails', reset, { 'Idempotency-Key': 'order-12345' });
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ id: 'orig', duplicate: true });
    expect(repo.insertOne.mock.calls[0]?.[0].idempotencyKey).toBe('order-12345');
    expect(publish).not.toHaveBeenCalled();
  });

  it('still accepts the email when the broker is down (outbox recovers it)', async () => {
    const { post, repo } = setup({}, vi.fn().mockRejectedValue(new Error('broker down')));
    expect((await post('/v1/emails', reset)).status).toBe(202);
    expect(repo.markEnqueued).not.toHaveBeenCalled();
  });
});

describe('POST /v1/emails/bulk', () => {
  const campaign = {
    template: 'campaign_report',
    shared_vars: { campaign: 'Autumn', unsubscribe_url: 'https://u' },
    recipients: [
      { to: 'a@example.com', vars: { impressions: 10 } },
      { to: 'b@example.com', vars: { impressions: 20 } },
    ],
  };

  it('merges shared and per-recipient vars and publishes to the bulk class', async () => {
    const { post, repo, publish } = setup();
    const res = await post('/v1/emails/bulk', campaign);
    expect(res.status).toBe(202);
    expect(await res.json()).toMatchObject({ count: 2 });
    expect(repo.insertMany.mock.calls[0]?.[0]).toHaveLength(2);
    expect(publish).toHaveBeenCalledWith('bulk', ['id-0', 'id-1']);
  });

  it('points at the recipient with missing vars', async () => {
    const { post } = setup();
    const bad = { ...campaign, recipients: [...campaign.recipients, { to: 'c@example.com', vars: {} }] };
    const res = await post('/v1/emails/bulk', bad);
    expect(res.status).toBe(422);
    expect(await res.json()).toMatchObject({ index: 2, missing: ['impressions'] });
  });
});
