/**
 * End-to-end tests against the running compose stack (API, 2 workers, Postgres,
 * RabbitMQ, Mailpit). Run with `make test-integration`. Tests share the SMTP server's
 * chaos settings, so they run sequentially.
 */
import pg from 'pg';
import { afterAll, afterEach, describe, expect, it } from 'vitest';

const API = process.env.API_URL ?? 'http://localhost:3000';
const MAILPIT = process.env.MAILPIT_URL ?? 'http://localhost:8025';
const db = new pg.Pool({ connectionString: process.env.DATABASE_URL ?? 'postgres://mail:mail@localhost:5433/mail' });

afterAll(() => db.end());
afterEach(() => chaos(0, 451));

const addr = (tag: string) => `${tag}-${crypto.randomUUID().slice(0, 8)}@example.com`;

async function send(body: object, headers: Record<string, string> = {}) {
  const res = await fetch(`${API}/v1/emails`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify(body),
  });
  return { status: res.status, body: (await res.json()) as { id: string; duplicate?: boolean } };
}

async function email(id: string) {
  return (await (await fetch(`${API}/v1/emails/${id}`)).json()) as { status: string; attempts: number };
}

async function until<T>(fn: () => Promise<T>, ok: (v: T) => boolean, timeoutMs = 15_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last = await fn();
  while (!ok(last)) {
    if (Date.now() > deadline) throw new Error(`timed out, last value: ${JSON.stringify(last)}`);
    await new Promise((r) => setTimeout(r, 200));
    last = await fn();
  }
  return last;
}

async function inbox(to: string): Promise<number> {
  const res = await fetch(`${MAILPIT}/api/v1/search?query=${encodeURIComponent(`to:"${to}"`)}`);
  return ((await res.json()) as { messages_count: number }).messages_count;
}

/** Makes Mailpit reject RCPT TO with `code` for `probability` percent of messages. */
async function chaos(probability: number, code: number) {
  await fetch(`${MAILPIT}/api/v1/chaos`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ Recipient: { ErrorCode: code, Probability: probability } }),
  });
}

const welcome = (to: string) => ({ template: 'welcome', to, vars: { name: 'Test' } });

describe('delivery', () => {
  it('delivers a transactional email', async () => {
    const to = addr('tx');
    const { status, body } = await send(welcome(to));
    expect(status).toBe(202);
    await until(
      () => email(body.id),
      (e) => e.status === 'sent',
    );
    expect(await inbox(to)).toBe(1);
  });

  it('deduplicates on Idempotency-Key', async () => {
    const to = addr('idem');
    const key = `order-${crypto.randomUUID()}`;
    const first = await send(welcome(to), { 'Idempotency-Key': key });
    const second = await send(welcome(to), { 'Idempotency-Key': key });
    expect(second.status).toBe(200);
    expect(second.body).toMatchObject({ id: first.body.id, duplicate: true });
    await until(
      () => email(first.body.id),
      (e) => e.status === 'sent',
    );
    await new Promise((r) => setTimeout(r, 500));
    expect(await inbox(to)).toBe(1);
  });

  it('delivers a bulk batch', async () => {
    const recipients = Array.from({ length: 200 }, (_, i) => ({ to: addr(`bulk${i}`), vars: { impressions: i } }));
    const res = await fetch(`${API}/v1/emails/bulk`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        template: 'campaign_report',
        shared_vars: { campaign: 'IT', unsubscribe_url: 'https://u' },
        recipients,
      }),
    });
    expect(res.status).toBe(202);
    const { batch_id } = (await res.json()) as { batch_id: string };
    const stats = await until(
      async () => (await (await fetch(`${API}/v1/batches/${batch_id}`)).json()) as Record<string, number>,
      (s) => s.sent === 200,
    );
    expect(stats.failed).toBeUndefined();
  });
});

describe('failures', () => {
  it('retries a temporary SMTP failure after the 5s delay and then delivers', async () => {
    await chaos(100, 451);
    const to = addr('retry');
    const { body } = await send(welcome(to));
    await until(
      () => email(body.id),
      (e) => e.status === 'retrying',
    );
    await chaos(0, 451);
    const done = await until(
      () => email(body.id),
      (e) => e.status === 'sent',
      20_000,
    );
    expect(done.attempts).toBe(2);
    expect(await inbox(to)).toBe(1);
  });

  it('dead-letters a permanent SMTP failure without retrying', async () => {
    await chaos(100, 550);
    const { body } = await send(welcome(addr('perm')));
    const done = await until(
      () => email(body.id),
      (e) => e.status === 'failed',
    );
    expect(done.attempts).toBe(1);
  });
});

describe('dead letters', () => {
  it('replays a dead-lettered email once the cause is fixed', async () => {
    await chaos(100, 550);
    const to = addr('dlq');
    const { body } = await send(welcome(to));
    await until(
      () => email(body.id),
      (e) => e.status === 'failed',
    );
    await chaos(0, 451);

    const denied = await fetch(`${API}/admin/dlq/replay`, { method: 'POST', headers: { 'X-Admin-Token': 'wrong' } });
    expect(denied.status).toBe(401);
    const res = await fetch(`${API}/admin/dlq/replay?limit=1000`, {
      method: 'POST',
      headers: { 'X-Admin-Token': 'dev-admin-token' },
    });
    expect(((await res.json()) as { replayed: number }).replayed).toBeGreaterThanOrEqual(1);

    await until(
      () => email(body.id),
      (e) => e.status === 'sent',
    );
    expect(await inbox(to)).toBe(1);
  });
});

describe('outbox', () => {
  it('republishes an email that was stored but never published', async () => {
    const to = addr('outbox');
    // Simulates the API crashing between the INSERT and the broker confirm.
    const { rows } = await db.query<{ id: string }>(
      `INSERT INTO emails (class, template, to_address, subject, html, created_at)
       VALUES ('transactional', 'welcome', $1, 'Outbox', '<p>hi</p>', now() - interval '1 minute')
       RETURNING id`,
      [to],
    );
    const id = rows[0]?.id as string;
    await until(
      () => email(id),
      (e) => e.status === 'sent',
      20_000,
    );
    expect(await inbox(to)).toBe(1);
  });
});
