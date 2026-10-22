import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import pg from 'pg';
import type { MailClass } from './templates.js';

export type Status = 'queued' | 'sending' | 'retrying' | 'sent' | 'failed';

export interface EmailRow {
  id: string;
  class: MailClass;
  to_address: string;
  subject: string;
  html: string;
  status: Status;
  attempts: number;
  created_at: Date;
}

export interface NewEmail {
  class: MailClass;
  template: string;
  to: string;
  subject: string;
  html: string;
  idempotencyKey?: string;
  batchId?: string;
}

export function createPool(url: string, max = 20): pg.Pool {
  return new pg.Pool({ connectionString: url, max });
}

export async function migrate(pool: pg.Pool, dir = join(import.meta.dirname, '..', 'migrations')): Promise<void> {
  const files = (await readdir(dir)).filter((f) => f.endsWith('.sql')).sort();
  for (const f of files) {
    await pool.query(await readFile(join(dir, f), 'utf8'));
  }
}

export class EmailRepo {
  constructor(private readonly pool: pg.Pool) {}

  /**
   * Inserts one email. With an idempotency key, a repeated request returns the
   * original row instead of creating a second email.
   */
  async insertOne(e: NewEmail): Promise<{ id: string; status: Status; duplicate: boolean }> {
    const inserted = await this.pool.query<{ id: string; status: Status }>(
      `INSERT INTO emails (idempotency_key, class, template, to_address, subject, html)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (idempotency_key) DO NOTHING
       RETURNING id, status`,
      [e.idempotencyKey ?? null, e.class, e.template, e.to, e.subject, e.html],
    );
    const row = inserted.rows[0];
    if (row) return { ...row, duplicate: false };

    const existing = await this.pool.query<{ id: string; status: Status }>(
      'SELECT id, status FROM emails WHERE idempotency_key = $1',
      [e.idempotencyKey],
    );
    const dup = existing.rows[0];
    if (!dup) throw new Error('idempotency conflict without existing row');
    return { ...dup, duplicate: true };
  }

  /** Inserts many emails in one statement. */
  async insertMany(rows: NewEmail[]): Promise<string[]> {
    const res = await this.pool.query<{ id: string }>(
      `INSERT INTO emails (batch_id, class, template, to_address, subject, html)
       SELECT * FROM unnest($1::uuid[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[])
       RETURNING id`,
      [
        rows.map((r) => r.batchId ?? null),
        rows.map((r) => r.class),
        rows.map((r) => r.template),
        rows.map((r) => r.to),
        rows.map((r) => r.subject),
        rows.map((r) => r.html),
      ],
    );
    return res.rows.map((r) => r.id);
  }

  async markEnqueued(ids: string[]): Promise<void> {
    if (ids.length === 0) return;
    await this.pool.query('UPDATE emails SET enqueued_at = now() WHERE id = ANY($1::uuid[]) AND enqueued_at IS NULL', [
      ids,
    ]);
  }

  /**
   * Moves an email to "sending" and bumps its attempt counter. Returns null when the
   * email was already handled, which makes duplicate deliveries harmless. A message the
   * broker redelivers after a worker crash may reclaim a row stuck in "sending".
   */
  async claim(id: string, redelivered: boolean): Promise<EmailRow | null> {
    const states = redelivered ? ['queued', 'retrying', 'sending'] : ['queued', 'retrying'];
    const res = await this.pool.query<EmailRow>(
      `UPDATE emails SET status = 'sending', attempts = attempts + 1, updated_at = now()
       WHERE id = $1 AND status = ANY($2::text[])
       RETURNING id, class, to_address, subject, html, status, attempts, created_at`,
      [id, states],
    );
    return res.rows[0] ?? null;
  }

  async markSent(id: string): Promise<void> {
    await this.pool.query(
      `UPDATE emails SET status = 'sent', sent_at = now(), last_error = NULL, updated_at = now() WHERE id = $1`,
      [id],
    );
  }

  async markRetrying(id: string, error: string): Promise<void> {
    await this.pool.query(`UPDATE emails SET status = 'retrying', last_error = $2, updated_at = now() WHERE id = $1`, [
      id,
      error,
    ]);
  }

  async markFailed(id: string, error: string): Promise<void> {
    await this.pool.query(`UPDATE emails SET status = 'failed', last_error = $2, updated_at = now() WHERE id = $1`, [
      id,
      error,
    ]);
  }

  /**
   * Makes a failed email eligible to send again and returns its class (null if it is not
   * failed). enqueued_at is cleared, so if the replay publish fails the outbox sweeper
   * publishes it instead.
   */
  async resetFailed(id: string): Promise<MailClass | null> {
    const res = await this.pool.query<{ class: MailClass }>(
      `UPDATE emails SET status = 'retrying', attempts = 0, last_error = NULL, enqueued_at = NULL, updated_at = now()
       WHERE id = $1 AND status = 'failed' RETURNING class`,
      [id],
    );
    return res.rows[0]?.class ?? null;
  }

  async get(id: string) {
    const res = await this.pool.query(
      `SELECT id, class, template, to_address AS to, status, attempts, last_error, created_at, sent_at
       FROM emails WHERE id = $1`,
      [id],
    );
    return res.rows[0] ?? null;
  }

  async batchStats(batchId: string): Promise<Record<string, number>> {
    const res = await this.pool.query<{ status: string; n: string }>(
      'SELECT status, count(*) AS n FROM emails WHERE batch_id = $1 GROUP BY status',
      [batchId],
    );
    return Object.fromEntries(res.rows.map((r) => [r.status, Number(r.n)]));
  }

  /**
   * Transactional outbox: rows whose publish was never confirmed (API crashed, broker
   * down). `fn` republishes them; the rows stay locked so other workers skip them.
   */
  async withStaleOutbox(
    olderThanMs: number,
    limit: number,
    fn: (rows: Array<{ id: string; class: MailClass }>) => Promise<void>,
  ): Promise<number> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const res = await client.query<{ id: string; class: MailClass }>(
        `SELECT id, class FROM emails
         WHERE enqueued_at IS NULL AND created_at < now() - make_interval(secs => $1 / 1000.0)
         ORDER BY created_at LIMIT $2 FOR UPDATE SKIP LOCKED`,
        [olderThanMs, limit],
      );
      if (res.rows.length > 0) {
        await fn(res.rows);
        await client.query('UPDATE emails SET enqueued_at = now() WHERE id = ANY($1::uuid[])', [
          res.rows.map((r) => r.id),
        ]);
      }
      await client.query('COMMIT');
      return res.rows.length;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }
}
