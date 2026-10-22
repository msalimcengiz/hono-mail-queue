import { timingSafeEqual } from 'node:crypto';
import { zValidator } from '@hono/zod-validator';
import { type Context, Hono } from 'hono';
import type { Logger } from 'pino';
import { z } from 'zod';
import type { EmailRepo, NewEmail } from './db.js';
import { enqueued, registry } from './metrics.js';
import { type MailClass, missingVars, render, type TemplateId, templateIds, templates } from './templates.js';

export const MAX_BULK = 1000;

export interface ApiDeps {
  repo: Pick<EmailRepo, 'insertOne' | 'insertMany' | 'markEnqueued' | 'get' | 'batchStats'>;
  publish: (c: MailClass, ids: string[]) => Promise<void>;
  ready: () => Promise<void>;
  logger: Logger;
  replayDead?: (limit: number) => Promise<{ replayed: number; kept: number }>;
  adminToken?: string;
}

function safeEqual(a: string, b: string): boolean {
  const x = Buffer.from(a);
  const y = Buffer.from(b);
  return b.length > 0 && x.length === y.length && timingSafeEqual(x, y);
}

const vars = z.record(z.string(), z.coerce.string()).default({});

const single = z.object({
  template: z.enum(templateIds),
  to: z.email(),
  vars,
  idempotency_key: z.string().min(8).max(128).optional(),
});

const bulk = z.object({
  template: z.enum(templateIds),
  shared_vars: vars,
  recipients: z
    .array(z.object({ to: z.email(), vars }))
    .min(1)
    .max(MAX_BULK),
});

const uuid = z.object({ id: z.uuid() });

function invalid(result: { success: boolean; error?: z.core.$ZodError }, c: Context) {
  if (!result.success && result.error) {
    return c.json({ error: 'validation failed', issues: z.flattenError(result.error).fieldErrors }, 422);
  }
}

function build(template: TemplateId, to: string, v: Record<string, string>): NewEmail {
  const t = templates[template];
  return {
    class: t.class,
    template,
    to,
    subject: render(t.subject, v, { escape: false }),
    html: render(t.html, v, { escape: true }),
  };
}

export function createApp(deps: ApiDeps) {
  const app = new Hono();

  /**
   * The row is committed before publishing. If publishing fails the request still
   * succeeds: the worker's outbox sweeper finds rows without enqueued_at and
   * republishes them, so an accepted email is never lost.
   */
  async function enqueue(c: MailClass, ids: string[]) {
    try {
      await deps.publish(c, ids);
      await deps.repo.markEnqueued(ids);
    } catch (err) {
      deps.logger.warn({ err, count: ids.length }, 'publish failed; outbox will retry');
    }
    enqueued.inc({ class: c }, ids.length);
  }

  app.post('/v1/emails', zValidator('json', single, invalid), async (c) => {
    const body = c.req.valid('json');
    const missing = missingVars(body.template, body.vars);
    if (missing.length > 0) return c.json({ error: 'missing template vars', missing }, 422);

    const idempotencyKey = c.req.header('Idempotency-Key') ?? body.idempotency_key;
    const email = build(body.template, body.to, body.vars);
    const res = await deps.repo.insertOne({ ...email, idempotencyKey });
    if (res.duplicate) return c.json({ id: res.id, status: res.status, duplicate: true }, 200);

    await enqueue(email.class, [res.id]);
    return c.json({ id: res.id, status: 'queued' }, 202);
  });

  app.post('/v1/emails/bulk', zValidator('json', bulk, invalid), async (c) => {
    const body = c.req.valid('json');
    const batchId = crypto.randomUUID();
    const rows: NewEmail[] = [];
    for (const [i, r] of body.recipients.entries()) {
      const v = { ...body.shared_vars, ...r.vars };
      const missing = missingVars(body.template, v);
      if (missing.length > 0) return c.json({ error: 'missing template vars', index: i, missing }, 422);
      rows.push({ ...build(body.template, r.to, v), batchId });
    }
    const ids = await deps.repo.insertMany(rows);
    await enqueue(templates[body.template].class, ids);
    return c.json({ batch_id: batchId, count: ids.length }, 202);
  });

  app.get('/v1/emails/:id', zValidator('param', uuid, invalid), async (c) => {
    const row = await deps.repo.get(c.req.valid('param').id);
    return row ? c.json(row) : c.json({ error: 'not found' }, 404);
  });

  app.get('/v1/batches/:id', zValidator('param', uuid, invalid), async (c) => {
    const stats = await deps.repo.batchStats(c.req.valid('param').id);
    if (Object.keys(stats).length === 0) return c.json({ error: 'not found' }, 404);
    return c.json({ batch_id: c.req.valid('param').id, ...stats });
  });

  // Operations: send dead-lettered emails again once the cause is fixed.
  app.post('/admin/dlq/replay', async (c) => {
    if (!deps.replayDead || !safeEqual(c.req.header('X-Admin-Token') ?? '', deps.adminToken ?? '')) {
      return c.json({ error: 'unauthorized' }, 401);
    }
    const limit = Math.min(Number(c.req.query('limit') ?? 100) || 100, 10_000);
    return c.json(await deps.replayDead(limit));
  });

  app.get('/healthz', (c) => c.text('ok'));
  app.get('/readyz', async (c) => {
    try {
      await deps.ready();
      return c.text('ok');
    } catch (err) {
      return c.json({ error: String(err) }, 503);
    }
  });
  app.get('/metrics', async (c) => c.text(await registry.metrics(), 200, { 'Content-Type': registry.contentType }));

  app.onError((err, c) => {
    deps.logger.error({ err }, 'unhandled');
    return c.json({ error: 'internal error' }, 500);
  });

  return app;
}
