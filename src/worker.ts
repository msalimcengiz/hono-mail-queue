import { serve } from '@hono/node-server';
import { Hono } from 'hono';
import nodemailer from 'nodemailer';
import { Broker, CLASSES, decode, queueFor } from './broker.js';
import { loadConfig } from './config.js';
import { handle } from './consumer.js';
import { createPool, EmailRepo, migrate } from './db.js';
import { createLogger } from './logger.js';
import { outboxRepublished, queueDepth, registry } from './metrics.js';
import { TokenBucket } from './ratelimit.js';
import type { MailClass } from './templates.js';

const cfg = loadConfig();
const logger = createLogger(cfg.LOG_LEVEL, 'worker');

const pool = createPool(cfg.DATABASE_URL);
await migrate(pool);
const repo = new EmailRepo(pool);
// Publishing (retries, dead letters, outbox) and consuming use separate connections so
// publisher flow control can never stall deliveries.
const publisher = await Broker.connect(cfg.AMQP_URL);
const consumer = await Broker.connect(cfg.AMQP_URL);
for (const b of [publisher, consumer]) {
  b.conn.on('close', () => {
    logger.error('broker connection closed');
    process.exit(1);
  });
}

const transport = nodemailer.createTransport({
  host: cfg.SMTP_HOST,
  port: cfg.SMTP_PORT,
  secure: false,
  pool: true,
  maxConnections: cfg.SMTP_POOL_SIZE,
});

const limits: Record<MailClass, { prefetch: number; bucket: TokenBucket }> = {
  transactional: { prefetch: cfg.PREFETCH_TRANSACTIONAL, bucket: new TokenBucket(cfg.RATE_TRANSACTIONAL) },
  bulk: { prefetch: cfg.PREFETCH_BULK, bucket: new TokenBucket(cfg.RATE_BULK) },
};

const closers: Array<() => Promise<void>> = [];
const inFlight = new Set<Promise<unknown>>();

// One channel per class: prefetch is per channel, so a backlog of bulk mail
// cannot occupy the slots transactional mail needs.
for (const c of CLASSES) {
  const ch = await consumer.conn.createChannel();
  await ch.prefetch(limits[c].prefetch);
  const { consumerTag } = await ch.consume(queueFor(c), (msg) => {
    if (!msg) return;
    const parsed = decode(msg.content);
    const work = (async () => {
      if (!parsed) {
        await publisher.publishDead(c, 'unknown', 'undecodable message', msg.content);
        ch.ack(msg);
        return;
      }
      try {
        await handle(
          {
            repo,
            send: async (mail) => {
              await transport.sendMail({ from: cfg.MAIL_FROM, ...mail });
            },
            publishRetry: (cls, suffix, id) => publisher.publishRetry(cls, suffix, id),
            publishDead: (cls, id, reason) => publisher.publishDead(cls, id, reason),
            throttle: () => limits[c].bucket.take(),
            logger,
          },
          c,
          parsed.id,
          msg.fields.redelivered,
        );
        ch.ack(msg);
      } catch (err) {
        // Infrastructure failure (database down, broker hiccup). Wait before requeueing:
        // an immediate nack would spin through RabbitMQ's delivery limit in milliseconds.
        const deliveries = Number(msg.properties.headers?.['x-delivery-count'] ?? 0);
        const delayMs = Math.min(30_000, 500 * 2 ** deliveries) * (0.5 + Math.random() / 2);
        logger.error({ err, id: parsed.id, deliveries, delayMs: Math.round(delayMs) }, 'handler failed; requeueing');
        await new Promise((r) => setTimeout(r, delayMs));
        ch.nack(msg, false, true);
      }
    })();
    inFlight.add(work);
    void work.finally(() => inFlight.delete(work));
  });
  closers.push(async () => {
    await ch.cancel(consumerTag);
  });
}

const sweeper = setInterval(async () => {
  try {
    const n = await repo.withStaleOutbox(cfg.OUTBOX_STALE_MS, 500, async (rows) => {
      for (const c of CLASSES) {
        const ids = rows.filter((r) => r.class === c).map((r) => r.id);
        if (ids.length > 0) await publisher.publishMany(c, ids);
      }
    });
    if (n > 0) {
      outboxRepublished.inc(n);
      logger.info({ count: n }, 'outbox republished');
    }
    for (const c of CLASSES) queueDepth.set({ queue: queueFor(c) }, await publisher.depth(queueFor(c)));
  } catch (err) {
    logger.warn({ err }, 'sweeper failed');
  }
}, 5_000);

const metricsApp = new Hono()
  .get('/healthz', (c) => c.text('ok'))
  .get('/metrics', async (c) => c.text(await registry.metrics(), 200, { 'Content-Type': registry.contentType }));
serve({ fetch: metricsApp.fetch, port: cfg.PORT });
logger.info({ prefetch: limits, port: cfg.PORT }, 'consuming');

for (const sig of ['SIGINT', 'SIGTERM'] as const) {
  process.once(sig, async () => {
    logger.info({ sig, inFlight: inFlight.size }, 'draining');
    clearInterval(sweeper);
    for (const close of closers) await close();
    await Promise.allSettled(inFlight);
    for (const b of [publisher, consumer]) {
      b.conn.removeAllListeners('close');
      await b.close();
    }
    transport.close();
    await pool.end();
    process.exit(0);
  });
}
