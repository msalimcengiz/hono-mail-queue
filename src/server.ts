import { serve } from '@hono/node-server';
import { createApp } from './api.js';
import { ReconnectingBroker } from './broker.js';
import { loadConfig } from './config.js';
import { createPool, EmailRepo, migrate } from './db.js';
import { createLogger } from './logger.js';

const cfg = loadConfig();
const logger = createLogger(cfg.LOG_LEVEL, 'api');

const pool = createPool(cfg.DATABASE_URL);
await migrate(pool);
// Reconnects in the background. Without a broker the API keeps accepting emails: they
// are stored first, and the workers' outbox sweeper publishes them later.
const broker = new ReconnectingBroker(cfg.AMQP_URL, logger);
await broker.start();

const app = createApp({
  repo: new EmailRepo(pool),
  publish: (c, ids) => broker.with((b) => b.publishMany(c, ids)),
  // Ready as long as the database is: emails are accepted even while RabbitMQ is down.
  ready: async () => {
    await pool.query('SELECT 1');
  },
  logger,
  adminToken: cfg.ADMIN_TOKEN,
  replayDead: async (limit) => {
    const repo = new EmailRepo(pool);
    const { replayed, kept } = await broker.with((b) => b.replayDead(limit, (id) => repo.resetFailed(id)), 60_000);
    await repo.markEnqueued(replayed);
    return { replayed: replayed.length, kept };
  },
});

const server = serve({ fetch: app.fetch, port: cfg.PORT }, (info) => logger.info({ port: info.port }, 'listening'));

for (const sig of ['SIGINT', 'SIGTERM'] as const) {
  process.once(sig, () => {
    logger.info({ sig }, 'shutting down');
    server.close(async () => {
      await broker.close();
      await pool.end();
      process.exit(0);
    });
  });
}
