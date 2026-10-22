import { z } from 'zod';

const schema = z.object({
  PORT: z.coerce.number().int().default(3000),
  DATABASE_URL: z.string().default('postgres://mail:mail@localhost:5432/mail'),
  AMQP_URL: z.string().default('amqp://guest:guest@localhost:5672'),
  SMTP_HOST: z.string().default('localhost'),
  SMTP_PORT: z.coerce.number().int().default(1025),
  SMTP_POOL_SIZE: z.coerce.number().int().positive().default(5),
  MAIL_FROM: z.string().default('Mail Queue <no-reply@example.com>'),
  /** Messages each worker processes at once, per class. */
  PREFETCH_TRANSACTIONAL: z.coerce.number().int().positive().default(20),
  PREFETCH_BULK: z.coerce.number().int().positive().default(20),
  /** Provider-imposed send rate per worker, per class (emails/second). */
  RATE_TRANSACTIONAL: z.coerce.number().positive().default(200),
  RATE_BULK: z.coerce.number().positive().default(100),
  /** Rows created but never confirmed by the broker are republished after this long. */
  OUTBOX_STALE_MS: z.coerce.number().int().positive().default(15_000),
  LOG_LEVEL: z.string().default('info'),
  ADMIN_TOKEN: z.string().default(''),
});

export type Config = z.infer<typeof schema>;

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  return schema.parse(env);
}
