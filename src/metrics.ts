import client from 'prom-client';

export const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });

export const enqueued = new client.Counter({
  name: 'mail_enqueued_total',
  help: 'Emails accepted by the API.',
  labelNames: ['class'],
  registers: [registry],
});

export const sent = new client.Counter({
  name: 'mail_sent_total',
  help: 'Emails accepted by the SMTP server.',
  labelNames: ['class'],
  registers: [registry],
});

export const retried = new client.Counter({
  name: 'mail_retries_total',
  help: 'Failed attempts scheduled for a delayed retry.',
  labelNames: ['class', 'delay'],
  registers: [registry],
});

export const failed = new client.Counter({
  name: 'mail_failed_total',
  help: 'Emails moved to the dead-letter queue.',
  labelNames: ['class', 'reason'],
  registers: [registry],
});

export const duplicates = new client.Counter({
  name: 'mail_duplicate_deliveries_total',
  help: 'Broker deliveries skipped because the email was already handled.',
  registers: [registry],
});

export const outboxRepublished = new client.Counter({
  name: 'mail_outbox_republished_total',
  help: 'Emails republished by the outbox sweeper.',
  registers: [registry],
});

export const sendDuration = new client.Histogram({
  name: 'mail_smtp_send_duration_seconds',
  help: 'SMTP send latency.',
  labelNames: ['class'],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
  registers: [registry],
});

export const endToEnd = new client.Histogram({
  name: 'mail_end_to_end_seconds',
  help: 'Time from API acceptance to successful send, first attempt only.',
  labelNames: ['class'],
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120],
  registers: [registry],
});

export const queueDepth = new client.Gauge({
  name: 'mail_queue_depth',
  help: 'Messages ready in each work queue.',
  labelNames: ['queue'],
  registers: [registry],
});
