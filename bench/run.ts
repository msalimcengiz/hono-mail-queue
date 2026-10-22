/**
 * Priority isolation benchmark. Queues a large bulk campaign, then sends
 * transactional emails while that backlog drains, with a share of SMTP calls
 * failing temporarily. Reports how long transactional mail waited and whether
 * every email was eventually delivered.
 *
 *   BULK=20000 TX=300 CHAOS=10 npm run bench
 */
import { writeFile } from 'node:fs/promises';
import pg from 'pg';

const API = process.env.API_URL ?? 'http://localhost:3000';
const MAILPIT = process.env.MAILPIT_URL ?? 'http://localhost:8025';
const BULK = Number(process.env.BULK ?? 20_000);
const TX = Number(process.env.TX ?? 300);
const TX_RATE = Number(process.env.TX_RATE ?? 30);
const CHAOS = Number(process.env.CHAOS ?? 10);
const db = new pg.Pool({ connectionString: process.env.DATABASE_URL ?? 'postgres://mail:mail@localhost:5433/mail' });

const post = (path: string, body: unknown) =>
  fetch(`${API}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function setChaos(probability: number) {
  await fetch(`${MAILPIT}/api/v1/chaos`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ Recipient: { ErrorCode: 451, Probability: probability } }),
  });
}

function pct(sorted: number[], p: number): number {
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))] ?? Number.NaN;
}

await db.query('TRUNCATE emails');
await fetch(`${MAILPIT}/api/v1/messages`, { method: 'DELETE' });
await setChaos(CHAOS);

const t0 = Date.now();
const run = crypto.randomUUID().slice(0, 8);

// 1. Bulk campaign, 1000 recipients per request.
for (let i = 0; i < BULK; i += 1000) {
  const n = Math.min(1000, BULK - i);
  const res = await post('/v1/emails/bulk', {
    template: 'campaign_report',
    shared_vars: { campaign: `bench-${run}`, unsubscribe_url: 'https://example.com/u' },
    recipients: Array.from({ length: n }, (_, j) => ({ to: `b${i + j}-${run}@example.com`, vars: { impressions: j } })),
  });
  if (res.status !== 202) throw new Error(`bulk enqueue failed: ${res.status}`);
}
const bulkQueuedMs = Date.now() - t0;
console.log(`queued ${BULK} bulk emails in ${bulkQueuedMs} ms`);

// 2. Transactional traffic while the bulk backlog is draining.
for (let i = 0; i < TX; i++) {
  const res = await post('/v1/emails', {
    template: 'password_reset',
    to: `tx${i}-${run}@example.com`,
    vars: { name: 'Bench', reset_url: 'https://example.com/r' },
  });
  if (res.status !== 202) throw new Error(`tx enqueue failed: ${res.status}`);
  await sleep(1000 / TX_RATE);
}
console.log(`queued ${TX} transactional emails at ~${TX_RATE}/s`);

// 3. Wait until nothing is queued, sending or waiting for a retry.
for (;;) {
  const { rows } = await db.query<{ open: string }>(
    `SELECT count(*) AS open FROM emails WHERE status IN ('queued', 'sending', 'retrying')`,
  );
  if (Number(rows[0]?.open) === 0) break;
  await sleep(500);
}
const totalMs = Date.now() - t0;
await setChaos(0);

const { rows: stats } = await db.query<{
  class: string;
  status: string;
  n: string;
  retried: string;
  first_try_ms: number[] | null;
}>(
  `SELECT class, status, count(*) AS n, count(*) FILTER (WHERE attempts > 1) AS retried,
          array_agg(extract(epoch FROM sent_at - created_at) * 1000 ORDER BY sent_at - created_at)
            FILTER (WHERE status = 'sent' AND attempts = 1) AS first_try_ms
   FROM emails GROUP BY class, status ORDER BY class, status`,
);
// Throughput of the first pass only; the tail of the run is dominated by the 5-minute retry delay.
const { rows: span } = await db.query<{ ms: number; n: string }>(
  `SELECT extract(epoch FROM max(sent_at) - min(created_at)) * 1000 AS ms, count(*) AS n
   FROM emails WHERE class = 'bulk' AND status = 'sent' AND attempts = 1`,
);
const delivered = (await (await fetch(`${MAILPIT}/api/v1/messages?limit=1`)).json()) as { total: number };

const lines: string[] = [];
lines.push(`Bulk: ${BULK}, transactional: ${TX} at ~${TX_RATE}/s, SMTP temporary failure rate: ${CHAOS}%.`);
lines.push('');
lines.push('| class | status | emails | needed a retry | first-try latency p50 | p95 | p99 |');
lines.push('|---|---|---:|---:|---:|---:|---:|');
for (const s of stats) {
  const lat = (s.first_try_ms ?? []).map(Number);
  const f = (v: number) => (Number.isNaN(v) ? '–' : `${Math.round(v).toLocaleString('en-US')} ms`);
  lines.push(
    `| ${s.class} | ${s.status} | ${Number(s.n).toLocaleString('en-US')} | ${Number(s.retried).toLocaleString('en-US')} | ${f(pct(lat, 50))} | ${f(pct(lat, 95))} | ${f(pct(lat, 99))} |`,
  );
}
const bulkSecs = (span[0]?.ms ?? 0) / 1000;
const firstPass = Number(span[0]?.n ?? 0);
lines.push('');
lines.push(
  `Bulk first-pass throughput: ${Math.round(firstPass / bulkSecs).toLocaleString('en-US')} emails/s ` +
    `(${firstPass.toLocaleString('en-US')} sent on the first try in ${bulkSecs.toFixed(1)} s). ` +
    `Messages in SMTP sink: ${delivered.total.toLocaleString('en-US')} of ${(BULK + TX).toLocaleString('en-US')}. ` +
    `Total run including the retry ladder (5 s, 30 s, 5 min): ${(totalMs / 1000).toFixed(1)} s.`,
);
const out = lines.join('\n');
console.log(`\n${out}`);
await writeFile(new URL('./results/summary.md', import.meta.url), `${out}\n`);
await db.end();
