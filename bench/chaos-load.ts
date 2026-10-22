/**
 * Load for the chaos run: single transactional emails with an Idempotency-Key, plus
 * bulk batches. Every request is retried until the API accepts it (it may be down).
 * Prints the number of accepted emails.
 */
const API = process.env.API_URL ?? 'http://api:3000';
const SECONDS = Number(process.env.SECONDS ?? 60);
const run = crypto.randomUUID().slice(0, 8);

async function post(path: string, body: unknown, headers: Record<string, string> = {}): Promise<Response> {
  for (let attempt = 0; ; attempt++) {
    try {
      const res = await fetch(`${API}${path}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...headers },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(5000),
      });
      if (res.status < 500) return res;
    } catch {
      // API down: retry with the same body and key
    }
    await new Promise((r) => setTimeout(r, Math.min(100 * 2 ** attempt, 2000)));
  }
}

let accepted = 0;
const end = Date.now() + SECONDS * 1000;

async function transactional(worker: number) {
  for (let i = 0; Date.now() < end; i++) {
    const res = await post(
      '/v1/emails',
      {
        template: 'password_reset',
        to: `tx-${worker}-${i}-${run}@example.com`,
        vars: { name: 'Chaos', reset_url: 'https://x' },
      },
      { 'Idempotency-Key': `chaos-${run}-${worker}-${i}` },
    );
    if (res.status === 202 || res.status === 200) accepted++;
    await new Promise((r) => setTimeout(r, 40));
  }
}

async function bulk() {
  for (let b = 0; Date.now() < end; b++) {
    const res = await post('/v1/emails/bulk', {
      template: 'campaign_report',
      shared_vars: { campaign: `chaos-${run}`, unsubscribe_url: 'https://u' },
      recipients: Array.from({ length: 500 }, (_, j) => ({
        to: `bulk-${b}-${j}-${run}@example.com`,
        vars: { impressions: j },
      })),
    });
    if (res.status === 202) accepted += 500;
    await new Promise((r) => setTimeout(r, 5000));
  }
}

await Promise.all([...Array.from({ length: 8 }, (_, w) => transactional(w)), bulk()]);
console.log(`ACCEPTED ${accepted}`);
