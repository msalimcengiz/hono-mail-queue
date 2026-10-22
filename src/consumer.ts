import type { Logger } from 'pino';
import type { EmailRepo } from './db.js';
import * as m from './metrics.js';
import { decide, describe } from './retry.js';
import type { MailClass } from './templates.js';

export interface Mail {
  to: string;
  subject: string;
  html: string;
  messageId: string;
}

export interface ConsumerDeps {
  repo: Pick<EmailRepo, 'claim' | 'markSent' | 'markRetrying' | 'markFailed'>;
  send: (mail: Mail) => Promise<void>;
  publishRetry: (c: MailClass, suffix: string, id: string) => Promise<void>;
  publishDead: (c: MailClass, id: string, reason: string) => Promise<void>;
  /** Waits for a send slot from the class's rate limiter. */
  throttle: () => Promise<void>;
  logger: Logger;
}

export type Outcome = 'sent' | 'retry' | 'dead' | 'skipped';

/**
 * Handles one delivery. The caller acks the broker message after this resolves,
 * whatever the outcome, because a follow-up message (retry/dead) has already been
 * confirmed by the broker. If this throws (database down), the caller nacks with
 * requeue so the delivery is attempted again.
 */
export async function handle(deps: ConsumerDeps, c: MailClass, id: string, redelivered: boolean): Promise<Outcome> {
  const email = await deps.repo.claim(id, redelivered);
  if (!email) {
    m.duplicates.inc();
    return 'skipped';
  }

  await deps.throttle();
  const end = m.sendDuration.startTimer({ class: c });
  try {
    await deps.send({
      to: email.to_address,
      subject: email.subject,
      html: email.html,
      messageId: `<${id}@mail-queue>`,
    });
    end();
  } catch (err) {
    end();
    const d = decide(email.attempts, err);
    if (d.kind === 'retry') {
      await deps.publishRetry(c, d.queueSuffix, id);
      await deps.repo.markRetrying(id, describe(err));
      m.retried.inc({ class: c, delay: d.queueSuffix });
      deps.logger.info({ id, attempt: email.attempts, delay: d.queueSuffix, err: describe(err) }, 'retry scheduled');
      return 'retry';
    }
    await deps.publishDead(c, id, d.reason);
    await deps.repo.markFailed(id, d.reason);
    m.failed.inc({ class: c, reason: d.reason.startsWith('permanent') ? 'permanent' : 'exhausted' });
    deps.logger.warn({ id, attempt: email.attempts, reason: d.reason }, 'dead-lettered');
    return 'dead';
  }

  await deps.repo.markSent(id);
  m.sent.inc({ class: c });
  if (email.attempts === 1) {
    m.endToEnd.observe({ class: c }, (Date.now() - email.created_at.getTime()) / 1000);
  }
  return 'sent';
}
