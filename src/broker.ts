import amqp, { type ChannelModel, type ConfirmChannel } from 'amqplib';
import { RETRY_DELAYS_MS, retrySuffix } from './retry.js';
import type { MailClass } from './templates.js';

export const EXCHANGE = 'mail';
export const RETRY_EXCHANGE = 'mail.retry';
export const DEAD_EXCHANGE = 'mail.dead';
export const DEAD_QUEUE = 'mail.dead';
export const CLASSES: readonly MailClass[] = ['transactional', 'bulk'];

/**
 * Deliveries of one message before RabbitMQ gives up on it. RabbitMQ 4 already applies
 * a default limit of 20 to quorum queues and, without a dead-letter exchange, silently
 * DROPS the message when it is reached. Setting it explicitly together with a DLX turns
 * that into a dead letter that can be inspected and replayed.
 */
export const DELIVERY_LIMIT = 20;

export const queueFor = (c: MailClass) => `mail.${c}`;
export const retryQueueFor = (c: MailClass, suffix: string) => `mail.retry.${c}.${suffix}`;

export interface Message {
  id: string;
}

/**
 * Declares the topology. Retries use one TTL queue per (class, delay); when a
 * message expires it is dead-lettered back to the work queue of its class, so a
 * bulk retry never lands in the transactional queue.
 *
 *   mail ──transactional──▶ mail.transactional
 *        └─bulk──────────▶ mail.bulk
 *   mail.retry ──<class>.<delay>──▶ mail.retry.<class>.<delay> ──(TTL)──▶ mail/<class>
 *   mail.dead ──▶ mail.dead      (also receives work-queue messages past DELIVERY_LIMIT)
 */
export async function assertTopology(ch: ConfirmChannel): Promise<void> {
  await ch.assertExchange(EXCHANGE, 'direct', { durable: true });
  await ch.assertExchange(RETRY_EXCHANGE, 'direct', { durable: true });
  await ch.assertExchange(DEAD_EXCHANGE, 'fanout', { durable: true });
  await ch.assertQueue(DEAD_QUEUE, { durable: true, arguments: { 'x-queue-type': 'quorum' } });
  await ch.bindQueue(DEAD_QUEUE, DEAD_EXCHANGE, '');

  for (const c of CLASSES) {
    await ch.assertQueue(queueFor(c), {
      durable: true,
      arguments: {
        'x-queue-type': 'quorum',
        'x-delivery-limit': DELIVERY_LIMIT,
        'x-dead-letter-exchange': DEAD_EXCHANGE,
        'x-dead-letter-strategy': 'at-least-once',
        'x-overflow': 'reject-publish',
      },
    });
    await ch.bindQueue(queueFor(c), EXCHANGE, c);
    for (const delay of RETRY_DELAYS_MS) {
      const suffix = retrySuffix(delay);
      const q = retryQueueFor(c, suffix);
      await ch.assertQueue(q, {
        durable: true,
        arguments: {
          'x-queue-type': 'quorum',
          'x-message-ttl': delay,
          'x-dead-letter-exchange': EXCHANGE,
          'x-dead-letter-routing-key': c,
          'x-dead-letter-strategy': 'at-least-once',
          'x-overflow': 'reject-publish',
        },
      });
      await ch.bindQueue(q, RETRY_EXCHANGE, `${c}.${suffix}`);
    }
  }
}

export class Broker {
  private constructor(
    readonly conn: ChannelModel,
    readonly ch: ConfirmChannel,
  ) {}

  static async connect(url: string): Promise<Broker> {
    const conn = await amqp.connect(url);
    const ch = await conn.createConfirmChannel();
    await assertTopology(ch);
    return new Broker(conn, ch);
  }

  /** Publishes and waits until the broker has persisted every message. */
  async publishMany(c: MailClass, ids: string[]): Promise<void> {
    for (const id of ids) {
      this.ch.publish(EXCHANGE, c, encode({ id }), {
        persistent: true,
        messageId: id,
        contentType: 'application/json',
      });
    }
    await this.ch.waitForConfirms();
  }

  async publishRetry(c: MailClass, suffix: string, id: string): Promise<void> {
    this.ch.publish(RETRY_EXCHANGE, `${c}.${suffix}`, encode({ id }), { persistent: true, messageId: id });
    await this.ch.waitForConfirms();
  }

  async publishDead(c: MailClass | 'unknown', id: string, reason: string, raw?: Buffer): Promise<void> {
    this.ch.publish(DEAD_EXCHANGE, '', raw ?? encode({ id }), {
      persistent: true,
      messageId: id,
      headers: { 'x-class': c, 'x-reason': reason.slice(0, 500) },
    });
    await this.ch.waitForConfirms();
  }

  /**
   * Moves dead letters back to their work queue. Only the messages present when the call
   * starts are processed: one that fails again returns to the DLQ and must not be picked
   * up again by the same run. Undecodable messages stay in the DLQ.
   */
  async replayDead(
    limit: number,
    prepare: (id: string) => Promise<MailClass | null>,
  ): Promise<{ replayed: string[]; kept: number }> {
    const snapshot = Math.min(limit, (await this.ch.checkQueue(DEAD_QUEUE)).messageCount);
    const replayed: string[] = [];
    let kept = 0;
    for (let i = 0; i < snapshot; i++) {
      const msg = await this.ch.get(DEAD_QUEUE, { noAck: false });
      if (!msg) break;
      const m = decode(msg.content);
      const c = m ? await prepare(m.id) : null;
      if (!m || !c) {
        // Not replayable (garbage, or the email is no longer failed): park it at the back.
        this.ch.publish(DEAD_EXCHANGE, '', msg.content, { persistent: true, headers: msg.properties.headers });
        await this.ch.waitForConfirms();
        this.ch.ack(msg);
        kept++;
        continue;
      }
      await this.publishMany(c, [m.id]);
      this.ch.ack(msg);
      replayed.push(m.id);
    }
    return { replayed, kept };
  }

  async depth(queue: string): Promise<number> {
    return (await this.ch.checkQueue(queue)).messageCount;
  }

  async close(): Promise<void> {
    await this.ch.close().catch(() => {});
    await this.conn.close().catch(() => {});
  }
}

export function encode(m: Message): Buffer {
  return Buffer.from(JSON.stringify(m));
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function decode(buf: Buffer): Message | null {
  try {
    const m = JSON.parse(buf.toString()) as Partial<Message>;
    return typeof m.id === 'string' && UUID_RE.test(m.id) ? { id: m.id } : null;
  } catch {
    return null;
  }
}

/**
 * A broker connection that reconnects in the background. Used by the API: while RabbitMQ
 * is unreachable, `get()` throws at once, the API still accepts emails (they are stored
 * first), and the outbox sweeper publishes them once RabbitMQ is back. The API never
 * becomes unavailable just because the broker is.
 */
export class ReconnectingBroker {
  private broker: Broker | null = null;
  private stopped = false;
  private delayMs = 500;

  constructor(
    private readonly url: string,
    private readonly log: { info: (o: object, m: string) => void; warn: (o: object, m: string) => void },
  ) {}

  async start(): Promise<void> {
    await this.connect();
  }

  get connected(): boolean {
    return this.broker !== null;
  }

  get(): Broker {
    if (!this.broker) throw new Error('broker unavailable');
    return this.broker;
  }

  /** Runs fn against the broker, failing after timeoutMs instead of hanging on a stalled connection. */
  async with<T>(fn: (b: Broker) => Promise<T>, timeoutMs = 5_000): Promise<T> {
    const b = this.get();
    let timer: NodeJS.Timeout | undefined;
    try {
      return await Promise.race([
        fn(b),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error(`broker did not confirm within ${timeoutMs} ms`)), timeoutMs);
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  }

  async close(): Promise<void> {
    this.stopped = true;
    const b = this.broker;
    this.broker = null;
    if (b) {
      b.conn.removeAllListeners('close');
      await b.close();
    }
  }

  private async connect(): Promise<void> {
    if (this.stopped) return;
    try {
      const b = await Broker.connect(this.url);
      b.conn.on('error', () => {}); // 'close' follows; handled there
      b.conn.on('close', () => {
        this.broker = null;
        this.log.warn({}, 'broker connection lost; reconnecting');
        this.schedule();
      });
      this.broker = b;
      this.delayMs = 500;
      this.log.info({}, 'broker connected');
    } catch (err) {
      this.log.warn({ err: String(err), retryInMs: this.delayMs }, 'broker connect failed');
      this.schedule();
    }
  }

  private schedule(): void {
    if (this.stopped) return;
    setTimeout(() => void this.connect(), this.delayMs);
    this.delayMs = Math.min(this.delayMs * 2, 30_000);
  }
}
