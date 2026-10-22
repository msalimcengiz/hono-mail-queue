/** Delays between attempts. After the last one the message is dead-lettered. */
export const RETRY_DELAYS_MS = [5_000, 30_000, 300_000] as const;

export type Decision = { kind: 'retry'; delayMs: number; queueSuffix: string } | { kind: 'dead'; reason: string };

/**
 * SMTP 4xx replies and network errors are temporary; 5xx replies mean the
 * message will never be accepted (bad address, policy rejection).
 */
export function isPermanent(err: unknown): boolean {
  const code = (err as { responseCode?: number } | null)?.responseCode;
  return typeof code === 'number' && code >= 500 && code < 600;
}

/** attempt is the number of the attempt that just failed, starting at 1. */
export function decide(attempt: number, err: unknown): Decision {
  if (isPermanent(err)) {
    return { kind: 'dead', reason: `permanent: ${describe(err)}` };
  }
  const delayMs = RETRY_DELAYS_MS[attempt - 1];
  if (delayMs === undefined) {
    return { kind: 'dead', reason: `gave up after ${attempt} attempts: ${describe(err)}` };
  }
  return { kind: 'retry', delayMs, queueSuffix: retrySuffix(delayMs) };
}

export function retrySuffix(delayMs: number): string {
  return delayMs % 60_000 === 0 ? `${delayMs / 60_000}m` : `${delayMs / 1000}s`;
}

export function describe(err: unknown): string {
  if (err instanceof Error) {
    const code = (err as { responseCode?: number }).responseCode;
    return code ? `${code} ${err.message}` : err.message;
  }
  return String(err);
}
