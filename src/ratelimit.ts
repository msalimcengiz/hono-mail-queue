/**
 * Token bucket. take() resolves when a token is available, so callers are
 * paced to `ratePerSec` with bursts up to `burst`.
 */
export class TokenBucket {
  private tokens: number;
  private last: number;
  private queue: Array<() => void> = [];
  private timer: NodeJS.Timeout | undefined;

  constructor(
    private readonly ratePerSec: number,
    private readonly burst: number = Math.max(1, Math.ceil(ratePerSec)),
    private readonly now: () => number = () => performance.now(),
  ) {
    this.tokens = burst;
    this.last = now();
  }

  take(): Promise<void> {
    return new Promise((resolve) => {
      this.queue.push(resolve);
      this.drain();
    });
  }

  private refill(): void {
    const t = this.now();
    this.tokens = Math.min(this.burst, this.tokens + ((t - this.last) / 1000) * this.ratePerSec);
    this.last = t;
  }

  private drain(): void {
    this.refill();
    while (this.queue.length > 0 && this.tokens >= 1) {
      this.tokens -= 1;
      this.queue.shift()?.();
    }
    if (this.queue.length > 0 && !this.timer) {
      const waitMs = ((1 - this.tokens) / this.ratePerSec) * 1000;
      this.timer = setTimeout(
        () => {
          this.timer = undefined;
          this.drain();
        },
        Math.max(1, Math.ceil(waitMs)),
      );
    }
  }
}
