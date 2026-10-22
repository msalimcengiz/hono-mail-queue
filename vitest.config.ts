import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // Integration tests wait for real retry delays (5s) and outbox sweeps.
    testTimeout: 30_000,
    fileParallelism: false,
  },
});
