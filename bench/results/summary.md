Bulk: 20000, transactional: 300 at ~30/s, SMTP temporary failure rate: 10%.

| class         | status | emails | needed a retry | first-try latency p50 |       p95 |       p99 |
| ------------- | ------ | -----: | -------------: | --------------------: | --------: | --------: |
| bulk          | failed |      1 |              1 |                     – |         – |         – |
| bulk          | sent   | 19,999 |          1,995 |              8,398 ms | 16,845 ms | 17,615 ms |
| transactional | sent   |    300 |             29 |                  3 ms |      9 ms |     12 ms |

Bulk first-pass throughput: 943 emails/s (18,004 sent on the first try in 19.1 s). Messages in SMTP sink: 20,299 of 20,300. Total run including the retry ladder (5 s, 30 s, 5 min): 354.1 s.
