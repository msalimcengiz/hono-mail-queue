Load: 60s of transactional emails (8 senders, Idempotency-Key) plus a 500-recipient bulk batch every 5 s. SIGKILLed 12 worker containers (every 4 s), the API once and RabbitMQ once; 5% of SMTP calls failed with 451 during the load.

| emails accepted | stored |  sent | needed a retry | failed | not finished | sent but missing from the SMTP sink | extra copies delivered |
| --------------: | -----: | ----: | -------------: | -----: | -----------: | ----------------------------------: | ---------------------: |
|           13733 |  13733 | 13733 |            650 |  **0** |        **0** |                               **0** |                      0 |

Extra copies: email is at-least-once. A worker killed after the SMTP server accepted a message but before the row was marked sent sends it again on redelivery. With 12 kills this happened 0 times out of 13733 emails.

**PASS**
