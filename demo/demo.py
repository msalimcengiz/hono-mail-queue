"""Guided demo of hono-mail-queue. Run with `make demo`; demo/run.sh stops and starts
RabbitMQ for the outbox step."""

import sys
import time
import urllib.parse
import uuid

from lib import done, http, next_step, note, ok, show, step, wait_until

API = "http://api:3000"
MAILPIT = "http://mailpit:8025"
RUN = uuid.uuid4().hex[:6]


def addr(tag: str) -> str:
    return f"{tag}-{RUN}@example.com"


def send(to: str, template: str = "password_reset", key: str | None = None, **vars: str):
    v = vars or {"name": "Ayşe", "reset_url": "https://example.com/reset/abc"}
    return http("POST", f"{API}/v1/emails", {"template": template, "to": to, "vars": v},
                {"Idempotency-Key": key} if key else None)


def email(id_: str) -> dict:
    return http("GET", f"{API}/v1/emails/{id_}").json()


def inbox(to: str) -> int:
    q = urllib.parse.quote(f'to:"{to}"')
    return http("GET", f"{MAILPIT}/api/v1/search?query={q}").json()["messages_count"]


def chaos(code: int, probability: int) -> None:
    http("PUT", f"{MAILPIT}/api/v1/chaos", {"Recipient": {"ErrorCode": code, "Probability": probability}})


def basics() -> None:
    step("A password-reset email is accepted, queued on RabbitMQ and sent over SMTP")
    to = addr("ayse")
    r = send(to, key=f"reset-{RUN}")
    show(r)
    ok(r.status == 202, "202 Accepted with the email id")
    e = wait_until(lambda: email(r.json()["id"]), lambda e: e["status"] == "sent", timeout=20)
    ok(inbox(to) == 1, f"delivered after {e['attempts']} attempt; see it in Mailpit: http://localhost:8025")

    step("The same Idempotency-Key returns the original email instead of sending a second one")
    again = send(to, key=f"reset-{RUN}")
    show(again)
    ok(again.status == 200 and again.json()["id"] == r.json()["id"] and again.json()["duplicate"], "200, same id, duplicate: true")
    time.sleep(1)
    ok(inbox(to) == 1, "still exactly one email in the inbox")

    step("Template variables are checked before anything is stored")
    bad = http("POST", f"{API}/v1/emails", {"template": "password_reset", "to": addr("x"), "vars": {"name": "A"}})
    show(bad)
    ok(bad.status == 422 and bad.json()["missing"] == ["reset_url"], "422 naming the missing variable")

    step("A 300-recipient campaign is stored with one INSERT and sent through the bulk queue")
    recipients = [{"to": addr(f"bulk{i}"), "vars": {"impressions": str(i * 10)}} for i in range(300)]
    r = http("POST", f"{API}/v1/emails/bulk", {"template": "campaign_report",
             "shared_vars": {"campaign": "Sonbahar", "unsubscribe_url": "https://example.com/u"}, "recipients": recipients})
    show(r)
    batch = r.json()["batch_id"]
    stats = wait_until(lambda: http("GET", f"{API}/v1/batches/{batch}").json(), lambda s: s.get("sent") == 300, timeout=60)
    ok(True, f"batch {batch[:8]}…: {stats}")


def priority() -> None:
    step("Priority: a password reset is not stuck behind a 3,000-email campaign")
    recipients = [{"to": addr(f"big{i}"), "vars": {"impressions": "1"}} for i in range(1000)]
    batches = [
        http("POST", f"{API}/v1/emails/bulk", {"template": "campaign_report",
             "shared_vars": {"campaign": "Kara Cuma", "unsubscribe_url": "https://u"}, "recipients": recipients}).json()["batch_id"]
        for _ in range(3)
    ]
    t0 = time.time()
    r = send(addr("urgent"))
    wait_until(lambda: email(r.json()["id"]), lambda e: e["status"] == "sent", timeout=20, every=0.05)
    took = time.time() - t0
    sent_bulk = sum(http("GET", f"{API}/v1/batches/{b}").json().get("sent", 0) for b in batches)
    note(f"campaign progress at that moment: {sent_bulk} of 3000 sent (bulk is rate-limited to 100/s per worker)")
    ok(sent_bulk < 3000 and took < 3, f"password reset delivered in {took * 1000:.0f} ms, ahead of {3000 - sent_bulk} queued campaign emails")
    note("The campaign keeps draining in the background.")


def failures() -> None:
    step("A temporary SMTP error (451) is retried after 5 s, without blocking a worker")
    chaos(451, 100)
    to = addr("retry")
    r = send(to)
    wait_until(lambda: email(r.json()["id"]), lambda e: e["status"] == "retrying", timeout=20)
    note("status is 'retrying'; the message waits in a TTL queue (mail.retry.transactional.5s)")
    chaos(451, 0)
    e = wait_until(lambda: email(r.json()["id"]), lambda e: e["status"] == "sent", timeout=30)
    ok(e["attempts"] == 2 and inbox(to) == 1, "sent on the 2nd attempt, exactly one copy delivered")

    step("A permanent error (550) is not retried: the email is dead-lettered")
    chaos(550, 100)
    to = addr("dead")
    r = send(to)
    e = wait_until(lambda: email(r.json()["id"]), lambda e: e["status"] == "failed", timeout=20)
    show(http("GET", f"{API}/v1/emails/{r.json()['id']}"))
    ok(e["attempts"] == 1, "failed after 1 attempt and moved to the mail.dead queue")

    step("After the cause is fixed, an operator replays the dead letters")
    chaos(550, 0)
    denied = http("POST", f"{API}/admin/dlq/replay", headers={"X-Admin-Token": "wrong"})
    ok(denied.status == 401, "the replay endpoint needs the admin token")
    res = http("POST", f"{API}/admin/dlq/replay?limit=100", headers={"X-Admin-Token": "dev-admin-token"})
    show(res)
    wait_until(lambda: email(r.json()["id"]), lambda e: e["status"] == "sent", timeout=20)
    ok(inbox(to) == 1, "the replayed email was delivered")


def outbox_send() -> None:
    step("RabbitMQ is down. The API still accepts the email: it is stored first (transactional outbox)")
    r = send(addr("outbox"))
    show(r)
    ok(r.status == 202, "202 Accepted although nothing could be published")
    with open("/tmp/outbox_id", "w") as f:
        f.write(r.json()["id"])


def outbox_check() -> None:
    step("RabbitMQ is back. The outbox sweeper publishes the stored email and it is sent")
    id_ = open("/tmp/outbox_id").read().strip()
    e = wait_until(lambda: email(id_), lambda e: e["status"] == "sent", timeout=90, every=1)
    ok(e["status"] == "sent", "delivered without the client retrying anything")
    done("delivery, idempotency, validation, bulk, priority isolation, retries, dead letters with replay, and the outbox.")


{"basics": basics, "priority": priority, "failures": failures, "outbox_send": outbox_send,
 "outbox_check": outbox_check}[sys.argv[1]]()
with open("/tmp/next_step", "w") as f:
    f.write(str(next_step()))
