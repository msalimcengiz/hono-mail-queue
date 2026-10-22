CREATE TABLE IF NOT EXISTS emails (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key text        UNIQUE,
    batch_id        uuid,
    class           text        NOT NULL CHECK (class IN ('transactional', 'bulk')),
    template        text        NOT NULL,
    to_address      text        NOT NULL,
    subject         text        NOT NULL,
    html            text        NOT NULL,
    status          text        NOT NULL DEFAULT 'queued'
                                CHECK (status IN ('queued', 'sending', 'retrying', 'sent', 'failed')),
    attempts        int         NOT NULL DEFAULT 0,
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    enqueued_at     timestamptz,
    sent_at         timestamptz,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS emails_batch_idx ON emails (batch_id) WHERE batch_id IS NOT NULL;

-- Outbox: rows the broker never confirmed. Kept small by the partial predicate.
CREATE INDEX IF NOT EXISTS emails_outbox_idx ON emails (created_at) WHERE enqueued_at IS NULL;
