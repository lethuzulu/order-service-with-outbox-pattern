

CREATE TABLE orders (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id   UUID        NOT NULL,
    amount  BIGINT      NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'pending',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT amount_positive CHECK (amount > 0),
    CONSTRAINT valid_status    CHECK (status IN ('pending','confirmed','cancelled'))
);

CREATE TABLE outbox_messages (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type    TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    aggregate_id  TEXT        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'pending',
    attempts      INT         NOT NULL DEFAULT 0,
    locked_until  TIMESTAMPTZ,
    last_error    TEXT,
    published_at  TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT valid_status        CHECK (status IN ('pending','processing','published','failed')),
    CONSTRAINT attempts_non_negative CHECK (attempts >= 0)
);

CREATE INDEX outbox_pending_idx
    ON outbox_messages (created_at)
    WHERE status = 'pending';