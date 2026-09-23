-- idempotency_keys holds one row per Idempotency-Key. The row is inserted at the
-- start of the leader's transaction (which is what makes concurrent duplicates on
-- other instances block on the primary key) and filled with the response in the
-- same transaction that records the charge. A committed row therefore always
-- carries a response, and a charge can never be committed without its key.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key             text        PRIMARY KEY,
    request_hash    text        NOT NULL,
    response_status integer,
    response_header jsonb,
    response_body   bytea,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS charges (
    id           text        PRIMARY KEY,
    account      text        NOT NULL,
    amount_cents bigint      NOT NULL CHECK (amount_cents > 0),
    currency     char(3)     NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);
