-- Double-entry ledger schema.
-- Money is always BIGINT minor units (cents, paise). No floating point anywhere.

CREATE TABLE accounts (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            TEXT        NOT NULL,
    currency        TEXT        NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    allow_overdraft BOOLEAN     NOT NULL DEFAULT FALSE,
    -- Number of balance rows for this account. >1 spreads a hot account's balance (SHARDED strategy).
    shards          INT         NOT NULL DEFAULT 1 CHECK (shards BETWEEN 1 AND 64),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE transfers (
    id              UUID        PRIMARY KEY,
    -- One row per idempotency key. The row commits in the same transaction as the money movement,
    -- so a retry after a crash always finds it.
    idempotency_key TEXT        NOT NULL UNIQUE,
    request_hash    TEXT        NOT NULL,
    debit_account   BIGINT      NOT NULL REFERENCES accounts (id),
    credit_account  BIGINT      NOT NULL REFERENCES accounts (id),
    amount_minor    BIGINT      NOT NULL CHECK (amount_minor > 0),
    currency        TEXT        NOT NULL,
    status          TEXT        NOT NULL CHECK (status IN ('PENDING', 'POSTED', 'VOIDED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (debit_account <> credit_account)
);
-- Pending holds per account, used by the reconciler.
CREATE INDEX transfers_pending_idx ON transfers (debit_account) WHERE status = 'PENDING';

-- Append-only journal. A posted transfer has exactly one debit (negative) and one credit (positive)
-- entry of equal size, so every transfer and the whole table sum to zero.
CREATE TABLE entries (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transfer_id  UUID        NOT NULL REFERENCES transfers (id),
    account_id   BIGINT      NOT NULL REFERENCES accounts (id),
    direction    TEXT        NOT NULL CHECK (direction IN ('D', 'C')),
    amount_minor BIGINT      NOT NULL,
    currency     TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((direction = 'D' AND amount_minor < 0) OR (direction = 'C' AND amount_minor > 0)),
    UNIQUE (transfer_id, direction)
);
-- Serves keyset pagination of an account's entries (newest first).
CREATE INDEX entries_account_idx ON entries (account_id, id);

CREATE FUNCTION entries_append_only() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    RAISE EXCEPTION 'entries is append-only: % is not allowed', TG_OP;
END;
$$;

CREATE TRIGGER entries_no_update_or_delete
    BEFORE UPDATE OR DELETE ON entries
    FOR EACH ROW EXECUTE FUNCTION entries_append_only();

-- Derived balance, one row per (account, shard). Account balance = sum over its shards.
-- balance_minor is the available balance: posted entries minus pending holds.
CREATE TABLE balances (
    account_id      BIGINT  NOT NULL REFERENCES accounts (id),
    shard           INT     NOT NULL CHECK (shard >= 0),
    balance_minor   BIGINT  NOT NULL DEFAULT 0,
    -- Copied from accounts (immutable) so the database itself refuses an overdraft,
    -- whatever the application code does.
    allow_overdraft BOOLEAN NOT NULL,
    version         BIGINT  NOT NULL DEFAULT 0,
    PRIMARY KEY (account_id, shard),
    CONSTRAINT balances_no_overdraft CHECK (allow_overdraft OR balance_minor >= 0)
);

-- Transactional outbox: written in the same transaction as the transfer state change.
CREATE TABLE outbox (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id     UUID        NOT NULL UNIQUE,
    event_type   TEXT        NOT NULL,
    transfer_id  UUID        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- Read model built by the outbox consumer. processed_events makes redelivery harmless.
CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE statement_lines (
    account_id   BIGINT      NOT NULL,
    transfer_id  UUID        NOT NULL,
    amount_minor BIGINT      NOT NULL,
    currency     TEXT        NOT NULL,
    posted_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, transfer_id)
);
