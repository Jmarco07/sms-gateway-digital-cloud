-- Mini SMS Gateway schema.
--
-- Money is stored as BIGINT "micros" (1 currency unit = 1_000_000 micros) so that
-- there is never any floating point or rounding error in the ledger.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------------------
-- tenants
-- balance_micros is a denormalised running total that is always changed in the
-- same transaction as the matching ledger row. The CHECK constraint is the last
-- line of defence: even a buggy query cannot push the balance below zero.
-- ---------------------------------------------------------------------------
CREATE TABLE tenants (
    id                     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name                   text        NOT NULL UNIQUE,
    api_key_hash           text        NOT NULL UNIQUE,      -- sha256 hex, never the key itself
    opening_balance_micros bigint      NOT NULL CHECK (opening_balance_micros >= 0),
    balance_micros         bigint      NOT NULL CHECK (balance_micros >= 0),
    webhook_url            text,
    webhook_secret         text        NOT NULL DEFAULT encode(gen_random_bytes(24), 'hex'),
    created_at             timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- prices: price per SMS segment by destination country prefix (digits only,
-- without '+'). The longest matching prefix wins.
-- ---------------------------------------------------------------------------
CREATE TABLE prices (
    prefix       text PRIMARY KEY CHECK (prefix ~ '^[0-9]{1,4}$'),
    country      text   NOT NULL,
    price_micros bigint NOT NULL CHECK (price_micros > 0)
);

-- ---------------------------------------------------------------------------
-- messages
-- ---------------------------------------------------------------------------
CREATE FUNCTION status_rank(s text) RETURNS int
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE s
        WHEN 'ACCEPTED'  THEN 0
        WHEN 'QUEUED'    THEN 1
        WHEN 'SUBMITTED' THEN 2
        ELSE 3                      -- DELIVERED, UNDELIVERED, EXPIRED, FAILED: all final
    END
$$;

CREATE TABLE messages (
    id              uuid PRIMARY KEY,
    tenant_id       uuid        NOT NULL REFERENCES tenants(id),
    client_ref      text,
    to_number       text        NOT NULL,
    sender          text        NOT NULL,
    text            text        NOT NULL,
    encoding        text        NOT NULL CHECK (encoding IN ('GSM7', 'UCS2')),
    segments        int         NOT NULL CHECK (segments > 0),
    price_micros    bigint      NOT NULL CHECK (price_micros > 0),   -- total = segments * price/segment
    status          text        NOT NULL CHECK (status IN
                        ('ACCEPTED','QUEUED','SUBMITTED','DELIVERED','UNDELIVERED','EXPIRED','FAILED')),
    carrier_id      text,
    attempts        int         NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,                  -- when a retry is allowed
    lease_until     timestamptz,                  -- a worker owns the message until this time
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    submitted_at    timestamptz,
    final_at        timestamptz
);

-- A carrier id identifies exactly one message.
CREATE UNIQUE INDEX messages_carrier_id_uq ON messages (carrier_id) WHERE carrier_id IS NOT NULL;
CREATE INDEX messages_tenant_created_idx ON messages (tenant_id, created_at DESC, id);
CREATE INDEX messages_tenant_status_idx  ON messages (tenant_id, status);
CREATE INDEX messages_tenant_ref_idx     ON messages (tenant_id, client_ref) WHERE client_ref IS NOT NULL;
-- used by the reconciler: only rows that still need dispatching
CREATE INDEX messages_pending_idx ON messages (created_at) WHERE status IN ('ACCEPTED', 'QUEUED');

-- ---------------------------------------------------------------------------
-- message_events: append-only history. kind = STATUS for status changes and
-- ATTEMPT for failed submit attempts (retry audit trail).
-- ---------------------------------------------------------------------------
CREATE TABLE message_events (
    id         bigserial PRIMARY KEY,
    message_id uuid        NOT NULL REFERENCES messages(id),
    kind       text        NOT NULL DEFAULT 'STATUS' CHECK (kind IN ('STATUS', 'ATTEMPT')),
    status     text        NOT NULL,
    detail     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX message_events_message_idx ON message_events (message_id, id);

-- ---------------------------------------------------------------------------
-- ledger_entries: every charge and refund.
--   CHARGE: amount_micros > 0      REFUND: amount_micros < 0
--   => balance = opening_balance - SUM(amount_micros)
-- UNIQUE (message_id, entry_type) makes a double charge or double refund
-- impossible at the database level.
-- ---------------------------------------------------------------------------
CREATE TABLE ledger_entries (
    id           bigserial PRIMARY KEY,
    tenant_id    uuid        NOT NULL REFERENCES tenants(id),
    message_id   uuid        NOT NULL REFERENCES messages(id),
    entry_type   text        NOT NULL CHECK (entry_type IN ('CHARGE', 'REFUND')),
    amount_micros bigint     NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ledger_sign_ck CHECK (
        (entry_type = 'CHARGE' AND amount_micros > 0) OR
        (entry_type = 'REFUND' AND amount_micros < 0)
    ),
    CONSTRAINT ledger_one_per_type_uq UNIQUE (message_id, entry_type)
);
CREATE INDEX ledger_tenant_idx ON ledger_entries (tenant_id, id);

-- ---------------------------------------------------------------------------
-- idempotency_keys: the stored response is replayed for the same key + body.
-- Inserted in the same transaction as the messages, so it exists if and only if
-- the messages exist.
-- ---------------------------------------------------------------------------
CREATE TABLE idempotency_keys (
    tenant_id    uuid        NOT NULL REFERENCES tenants(id),
    key          text        NOT NULL,
    request_hash text        NOT NULL,
    status_code  int,
    response     jsonb,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, key)
);

-- ---------------------------------------------------------------------------
-- dlr_receipts: every verified DLR is stored first. This is what makes a DLR
-- that arrives before the submit response (carrier_id not yet known to us) safe:
-- it waits here and is applied as soon as the carrier id is saved.
-- ---------------------------------------------------------------------------
CREATE TABLE dlr_receipts (
    id                bigserial PRIMARY KEY,
    carrier_id        text        NOT NULL,
    status            text        NOT NULL,
    carrier_timestamp text        NOT NULL,
    received_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT dlr_receipts_dedupe_uq UNIQUE (carrier_id, status, carrier_timestamp)
);
CREATE INDEX dlr_receipts_carrier_idx ON dlr_receipts (carrier_id);

-- ---------------------------------------------------------------------------
-- webhook_deliveries: transactional outbox for tenant callbacks. Written in the
-- same transaction as the final status change, delivered by a separate process.
-- ---------------------------------------------------------------------------
CREATE TABLE webhook_deliveries (
    id              bigserial PRIMARY KEY,
    tenant_id       uuid        NOT NULL REFERENCES tenants(id),
    message_id      uuid        NOT NULL REFERENCES messages(id) UNIQUE,
    url             text        NOT NULL,
    payload         jsonb       NOT NULL,
    state           text        NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING', 'DELIVERED', 'FAILED')),
    attempts        int         NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    delivered_at    timestamptz
);
CREATE INDEX webhook_pending_idx ON webhook_deliveries (next_attempt_at) WHERE state = 'PENDING';
